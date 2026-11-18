use crate::api;
use crate::core::events::{Event, EventSink};
use crate::core::job::safe_relative_path;
use crate::core::manifest::{
    save_job, FileRecord, JobManifest, JobState, SourceKind, SourceManifest,
};
use crate::providers::mega_mac::{self, MacCheck};
use crate::providers::{CloudProvider, DownloadOptions, ProviderAuth};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use megalib::crypto::aes::aes128_ctr_decrypt;
use megalib::public::open_folder;
use megalib::Node;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

const MAX_FOLDER_PARALLELISM: usize = 4;
const MEGA_DIRECT_COOLDOWN: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Debug)]
pub struct MegaProvider;

impl CloudProvider for MegaProvider {
    fn name(&self) -> &'static str {
        "mega"
    }

    fn can_handle(&self, url: &str) -> bool {
        api::parser::detect_mega_link(url).is_ok()
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        false
    }

    async fn probe(&self, url: &str, _auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        api::parser::probe_source(url).await
    }

    async fn download_job(
        &self,
        manifest: JobManifest,
        _auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        match manifest.kind {
            SourceKind::File => {
                let info = api::parser::parse_and_fetch_node(&manifest.url).await?;
                let proxy_manager = crate::proxy_manager(options.proxies.as_deref()).await?;
                let target_path = PathBuf::from(&manifest.target_path);
                api::downloader::download_file_for_job(
                    info,
                    &target_path,
                    options.threads,
                    proxy_manager,
                    options.force_proxy,
                    Arc::new(Mutex::new(manifest)),
                    true,
                    EventSink::new(options.json_events),
                )
                .await
            }
            SourceKind::Folder => {
                let proxy_manager = crate::proxy_manager(options.proxies.as_deref()).await?;
                download_folder_job(
                    manifest,
                    options.json_events,
                    options.threads,
                    proxy_manager,
                    options.force_proxy,
                )
                .await
            }
        }
    }
}

pub async fn download_folder_job(
    mut manifest: JobManifest,
    json_events: bool,
    threads: usize,
    proxy_manager: Arc<crate::proxy::SmartProxyManager>,
    force_proxy: bool,
) -> anyhow::Result<()> {
    let events = EventSink::new(json_events);
    events.emit(Event::Started {
        job_id: manifest.job_id.clone(),
        provider: manifest.provider.clone(),
        total_files: manifest.total_files,
        total_bytes: manifest.total_bytes,
    });
    manifest.state = JobState::Running;
    manifest.error = None;
    save_job(&manifest)?;

    let mut fetch_url = manifest.url.to_string();
    if let Some(hash_idx) = manifest.url.find('#') {
        let key_part = &manifest.url[hash_idx + 1..];
        if let Some(sub_idx) = key_part.find("/folder/") {
            fetch_url = format!("{}#{}", &manifest.url[..hash_idx], &key_part[..sub_idx]);
        }
    }
    let folder = match open_folder(&fetch_url).await {
        Ok(folder) => folder,
        Err(e) => return fail_folder_job(&mut manifest, &events, None, e.to_string()),
    };
    let mut done_bytes: u64 = manifest
        .files
        .iter()
        .filter(|file| file.done)
        .map(|file| file.size)
        .sum();
    let context = FolderTransferContext::new(proxy_manager, force_proxy)?;
    let mut pending = Vec::new();

    for node in folder.nodes().iter().filter(|node| node.is_file()) {
        let Some(index) = manifest
            .files
            .iter()
            .position(|file| file.handle.as_deref() == Some(node.handle.as_str()))
        else {
            continue;
        };
        if manifest.files[index].done {
            continue;
        }

        let relative = node
            .path()
            .map(|path| path.trim_start_matches('/').to_string())
            .unwrap_or_else(|| node.name.clone());
        let target = Path::new(&manifest.job_dir).join(safe_relative_path(&relative));
        if let Some(parent) = target.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return fail_folder_job(&mut manifest, &events, Some(index), e.to_string());
            }
        }
        let part = part_path(&target);
        pending.push(PendingFolderFile {
            index,
            node,
            relative,
            target,
            part,
        });
    }

    let mut pending = pending.into_iter();
    let mut running = FuturesUnordered::new();
    loop {
        let limit = folder_parallelism(threads, context.is_rate_limited().await);
        while running.len() < limit {
            let Some(file) = pending.next() else {
                break;
            };
            running.push(transfer_folder_file(context.clone(), &folder.handle, file));
        }

        let Some((file, result)) = running.next().await else {
            break;
        };
        if let Err(error) = result {
            return fail_folder_job(&mut manifest, &events, Some(file.index), error.to_string());
        }

        let actual_size = std::fs::metadata(&file.part)?.len();
        if actual_size != file.node.size {
            let message = format!(
                "Tamanho invalido em {}: esperado {}, obtido {}",
                file.relative, file.node.size, actual_size
            );
            return fail_folder_job(&mut manifest, &events, Some(file.index), message);
        }

        // O MEGA nao publica checksum: a prova de integridade vem do meta-MAC
        // que a propria chave do arquivo carrega. Conferir aqui, antes de
        // promover o `.part`, e o que impede um arquivo com conteudo errado de
        // virar destino e ser marcado como concluido no manifesto.
        let mac = verify_folder_file_mac(file.node, &file.part).await;
        match mac {
            Ok(MacCheck::Match) => events.emit(Event::Verified {
                job_id: manifest.job_id.clone(),
                path: file.relative.clone(),
                algorithm: "mega-meta-mac".to_string(),
            }),
            Ok(MacCheck::Mismatch { expected, actual }) => {
                // Descarta o `.part` para a proxima execucao refazer, em vez de
                // guardar lixo para sempre. Mesma politica do checksum do
                // Google Drive.
                std::fs::remove_file(&file.part).ok();
                let message = format!(
                    "meta-MAC do MEGA nao confere em {}: esperado {expected}, obtido {actual}",
                    file.relative
                );
                return fail_folder_job(&mut manifest, &events, Some(file.index), message);
            }
            Err(error) => {
                // Sem conseguir calcular, nao ha como afirmar que o arquivo
                // esta certo — e promover sem conferir e justamente o defeito
                // que esta verificacao existe para fechar.
                let message = format!(
                    "nao foi possivel conferir o meta-MAC de {}: {error}",
                    file.relative
                );
                return fail_folder_job(&mut manifest, &events, Some(file.index), message);
            }
        }

        promote_completed_part(&file.part, &file.target).await?;
        manifest.files[file.index].done = true;
        manifest.files[file.index].error = None;
        done_bytes += file.node.size;
        save_job(&manifest)?;
        events.emit(Event::FileFinished {
            job_id: manifest.job_id.clone(),
            path: file.relative.clone(),
            bytes: file.node.size,
        });
        events.emit(Event::Progress {
            job_id: manifest.job_id.clone(),
            path: file.relative,
            bytes_done: done_bytes,
            bytes_total: manifest.total_bytes,
        });
    }

    if let Some(index) = pending_file_index(&manifest.files) {
        let message = format!(
            "Arquivo MEGA do manifesto nao foi encontrado ao retomar: {}",
            manifest.files[index].path
        );
        return fail_folder_job(&mut manifest, &events, Some(index), message);
    }

    manifest.state = JobState::Finished;
    manifest.error = None;
    save_job(&manifest)?;
    events.emit(Event::Finished {
        job_id: manifest.job_id.clone(),
        total_files: manifest.total_files,
        total_bytes: manifest.total_bytes,
    });
    Ok(())
}

struct PendingFolderFile<'a> {
    index: usize,
    node: &'a Node,
    relative: String,
    target: PathBuf,
    part: PathBuf,
}

async fn transfer_folder_file<'a>(
    context: FolderTransferContext,
    folder_handle: &'a str,
    file: PendingFolderFile<'a>,
) -> (PendingFolderFile<'a>, anyhow::Result<()>) {
    let result = download_folder_file(&context, folder_handle, file.node, &file.part).await;
    (file, result)
}

// ponytail: four parallel files amortize MEGA/TLS setup without multiplying 509 storms.
// A 509 lowers the live scheduler to one for five minutes; raising the ceiling belongs behind measurements.
fn folder_parallelism(threads: usize, rate_limited: bool) -> usize {
    if rate_limited {
        1
    } else {
        threads.clamp(1, MAX_FOLDER_PARALLELISM)
    }
}

#[derive(Clone)]
struct FolderTransferContext {
    proxy_manager: Arc<crate::proxy::SmartProxyManager>,
    direct_client: reqwest::Client,
    proxy_clients: Arc<Mutex<HashMap<String, reqwest::Client>>>,
    rate_limited_until: Arc<Mutex<Option<Instant>>>,
    force_proxy: bool,
}

impl FolderTransferContext {
    fn new(
        proxy_manager: Arc<crate::proxy::SmartProxyManager>,
        force_proxy: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            proxy_manager,
            direct_client: crate::proxy::SmartProxyManager::build_client(None)?,
            proxy_clients: Arc::new(Mutex::new(HashMap::new())),
            rate_limited_until: Arc::new(Mutex::new(None)),
            force_proxy,
        })
    }

    async fn is_rate_limited(&self) -> bool {
        let mut until = self.rate_limited_until.lock().await;
        if rate_limit_active(*until, Instant::now()) {
            true
        } else {
            *until = None;
            false
        }
    }

    async fn starting_proxy(&self) -> anyhow::Result<Option<String>> {
        let rate_limited = self.is_rate_limited().await;
        if !self.force_proxy && !rate_limited {
            return Ok(None);
        }
        match self.proxy_manager.get_proxy().await {
            Some(proxy) => Ok(Some(proxy)),
            None if rate_limited => Err(MegaRateLimit.into()),
            None => anyhow::bail!("force_proxy ativo, mas nenhum proxy esta disponivel"),
        }
    }

    async fn client_for(&self, proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
        let Some(proxy) = proxy else {
            return Ok(self.direct_client.clone());
        };
        let mut clients = self.proxy_clients.lock().await;
        if let Some(client) = clients.get(proxy) {
            return Ok(client.clone());
        }
        let client = crate::proxy::SmartProxyManager::build_client(Some(proxy))?;
        clients.insert(proxy.to_string(), client.clone());
        Ok(client)
    }

    async fn record_failure(&self, proxy: Option<&str>, rate_limited: bool) {
        if rate_limited {
            *self.rate_limited_until.lock().await = Some(Instant::now() + MEGA_DIRECT_COOLDOWN);
        }
        if let Some(proxy) = proxy {
            self.proxy_manager.ban_proxy(proxy).await;
            self.proxy_clients.lock().await.remove(proxy);
        }
    }
}

fn rate_limit_active(until: Option<Instant>, now: Instant) -> bool {
    until.is_some_and(|until| now < until)
}

fn pending_file_index(files: &[FileRecord]) -> Option<usize> {
    files.iter().position(|file| !file.done)
}

fn part_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin");
    target.with_file_name(format!("{file_name}.aditi.part"))
}

// ponytail: Windows scanners can briefly lock either path after a completed download.
// Retry only promotion; transfer data stays untouched and resumes do not redownload it.
async fn promote_completed_part(part: &Path, target: &Path) -> std::io::Result<()> {
    let mut last_sharing_error = None;
    for delay in [0, 100, 250, 500, 1_000] {
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let result = (|| {
            if target.exists() {
                std::fs::remove_file(target)?;
            }
            std::fs::rename(part, target)
        })();
        match result {
            Ok(()) => return Ok(()),
            Err(error) if is_windows_sharing_violation(&error) => last_sharing_error = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_sharing_error.expect("a sharing violation was recorded before retrying"))
}

fn is_windows_sharing_violation(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(32)
}

// ponytail: megalib 0.11 public-folder downloads accept an empty non-2xx response as success.
// Keep this small provider-local transfer path until the dependency exposes a status-checked API.
async fn download_folder_file(
    context: &FolderTransferContext,
    folder_handle: &str,
    node: &Node,
    part: &Path,
) -> anyhow::Result<()> {
    let mut current_proxy = context.starting_proxy().await?;

    for attempt in 1..=10 {
        let client = context.client_for(current_proxy.as_deref()).await?;
        match download_folder_file_once(&client, folder_handle, node, part).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let rate_limited = error.downcast_ref::<MegaRateLimit>().is_some();
                context
                    .record_failure(current_proxy.as_deref(), rate_limited)
                    .await;
                if attempt == 10 {
                    return Err(error);
                }
                current_proxy = context.proxy_manager.get_proxy().await;
                if current_proxy.is_none() {
                    if rate_limited {
                        return Err(error);
                    }
                    if context.force_proxy {
                        anyhow::bail!("force_proxy ativo, mas todos os proxies estao banidos");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
    unreachable!("o limite de tentativas retorna na ultima falha")
}

async fn download_folder_file_once(
    client: &reqwest::Client,
    folder_handle: &str,
    node: &Node,
    part: &Path,
) -> anyhow::Result<()> {
    let api_url = format!(
        "https://g.api.mega.co.nz/cs?id={}&n={folder_handle}&v=3",
        rand::random::<u32>()
    );
    let response = client
        .post(api_url)
        .json(&[serde_json::json!({ "a": "g", "g": 1, "n": node.handle })])
        .send()
        .await?;
    validate_mega_transfer_status(response.status())?;
    let transfer_url = public_folder_transfer_url(response.json().await?)?;

    let existing_size = match std::fs::metadata(part).map(|metadata| metadata.len()) {
        Ok(size) if size <= node.size => size,
        Ok(_) => {
            std::fs::remove_file(part)?;
            0
        }
        Err(_) => 0,
    };
    if existing_size == node.size {
        return Ok(());
    }

    let mut request = client.get(transfer_url);
    if existing_size > 0 {
        request = request.header(reqwest::header::RANGE, format!("bytes={existing_size}-"));
    }
    let response = request.send().await?;
    let status = response.status();
    validate_mega_transfer_status(status)?;
    if existing_size > 0
        && !crate::providers::resume_response_matches(&response, existing_size, node.size)
    {
        anyhow::bail!("MEGA retornou Content-Range invalido ao retomar: HTTP {status}");
    }

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(part)?;
    let node_key = folder_file_key(node)?;
    let mut key = [0u8; 16];
    for index in 0..16 {
        key[index] = node_key[index] ^ node_key[index + 16];
    }
    let nonce: [u8; 8] = node_key[16..24]
        .try_into()
        .map_err(|_| anyhow::anyhow!("Chave MEGA invalida para {}", node.handle))?;
    let mut offset = existing_size;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let decrypted = aes128_ctr_decrypt(&chunk, &key, &nonce, offset);
        file.write_all(&decrypted)?;
        offset += chunk.len() as u64;
    }
    if offset != node.size {
        anyhow::bail!(
            "MEGA transfer retornou {offset} bytes; esperado {}",
            node.size
        );
    }
    Ok(())
}

#[derive(Debug)]
struct MegaRateLimit;

impl std::fmt::Display for MegaRateLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MEGA transfer HTTP 509 (limite de banda)")
    }
}

impl std::error::Error for MegaRateLimit {}

fn validate_mega_transfer_status(status: reqwest::StatusCode) -> anyhow::Result<()> {
    if status.as_u16() == 509 {
        return Err(MegaRateLimit.into());
    }
    if !status.is_success() {
        anyhow::bail!("MEGA transfer HTTP {status}");
    }
    Ok(())
}

fn public_folder_transfer_url(response: serde_json::Value) -> anyhow::Result<String> {
    if let Some(code) = response
        .as_array()
        .and_then(|entries| entries.first())
        .and_then(serde_json::Value::as_i64)
        .filter(|code| *code < 0)
    {
        anyhow::bail!("MEGA API retornou codigo {code}");
    }
    response
        .as_array()
        .and_then(|entries| entries.first())
        .and_then(|entry| entry.get("g"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("MEGA API nao retornou URL de download"))
}

fn folder_file_key(node: &Node) -> anyhow::Result<[u8; 32]> {
    let encoded = node
        .get_key()
        .ok_or_else(|| anyhow::anyhow!("Chave MEGA ausente para {}", node.handle))?;
    megalib::base64::base64url_decode(&encoded)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Chave MEGA invalida para {}", node.handle))
}

/// Confere o `.part` contra o meta-MAC que a chave do node carrega.
///
/// Em `spawn_blocking` porque a leitura e sincrona e o arquivo pode ter varios
/// GB: travar o runtime aqui pararia os outros downloads em paralelo.
async fn verify_folder_file_mac(node: &Node, part: &Path) -> anyhow::Result<MacCheck> {
    let node_key = folder_file_key(node)?;
    let part = part.to_path_buf();
    let size = node.size;
    tokio::task::spawn_blocking(move || mega_mac::verify_file(&part, &node_key, size)).await?
}

fn fail_folder_job(
    manifest: &mut JobManifest,
    events: &EventSink,
    file_index: Option<usize>,
    message: String,
) -> anyhow::Result<()> {
    manifest.state = JobState::Error;
    manifest.error = Some(message.clone());
    if let Some(index) = file_index {
        manifest.files[index].error = Some(message.clone());
    }
    save_job(manifest)?;
    events.emit(Event::Error {
        job_id: manifest.job_id.clone(),
        message: message.clone(),
    });
    Err(anyhow::anyhow!(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_pending_manifest_file() {
        let files = vec![
            FileRecord {
                path: "done.txt".to_string(),
                name: "done.txt".to_string(),
                size: 1,
                handle: None,
                done: true,
                error: None,
                integrity: None,
            },
            FileRecord {
                path: "missing.txt".to_string(),
                name: "missing.txt".to_string(),
                size: 1,
                handle: None,
                done: false,
                error: None,
                integrity: None,
            },
        ];

        assert_eq!(pending_file_index(&files), Some(1));
    }

    #[test]
    fn accepts_only_a_public_folder_transfer_url() {
        assert_eq!(
            public_folder_transfer_url(serde_json::json!([{"g": "https://transfer.example"}]))
                .unwrap(),
            "https://transfer.example"
        );
        assert!(public_folder_transfer_url(serde_json::json!([-18])).is_err());
        assert!(public_folder_transfer_url(serde_json::json!([{}])).is_err());
    }

    #[test]
    fn recognizes_windows_sharing_violation() {
        assert!(is_windows_sharing_violation(
            &std::io::Error::from_raw_os_error(32)
        ));
        assert!(!is_windows_sharing_violation(
            &std::io::Error::from_raw_os_error(5)
        ));
    }

    #[test]
    fn classifies_mega_509_as_a_rate_limit() {
        let error =
            validate_mega_transfer_status(reqwest::StatusCode::from_u16(509).unwrap()).unwrap_err();
        assert!(error.downcast_ref::<MegaRateLimit>().is_some());
        assert!(validate_mega_transfer_status(reqwest::StatusCode::OK).is_ok());
    }

    #[test]
    fn caps_folder_parallelism_and_falls_back_after_rate_limit() {
        assert_eq!(folder_parallelism(0, false), 1);
        assert_eq!(folder_parallelism(2, false), 2);
        assert_eq!(folder_parallelism(8, false), MAX_FOLDER_PARALLELISM);
        assert_eq!(folder_parallelism(8, true), 1);
    }

    #[test]
    fn expires_the_direct_route_cooldown() {
        let now = Instant::now();
        assert!(rate_limit_active(Some(now + Duration::from_secs(1)), now));
        assert!(!rate_limit_active(Some(now), now));
        assert!(!rate_limit_active(None, now));
    }
}
