use crate::core::events::{Event, EventSink};
use crate::core::integrity::{parse_goog_hash, Check, Hasher, Integrity};
use crate::core::job::{safe_file_name, safe_relative_path};
use crate::core::manifest::{
    save_job, FileRecord, JobManifest, JobState, SourceKind, SourceManifest,
};
use crate::providers::{
    existing_target_is_complete, resume_response_matches, CloudProvider, DownloadOptions,
    ProviderAuth,
};
use anyhow::Context;
use futures::StreamExt;
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use reqwest::{Client, RequestBuilder, Response, Url};
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

const DRIVE_FOLDER_MIME: &str = "application/vnd.google-apps.folder";
// `md5Checksum` existe para todo arquivo binario no Drive; `sha1`/`sha256`
// vem "if available". Pedimos os tres e guardamos o que vier: nao custa
// chamada extra, e o manifesto fica com o mais forte disponivel.
const DRIVE_API_FIELDS: &str = "id,name,mimeType,size,md5Checksum,sha1Checksum,sha256Checksum";
const DRIVE_EXPORT_MIME: &str = "application/pdf";
/// Chamadas simultaneas a API na varredura de pastas.
///
/// A cota padrao do Google e da ordem de 10 requisicoes por segundo por
/// client_id. Oito deixa folga para o recuo exponencial de
/// `drive_api_get_with_backoff` sem estourar a cota.
const DRIVE_LIST_CONCURRENCY: usize = 8;
/// Tentativas por chamada antes de desistir, contando a primeira.
const DRIVE_LIST_ATTEMPTS: u32 = 5;

// Travas de build. A cota padrao do Google e da ordem de 10 req/s por
// client_id: passar disso troca lentidao por 429. Quem mexer nas constantes
// acima quebra a compilacao, em vez de descobrir em producao.
const _: () = assert!(DRIVE_LIST_CONCURRENCY >= 2);
const _: () = assert!(DRIVE_LIST_CONCURRENCY <= 10);
const _: () = assert!(DRIVE_LIST_ATTEMPTS >= 2);

#[derive(Clone, Copy, Debug)]
pub struct GoogleDriveProvider;

#[derive(Clone, Debug, PartialEq, Eq)]
enum DriveLink {
    File { id: String },
    Folder { id: String },
    Docs { id: String, app: String },
}

impl CloudProvider for GoogleDriveProvider {
    fn name(&self) -> &'static str {
        "google_drive"
    }

    fn can_handle(&self, url: &str) -> bool {
        parse_google_drive_link(url).is_ok()
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        false
    }

    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        match parse_google_drive_link(url)? {
            DriveLink::File { id } => {
                if let Some(token) = auth.google_drive_token.as_deref() {
                    probe_api_file(url, &id, token).await
                } else {
                    probe_public_file(url, &id).await
                }
            }
            DriveLink::Folder { id } => probe_api_folder(url, &id, google_token(auth)?).await,
            DriveLink::Docs { id, .. } => probe_api_file(url, &id, google_token(auth)?).await,
        }
    }

    async fn download_job(
        &self,
        mut manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        let client = drive_client()?;
        let events = EventSink::new(options.json_events);
        events.emit(Event::Started {
            job_id: manifest.job_id.clone(),
            provider: manifest.provider.clone(),
            total_files: manifest.total_files,
            total_bytes: manifest.total_bytes,
        });

        let mut done_bytes: u64 = manifest
            .files
            .iter()
            .filter(|file| file.done)
            .map(|file| file.size)
            .sum();

        for index in 0..manifest.files.len() {
            if manifest.files[index].done {
                continue;
            }

            let file = manifest.files[index].clone();
            let url = file
                .handle
                .clone()
                .ok_or_else(|| anyhow::anyhow!("manifesto Google Drive sem URL de download"))?;
            let token = token_for_download_url(&url, auth)?;
            let target = google_drive_target_path(&manifest, &file);

            if let Err(e) = download_file(
                &client,
                &url,
                token,
                &target,
                file.size,
                file.integrity.as_ref(),
                &events,
                &manifest.job_id,
                &file.path,
            )
            .await
            {
                let message = e.to_string();
                manifest.state = JobState::Error;
                manifest.error = Some(message.clone());
                manifest.files[index].error = Some(message.clone());
                save_job(&manifest)?;
                events.emit(Event::Error {
                    job_id: manifest.job_id.clone(),
                    message: message.clone(),
                });
                return Err(anyhow::anyhow!(message));
            }

            done_bytes += file.size;
            manifest.files[index].done = true;
            manifest.files[index].error = None;
            save_job(&manifest)?;
            events.emit(Event::FileFinished {
                job_id: manifest.job_id.clone(),
                path: file.path.clone(),
                bytes: file.size,
            });
            events.emit(Event::Progress {
                job_id: manifest.job_id.clone(),
                path: file.path,
                bytes_done: done_bytes,
                bytes_total: manifest.total_bytes,
            });
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
}

#[derive(Debug, Deserialize)]
struct DriveApiFile {
    id: String,
    name: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    size: Option<String>,
    #[serde(rename = "md5Checksum")]
    md5_checksum: Option<String>,
    #[serde(rename = "sha1Checksum")]
    sha1_checksum: Option<String>,
    #[serde(rename = "sha256Checksum")]
    sha256_checksum: Option<String>,
}

impl DriveApiFile {
    /// O que a API declarou de integridade para este arquivo.
    ///
    /// Arquivos nativos do Google (Docs, Sheets) e atalhos nao tem conteudo
    /// binario no Drive e por isso nao tem checksum nenhum: nesses casos o
    /// resultado e vazio, e o download cai na verificacao por tamanho.
    fn integrity(&self) -> Option<Integrity> {
        let declarado = Integrity {
            md5: self.md5_checksum.clone(),
            sha1: self.sha1_checksum.clone(),
            sha256: self.sha256_checksum.clone(),
            crc32c: None,
        };
        (!declarado.is_empty()).then_some(declarado)
    }
}

#[derive(Debug, Deserialize)]
struct DriveListResponse {
    files: Vec<DriveApiFile>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

async fn probe_api_file(
    original_url: &str,
    id: &str,
    token: &str,
) -> anyhow::Result<SourceManifest> {
    let client = drive_client()?;
    let file = fetch_api_file(&client, token, id).await?;
    if file.mime_type == DRIVE_FOLDER_MIME {
        return api_folder_source(original_url, &client, token, file).await;
    }
    api_file_source(original_url, &client, token, &file).await
}

async fn probe_api_folder(
    original_url: &str,
    id: &str,
    token: &str,
) -> anyhow::Result<SourceManifest> {
    let client = drive_client()?;
    let root = fetch_api_file(&client, token, id).await?;
    if root.mime_type != DRIVE_FOLDER_MIME {
        return api_file_source(original_url, &client, token, &root).await;
    }
    api_folder_source(original_url, &client, token, root).await
}

async fn api_file_source(
    original_url: &str,
    client: &Client,
    token: &str,
    file: &DriveApiFile,
) -> anyhow::Result<SourceManifest> {
    let record = api_file_record(client, token, file, &file.name).await?;
    Ok(SourceManifest {
        provider: "google_drive".to_string(),
        url: original_url.to_string(),
        kind: SourceKind::File,
        name: record.name.clone(),
        total_files: 1,
        total_bytes: record.size,
        files: vec![record],
    })
}

async fn api_folder_source(
    original_url: &str,
    client: &Client,
    token: &str,
    root: DriveApiFile,
) -> anyhow::Result<SourceManifest> {
    let files = list_api_folder_recursive(client, token, &root.id).await?;
    let total_bytes = files.iter().map(|file| file.size).sum();
    Ok(SourceManifest {
        provider: "google_drive".to_string(),
        url: original_url.to_string(),
        kind: SourceKind::Folder,
        name: root.name,
        total_files: files.len() as u64,
        total_bytes,
        files,
    })
}

async fn probe_public_file(original_url: &str, id: &str) -> anyhow::Result<SourceManifest> {
    let client = drive_client()?;
    let download_url = public_file_download_url(id);
    let (name, size) = fetch_public_file_metadata(&client, &download_url).await?;
    Ok(SourceManifest {
        provider: "google_drive".to_string(),
        url: original_url.to_string(),
        kind: SourceKind::File,
        name: name.clone(),
        total_files: 1,
        total_bytes: size,
        files: vec![FileRecord {
            path: name.clone(),
            name,
            size,
            handle: Some(download_url),
            done: false,
            error: None,
            integrity: None,
        }],
    })
}

async fn fetch_api_file(client: &Client, token: &str, id: &str) -> anyhow::Result<DriveApiFile> {
    let response = drive_api_get_with_backoff(
        client,
        token,
        api_file_metadata_url(id)?,
        "Drive API files.get",
    )
    .await?;
    Ok(response.json().await?)
}

/// Subpasta ainda por visitar: `(id no Drive, caminho relativo)`.
type PendingFolder = (String, String);
/// Arquivo ainda por registrar: `(metadados da API, caminho relativo)`.
type PendingFile = (DriveApiFile, String);

/// Separa os filhos de uma pasta entre subpastas a visitar e arquivos a
/// registrar, ja com o caminho relativo montado.
fn split_drive_children(
    prefix: &str,
    entries: Vec<DriveApiFile>,
) -> (Vec<PendingFolder>, Vec<PendingFile>) {
    let mut pastas = Vec::new();
    let mut arquivos = Vec::new();
    for entry in entries {
        let path = join_drive_path(prefix, &entry.name);
        if entry.mime_type == DRIVE_FOLDER_MIME {
            pastas.push((entry.id, path));
        } else {
            arquivos.push((entry, path));
        }
    }
    (pastas, arquivos)
}

/// Varre a arvore de pastas em largura, um nivel por rodada, com as chamadas
/// de cada nivel em paralelo.
///
/// A versao sequencial gastava uma ida e volta a API por pasta. Numa arvore
/// larga — dezenas de subpastas por nivel — isso fazia o tempo crescer com o
/// numero total de pastas. Aqui o custo passa a crescer com a *profundidade*,
/// e cada nivel resolve `DRIVE_LIST_CONCURRENCY` pastas ao mesmo tempo.
///
/// Arquivos nativos do Google exigem uma requisicao extra para descobrir o
/// tamanho; por isso os registros do nivel tambem sao montados em paralelo.
async fn list_api_folder_recursive(
    client: &Client,
    token: &str,
    root_id: &str,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    let mut frontier = vec![(root_id.to_string(), String::new())];
    // Uma pasta ja visitada nao e revisitada: protege contra ciclo e evita
    // contar o mesmo arquivo duas vezes se a arvore se cruzar.
    let mut vistos: HashSet<String> = HashSet::new();
    vistos.insert(root_id.to_string());

    while !frontier.is_empty() {
        let listagens: Vec<anyhow::Result<(String, Vec<DriveApiFile>)>> =
            futures::stream::iter(frontier.into_iter().map(|(id, prefix)| async move {
                let filhos = fetch_api_folder_children(client, token, &id).await?;
                Ok((prefix, filhos))
            }))
            .buffer_unordered(DRIVE_LIST_CONCURRENCY)
            .collect()
            .await;

        let mut proximo = Vec::new();
        let mut entradas = Vec::new();
        for resultado in listagens {
            let (prefix, filhos) = resultado?;
            let (pastas, arquivos) = split_drive_children(&prefix, filhos);
            for (id, path) in pastas {
                if vistos.insert(id.clone()) {
                    proximo.push((id, path));
                }
            }
            entradas.extend(arquivos);
        }

        let registros: Vec<anyhow::Result<FileRecord>> =
            futures::stream::iter(entradas.into_iter().map(|(entry, path)| async move {
                api_file_record(client, token, &entry, &path).await
            }))
            .buffer_unordered(DRIVE_LIST_CONCURRENCY)
            .collect()
            .await;
        for resultado in registros {
            files.push(resultado?);
        }

        frontier = proximo;
    }

    // Com concorrencia a ordem de chegada varia entre execucoes. Ordenar deixa
    // o manifesto estavel e comparavel.
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// Verdadeiro para status que valem nova tentativa: cota estourada e falha
/// transitoria do servidor. `401`/`403 forbidden` de permissao nao entram —
/// repetir nao resolveria e so gastaria cota.
fn drive_status_is_retryable(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// GET na API do Drive com recuo exponencial.
///
/// Paralelizar a varredura sem isto trocaria lentidao por `429`: a cota do
/// Google e por segundo, e um pico de chamadas simultaneas a estoura.
async fn drive_api_get_with_backoff(
    client: &Client,
    token: &str,
    url: Url,
    what: &str,
) -> anyhow::Result<Response> {
    let mut espera = std::time::Duration::from_millis(250);
    for tentativa in 1..=DRIVE_LIST_ATTEMPTS {
        let response = with_bearer(client.get(url.clone()), Some(token))
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        if !drive_status_is_retryable(status) || tentativa == DRIVE_LIST_ATTEMPTS {
            return Err(anyhow::anyhow!("{what} falhou: {status}"));
        }
        tokio::time::sleep(espera).await;
        espera = espera.saturating_mul(2);
    }
    unreachable!("o laco sempre retorna dentro de DRIVE_LIST_ATTEMPTS")
}

async fn fetch_api_folder_children(
    client: &Client,
    token: &str,
    folder_id: &str,
) -> anyhow::Result<Vec<DriveApiFile>> {
    let mut files = Vec::new();
    let mut page_token = None;

    loop {
        let response = drive_api_get_with_backoff(
            client,
            token,
            api_folder_children_url(folder_id, page_token.as_deref())?,
            "Drive API files.list",
        )
        .await?;

        let page: DriveListResponse = response.json().await?;
        files.extend(page.files);
        let Some(next_page_token) = page.next_page_token else {
            break;
        };
        page_token = Some(next_page_token);
    }

    Ok(files)
}

async fn fetch_public_file_metadata(client: &Client, url: &str) -> anyhow::Result<(String, u64)> {
    fetch_download_metadata(client, url, None).await
}

async fn fetch_download_metadata(
    client: &Client,
    url: &str,
    token: Option<&str>,
) -> anyhow::Result<(String, u64)> {
    if let Ok(response) = with_bearer(client.head(url), token).send().await {
        if let Ok(metadata) = metadata_from_response(&response) {
            return Ok(metadata);
        }
    }

    let response = with_bearer(client.get(url), token)
        .header(RANGE, "bytes=0-0")
        .send()
        .await?;
    metadata_from_response(&response).map_err(|e| anyhow::anyhow!(e))
}

// ponytail: helper interno espelha o fluxo WebDAV; struct aqui so moveria campos de lugar.
#[allow(clippy::too_many_arguments)]
async fn download_file(
    client: &Client,
    url: &str,
    token: Option<&str>,
    target: &Path,
    expected_size: u64,
    expected_integrity: Option<&Integrity>,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
) -> anyhow::Result<()> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if existing_target_is_complete(
        target,
        expected_size,
        expected_integrity,
        events,
        job_id,
        event_path,
    )? {
        return Ok(());
    }

    let part = part_path(target);
    let mut start = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
    if start > expected_size {
        std::fs::remove_file(&part)?;
        start = 0;
    }
    // Conferir checksum exige ver todos os bytes. Retomar do meio deixaria o
    // trecho ja gravado fora da conta, o que produziria uma divergencia falsa.
    // Havendo integridade declarada, o arquivo e refeito do zero.
    if start > 0 && expected_integrity.is_some_and(|i| !i.is_empty()) {
        events.emit(Event::Retry {
            job_id: job_id.to_string(),
            path: event_path.to_string(),
            chunk: 0,
            attempt: 1,
            reason: "checksum declarado exige o arquivo inteiro; refazendo do zero".to_string(),
        });
        std::fs::remove_file(&part).ok();
        start = 0;
    }

    let mut request = with_bearer(client.get(url), token);
    if start > 0 {
        request = request.header(RANGE, format!("bytes={start}-"));
    }
    let response = request.send().await?;
    if start > 0 && !resume_response_matches(&response, start, expected_size) {
        std::fs::remove_file(&part).ok();
        events.emit(Event::Retry {
            job_id: job_id.to_string(),
            path: event_path.to_string(),
            chunk: 0,
            attempt: 2,
            reason: "Google Drive respondeu Range invalido; reiniciando de zero".to_string(),
        });
        return download_from_zero(
            client,
            url,
            token,
            target,
            expected_size,
            expected_integrity,
            events,
            job_id,
            event_path,
        )
        .await;
    }
    write_response(
        &part,
        target,
        start,
        expected_size,
        expected_integrity,
        events,
        job_id,
        event_path,
        response,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn download_from_zero(
    client: &Client,
    url: &str,
    token: Option<&str>,
    target: &Path,
    expected_size: u64,
    expected_integrity: Option<&Integrity>,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
) -> anyhow::Result<()> {
    let response = with_bearer(client.get(url), token).send().await?;
    write_response(
        &part_path(target),
        target,
        0,
        expected_size,
        expected_integrity,
        events,
        job_id,
        event_path,
        response,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn write_response(
    part: &Path,
    target: &Path,
    start: u64,
    expected_size: u64,
    expected_integrity: Option<&Integrity>,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
    response: Response,
) -> anyhow::Result<()> {
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "GET Google Drive falhou: {}",
            response.status()
        ));
    }
    reject_html_confirmation(&response)?;

    // O que conferir: o que a API declarou, mais o que o proprio fio declarou
    // no cabecalho `x-goog-hash`. O segundo nao custa nada e pega corrupcao em
    // transito que o tamanho nao ve.
    let mut referencia = expected_integrity.cloned().unwrap_or_default();
    if let Some(cabecalho) = response
        .headers()
        .get("x-goog-hash")
        .and_then(|v| v.to_str().ok())
    {
        let do_fio = parse_goog_hash(cabecalho);
        if referencia.crc32c.is_none() {
            referencia.crc32c = do_fio.crc32c;
        }
        if referencia.md5.is_none() {
            referencia.md5 = do_fio.md5;
        }
    }
    // Retomada parcial nao pode conferir hash: os bytes anteriores nao passaram
    // por aqui. Nesse caso so o tamanho e verificado.
    let mut hasher = if start == 0 {
        Hasher::for_reference(&referencia)
    } else {
        Hasher::for_reference(&Integrity::default())
    };

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(start > 0)
        .write(true)
        .truncate(start == 0)
        .open(part)?;
    let mut done = start;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)?;
        if !hasher.is_noop() {
            hasher.update(&chunk);
        }
        done += chunk.len() as u64;
        events.emit(Event::Progress {
            job_id: job_id.to_string(),
            path: event_path.to_string(),
            bytes_done: done.min(expected_size),
            bytes_total: expected_size,
        });
    }
    drop(file);

    let actual = std::fs::metadata(part)?.len();
    if actual != expected_size {
        return Err(anyhow::anyhow!(
            "Tamanho invalido em {}: esperado {}, obtido {}",
            event_path,
            expected_size,
            actual
        ));
    }

    // Arquivo com conteudo divergente nao vira destino: o `.part` e descartado
    // para que a proxima execucao refaca, em vez de aceitar lixo para sempre.
    let calculado = hasher.finish();
    match referencia.check(&calculado) {
        Check::Mismatch {
            algorithm,
            expected,
            actual,
        } => {
            std::fs::remove_file(part).ok();
            return Err(anyhow::anyhow!(
                "Checksum {} nao confere em {}: esperado {}, obtido {}",
                algorithm.as_str(),
                event_path,
                expected,
                actual
            ));
        }
        Check::Match(algorithm) => {
            events.emit(Event::Verified {
                job_id: job_id.to_string(),
                path: event_path.to_string(),
                algorithm: algorithm.as_str().to_string(),
            });
        }
        Check::NoReference => {}
    }

    if target.exists() {
        std::fs::remove_file(target)?;
    }
    std::fs::rename(part, target)?;
    Ok(())
}

fn parse_google_drive_link(url: &str) -> anyhow::Result<DriveLink> {
    let url = Url::parse(url)?;
    let host = url.host_str().unwrap_or("").trim_start_matches("www.");
    let segments: Vec<_> = url
        .path_segments()
        .map(|segments| segments.collect())
        .unwrap_or_default();

    if host == "drive.google.com" {
        if segments.len() >= 3 && segments[0] == "file" && segments[1] == "d" {
            return drive_file(segments[2]);
        }
        if let Some(pos) = segments.iter().position(|segment| *segment == "folders") {
            if let Some(id) = segments.get(pos + 1) {
                return drive_folder(id);
            }
        }
        if matches!(segments.first(), Some(&"open" | &"uc" | &"download")) {
            if let Some(id) = query_id(&url) {
                return drive_file(&id);
            }
        }
    }

    if host == "docs.google.com" && segments.len() >= 3 && segments[1] == "d" {
        return drive_docs(segments[2], segments[0]);
    }

    Err(anyhow::anyhow!("URL Google Drive nao suportada"))
}

async fn api_file_record(
    client: &Client,
    token: &str,
    file: &DriveApiFile,
    path: &str,
) -> anyhow::Result<FileRecord> {
    if file.mime_type.starts_with("application/vnd.google-apps.") {
        return api_export_record(client, token, file, path).await;
    }
    let size = file
        .size
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Drive API nao retornou tamanho para '{}'", file.name))?
        .parse()?;
    Ok(FileRecord {
        path: path.to_string(),
        name: file.name.clone(),
        size,
        handle: Some(api_file_download_url(&file.id)?),
        done: false,
        error: None,
        integrity: file.integrity(),
    })
}

async fn api_export_record(
    client: &Client,
    token: &str,
    file: &DriveApiFile,
    path: &str,
) -> anyhow::Result<FileRecord> {
    let handle = api_file_export_url(&file.id)?;
    let (_, size) = fetch_download_metadata(client, &handle, Some(token)).await?;
    let name = with_pdf_extension(&file.name);
    Ok(FileRecord {
        path: with_pdf_extension(path),
        name,
        size,
        handle: Some(handle),
        done: false,
        error: None,
        integrity: None,
    })
}

fn api_file_metadata_url(id: &str) -> anyhow::Result<Url> {
    validate_drive_id(id)?;
    let mut url = Url::parse(&format!("https://www.googleapis.com/drive/v3/files/{id}"))?;
    url.query_pairs_mut()
        .append_pair("fields", DRIVE_API_FIELDS)
        .append_pair("supportsAllDrives", "true");
    Ok(url)
}

fn api_file_download_url(id: &str) -> anyhow::Result<String> {
    validate_drive_id(id)?;
    let mut url = Url::parse(&format!("https://www.googleapis.com/drive/v3/files/{id}"))?;
    url.query_pairs_mut()
        .append_pair("alt", "media")
        .append_pair("supportsAllDrives", "true");
    Ok(url.to_string())
}

fn api_file_export_url(id: &str) -> anyhow::Result<String> {
    validate_drive_id(id)?;
    let mut url = Url::parse(&format!(
        "https://www.googleapis.com/drive/v3/files/{id}/export"
    ))?;
    url.query_pairs_mut()
        .append_pair("mimeType", DRIVE_EXPORT_MIME);
    Ok(url.to_string())
}

fn api_folder_children_url(folder_id: &str, page_token: Option<&str>) -> anyhow::Result<Url> {
    validate_drive_id(folder_id)?;
    let mut url = Url::parse("https://www.googleapis.com/drive/v3/files")?;
    let query = format!("'{folder_id}' in parents and trashed=false");
    let mut pairs = url.query_pairs_mut();
    pairs
        .append_pair("q", &query)
        .append_pair(
            "fields",
            &format!("nextPageToken,files({DRIVE_API_FIELDS})"),
        )
        .append_pair("pageSize", "1000")
        .append_pair("supportsAllDrives", "true")
        .append_pair("includeItemsFromAllDrives", "true");
    if let Some(page_token) = page_token {
        pairs.append_pair("pageToken", page_token);
    }
    drop(pairs);
    Ok(url)
}

fn join_drive_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn google_token(auth: &ProviderAuth) -> anyhow::Result<&str> {
    auth.google_drive_token.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "Google Drive folder/docs precisa de --google-drive-token ou ADITI_GOOGLE_DRIVE_TOKEN"
        )
    })
}

fn token_for_download_url<'a>(
    url: &str,
    auth: &'a ProviderAuth,
) -> anyhow::Result<Option<&'a str>> {
    let parsed = Url::parse(url)?;
    if parsed.host_str() == Some("www.googleapis.com") {
        return Ok(Some(google_token(auth)?));
    }
    Ok(None)
}

fn query_id(url: &Url) -> Option<String> {
    url.query_pairs()
        .find_map(|(key, value)| (key == "id").then(|| value.into_owned()))
}

fn drive_file(id: &str) -> anyhow::Result<DriveLink> {
    validate_drive_id(id)?;
    Ok(DriveLink::File { id: id.to_string() })
}

fn drive_folder(id: &str) -> anyhow::Result<DriveLink> {
    validate_drive_id(id)?;
    Ok(DriveLink::Folder { id: id.to_string() })
}

fn drive_docs(id: &str, app: &str) -> anyhow::Result<DriveLink> {
    validate_drive_id(id)?;
    Ok(DriveLink::Docs {
        id: id.to_string(),
        app: app.to_string(),
    })
}

fn validate_drive_id(id: &str) -> anyhow::Result<()> {
    if !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Ok(());
    }
    Err(anyhow::anyhow!("ID Google Drive invalido"))
}

fn metadata_from_response(response: &Response) -> anyhow::Result<(String, u64)> {
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Google Drive respondeu {}",
            response.status()
        ));
    }
    reject_html_confirmation(response)?;

    let headers = response.headers();
    let size = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(content_range_total)
        .or_else(|| {
            headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
        .context("Google Drive nao retornou tamanho do arquivo")?;
    let name = headers
        .get(CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .and_then(content_disposition_filename)
        .unwrap_or_else(|| "google-drive-file.bin".to_string());

    Ok((safe_file_name(&name), size))
}

fn reject_html_confirmation(response: &Response) -> anyhow::Result<()> {
    let is_html = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains("text/html"))
        .unwrap_or(false);
    let has_attachment = response.headers().contains_key(CONTENT_DISPOSITION);
    if is_html && !has_attachment {
        return Err(anyhow::anyhow!(
            "Google Drive retornou pagina HTML; link pode exigir login/confirmacao"
        ));
    }
    Ok(())
}

fn content_range_total(value: &str) -> Option<u64> {
    value.rsplit_once('/')?.1.parse().ok()
}

fn content_disposition_filename(value: &str) -> Option<String> {
    for part in value.split(';').map(str::trim) {
        if let Some(value) = part.strip_prefix("filename*=UTF-8''") {
            return Some(percent_decode(value));
        }
        if let Some(value) = part.strip_prefix("filename=") {
            return Some(value.trim_matches('"').to_string());
        }
    }
    None
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(hex);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn public_file_download_url(id: &str) -> String {
    format!("https://drive.google.com/uc?export=download&id={id}")
}

fn with_pdf_extension(name: &str) -> String {
    if name.to_ascii_lowercase().ends_with(".pdf") {
        name.to_string()
    } else {
        format!("{name}.pdf")
    }
}

fn google_drive_target_path(manifest: &JobManifest, file: &FileRecord) -> PathBuf {
    match manifest.kind {
        SourceKind::File => PathBuf::from(&manifest.target_path),
        SourceKind::Folder => Path::new(&manifest.job_dir).join(safe_relative_path(&file.path)),
    }
}

fn with_bearer(builder: RequestBuilder, token: Option<&str>) -> RequestBuilder {
    match token {
        Some(token) => builder.bearer_auth(token),
        None => builder,
    }
}

fn part_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin");
    target.with_file_name(format!("{file_name}.aditi.part"))
}

fn drive_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_drive_file_folder_and_docs_links() {
        assert_eq!(
            parse_google_drive_link("https://drive.google.com/file/d/abc_123-DEF/view").unwrap(),
            DriveLink::File {
                id: "abc_123-DEF".to_string()
            }
        );
        assert_eq!(
            parse_google_drive_link("https://drive.google.com/open?id=abc_123-DEF").unwrap(),
            DriveLink::File {
                id: "abc_123-DEF".to_string()
            }
        );
        assert_eq!(
            parse_google_drive_link("https://drive.google.com/drive/folders/folder_123").unwrap(),
            DriveLink::Folder {
                id: "folder_123".to_string()
            }
        );
        assert_eq!(
            parse_google_drive_link("https://docs.google.com/document/d/doc_123/edit").unwrap(),
            DriveLink::Docs {
                id: "doc_123".to_string(),
                app: "document".to_string()
            }
        );
    }

    #[test]
    fn parses_download_metadata_headers() {
        assert_eq!(content_range_total("bytes 0-0/42").unwrap(), 42);
        assert_eq!(
            content_disposition_filename("attachment; filename*=UTF-8''a%20file.txt").unwrap(),
            "a file.txt"
        );
    }

    fn drive_file_fixture() -> DriveApiFile {
        DriveApiFile {
            id: "file_123".to_string(),
            name: "a.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: Some("42".to_string()),
            md5_checksum: None,
            sha1_checksum: None,
            sha256_checksum: None,
        }
    }

    #[tokio::test]
    async fn maps_api_files_to_relative_paths() {
        let file = drive_file_fixture();
        let client = drive_client().unwrap();
        let record = api_file_record(&client, "token", &file, &join_drive_path("dir", &file.name))
            .await
            .unwrap();
        assert_eq!(record.path, "dir/a.txt");
        assert_eq!(record.size, 42);
        assert!(record.handle.unwrap().contains("alt=media"));
    }

    fn drive_entry(id: &str, name: &str, mime: &str) -> DriveApiFile {
        DriveApiFile {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: mime.to_string(),
            size: Some("1".to_string()),
            md5_checksum: None,
            sha1_checksum: None,
            sha256_checksum: None,
        }
    }

    #[test]
    fn splits_children_into_folders_and_files_with_relative_paths() {
        let (pastas, arquivos) = split_drive_children(
            "raiz/sub",
            vec![
                drive_entry("f1", "modelos", DRIVE_FOLDER_MIME),
                drive_entry("a1", "peca.stl", "application/octet-stream"),
                drive_entry("a2", "nota", "application/vnd.google-apps.document"),
            ],
        );
        assert_eq!(
            pastas,
            vec![("f1".to_string(), "raiz/sub/modelos".to_string())]
        );
        let caminhos: Vec<&str> = arquivos.iter().map(|(_, p)| p.as_str()).collect();
        assert_eq!(caminhos, vec!["raiz/sub/peca.stl", "raiz/sub/nota"]);
    }

    #[test]
    fn splits_children_at_the_root_without_a_leading_separator() {
        let (_, arquivos) = split_drive_children(
            "",
            vec![drive_entry("a1", "peca.stl", "application/octet-stream")],
        );
        assert_eq!(arquivos[0].1, "peca.stl");
    }

    #[test]
    fn retries_only_on_quota_and_server_faults() {
        use reqwest::StatusCode;
        // Cota estourada e falha transitoria valem nova tentativa.
        assert!(drive_status_is_retryable(StatusCode::TOO_MANY_REQUESTS));
        assert!(drive_status_is_retryable(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(drive_status_is_retryable(StatusCode::SERVICE_UNAVAILABLE));
        assert!(drive_status_is_retryable(StatusCode::GATEWAY_TIMEOUT));
        // Falta de permissao ou token vencido nao: repetir nao resolve e
        // queima cota. Foi o caso do 401 observado com token expirado.
        assert!(!drive_status_is_retryable(StatusCode::UNAUTHORIZED));
        assert!(!drive_status_is_retryable(StatusCode::FORBIDDEN));
        assert!(!drive_status_is_retryable(StatusCode::NOT_FOUND));
        assert!(!drive_status_is_retryable(StatusCode::OK));
    }

    #[test]
    fn asks_the_api_for_every_checksum_field() {
        // Se este campo perder um checksum, o manifesto silenciosamente para de
        // registrar integridade sem que nada falhe. O teste tranca isso.
        assert!(DRIVE_API_FIELDS.contains("md5Checksum"));
        assert!(DRIVE_API_FIELDS.contains("sha1Checksum"));
        assert!(DRIVE_API_FIELDS.contains("sha256Checksum"));
        let url = api_folder_children_url("folder_1", None).unwrap();
        let query = url.query().unwrap();
        assert!(
            query.contains("sha256Checksum"),
            "listagem sem checksum: {query}"
        );
    }

    #[tokio::test]
    async fn carries_the_declared_checksums_into_the_record() {
        let mut file = drive_file_fixture();
        file.md5_checksum = Some("900150983cd24fb0d6963f7d28e17f72".to_string());
        file.sha256_checksum = Some("ba7816bf".to_string());
        let client = drive_client().unwrap();
        let record = api_file_record(&client, "token", &file, "a.txt")
            .await
            .unwrap();
        let integrity = record.integrity.expect("deveria trazer integridade");
        assert_eq!(
            integrity.md5.as_deref(),
            Some("900150983cd24fb0d6963f7d28e17f72")
        );
        assert_eq!(integrity.sha256.as_deref(), Some("ba7816bf"));
        assert!(integrity.sha1.is_none());
    }

    #[tokio::test]
    async fn leaves_integrity_absent_when_the_api_declares_none() {
        // Arquivo nativo do Google e atalho nao tem conteudo binario no Drive,
        // logo nao tem checksum. Ausencia nao pode virar divergencia.
        let client = drive_client().unwrap();
        let record = api_file_record(&client, "token", &drive_file_fixture(), "a.txt")
            .await
            .unwrap();
        assert!(record.integrity.is_none());
    }

    #[test]
    fn names_native_exports_as_pdf() {
        assert_eq!(with_pdf_extension("Doc"), "Doc.pdf");
        assert_eq!(with_pdf_extension("Doc.PDF"), "Doc.PDF");
        assert!(api_file_export_url("doc_123")
            .unwrap()
            .contains("/export?mimeType=application%2Fpdf"));
    }
}
