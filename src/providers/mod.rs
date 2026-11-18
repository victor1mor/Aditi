pub mod dropbox;
pub mod google_drive;
pub mod google_photos;
pub mod mega;
pub mod mega_mac;
pub mod onedrive;
pub mod webdav;

use crate::core::events::{Event, EventSink};
use crate::core::integrity::{Check, Hasher, Integrity};
use crate::core::manifest::{JobManifest, SourceManifest};
use reqwest::header::CONTENT_RANGE;
use reqwest::{Response, StatusCode, Url};
use std::io::Read;
use std::path::Path;

#[derive(Clone, Debug, Default)]
pub struct ProviderAuth {
    pub user: Option<String>,
    pub pass: Option<String>,
    pub bearer_token: Option<String>,
    pub google_drive_token: Option<String>,
    pub google_photos_token: Option<String>,
    pub onedrive_token: Option<String>,
    pub dropbox_token: Option<String>,
}

impl ProviderAuth {
    pub fn from_env(
        user: Option<String>,
        pass: Option<String>,
        bearer_token: Option<String>,
        google_drive_token: Option<String>,
        google_photos_token: Option<String>,
        onedrive_token: Option<String>,
        dropbox_token: Option<String>,
    ) -> Self {
        Self {
            user: user.or_else(|| std::env::var("ADITI_WEBDAV_USER").ok()),
            pass: pass.or_else(|| std::env::var("ADITI_WEBDAV_PASS").ok()),
            bearer_token: bearer_token.or_else(|| std::env::var("ADITI_WEBDAV_BEARER_TOKEN").ok()),
            google_drive_token: google_drive_token
                .or_else(|| std::env::var("ADITI_GOOGLE_DRIVE_TOKEN").ok()),
            google_photos_token: google_photos_token
                .or_else(|| std::env::var("ADITI_GOOGLE_PHOTOS_TOKEN").ok()),
            onedrive_token: onedrive_token.or_else(|| std::env::var("ADITI_ONEDRIVE_TOKEN").ok()),
            dropbox_token: dropbox_token.or_else(|| std::env::var("ADITI_DROPBOX_TOKEN").ok()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DownloadOptions {
    pub threads: usize,
    pub proxies: Option<String>,
    pub force_proxy: bool,
    pub json_events: bool,
}

#[allow(async_fn_in_trait)]
pub trait CloudProvider {
    /// Nome curto gravado no manifesto. Ex.: "mega" ou "webdav".
    fn name(&self) -> &'static str;
    /// Diz se este provider entende a URL antes de tentar probe/download.
    fn can_handle(&self, url: &str) -> bool;
    /// Verdadeiro quando um .part pode continuar de onde parou.
    fn supports_resume(&self) -> bool;
    /// Verdadeiro quando o provider nunca funciona sem credenciais.
    fn requires_auth(&self) -> bool;
    /// Olha a fonte e monta o source.aditi.json sem baixar arquivos.
    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest>;
    /// Executa ou retoma um job.aditi.json ja criado.
    async fn download_job(
        &self,
        manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()>;
}

pub async fn probe_source(url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
    if let Some(message) = unsupported_public_share_error(url) {
        return Err(anyhow::anyhow!(message));
    }
    if mega::MegaProvider.can_handle(url) {
        return mega::MegaProvider.probe(url, auth).await;
    }
    if google_drive::GoogleDriveProvider.can_handle(url) {
        return google_drive::GoogleDriveProvider.probe(url, auth).await;
    }
    if google_photos::GooglePhotosProvider.can_handle(url) {
        return google_photos::GooglePhotosProvider.probe(url, auth).await;
    }
    if onedrive::OneDriveProvider.can_handle(url) {
        return onedrive::OneDriveProvider.probe(url, auth).await;
    }
    if dropbox::DropboxProvider.can_handle(url) {
        return dropbox::DropboxProvider.probe(url, auth).await;
    }
    if webdav::WebDavProvider.can_handle(url) {
        return webdav::WebDavProvider.probe(url, auth).await;
    }
    Err(anyhow::anyhow!("URL nao suportada por nenhum provider"))
}

fn unsupported_public_share_error(url: &str) -> Option<&'static str> {
    let url = Url::parse(url).ok()?;
    let host = url.host_str()?.trim_start_matches("www.");
    if matches!(host, "photos.app.goo.gl" | "photos.google.com") {
        return Some("Share URL do Google Photos nao suportada; use gphotos://album/ID ou gphotos://media/ID com token");
    }
    if matches!(host, "terabox.com" | "1024terabox.com") || host.ends_with(".terabox.com") {
        return Some("Link publico TeraBox nao suportado sem fluxo oficial token-only");
    }
    None
}

pub async fn download_job(
    manifest: JobManifest,
    auth: &ProviderAuth,
    options: &DownloadOptions,
) -> anyhow::Result<()> {
    validate_proxy_options(&manifest, options)?;
    match manifest.provider.as_str() {
        "mega" => {
            mega::MegaProvider
                .download_job(manifest, auth, options)
                .await
        }
        "webdav" => {
            webdav::WebDavProvider
                .download_job(manifest, auth, options)
                .await
        }
        "google_drive" => {
            google_drive::GoogleDriveProvider
                .download_job(manifest, auth, options)
                .await
        }
        "google_photos" => {
            google_photos::GooglePhotosProvider
                .download_job(manifest, auth, options)
                .await
        }
        "onedrive" => {
            onedrive::OneDriveProvider
                .download_job(manifest, auth, options)
                .await
        }
        "dropbox" => {
            dropbox::DropboxProvider
                .download_job(manifest, auth, options)
                .await
        }
        other => Err(anyhow::anyhow!(
            "Provider nao suportado no manifesto: {other}"
        )),
    }
}

pub fn validate_proxy_options(
    manifest: &JobManifest,
    options: &DownloadOptions,
) -> anyhow::Result<()> {
    let supports_proxy = manifest.provider == "mega";
    if options.force_proxy && !supports_proxy {
        return Err(anyhow::anyhow!(
            "--force-proxy so e suportado para download de arquivo MEGA"
        ));
    }
    if options.proxies.is_some() && !supports_proxy {
        return Err(anyhow::anyhow!(
            "--proxies so e suportado para download de arquivo MEGA"
        ));
    }
    Ok(())
}

/// Decide se um arquivo ja presente no destino pode ser dado como concluido.
///
/// Tamanho igual nunca foi prova de conteudo igual. Um `.part` promovido a
/// destino cedo demais, um arquivo trocado no disco, ou dois nomes de origem
/// que uma versao anterior fundia no mesmo caminho produzem o tamanho certo
/// com o conteudo errado — e a retomada aceitava todos eles como prontos, para
/// sempre, sem nunca reclamar.
///
/// Havendo checksum declarado pela origem, ele e conferido antes de pular o
/// arquivo.
///
/// Divergindo, o arquivo **nao** e apagado aqui. A funcao apenas recusa, e o
/// download normal segue seu curso: grava num `.part` e so substitui o destino
/// depois que o conteudo novo passar na verificacao dele. Nunca se destroi o
/// que ja existe antes de ter o substituto pronto — uma origem fora do ar
/// deixaria o usuario sem nenhuma das duas versoes.
///
/// Sem checksum declarado, o tamanho continua sendo o unico criterio possivel.
/// Isso nao e uma falha desta funcao: e o limite do que a origem informou.
pub fn existing_target_is_complete(
    target: &Path,
    expected_size: u64,
    expected_integrity: Option<&Integrity>,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
) -> anyhow::Result<bool> {
    if !target.exists() || std::fs::metadata(target)?.len() != expected_size {
        return Ok(false);
    }

    let referencia = match expected_integrity {
        Some(integrity) if !integrity.is_empty() => integrity,
        _ => return Ok(true),
    };

    let calculado = hash_existing_file(target, referencia)?;
    match referencia.check(&calculado) {
        Check::Match(algorithm) => {
            events.emit(Event::Verified {
                job_id: job_id.to_string(),
                path: event_path.to_string(),
                algorithm: algorithm.as_str().to_string(),
            });
            Ok(true)
        }
        Check::Mismatch {
            algorithm,
            expected,
            actual,
        } => {
            events.emit(Event::Retry {
                job_id: job_id.to_string(),
                path: event_path.to_string(),
                chunk: 0,
                attempt: 1,
                reason: format!(
                    "arquivo ja presente diverge do checksum {} declarado (esperado {expected}, obtido {actual}); refazendo",
                    algorithm.as_str()
                ),
            });
            Ok(false)
        }
        // Tamanho bate e nao sobrou nada comparavel para conferir.
        Check::NoReference => Ok(true),
    }
}

/// Le o arquivo em blocos e calcula somente os algoritmos que `reference`
/// declarou.
///
/// Em blocos porque o arquivo grande e justamente o caso que importa: e o de
/// 4 GB que ninguem quer rebaixar por engano, e o que nao cabe na memoria.
fn hash_existing_file(path: &Path, reference: &Integrity) -> anyhow::Result<Integrity> {
    let mut hasher = Hasher::for_reference(reference);
    if hasher.is_noop() {
        return Ok(Integrity::default());
    }
    let mut file = std::fs::File::open(path)?;
    let mut buffer = vec![0u8; 128 * 1024];
    loop {
        let lidos = file.read(&mut buffer)?;
        if lidos == 0 {
            break;
        }
        hasher.update(&buffer[..lidos]);
    }
    Ok(hasher.finish())
}

pub fn resume_response_matches(response: &Response, start: u64, expected_size: u64) -> bool {
    response.status() == StatusCode::PARTIAL_CONTENT
        && response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| content_range_matches(value, start, expected_size))
}

fn content_range_matches(value: &str, start: u64, expected_size: u64) -> bool {
    let Some(value) = value.strip_prefix("bytes ") else {
        return false;
    };
    let Some((range, total)) = value.split_once('/') else {
        return false;
    };
    let Some((actual_start, actual_end)) = range.split_once('-') else {
        return false;
    };
    matches!(
        (
            actual_start.parse::<u64>(),
            actual_end.parse::<u64>(),
            total.parse::<u64>()
        ),
        (Ok(actual_start), Ok(actual_end), Ok(total))
            if actual_start == start
                && actual_end >= actual_start
                && actual_end < total
                && total == expected_size
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::manifest::{JobState, SourceKind};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// SHA-256 e MD5 de "conteudo certo", calculados fora deste codigo
    /// (`sha256sum` / `md5sum`) para servirem de oraculo independente. Conferir
    /// contra o proprio `Hasher` so provaria que ele concorda consigo mesmo.
    const CERTO: &str = "conteudo certo";
    const CERTO_SHA256: &str = "e768fcd7f068282912c901ea5eb9ac339b0563ae04563e1c97fb370c5e79f108";
    const CERTO_MD5: &str = "22ff55d485655feb4680636efcb03511";
    /// Mesmo numero de bytes que `CERTO`: e exatamente o caso que o tamanho
    /// sozinho nao distingue.
    const ERRADO: &str = "CONTEUDO CERTO";

    fn arquivo_com(rotulo: &str, conteudo: &str) -> PathBuf {
        static CONTADOR: AtomicU32 = AtomicU32::new(0);
        let unico = CONTADOR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "aditi-alvo-{}-{rotulo}-{unico}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let alvo = dir.join("arquivo.bin");
        std::fs::write(&alvo, conteudo).unwrap();
        alvo
    }

    fn silencioso() -> EventSink {
        EventSink::new(false)
    }

    fn pergunta(alvo: &Path, tamanho: u64, declarado: Option<&Integrity>) -> bool {
        existing_target_is_complete(
            alvo,
            tamanho,
            declarado,
            &silencioso(),
            "job-1",
            "arquivo.bin",
        )
        .unwrap()
    }

    /// A regressao. Antes, tamanho igual bastava e um arquivo com o conteudo
    /// errado era dado como concluido para sempre.
    #[test]
    fn rejects_a_preexisting_file_whose_checksum_diverges() {
        let alvo = arquivo_com("diverge", ERRADO);
        let declarado = Integrity {
            sha256: Some(CERTO_SHA256.to_string()),
            ..Default::default()
        };
        assert_eq!(ERRADO.len(), CERTO.len(), "o teste perdeu o sentido");
        assert!(
            !pergunta(&alvo, ERRADO.len() as u64, Some(&declarado)),
            "aceitou arquivo com conteudo divergente"
        );
    }

    /// Recusar nao pode significar destruir: o arquivo que ja estava la fica
    /// no lugar ate existir um substituto verificado.
    #[test]
    fn does_not_delete_the_diverging_file() {
        let alvo = arquivo_com("preserva", ERRADO);
        let declarado = Integrity {
            sha256: Some(CERTO_SHA256.to_string()),
            ..Default::default()
        };
        let _ = pergunta(&alvo, ERRADO.len() as u64, Some(&declarado));
        assert!(alvo.exists(), "o arquivo existente foi apagado");
        assert_eq!(std::fs::read_to_string(&alvo).unwrap(), ERRADO);
    }

    #[test]
    fn accepts_a_preexisting_file_whose_checksum_matches() {
        let alvo = arquivo_com("confere", CERTO);
        let declarado = Integrity {
            sha256: Some(CERTO_SHA256.to_string()),
            md5: Some(CERTO_MD5.to_string()),
            ..Default::default()
        };
        assert!(pergunta(&alvo, CERTO.len() as u64, Some(&declarado)));
    }

    /// Sem checksum declarado o tamanho volta a ser o unico criterio — nao ha
    /// o que conferir, e recusar tudo faria toda retomada rebaixar o job
    /// inteiro.
    #[test]
    fn falls_back_to_size_when_the_source_declared_nothing() {
        let alvo = arquivo_com("sem-checksum", ERRADO);
        assert!(pergunta(&alvo, ERRADO.len() as u64, None));
        assert!(pergunta(
            &alvo,
            ERRADO.len() as u64,
            Some(&Integrity::default())
        ));
    }

    #[test]
    fn rejects_when_the_size_differs() {
        let alvo = arquivo_com("tamanho", CERTO);
        assert!(!pergunta(&alvo, 999, None));
    }

    #[test]
    fn rejects_a_target_that_does_not_exist() {
        let ausente = std::env::temp_dir().join("aditi-nao-existe-nunca.bin");
        let _ = std::fs::remove_file(&ausente);
        assert!(!pergunta(&ausente, 0, None));
    }

    /// Le em blocos: um arquivo maior que o buffer nao pode confundir a conta.
    #[test]
    fn hashes_files_larger_than_the_read_buffer() {
        let grande = "a".repeat(300 * 1024);
        let alvo = arquivo_com("grande", &grande);

        let mut hasher = Hasher::all();
        hasher.update(grande.as_bytes());
        let declarado = Integrity {
            sha256: hasher.finish().sha256,
            ..Default::default()
        };

        assert!(pergunta(&alvo, grande.len() as u64, Some(&declarado)));
        // E o buffer nao pode "empatar" um arquivo diferente do mesmo tamanho.
        let outro = arquivo_com("grande-outro", &"b".repeat(300 * 1024));
        assert!(!pergunta(&outro, grande.len() as u64, Some(&declarado)));
    }

    fn manifest(provider: &str, kind: SourceKind) -> JobManifest {
        JobManifest {
            version: 1,
            job_id: "job-1".to_string(),
            provider: provider.to_string(),
            url: "https://example.com/file".to_string(),
            kind,
            state: JobState::Running,
            out_dir: "out".to_string(),
            job_dir: "out/job-1".to_string(),
            target_path: "out/job-1/file".to_string(),
            total_files: 1,
            total_bytes: 1,
            chunk_size: 1,
            chunks: Vec::new(),
            files: Vec::new(),
            error: None,
        }
    }

    #[test]
    fn proxy_options_fail_closed_outside_mega_file() {
        let options = DownloadOptions {
            threads: 1,
            proxies: Some("proxies.txt".to_string()),
            force_proxy: true,
            json_events: false,
        };
        assert!(validate_proxy_options(&manifest("mega", SourceKind::File), &options).is_ok());
        assert!(validate_proxy_options(&manifest("mega", SourceKind::Folder), &options).is_ok());
        assert!(validate_proxy_options(&manifest("webdav", SourceKind::File), &options).is_err());

        let options = DownloadOptions {
            force_proxy: false,
            ..options
        };
        assert!(validate_proxy_options(&manifest("webdav", SourceKind::File), &options).is_err());
    }

    #[test]
    fn validates_resumed_content_range() {
        assert!(content_range_matches("bytes 42-99/100", 42, 100));
        assert!(!content_range_matches("bytes 0-57/100", 42, 100));
        assert!(!content_range_matches("bytes 42-99/101", 42, 100));
        assert!(!content_range_matches("bytes 42-100/100", 42, 100));
    }

    #[test]
    fn rejects_public_share_hosts_before_webdav() {
        assert!(unsupported_public_share_error("https://photos.app.goo.gl/share").is_some());
        assert!(unsupported_public_share_error("https://1024terabox.com/s/share").is_some());
        assert!(
            unsupported_public_share_error("https://drive.google.com/file/d/id/view").is_none()
        );
    }
}
