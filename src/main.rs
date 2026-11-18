pub mod api;
pub mod auth;
pub mod cli;
pub mod core;
pub mod providers;
pub mod proxy;
pub mod transfer;

use crate::auth::OAuthArgs;
use crate::core::job::{job_dir, load_registry, safe_file_name, save_registry, validate_job_id};
use crate::core::manifest::{
    load_job, save_job, source_manifest_path, write_json, JobManifest, JobState, SourceKind,
    SourceManifest,
};
use crate::providers::{DownloadOptions, ProviderAuth};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about = "Aditi - Cloud Drive Engine com suporte MEGA", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// URL publica do MEGA. Mantido para compatibilidade com a CLI antiga.
    #[arg(short, long)]
    pub url: Option<String>,

    /// Caminho do arquivo de proxies para downloads MEGA.
    #[arg(short, long)]
    pub proxies: Option<String>,

    /// Caminho de destino legado. Nos comandos novos, use `start --out DIR`.
    #[arg(short, long)]
    pub out: Option<String>,

    /// Forca o uso de proxy no download MEGA; falha nos outros providers.
    #[arg(long, default_value_t = false)]
    pub force_proxy: bool,

    /// Quantidade de chunks concorrentes
    #[arg(short = 't', long, default_value_t = 8)]
    pub threads: usize,

    /// Emite logs tracing em JSON no modo antigo.
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Auth(OAuthArgs),
    Probe(ProbeArgs),
    Start(StartArgs),
    Resume(ResumeArgs),
}

#[derive(Parser, Debug)]
pub struct ProbeArgs {
    #[arg(long)]
    pub url: String,
    #[arg(long, default_value_t = false)]
    pub json: bool,
    #[command(flatten)]
    pub auth: AuthArgs,
}

#[derive(Parser, Debug)]
pub struct StartArgs {
    #[arg(long)]
    pub job_id: String,
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub out: PathBuf,
    #[arg(long, default_value_t = false)]
    pub json_events: bool,
    #[arg(short = 't', long, default_value_t = 8)]
    pub threads: usize,
    #[arg(long)]
    pub proxies: Option<String>,
    #[arg(long, default_value_t = false)]
    pub force_proxy: bool,
    #[command(flatten)]
    pub auth: AuthArgs,
}

#[derive(Parser, Debug)]
pub struct ResumeArgs {
    #[arg(long)]
    pub job_id: String,
    #[arg(long, default_value_t = false)]
    pub json_events: bool,
    /// Opcional: use quando o registry local ainda nao conhece o job.
    #[arg(long)]
    pub out: Option<PathBuf>,
    #[arg(short = 't', long, default_value_t = 8)]
    pub threads: usize,
    #[arg(long)]
    pub proxies: Option<String>,
    #[arg(long, default_value_t = false)]
    pub force_proxy: bool,
    #[command(flatten)]
    pub auth: AuthArgs,
}

#[derive(clap::Args, Clone, Debug, Default)]
pub struct AuthArgs {
    /// Usuario WebDAV. Se omitido, usa ADITI_WEBDAV_USER.
    #[arg(long)]
    pub webdav_user: Option<String>,
    /// Senha WebDAV. Se omitida, usa ADITI_WEBDAV_PASS.
    #[arg(long)]
    pub webdav_pass: Option<String>,
    /// Bearer token WebDAV. Se omitido, usa ADITI_WEBDAV_BEARER_TOKEN.
    #[arg(long)]
    pub webdav_bearer_token: Option<String>,
    /// Bearer token Google Drive. Se omitido, usa ADITI_GOOGLE_DRIVE_TOKEN.
    #[arg(long)]
    pub google_drive_token: Option<String>,
    /// Bearer token Google Photos. Se omitido, usa ADITI_GOOGLE_PHOTOS_TOKEN.
    #[arg(long)]
    pub google_photos_token: Option<String>,
    /// Bearer token Microsoft Graph para OneDrive/SharePoint. Se omitido, usa ADITI_ONEDRIVE_TOKEN.
    #[arg(long)]
    pub onedrive_token: Option<String>,
    /// Bearer token Dropbox. Se omitido, usa ADITI_DROPBOX_TOKEN.
    #[arg(long)]
    pub dropbox_token: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let machine_output = matches!(
        &cli.command,
        Some(Command::Probe(args)) if args.json
    ) || matches!(
        &cli.command,
        Some(Command::Start(args)) if args.json_events
    ) || matches!(
        &cli.command,
        Some(Command::Resume(args)) if args.json_events
    );
    let quiet_tracing = machine_output;
    init_tracing(cli.json, quiet_tracing);

    let result = match cli.command {
        Some(Command::Auth(args)) => auth::authenticate(args).await,
        Some(Command::Probe(args)) => probe(args).await,
        Some(Command::Start(args)) => start(args).await,
        Some(Command::Resume(args)) => resume(args).await,
        None => legacy_download(cli).await,
    };

    if let Err(e) = result {
        if machine_output {
            println!(
                "{}",
                serde_json::json!({
                    "event": "error",
                    "message": e.to_string()
                })
            );
        } else {
            tracing::error!("{}", e);
            eprintln!("{e}");
        }
        std::process::exit(1);
    }
}

fn init_tracing(json: bool, quiet: bool) {
    if quiet {
        return;
    }
    if json {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt::init();
    }
}

async fn probe(args: ProbeArgs) -> anyhow::Result<()> {
    let auth = provider_auth(&args.auth);
    let source = providers::probe_source(&args.url, &auth).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&source)?);
    } else {
        println!(
            "{}: {} arquivo(s), {} bytes",
            source.name, source.total_files, source.total_bytes
        );
    }
    Ok(())
}

async fn start(args: StartArgs) -> anyhow::Result<()> {
    validate_job_id(&args.job_id)?;
    validate_threads(args.threads)?;
    let job_dir = job_dir(&args.out, &args.job_id);
    let auth = provider_auth(&args.auth);
    let source = providers::probe_source(&args.url, &auth).await?;
    let options = download_options(
        args.threads,
        args.proxies,
        args.force_proxy,
        args.json_events,
    );
    let manifest = match source.kind {
        SourceKind::File => {
            let target = job_dir.join(safe_file_name(&source.name));
            file_job_manifest(
                &args.job_id,
                &args.url,
                &args.out,
                &job_dir,
                &target,
                &source,
            )
        }
        SourceKind::Folder => {
            folder_job_manifest(&args.job_id, &args.url, &args.out, &job_dir, &source)
        }
    };
    providers::validate_proxy_options(&manifest, &options)?;

    if let Some(parent) = job_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir(&job_dir)
        .map_err(|_| anyhow::anyhow!("job-id ja existe; use resume ou escolha outro"))?;
    write_json(&source_manifest_path(&job_dir), &source)?;
    save_registry(&args.job_id, &job_dir)?;
    save_job(&manifest)?;
    providers::download_job(manifest, &auth, &options).await?;

    Ok(())
}

async fn resume(args: ResumeArgs) -> anyhow::Result<()> {
    validate_job_id(&args.job_id)?;
    validate_threads(args.threads)?;
    let job_dir = match args.out {
        Some(out) => job_dir(&out, &args.job_id),
        None => PathBuf::from(load_registry(&args.job_id)?.job_dir),
    };
    let manifest = load_job(&job_dir)?;
    let auth = provider_auth(&args.auth);
    let options = download_options(
        args.threads,
        args.proxies,
        args.force_proxy,
        args.json_events,
    );
    providers::download_job(manifest, &auth, &options).await?;

    Ok(())
}

async fn legacy_download(cli: Cli) -> anyhow::Result<()> {
    let url = cli
        .url
        .ok_or_else(|| anyhow::anyhow!("informe --url ou use um subcomando"))?;
    validate_threads(cli.threads)?;
    tracing::info!("Inicializando Motor Aditi");
    let info = api::parser::parse_and_fetch_node(&url).await?;
    tracing::info!(
        "Arquivo '{}' ({} bytes) extraido da API!",
        info.name,
        info.size
    );

    let proxy_manager = proxy_manager(cli.proxies.as_deref()).await?;
    let out_path = legacy_output_path(cli.out, &info.name)?;
    api::downloader::download_file_concurrently(
        info,
        &out_path,
        cli.threads,
        proxy_manager,
        cli.force_proxy,
    )
    .await
}

pub(crate) async fn proxy_manager(
    proxies: Option<&str>,
) -> anyhow::Result<Arc<proxy::SmartProxyManager>> {
    let manager = proxy::SmartProxyManager::new(300);
    if let Some(path) = proxies {
        manager.load_from_file(path).await?;
    }
    Ok(manager)
}

fn provider_auth(args: &AuthArgs) -> ProviderAuth {
    ProviderAuth::from_env(
        args.webdav_user.clone(),
        args.webdav_pass.clone(),
        args.webdav_bearer_token.clone(),
        args.google_drive_token.clone(),
        args.google_photos_token.clone(),
        args.onedrive_token.clone(),
        args.dropbox_token.clone(),
    )
}

fn download_options(
    threads: usize,
    proxies: Option<String>,
    force_proxy: bool,
    json_events: bool,
) -> DownloadOptions {
    DownloadOptions {
        threads,
        proxies,
        force_proxy,
        json_events,
    }
}

fn validate_threads(threads: usize) -> anyhow::Result<()> {
    if threads == 0 {
        anyhow::bail!("threads deve ser maior que zero");
    }
    Ok(())
}

fn legacy_output_path(out: Option<String>, name: &str) -> anyhow::Result<String> {
    match out {
        Some(out) => Ok(out),
        None => Ok(std::env::current_dir()?
            .join(safe_file_name(name))
            .to_string_lossy()
            .to_string()),
    }
}

fn file_job_manifest(
    job_id: &str,
    url: &str,
    out_dir: &Path,
    job_dir: &Path,
    target: &Path,
    source: &SourceManifest,
) -> JobManifest {
    JobManifest {
        version: 1,
        job_id: job_id.to_string(),
        provider: source.provider.clone(),
        url: url.to_string(),
        kind: SourceKind::File,
        state: JobState::Running,
        out_dir: out_dir.to_string_lossy().to_string(),
        job_dir: job_dir.to_string_lossy().to_string(),
        target_path: target.to_string_lossy().to_string(),
        total_files: source.total_files,
        total_bytes: source.total_bytes,
        chunk_size: crate::transfer::chunks::DEFAULT_CHUNK_SIZE,
        chunks: crate::transfer::chunks::plan_chunks(
            source.total_bytes,
            crate::transfer::chunks::DEFAULT_CHUNK_SIZE,
        ),
        files: source.files.clone(),
        error: None,
    }
}

fn folder_job_manifest(
    job_id: &str,
    url: &str,
    out_dir: &Path,
    job_dir: &Path,
    source: &SourceManifest,
) -> JobManifest {
    JobManifest {
        version: 1,
        job_id: job_id.to_string(),
        provider: source.provider.clone(),
        url: url.to_string(),
        kind: SourceKind::Folder,
        state: JobState::Running,
        out_dir: out_dir.to_string_lossy().to_string(),
        job_dir: job_dir.to_string_lossy().to_string(),
        target_path: job_dir.to_string_lossy().to_string(),
        total_files: source.total_files,
        total_bytes: source.total_bytes,
        chunk_size: crate::transfer::chunks::DEFAULT_CHUNK_SIZE,
        chunks: Vec::new(),
        files: source.files.clone(),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_threads_before_a_download() {
        assert!(validate_threads(0).is_err());
        assert!(validate_threads(1).is_ok());
    }

    #[test]
    fn legacy_output_defaults_to_the_working_directory() {
        let cwd = std::env::current_dir().unwrap();

        // Sem `--out`, o alvo e o nome da origem dentro do diretorio atual.
        let path = legacy_output_path(None, "relatorio.zip").unwrap();
        assert_eq!(Path::new(&path), cwd.join("relatorio.zip"));

        // E um nome reservado pelo Windows nunca chega cru ao disco.
        let reservado = legacy_output_path(None, "CON").unwrap();
        let nome = Path::new(&reservado)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(Path::new(&reservado).starts_with(&cwd));
        assert_ne!(nome, "CON", "nome reservado passou cru");
        assert!(nome.starts_with("_CON"), "nome inesperado: {nome}");

        assert_eq!(
            legacy_output_path(Some("custom.bin".to_string()), "x").unwrap(),
            "custom.bin"
        );
    }
}
