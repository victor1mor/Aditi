use anyhow::Context;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use clap::{Args, ValueEnum};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};
#[cfg(target_os = "windows")]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(target_os = "windows")]
use std::process::Stdio;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{sleep, timeout},
};

type Result<T> = anyhow::Result<T>;

#[derive(Args, Debug)]
pub struct OAuthArgs {
    /// Provider a autenticar nativamente.
    #[arg(value_enum)]
    pub provider: OAuthProvider,
    /// Client ID do aplicativo registrado no provider. Sem segredo embutido.
    #[arg(long)]
    pub client_id: Option<String>,
    /// Client secret do OAuth Google Drive. Nao e salvo pelo Aditi.
    #[arg(long)]
    pub client_secret: Option<String>,
    /// Escopo OAuth; se omitido usa o minimo necessario para o provider.
    #[arg(long)]
    pub scope: Option<String>,
    /// Tenant Microsoft (somente OneDrive), por padrao common.
    #[arg(long, default_value = "common")]
    pub tenant: String,
    /// Porta loopback para Google Drive e Dropbox.
    #[arg(long, default_value_t = 53_682)]
    pub redirect_port: u16,
    /// Nao tenta abrir o navegador automaticamente.
    #[arg(long, default_value_t = false)]
    pub no_browser: bool,
    /// Arquivo temporario opcional para o access token; apague-o apos o uso.
    #[arg(long)]
    pub token_out: Option<PathBuf>,
    /// Renova um access token a partir de um refresh token, sem abrir o
    /// navegador. Se omitido, usa ADITI_<PROVIDER>_REFRESH_TOKEN.
    #[arg(long)]
    pub refresh_token: Option<String>,
    /// Arquivo onde gravar o refresh token recebido. Aditi nao o guarda em
    /// outro lugar: quem orquestra decide onde ele mora.
    #[arg(long)]
    pub refresh_token_out: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OAuthProvider {
    Onedrive,
    GoogleDrive,
    Dropbox,
}

impl OAuthProvider {
    fn client_id_env(self) -> &'static str {
        match self {
            Self::Onedrive => "ADITI_ONEDRIVE_CLIENT_ID",
            Self::GoogleDrive => "ADITI_GOOGLE_DRIVE_CLIENT_ID",
            Self::Dropbox => "ADITI_DROPBOX_CLIENT_ID",
        }
    }

    fn refresh_token_env(self) -> &'static str {
        match self {
            Self::Onedrive => "ADITI_ONEDRIVE_REFRESH_TOKEN",
            Self::GoogleDrive => "ADITI_GOOGLE_DRIVE_REFRESH_TOKEN",
            Self::Dropbox => "ADITI_DROPBOX_REFRESH_TOKEN",
        }
    }

    fn token_env(self) -> &'static str {
        match self {
            Self::Onedrive => "ADITI_ONEDRIVE_TOKEN",
            Self::GoogleDrive => "ADITI_GOOGLE_DRIVE_TOKEN",
            Self::Dropbox => "ADITI_DROPBOX_TOKEN",
        }
    }

    fn default_scope(self) -> &'static str {
        match self {
            Self::Onedrive => "Files.ReadWrite",
            Self::GoogleDrive => "https://www.googleapis.com/auth/drive.readonly",
            Self::Dropbox => "files.content.read sharing.read",
        }
    }

    fn authorize_endpoint(self) -> Option<&'static str> {
        match self {
            Self::GoogleDrive => Some("https://accounts.google.com/o/oauth2/v2/auth"),
            Self::Dropbox => Some("https://www.dropbox.com/oauth2/authorize"),
            Self::Onedrive => None,
        }
    }

    fn token_endpoint(self) -> &'static str {
        match self {
            Self::Onedrive => "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            Self::GoogleDrive => "https://oauth2.googleapis.com/token",
            Self::Dropbox => "https://api.dropboxapi.com/oauth2/token",
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Só vem quando o pedido de autorizacao usou acesso offline, e apenas na
    /// primeira concessao. Renovacoes nao repetem o refresh token.
    #[serde(default)]
    refresh_token: Option<String>,
    /// Segundos de validade do access token. O Google devolve 3599.
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default = "default_poll_interval")]
    interval: u64,
    expires_in: u64,
}

#[derive(Deserialize)]
struct OAuthError {
    error: Option<String>,
    error_description: Option<String>,
}

fn default_poll_interval() -> u64 {
    5
}

pub async fn authenticate(args: OAuthArgs) -> Result<()> {
    let client_id = args
        .client_id
        .clone()
        .or_else(|| std::env::var(args.provider.client_id_env()).ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "informe --client-id ou defina {}",
                args.provider.client_id_env()
            )
        })?;
    let client_secret = required_client_secret(
        args.provider,
        args.client_secret
            .clone()
            .or_else(|| std::env::var("ADITI_GOOGLE_DRIVE_CLIENT_SECRET").ok()),
    )?;
    // Um refresh token em maos dispensa o navegador por completo.
    let refresh = args
        .refresh_token
        .clone()
        .or_else(|| std::env::var(args.provider.refresh_token_env()).ok())
        .filter(|value| !value.trim().is_empty());

    let token = match (&refresh, args.provider) {
        (Some(refresh), _) => {
            refresh_token_flow(&args, &client_id, client_secret.as_deref(), refresh).await?
        }
        (None, OAuthProvider::Onedrive) => device_code_flow(&args, &client_id).await?,
        (None, OAuthProvider::GoogleDrive) | (None, OAuthProvider::Dropbox) => {
            authorization_code_flow(&args, &client_id, client_secret.as_deref()).await?
        }
    };

    if let Some(path) = &args.token_out {
        save_token(path, &token.access_token)?;
        println!("Token salvo em {}.", path.display());
    } else {
        print_session_token(args.provider, &token.access_token);
    }
    if let Some(segundos) = token.expires_in {
        println!("Access token vale {segundos} s.");
    }

    match (&token.refresh_token, &args.refresh_token_out) {
        (Some(valor), Some(path)) => {
            save_token(path, valor)?;
            println!("Refresh token salvo em {}.", path.display());
        }
        (Some(_), None) => println!(
            "Este provider emitiu refresh token. Use --refresh-token-out para              guarda-lo e depois --refresh-token para renovar sem navegador."
        ),
        (None, Some(_)) => println!(
            "AVISO: o provider nao emitiu refresh token; nada foi gravado em              --refresh-token-out."
        ),
        (None, None) => {}
    }
    Ok(())
}

async fn device_code_flow(args: &OAuthArgs, client_id: &str) -> Result<TokenResponse> {
    let client = reqwest::Client::new();
    let root = format!("https://login.microsoftonline.com/{}", args.tenant);
    let device: DeviceCodeResponse = client
        .post(format!("{root}/oauth2/v2.0/devicecode"))
        .form(&[
            ("client_id", client_id),
            (
                "scope",
                args.scope
                    .as_deref()
                    .unwrap_or(args.provider.default_scope()),
            ),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    println!(
        "Abra {} e informe o codigo {}.",
        device.verification_uri, device.user_code
    );
    if !args.no_browser {
        open_browser(&device.verification_uri);
        copy_to_clipboard(&device.user_code);
    }
    let mut interval = Duration::from_secs(device.interval.max(1));
    let attempts = (device.expires_in / interval.as_secs()).max(1);
    for _ in 0..attempts {
        sleep(interval).await;
        let response = client
            .post(format!("{root}/oauth2/v2.0/token"))
            .form(&[
                ("client_id", client_id),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", device.device_code.as_str()),
            ])
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(response.json().await?);
        }
        let (code, message) = oauth_error(&response.text().await?);
        match code.as_deref() {
            Some("authorization_pending") => continue,
            Some("slow_down") => interval += Duration::from_secs(5),
            _ => return Err(anyhow::anyhow!("OAuth OneDrive: {message}")),
        }
    }
    Err(anyhow::anyhow!(
        "OAuth OneDrive expirou antes da autorizacao"
    ))
}

/// Troca um refresh token por um access token novo. Nao abre navegador.
///
/// O provider nao reemite o refresh token nesta troca: quem chama continua
/// responsavel por guardar o que recebeu na concessao original.
async fn refresh_token_flow(
    args: &OAuthArgs,
    client_id: &str,
    client_secret: Option<&str>,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let provider = args.provider;
    let token_endpoint = provider.token_endpoint().replace("{tenant}", &args.tenant);
    let mut form = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
    ];
    if let Some(client_secret) = client_secret {
        form.push(("client_secret", client_secret));
    }
    let response = reqwest::Client::new()
        .post(token_endpoint)
        .form(&form)
        .send()
        .await?;
    if response.status().is_success() {
        let mut token: TokenResponse = response.json().await?;
        // A troca nao repete o refresh token; devolvemos o mesmo para que o
        // chamador possa reescrever seu arquivo sem perde-lo.
        if token.refresh_token.is_none() {
            token.refresh_token = Some(refresh_token.to_string());
        }
        Ok(token)
    } else {
        let (_, message) = oauth_error(&response.text().await?);
        Err(anyhow::anyhow!(
            "OAuth {} (refresh): {message}",
            provider_name(provider)
        ))
    }
}

async fn authorization_code_flow(
    args: &OAuthArgs,
    client_id: &str,
    client_secret: Option<&str>,
) -> Result<TokenResponse> {
    let provider = args.provider;
    let redirect_uri = format!("http://127.0.0.1:{}/callback", args.redirect_port);
    let listener = TcpListener::bind(("127.0.0.1", args.redirect_port)).await?;
    let verifier = random_url_safe(32);
    let state = random_url_safe(24);
    let mut authorize = Url::parse(provider.authorize_endpoint().expect("code flow provider"))?;
    {
        let mut query = authorize.query_pairs_mut();
        query.append_pair("response_type", "code");
        query.append_pair("client_id", client_id);
        query.append_pair("redirect_uri", &redirect_uri);
        query.append_pair(
            "scope",
            args.scope.as_deref().unwrap_or(provider.default_scope()),
        );
        query.append_pair("state", &state);
        query.append_pair("code_challenge", &pkce_challenge(&verifier));
        query.append_pair("code_challenge_method", "S256");
        // Sem isto o Google devolve apenas um access token de ~1 hora e nao ha
        // como renovar sem outra ida ao navegador. `prompt=consent` e o que
        // faz o refresh token ser reemitido quando o app ja foi autorizado
        // antes — sem ele, uma segunda autorizacao volta sem refresh token.
        if matches!(provider, OAuthProvider::GoogleDrive) {
            query.append_pair("access_type", "offline");
            query.append_pair("prompt", "consent");
        }
        if matches!(provider, OAuthProvider::Dropbox) {
            query.append_pair("token_access_type", "offline");
        }
    }
    println!("Autorize no navegador: {authorize}");
    if !args.no_browser {
        open_browser(authorize.as_str());
    }
    let code = wait_for_code(listener, &state).await?;
    let token_endpoint = provider.token_endpoint().replace("{tenant}", &args.tenant);
    let mut form = vec![
        ("code", code.as_str()),
        ("grant_type", "authorization_code"),
        ("client_id", client_id),
        ("redirect_uri", redirect_uri.as_str()),
        ("code_verifier", verifier.as_str()),
    ];
    if let Some(client_secret) = client_secret {
        form.push(("client_secret", client_secret));
    }
    let response = reqwest::Client::new()
        .post(token_endpoint)
        .form(&form)
        .send()
        .await?;
    if response.status().is_success() {
        Ok(response.json().await?)
    } else {
        let (_, message) = oauth_error(&response.text().await?);
        Err(anyhow::anyhow!(
            "OAuth {}: {message}",
            provider_name(provider)
        ))
    }
}

async fn wait_for_code(listener: TcpListener, expected_state: &str) -> Result<String> {
    let (mut stream, _) = timeout(Duration::from_secs(300), listener.accept()).await??;
    let mut request = vec![0; 8 * 1024];
    let read = stream.read(&mut request).await?;
    let request = String::from_utf8_lossy(&request[..read]);
    let target = request
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .ok_or_else(|| anyhow::anyhow!("callback OAuth invalido"))?;
    let url = Url::parse(&format!("http://localhost{target}"))?;
    let values: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    let result = match values.get("error") {
        Some(error) => Err(anyhow::anyhow!("autorizacao recusada: {error}")),
        None if values.get("state").map(String::as_str) != Some(expected_state) => {
            Err(anyhow::anyhow!("state OAuth invalido"))
        }
        None => values
            .get("code")
            .cloned()
            .context("callback OAuth sem code"),
    };
    let body = if result.is_ok() {
        "Autenticacao concluida. Pode fechar esta janela."
    } else {
        "Autenticacao falhou. Volte ao terminal."
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .as_bytes(),
        )
        .await?;
    result
}

fn oauth_error(body: &str) -> (Option<String>, String) {
    match serde_json::from_str::<OAuthError>(body) {
        Ok(error) => {
            let code = error.error.clone();
            let message = error
                .error_description
                .or(error.error)
                .unwrap_or_else(|| body.to_string());
            (code, message)
        }
        Err(_) => (None, body.to_string()),
    }
}

fn required_client_secret(
    provider: OAuthProvider,
    value: Option<String>,
) -> Result<Option<String>> {
    if matches!(provider, OAuthProvider::GoogleDrive) {
        return value
            .filter(|value| !value.trim().is_empty())
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Google Drive precisa de --client-secret ou ADITI_GOOGLE_DRIVE_CLIENT_SECRET"
                )
            });
    }
    Ok(None)
}

fn random_url_safe(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    rand::fill(&mut value[..]);
    URL_SAFE_NO_PAD.encode(value)
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn print_session_token(provider: OAuthProvider, token: &str) {
    println!(
        "Autenticado para {}. No PowerShell desta sessao:",
        provider_name(provider)
    );
    println!(
        "$env:{} = '{}'",
        provider.token_env(),
        token.replace('\'', "''")
    );
}

fn save_token(path: &Path, token: &str) -> Result<()> {
    // O arquivo carrega um access token: nasce legivel somente pelo dono.
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("nao foi possivel criar {}", path.display()))?;
        file.write_all(token.as_bytes())?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, token)?;
        Ok(())
    }
}

fn provider_name(provider: OAuthProvider) -> &'static str {
    match provider {
        OAuthProvider::Onedrive => "OneDrive",
        OAuthProvider::GoogleDrive => "Google Drive",
        OAuthProvider::Dropbox => "Dropbox",
    }
}

fn open_browser(url: &str) {
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("rundll32")
            .args(["url.dll,FileProtocolHandler", url])
            .spawn();
    }
    #[cfg(not(target_os = "windows"))]
    let _ = url;
}

#[cfg(target_os = "windows")]
fn copy_to_clipboard(text: &str) {
    if let Ok(mut child) = std::process::Command::new("clip")
        .stdin(Stdio::piped())
        .spawn()
    {
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(text.as_bytes());
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn copy_to_clipboard(_text: &str) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_uses_s256_url_safe_encoding() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_verifier_has_the_minimum_pkce_length() {
        assert!(random_url_safe(32).len() >= 43);
    }

    #[test]
    fn google_authorization_asks_for_offline_access() {
        // Sem access_type=offline o Google devolve so um access token de ~1h e
        // nao ha como renovar sem navegador. Sem prompt=consent, uma segunda
        // autorizacao do mesmo app volta SEM refresh token.
        let fonte = include_str!("auth.rs");
        let bloco = fonte
            .split("async fn authorization_code_flow")
            .nth(1)
            .expect("fluxo de codigo de autorizacao");
        let bloco = &bloco[..bloco.find("async fn wait_for_code").unwrap_or(bloco.len())];
        assert!(
            bloco.contains(r#"query.append_pair("access_type", "offline")"#),
            "o pedido perdeu access_type=offline"
        );
        assert!(
            bloco.contains(r#"query.append_pair("prompt", "consent")"#),
            "o pedido perdeu prompt=consent"
        );
        assert!(
            !bloco.contains(r#""token_access_type", "online""#),
            "Dropbox voltou a pedir acesso online"
        );
    }

    #[test]
    fn token_response_keeps_the_refresh_token_and_expiry() {
        let corpo = r#"{"access_token":"at-1","refresh_token":"rt-1","expires_in":3599}"#;
        let token: TokenResponse = serde_json::from_str(corpo).unwrap();
        assert_eq!(token.access_token, "at-1");
        assert_eq!(token.refresh_token.as_deref(), Some("rt-1"));
        assert_eq!(token.expires_in, Some(3599));
    }

    #[test]
    fn token_response_still_parses_a_response_without_refresh() {
        // Renovacao devolve apenas access_token; nao pode virar erro de parse.
        let token: TokenResponse = serde_json::from_str(r#"{"access_token":"at-2"}"#).unwrap();
        assert_eq!(token.access_token, "at-2");
        assert!(token.refresh_token.is_none());
        assert!(token.expires_in.is_none());
    }

    #[test]
    fn every_provider_names_a_refresh_token_env_var() {
        for provider in [
            OAuthProvider::GoogleDrive,
            OAuthProvider::Onedrive,
            OAuthProvider::Dropbox,
        ] {
            let nome = provider.refresh_token_env();
            assert!(nome.starts_with("ADITI_"), "{nome}");
            assert!(nome.ends_with("_REFRESH_TOKEN"), "{nome}");
            assert_ne!(nome, provider.token_env(), "colidiu com a do access token");
        }
    }

    #[test]
    fn google_drive_requires_the_issued_client_secret() {
        assert!(required_client_secret(OAuthProvider::GoogleDrive, None).is_err());
        assert_eq!(
            required_client_secret(OAuthProvider::GoogleDrive, Some("secret".to_string())).unwrap(),
            Some("secret".to_string())
        );
        assert_eq!(
            required_client_secret(OAuthProvider::Dropbox, None).unwrap(),
            None
        );
    }

    #[test]
    fn keeps_the_oauth_error_code_for_device_polling() {
        assert_eq!(
            oauth_error(r#"{"error":"authorization_pending","error_description":"waiting"}"#),
            (
                Some("authorization_pending".to_string()),
                "waiting".to_string()
            )
        );
    }

    #[test]
    fn writes_a_token_to_an_explicit_path() {
        let path = std::env::temp_dir().join(format!(
            "aditi-token-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        save_token(&path, "token").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "token");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn onedrive_shared_links_request_the_supported_delegated_scope() {
        assert_eq!(OAuthProvider::Onedrive.default_scope(), "Files.ReadWrite");
    }
}
