use reqwest::Proxy;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ProxyItem {
    pub url: String,
    pub banned_until: Option<Instant>,
}

pub struct SmartProxyManager {
    proxies: RwLock<Vec<ProxyItem>>,
    current_index: AtomicUsize,
    ban_duration: Duration,
}

impl SmartProxyManager {
    pub fn new(ban_duration_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            proxies: RwLock::new(Vec::new()),
            current_index: AtomicUsize::new(0),
            ban_duration: Duration::from_secs(ban_duration_secs),
        })
    }

    /// Carrega proxies de um arquivo local (1 por linha)
    pub async fn load_from_file(&self, path: &str) -> anyhow::Result<()> {
        let content = tokio::fs::read_to_string(path).await?;
        let mut list = self.proxies.write().await;
        list.clear();
        for line in content.lines() {
            let line = line.trim();
            if !line.is_empty() && !line.starts_with('#') {
                // Adiciona o prefixo http:// se nao houver nenhum scheme (necessário pro reqwest)
                let url = if !line.starts_with("http") && !line.starts_with("socks") {
                    format!("http://{}", line)
                } else {
                    line.to_string()
                };
                list.push(ProxyItem {
                    url,
                    banned_until: None,
                });
            }
        }
        tracing::info!("Carregados {} proxies de {}", list.len(), path);
        Ok(())
    }

    /// Retorna um proxy disponível (Round-Robin) e limpa bans expirados
    pub async fn get_proxy(&self) -> Option<String> {
        let mut list = self.proxies.write().await;
        if list.is_empty() {
            return None;
        }

        let now = Instant::now();
        let total = list.len();

        let start_idx = self.current_index.fetch_add(1, Ordering::Relaxed) % total;
        let mut idx = start_idx;

        loop {
            let p = &mut list[idx];

            // Verifica se o ban já expirou
            if let Some(banned_until) = p.banned_until {
                if now >= banned_until {
                    p.banned_until = None; // Retira do ban
                }
            }

            // Se estiver ativo, retorna
            if p.banned_until.is_none() {
                return Some(p.url.clone());
            }

            idx = (idx + 1) % total;
            if idx == start_idx {
                // Deu a volta, todos estão banidos
                return None;
            }
        }
    }

    /// Bane um proxy temporariamente
    pub async fn ban_proxy(&self, url: &str) {
        let mut list = self.proxies.write().await;
        let now = Instant::now();
        for p in list.iter_mut() {
            if p.url == url {
                p.banned_until = Some(now + self.ban_duration);
                tracing::warn!(
                    "Proxy {} banido temporariamente ({}s)",
                    url,
                    self.ban_duration.as_secs()
                );
                break;
            }
        }
    }

    /// Constrói um cliente reqwest envelopado com o proxy selecionado (com timeout)
    pub fn build_client(proxy_url: Option<&str>) -> Result<reqwest::Client, reqwest::Error> {
        // ponytail: a timeout total derruba streams lentos ainda saudaveis; limite so conexao e ociosidade.
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .read_timeout(std::time::Duration::from_secs(30));

        if let Some(url) = proxy_url {
            builder = builder.proxy(Proxy::all(url)?);
        }
        builder.build()
    }
}
