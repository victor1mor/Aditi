use crate::api::parser::FileInfo;
use crate::core::events::{Event, EventSink};
use crate::core::manifest::{save_job, JobManifest, JobState, SourceKind};
use crate::providers::mega_mac::{self, MacCheck};
use crate::proxy::SmartProxyManager;
use crate::transfer::chunks::{plan_chunks, DEFAULT_CHUNK_SIZE};
use futures::stream::{self, StreamExt};
use megalib::crypto::aes::aes128_ctr_decrypt;
use reqwest::header::RANGE;
use reqwest::StatusCode;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

pub async fn download_file_concurrently(
    info: FileInfo,
    out_path: &str,
    num_threads: usize,
    proxy_manager: Arc<SmartProxyManager>,
    force_proxy: bool,
) -> anyhow::Result<()> {
    let target = Path::new(out_path);
    let chunks = plan_chunks(info.size, DEFAULT_CHUNK_SIZE);
    let manifest = JobManifest {
        version: 1,
        job_id: "legacy".to_string(),
        provider: "mega".to_string(),
        url: info.public_url.clone().unwrap_or_default(),
        kind: crate::core::manifest::SourceKind::File,
        state: JobState::Running,
        out_dir: target
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_string_lossy()
            .to_string(),
        job_dir: String::new(),
        target_path: out_path.to_string(),
        total_files: 1,
        total_bytes: info.size,
        chunk_size: DEFAULT_CHUNK_SIZE,
        chunks,
        files: Vec::new(),
        error: None,
    };

    download_file_for_job(
        info,
        target,
        num_threads,
        proxy_manager,
        force_proxy,
        Arc::new(Mutex::new(manifest)),
        false,
        EventSink::new(false),
    )
    .await
}

// ponytail: mantem a assinatura explicita porque esta funcao e a ponte entre a CLI antiga,
// o provider MEGA novo e os testes de resume; agrupar agora so espalharia boilerplate.
#[allow(clippy::too_many_arguments)]
pub async fn download_file_for_job(
    info: FileInfo,
    out_path: &Path,
    num_threads: usize,
    proxy_manager: Arc<SmartProxyManager>,
    force_proxy: bool,
    manifest: Arc<Mutex<JobManifest>>,
    persist_manifest: bool,
    events: EventSink,
) -> anyhow::Result<()> {
    tracing::info!("Iniciando download concorrente para: {}", info.name);
    if num_threads == 0 {
        return Err(anyhow::anyhow!("threads deve ser maior que zero"));
    }

    let file_size = info.size;
    let existing_len = tokio::fs::metadata(out_path)
        .await
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let snapshot = {
        let mut job = manifest.lock().await;
        let mut changed = false;
        for chunk in &mut job.chunks {
            if chunk.done && existing_len <= chunk.end {
                chunk.done = false;
                changed = true;
            }
        }
        if changed && persist_manifest {
            save_job_or_warn(&job);
        }
        job.clone()
    };
    let bytes_done = snapshot
        .chunks
        .iter()
        .filter(|chunk| chunk.done)
        .map(|chunk| chunk.len())
        .sum();
    let chunks_to_download: Vec<_> = snapshot
        .chunks
        .iter()
        .filter(|chunk| !chunk.done)
        .cloned()
        .collect();

    tracing::info!(
        "Tamanho: {} bytes | Chunks: {} | Threads: {}",
        file_size,
        snapshot.chunks.len(),
        num_threads
    );
    events.emit(Event::Started {
        job_id: snapshot.job_id.clone(),
        provider: snapshot.provider.clone(),
        total_files: snapshot.total_files,
        total_bytes: snapshot.total_bytes,
    });
    if bytes_done > 0 {
        events.emit(Event::Progress {
            job_id: snapshot.job_id.clone(),
            path: snapshot.target_path.clone(),
            bytes_done,
            bytes_total: file_size,
        });
    }

    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(out_path)
        .await?;
    // Cresce o arquivo sem apagar bytes que ja chegaram.
    file.set_len(file_size).await?;
    drop(file);

    let mut aes_key = [0u8; 16];
    let mut nonce = [0u8; 8];
    for (i, key_byte) in aes_key.iter_mut().enumerate() {
        *key_byte = info.key[i] ^ info.key[i + 16];
    }
    nonce.copy_from_slice(&info.key[16..24]);

    let out_path = Arc::new(out_path.to_string_lossy().to_string());
    let download_url = Arc::new(info.download_url.clone());

    let mut stream = stream::iter(chunks_to_download)
        .map(|chunk| {
            let download_url = Arc::clone(&download_url);
            let out_path = Arc::clone(&out_path);
            let aes_key_clone = aes_key;
            let nonce_clone = nonce;
            let proxy_manager = Arc::clone(&proxy_manager);
            let manifest = Arc::clone(&manifest);
            let events = events.clone();

            tokio::spawn(async move {
                download_and_save_chunk(
                    &download_url,
                    &out_path,
                    chunk.index,
                    chunk.start,
                    chunk.end,
                    aes_key_clone,
                    nonce_clone,
                    proxy_manager,
                    force_proxy,
                    manifest,
                    persist_manifest,
                    events,
                )
                .await
            })
        })
        .buffer_unordered(num_threads);

    let mut has_errors = false;
    while let Some(result) = stream.next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("Erro persistente no chunk: {}", e);
                has_errors = true;
            }
            Err(e) => {
                tracing::error!("Thread falhou: {}", e);
                has_errors = true;
            }
        }
    }

    if has_errors {
        mark_job_error(
            &manifest,
            persist_manifest,
            "Download finalizado com erros em um ou mais chunks.",
        )
        .await;
        let snapshot = manifest.lock().await.clone();
        events.emit(Event::Error {
            job_id: snapshot.job_id,
            message: "Download finalizado com erros em um ou mais chunks.".to_string(),
        });
        return Err(anyhow::anyhow!(
            "Download finalizado com erros em um ou mais chunks."
        ));
    }

    let actual_size = tokio::fs::metadata(&*out_path).await?.len();
    if actual_size != file_size {
        let msg = format!("Tamanho final invalido: esperado {file_size}, obtido {actual_size}");
        mark_job_error(&manifest, persist_manifest, &msg).await;
        return Err(anyhow::anyhow!(msg));
    }

    // O MEGA nao publica checksum: a prova de integridade e o meta-MAC que a
    // propria chave do arquivo carrega. Sem esta conferencia, tamanho certo com
    // conteudo errado passava como sucesso.
    //
    // Em `spawn_blocking` porque a leitura e sincrona e o arquivo pode ter
    // varios GB — travar o runtime aqui pararia todo o resto.
    let mac_path = PathBuf::from(out_path.as_str());
    let mac_key = info.key;
    let mac =
        tokio::task::spawn_blocking(move || mega_mac::verify_file(&mac_path, &mac_key, file_size))
            .await?;
    match mac {
        Ok(MacCheck::Match) => {
            let job_id = manifest.lock().await.job_id.clone();
            events.emit(Event::Verified {
                job_id,
                path: (*out_path).clone(),
                algorithm: "mega-meta-mac".to_string(),
            });
        }
        Ok(MacCheck::Mismatch { expected, actual }) => {
            let msg = format!("meta-MAC do MEGA nao confere: esperado {expected}, obtido {actual}");
            mark_job_error(&manifest, persist_manifest, &msg).await;
            return Err(anyhow::anyhow!(msg));
        }
        Err(error) => {
            // Sem conseguir calcular, nao ha como afirmar que o arquivo esta
            // certo — e dar por concluido sem conferir e o defeito que esta
            // verificacao fecha.
            let msg = format!("nao foi possivel conferir o meta-MAC do MEGA: {error}");
            mark_job_error(&manifest, persist_manifest, &msg).await;
            return Err(anyhow::anyhow!(msg));
        }
    }

    {
        let mut job = manifest.lock().await;
        job.state = JobState::Finished;
        job.error = None;
        if job.kind == SourceKind::File {
            for file in &mut job.files {
                file.done = true;
                file.error = None;
            }
        }
        if persist_manifest {
            save_job_or_warn(&job);
        }
        events.emit(Event::FileFinished {
            job_id: job.job_id.clone(),
            path: job.target_path.clone(),
            bytes: file_size,
        });
        events.emit(Event::Finished {
            job_id: job.job_id.clone(),
            total_files: job.total_files,
            total_bytes: job.total_bytes,
        });
    }

    tracing::info!("Download de '{}' finalizado com sucesso!", info.name);
    Ok(())
}

// ponytail: private worker; a config struct would only move these arguments around.
#[allow(clippy::too_many_arguments)]
async fn download_and_save_chunk(
    url: &str,
    out_path: &str,
    index: u64,
    start: u64,
    end: u64,
    aes_key: [u8; 16],
    nonce: [u8; 8],
    proxy_manager: Arc<SmartProxyManager>,
    force_proxy: bool,
    manifest: Arc<Mutex<JobManifest>>,
    persist_manifest: bool,
    events: EventSink,
) -> anyhow::Result<()> {
    let range_header = format!("bytes={}-{}", start, end);
    let mut current_proxy = None;

    if force_proxy {
        current_proxy = proxy_manager.get_proxy().await;
        if current_proxy.is_none() {
            return Err(anyhow::anyhow!(
                "force_proxy ativo, mas nenhum proxy esta disponivel"
            ));
        }
    }

    let mut attempts = 0;
    loop {
        attempts += 1;
        if attempts > 10 {
            return Err(anyhow::anyhow!(
                "Falha ao baixar o chunk {} após 10 tentativas. Esgotamento de proxies.",
                index
            ));
        }
        if force_proxy && current_proxy.is_none() {
            return Err(anyhow::anyhow!(
                "force_proxy ativo, mas nenhum proxy esta disponivel"
            ));
        }

        let client = match SmartProxyManager::build_client(current_proxy.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                if force_proxy {
                    return Err(anyhow::anyhow!(
                        "Falha ao construir proxy (modo force_proxy ativo): {}",
                        e
                    ));
                }
                tracing::warn!(
                    "Falha ao construir client com proxy {:?}. Tentando sem proxy.",
                    current_proxy
                );
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .unwrap_or_default()
            }
        };

        match client.get(url).header(RANGE, &range_header).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.as_u16() == 509 {
                    // Bandwidth Limit Exceeded
                    tracing::warn!(
                        "[Chunk {}] Erro 509 (Bandwidth Limit)! Alternando Proxy.",
                        index
                    );
                    emit_retry(&events, &manifest, index, attempts, "MEGA HTTP 509").await;
                    if let Some(ref p) = current_proxy {
                        proxy_manager.ban_proxy(p).await;
                    }
                    // Adquire um novo proxy imediatamente
                    current_proxy = proxy_manager.get_proxy().await;
                    if current_proxy.is_none() {
                        if force_proxy {
                            return Err(anyhow::anyhow!(
                                "force_proxy ativo, mas todos os proxies estao banidos"
                            ));
                        }
                        tracing::error!("Nenhum proxy disponível após erro 509.");
                        // Aqui poderíamos pausar e esperar (sleep), mas para o motor, só tentamos na próxima.
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                    continue; // Tenta de novo
                } else if !status.is_success() {
                    tracing::warn!(
                        "[Chunk {}] Status HTTP {}. Tentando novamente...",
                        index,
                        status
                    );
                    emit_retry(
                        &events,
                        &manifest,
                        index,
                        attempts,
                        &format!("HTTP {status}"),
                    )
                    .await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }

                let encrypted_bytes = resp.bytes().await?;

                let expected_len = (end - start + 1) as usize;
                validate_chunk_response(status, start, expected_len, encrypted_bytes.len(), index)?;

                let decrypted_bytes = aes128_ctr_decrypt(&encrypted_bytes, &aes_key, &nonce, start);

                let mut file = OpenOptions::new().write(true).open(out_path).await?;
                file.seek(SeekFrom::Start(start)).await?;
                file.write_all(&decrypted_bytes).await?;
                mark_chunk_done(&manifest, persist_manifest, &events, index).await?;

                tracing::debug!("Chunk {} salvo ({} bytes).", index, decrypted_bytes.len());
                return Ok(());
            }
            Err(e) => {
                tracing::warn!("[Chunk {}] Erro de rede: {}. Alternando proxy.", index, e);
                emit_retry(&events, &manifest, index, attempts, &e.to_string()).await;
                if let Some(ref p) = current_proxy {
                    proxy_manager.ban_proxy(p).await;
                }
                current_proxy = proxy_manager.get_proxy().await;
                if force_proxy && current_proxy.is_none() {
                    return Err(anyhow::anyhow!(
                        "force_proxy ativo, mas nenhum proxy esta disponivel apos erro de rede"
                    ));
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        }
    }
}

async fn emit_retry(
    events: &EventSink,
    manifest: &Arc<Mutex<JobManifest>>,
    chunk: u64,
    attempt: u32,
    reason: &str,
) {
    let job = manifest.lock().await;
    events.emit(Event::Retry {
        job_id: job.job_id.clone(),
        path: job.target_path.clone(),
        chunk,
        attempt,
        reason: reason.to_string(),
    });
}

async fn mark_chunk_done(
    manifest: &Arc<Mutex<JobManifest>>,
    persist_manifest: bool,
    events: &EventSink,
    index: u64,
) -> anyhow::Result<()> {
    let mut job = manifest.lock().await;
    if let Some(chunk) = job.chunks.iter_mut().find(|chunk| chunk.index == index) {
        chunk.done = true;
    }
    let bytes_done = job
        .chunks
        .iter()
        .filter(|chunk| chunk.done)
        .map(|chunk| chunk.len())
        .sum();
    if persist_manifest {
        save_job_or_io(&job)?;
    }
    events.emit(Event::Progress {
        job_id: job.job_id.clone(),
        path: job.target_path.clone(),
        bytes_done,
        bytes_total: job.total_bytes,
    });
    Ok(())
}

async fn mark_job_error(manifest: &Arc<Mutex<JobManifest>>, persist_manifest: bool, message: &str) {
    let mut job = manifest.lock().await;
    job.state = JobState::Error;
    job.error = Some(message.to_string());
    if persist_manifest {
        save_job_or_warn(&job);
    }
}

fn save_job_or_io(job: &JobManifest) -> anyhow::Result<()> {
    save_job(job)
        .map_err(|e| std::io::Error::other(format!("Falha ao salvar manifesto: {e}")).into())
}

fn save_job_or_warn(job: &JobManifest) {
    if let Err(e) = save_job(job) {
        tracing::warn!("Falha ao salvar manifesto: {}", e);
    }
}

fn validate_chunk_response(
    status: StatusCode,
    start: u64,
    expected_len: usize,
    actual_len: usize,
    index: u64,
) -> anyhow::Result<()> {
    match status {
        StatusCode::PARTIAL_CONTENT if actual_len == expected_len => Ok(()),
        StatusCode::PARTIAL_CONTENT => Err(anyhow::anyhow!(
            "Tamanho do chunk {} incorreto (Esperado {}, Recebido {})",
            index,
            expected_len,
            actual_len
        )),
        StatusCode::OK if start == 0 && actual_len == expected_len => Ok(()),
        StatusCode::OK => Err(anyhow::anyhow!(
            "Servidor ignorou Range no chunk {}.",
            index
        )),
        _ => Err(anyhow::anyhow!(
            "Status HTTP inesperado no chunk {}: {}",
            index,
            status
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_partial_chunk_size() {
        assert!(validate_chunk_response(StatusCode::PARTIAL_CONTENT, 10, 5, 5, 1).is_ok());
        assert!(validate_chunk_response(StatusCode::PARTIAL_CONTENT, 10, 5, 6, 1).is_err());
    }

    #[test]
    fn rejects_ok_when_range_was_ignored() {
        assert!(validate_chunk_response(StatusCode::OK, 0, 5, 5, 0).is_ok());
        assert!(validate_chunk_response(StatusCode::OK, 0, 5, 6, 0).is_err());
        assert!(validate_chunk_response(StatusCode::OK, 10, 5, 5, 1).is_err());
    }
}
