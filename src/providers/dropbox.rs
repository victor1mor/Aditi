use crate::core::events::{Event, EventSink};
use crate::core::integrity::Integrity;
use crate::core::job::safe_relative_path;
use crate::core::manifest::{
    save_job, FileRecord, JobManifest, JobState, SourceKind, SourceManifest,
};
use crate::providers::{
    existing_target_is_complete, resume_response_matches, CloudProvider, DownloadOptions,
    ProviderAuth,
};
use anyhow::Context;
use futures::StreamExt;
use reqwest::header::RANGE;
use reqwest::{Client, RequestBuilder, Response, Url};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const METADATA_URL: &str = "https://api.dropboxapi.com/2/sharing/get_shared_link_metadata";
const LIST_URL: &str = "https://api.dropboxapi.com/2/files/list_folder";
const LIST_CONTINUE_URL: &str = "https://api.dropboxapi.com/2/files/list_folder/continue";
const DOWNLOAD_URL: &str = "https://content.dropboxapi.com/2/sharing/get_shared_link_file";

#[derive(Clone, Copy, Debug)]
pub struct DropboxProvider;

#[derive(Debug, Deserialize)]
struct DropboxMetadata {
    #[serde(rename = ".tag")]
    tag: String,
    name: String,
    size: Option<u64>,
    #[serde(rename = "path_display")]
    path_display: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DropboxListResponse {
    entries: Vec<DropboxMetadata>,
    cursor: String,
    #[serde(rename = "has_more")]
    has_more: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDownloadArg {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

impl CloudProvider for DropboxProvider {
    fn name(&self) -> &'static str {
        "dropbox"
    }

    fn can_handle(&self, url: &str) -> bool {
        Url::parse(url)
            .map(|url| is_dropbox_host(&url))
            .unwrap_or(false)
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        true
    }

    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        parse_dropbox_url(url)?;
        let token = dropbox_token(auth)?;
        let client = dropbox_client()?;
        let metadata = shared_metadata(&client, token, url, None).await?;

        match metadata.tag.as_str() {
            "file" => {
                let record = file_record(url, &metadata, None)?;
                Ok(SourceManifest {
                    provider: "dropbox".to_string(),
                    url: url.to_string(),
                    kind: SourceKind::File,
                    name: record.name.clone(),
                    total_files: 1,
                    total_bytes: record.size,
                    files: vec![record],
                })
            }
            "folder" => {
                let files = list_shared_folder(&client, token, url).await?;
                let total_bytes = files.iter().map(|file| file.size).sum();
                Ok(SourceManifest {
                    provider: "dropbox".to_string(),
                    url: url.to_string(),
                    kind: SourceKind::Folder,
                    name: metadata.name,
                    total_files: files.len() as u64,
                    total_bytes,
                    files,
                })
            }
            other => Err(anyhow::anyhow!(
                "Dropbox retornou tipo nao suportado: {other}"
            )),
        }
    }

    async fn download_job(
        &self,
        mut manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        let token = dropbox_token(auth)?;
        let client = dropbox_client()?;
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
            let arg: DropboxDownloadArg = serde_json::from_str(
                file.handle
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("manifesto Dropbox sem handle"))?,
            )?;
            let target = dropbox_target_path(&manifest, &file);

            if let Err(e) = download_file(
                &client,
                token,
                &arg,
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

async fn shared_metadata(
    client: &Client,
    token: &str,
    url: &str,
    path: Option<&str>,
) -> anyhow::Result<DropboxMetadata> {
    let body = match path {
        Some(path) => serde_json::json!({ "url": url, "path": path }),
        None => serde_json::json!({ "url": url }),
    };
    let response = with_bearer(client.post(METADATA_URL), token)
        .json(&body)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Dropbox metadata falhou: {}",
            response.status()
        ));
    }
    Ok(response.json().await?)
}

async fn list_shared_folder(
    client: &Client,
    token: &str,
    url: &str,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    let mut response = with_bearer(client.post(LIST_URL), token)
        .json(&serde_json::json!({
            "path": "",
            "recursive": true,
            "include_deleted": false,
            "shared_link": { "url": url }
        }))
        .send()
        .await?;

    loop {
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Dropbox list_folder falhou: {}",
                response.status()
            ));
        }
        let page: DropboxListResponse = response.json().await?;
        for entry in page.entries {
            if entry.tag == "file" {
                let path = dropbox_relative_path(&entry);
                let api_path = format!("/{}", path.replace('\\', "/"));
                files.push(file_record(url, &entry, Some(api_path))?);
            }
        }
        if !page.has_more {
            break;
        }
        response = with_bearer(client.post(LIST_CONTINUE_URL), token)
            .json(&serde_json::json!({ "cursor": page.cursor }))
            .send()
            .await?;
    }

    Ok(files)
}

fn file_record(
    url: &str,
    metadata: &DropboxMetadata,
    api_path: Option<String>,
) -> anyhow::Result<FileRecord> {
    let size = metadata
        .size
        .ok_or_else(|| anyhow::anyhow!("Dropbox nao retornou tamanho para '{}'", metadata.name))?;
    let path = api_path
        .as_deref()
        .map(|path| path.trim_start_matches('/').to_string())
        .unwrap_or_else(|| metadata.name.clone());
    let handle = DropboxDownloadArg {
        url: url.to_string(),
        path: api_path,
    };
    Ok(FileRecord {
        path,
        name: metadata.name.clone(),
        size,
        handle: Some(serde_json::to_string(&handle)?),
        done: false,
        error: None,
        integrity: None,
    })
}

// ponytail: Dropbox content endpoint is POST+header, so this tiny worker stays provider-local.
#[allow(clippy::too_many_arguments)]
async fn download_file(
    client: &Client,
    token: &str,
    arg: &DropboxDownloadArg,
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

    let mut request = download_request(client, token, arg)?;
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
            reason: "Dropbox respondeu Range invalido; reiniciando de zero".to_string(),
        });
        return download_from_zero(
            client,
            token,
            arg,
            target,
            expected_size,
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
    token: &str,
    arg: &DropboxDownloadArg,
    target: &Path,
    expected_size: u64,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
) -> anyhow::Result<()> {
    let response = download_request(client, token, arg)?.send().await?;
    write_response(
        &part_path(target),
        target,
        0,
        expected_size,
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
    events: &EventSink,
    job_id: &str,
    event_path: &str,
    response: Response,
) -> anyhow::Result<()> {
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("GET Dropbox falhou: {}", response.status()));
    }
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
    if target.exists() {
        std::fs::remove_file(target)?;
    }
    std::fs::rename(part, target)?;
    Ok(())
}

fn download_request(
    client: &Client,
    token: &str,
    arg: &DropboxDownloadArg,
) -> anyhow::Result<RequestBuilder> {
    Ok(with_bearer(client.post(DOWNLOAD_URL), token)
        .header("Dropbox-API-Arg", serde_json::to_string(arg)?))
}

fn parse_dropbox_url(url: &str) -> anyhow::Result<Url> {
    let url = Url::parse(url)?;
    if is_dropbox_host(&url)
        && url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
    {
        Ok(url)
    } else {
        Err(anyhow::anyhow!("URL Dropbox nao suportada"))
    }
}

fn is_dropbox_host(url: &Url) -> bool {
    url.host_str().unwrap_or("").trim_start_matches("www.") == "dropbox.com"
}

fn dropbox_relative_path(metadata: &DropboxMetadata) -> String {
    metadata
        .path_display
        .as_deref()
        .unwrap_or(&metadata.name)
        .trim_start_matches('/')
        .to_string()
}

fn dropbox_target_path(manifest: &JobManifest, file: &FileRecord) -> PathBuf {
    match manifest.kind {
        SourceKind::File => PathBuf::from(&manifest.target_path),
        SourceKind::Folder => Path::new(&manifest.job_dir).join(safe_relative_path(&file.path)),
    }
}

fn dropbox_token(auth: &ProviderAuth) -> anyhow::Result<&str> {
    auth.dropbox_token
        .as_deref()
        .context("Dropbox precisa de --dropbox-token ou ADITI_DROPBOX_TOKEN")
}

fn with_bearer(builder: RequestBuilder, token: &str) -> RequestBuilder {
    builder.bearer_auth(token)
}

fn part_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin");
    target.with_file_name(format!("{file_name}.aditi.part"))
}

fn dropbox_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dropbox_links() {
        assert!(parse_dropbox_url("https://www.dropbox.com/s/abc/file.txt?dl=0").is_ok());
        assert!(parse_dropbox_url("https://dropbox.com/scl/fi/abc/file.txt").is_ok());
        assert!(parse_dropbox_url("http://dropbox.com/s/abc/file.txt").is_err());
        assert!(parse_dropbox_url("https://user:pass@dropbox.com/s/abc/file.txt").is_err());
        assert!(parse_dropbox_url("https://example.com/file.txt").is_err());
        assert!(DropboxProvider.can_handle("http://dropbox.com/s/abc/file.txt"));
    }

    #[test]
    fn builds_file_record_handle() {
        let metadata = DropboxMetadata {
            tag: "file".to_string(),
            name: "a.txt".to_string(),
            size: Some(42),
            path_display: Some("/dir/a.txt".to_string()),
        };
        let record = file_record(
            "https://www.dropbox.com/s/abc/a.txt?dl=0",
            &metadata,
            Some("/dir/a.txt".to_string()),
        )
        .unwrap();
        assert_eq!(record.path, "dir/a.txt");
        assert_eq!(record.size, 42);
        let arg: DropboxDownloadArg = serde_json::from_str(&record.handle.unwrap()).unwrap();
        assert_eq!(arg.path.as_deref(), Some("/dir/a.txt"));
    }
}
