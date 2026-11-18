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
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures::StreamExt;
use reqwest::header::RANGE;
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug)]
pub struct OneDriveProvider;

#[derive(Debug, Deserialize)]
struct GraphDriveItem {
    id: String,
    name: String,
    size: Option<u64>,
    folder: Option<Value>,
    file: Option<Value>,
    package: Option<Value>,
    #[serde(rename = "parentReference")]
    parent_reference: Option<GraphParentReference>,
}

#[derive(Debug, Deserialize)]
struct GraphParentReference {
    #[serde(rename = "driveId")]
    drive_id: String,
}

#[derive(Debug, Deserialize)]
struct GraphChildren {
    value: Vec<GraphDriveItem>,
    #[serde(rename = "@odata.nextLink")]
    next_link: Option<String>,
}

impl CloudProvider for OneDriveProvider {
    fn name(&self) -> &'static str {
        "onedrive"
    }

    fn can_handle(&self, url: &str) -> bool {
        Url::parse(url)
            .map(|url| is_onedrive_host(&url))
            .unwrap_or(false)
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        true
    }

    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        parse_onedrive_url(url)?;
        let token = onedrive_token(auth)?;
        let client = graph_client()?;
        let root = fetch_shared_item(&client, token, url).await?;
        let drive_id = root
            .parent_reference
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Microsoft Graph nao retornou parentReference"))?
            .drive_id
            .clone();

        if root.folder.is_some() {
            let files = list_folder_recursive(&client, token, &drive_id, &root.id).await?;
            let total_bytes = files.iter().map(|file| file.size).sum();
            return Ok(SourceManifest {
                provider: "onedrive".to_string(),
                url: url.to_string(),
                kind: SourceKind::Folder,
                name: root.name,
                total_files: files.len() as u64,
                total_bytes,
                files,
            });
        }

        let record = file_record(&drive_id, &root, &root.name)?;
        Ok(SourceManifest {
            provider: "onedrive".to_string(),
            url: url.to_string(),
            kind: SourceKind::File,
            name: record.name.clone(),
            total_files: 1,
            total_bytes: record.size,
            files: vec![record],
        })
    }

    async fn download_job(
        &self,
        mut manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        let token = onedrive_token(auth)?;
        let client = graph_client()?;
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
                .ok_or_else(|| anyhow::anyhow!("manifesto OneDrive sem URL de download"))?;
            validate_graph_content_url(&url)?;
            let target = onedrive_target_path(&manifest, &file);

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

async fn fetch_shared_item(
    client: &Client,
    token: &str,
    url: &str,
) -> anyhow::Result<GraphDriveItem> {
    let response = with_bearer(client.get(shared_item_url(url)?), token)
        .header("Prefer", "redeemSharingLinkIfNecessary")
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(anyhow::anyhow!(graph_failure(
            "operation",
            status,
            &response.text().await?
        )));
    }
    Ok(response.json().await?)
}

async fn list_folder_recursive(
    client: &Client,
    token: &str,
    drive_id: &str,
    root_id: &str,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    let mut stack = vec![(root_id.to_string(), String::new())];

    while let Some((folder_id, prefix)) = stack.pop() {
        for item in fetch_children(client, token, drive_id, &folder_id).await? {
            let path = join_path(&prefix, &item.name);
            if item.folder.is_some() {
                stack.push((item.id, path));
            } else {
                files.push(file_record(drive_id, &item, &path)?);
            }
        }
    }

    Ok(files)
}

async fn fetch_children(
    client: &Client,
    token: &str,
    drive_id: &str,
    item_id: &str,
) -> anyhow::Result<Vec<GraphDriveItem>> {
    let mut items = Vec::new();
    let mut next = Some(children_url(drive_id, item_id)?);

    while let Some(url) = next {
        let response = with_bearer(client.get(url), token).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!(graph_failure(
                "operation",
                status,
                &response.text().await?
            )));
        }
        let page: GraphChildren = response.json().await?;
        items.extend(page.value);
        next = page.next_link.map(|url| Url::parse(&url)).transpose()?;
    }

    Ok(items)
}

fn graph_failure(operation: &str, status: StatusCode, body: &str) -> String {
    let detail = serde_json::from_str::<Value>(body).ok().and_then(|body| {
        body.get("error")?
            .get("message")?
            .as_str()
            .map(str::to_owned)
    });
    match detail {
        Some(detail) => format!("Microsoft Graph {operation} falhou: {status}: {detail}"),
        None => format!("Microsoft Graph {operation} falhou: {status}"),
    }
}

// ponytail: mesma assinatura pequena dos providers HTTP; struct so empurraria campos.
#[allow(clippy::too_many_arguments)]
async fn download_file(
    client: &Client,
    url: &str,
    token: &str,
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
            reason: "OneDrive respondeu Range invalido; reiniciando de zero".to_string(),
        });
        return download_from_zero(
            client,
            url,
            token,
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
    url: &str,
    token: &str,
    target: &Path,
    expected_size: u64,
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
        return Err(anyhow::anyhow!(
            "GET OneDrive falhou: {}",
            response.status()
        ));
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

fn parse_onedrive_url(url: &str) -> anyhow::Result<Url> {
    let url = Url::parse(url)?;
    if is_onedrive_host(&url)
        && url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
    {
        Ok(url)
    } else {
        Err(anyhow::anyhow!("URL OneDrive/SharePoint nao suportada"))
    }
}

fn is_onedrive_host(url: &Url) -> bool {
    let host = url.host_str().unwrap_or("").trim_start_matches("www.");
    host == "1drv.ms"
        || host.ends_with(".1drv.ms")
        || host == "onedrive.live.com"
        || host.ends_with(".sharepoint.com")
}

fn shared_item_url(url: &str) -> anyhow::Result<Url> {
    let encoded = URL_SAFE_NO_PAD.encode(parse_onedrive_url(url)?.as_str());
    Ok(Url::parse(&format!(
        "https://graph.microsoft.com/v1.0/shares/u!{encoded}/driveItem"
    ))?)
}

fn children_url(drive_id: &str, item_id: &str) -> anyhow::Result<Url> {
    Ok(Url::parse(&format!(
        "https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    ))?)
}

fn content_url(drive_id: &str, item_id: &str) -> anyhow::Result<String> {
    Ok(format!(
        "https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    ))
}

fn validate_graph_content_url(url: &str) -> anyhow::Result<()> {
    let url = Url::parse(url)?;
    let valid_path = matches!(
        url.path_segments()
            .map(|mut segments| (
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
            )),
        Some((
            Some("v1.0"),
            Some("drives"),
            Some(drive_id),
            Some("items"),
            Some(item_id),
            Some("content"),
            None,
        )) if !drive_id.is_empty() && !item_id.is_empty()
    );
    if url.scheme() == "https"
        && url.host_str() == Some("graph.microsoft.com")
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && valid_path
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "manifesto OneDrive contem URL de download invalida"
        ))
    }
}

fn file_record(drive_id: &str, item: &GraphDriveItem, path: &str) -> anyhow::Result<FileRecord> {
    if item.package.is_some() || item.file.is_none() {
        return Err(anyhow::anyhow!(
            "Item OneDrive '{}' nao e arquivo baixavel",
            item.name
        ));
    }
    let size = item.size.ok_or_else(|| {
        anyhow::anyhow!("Microsoft Graph nao retornou tamanho para '{}'", item.name)
    })?;
    Ok(FileRecord {
        path: path.to_string(),
        name: item.name.clone(),
        size,
        handle: Some(content_url(drive_id, &item.id)?),
        done: false,
        error: None,
        integrity: None,
    })
}

fn onedrive_target_path(manifest: &JobManifest, file: &FileRecord) -> PathBuf {
    match manifest.kind {
        SourceKind::File => PathBuf::from(&manifest.target_path),
        SourceKind::Folder => Path::new(&manifest.job_dir).join(safe_relative_path(&file.path)),
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn onedrive_token(auth: &ProviderAuth) -> anyhow::Result<&str> {
    auth.onedrive_token
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!(""))
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

fn graph_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_onedrive_and_sharepoint_links() {
        assert!(parse_onedrive_url("https://1drv.ms/u/s!abc").is_ok());
        assert!(parse_onedrive_url("https://onedrive.live.com/?id=abc").is_ok());
        assert!(parse_onedrive_url("https://contoso.sharepoint.com/:f:/s/site/abc").is_ok());
        assert!(parse_onedrive_url("http://1drv.ms/u/s!abc").is_err());
        assert!(parse_onedrive_url("https://user:pass@1drv.ms/u/s!abc").is_err());
        assert!(parse_onedrive_url("https://example.com/file").is_err());
        assert!(OneDriveProvider.can_handle("http://1drv.ms/u/s!abc"));
    }

    #[test]
    fn encodes_shared_links_for_graph() {
        let url = shared_item_url("https://1drv.ms/u/s!abc")
            .unwrap()
            .to_string();
        assert!(url.starts_with("https://graph.microsoft.com/v1.0/shares/u!"));
        assert!(url.ends_with("/driveItem"));
    }

    #[test]
    fn reports_the_graph_error_message() {
        assert_eq!(
            graph_failure(
                "shared item",
                StatusCode::FORBIDDEN,
                r#"{"error":{"message":"Access denied"}}"#,
            ),
            "Microsoft Graph shared item falhou: 403 Forbidden: Access denied"
        );
    }

    #[test]
    fn maps_graph_file_record() {
        let item = GraphDriveItem {
            id: "item123".to_string(),
            name: "a.txt".to_string(),
            size: Some(42),
            folder: None,
            file: Some(serde_json::json!({})),
            package: None,
            parent_reference: None,
        };
        let record = file_record("drive123", &item, &join_path("dir", &item.name)).unwrap();
        assert_eq!(record.path, "dir/a.txt");
        assert_eq!(record.size, 42);
        assert!(record.handle.unwrap().contains("/content"));
    }

    #[test]
    fn rejects_non_graph_manifest_handles() {
        assert!(validate_graph_content_url(
            "https://graph.microsoft.com/v1.0/drives/drive/items/item/content"
        )
        .is_ok());
        assert!(validate_graph_content_url("http://127.0.0.1:8080/content").is_err());
        assert!(validate_graph_content_url(
            "https://graph.microsoft.com/v1.0/drives/drive/items/item/content?select=name"
        )
        .is_err());
    }
}
