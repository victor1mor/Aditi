use crate::core::events::{Event, EventSink};
use crate::core::integrity::Integrity;
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
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use reqwest::{Client, RequestBuilder, Response, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

const MEDIA_URL: &str = "https://photoslibrary.googleapis.com/v1/mediaItems";
const SEARCH_URL: &str = "https://photoslibrary.googleapis.com/v1/mediaItems:search";

#[derive(Clone, Copy, Debug)]
pub struct GooglePhotosProvider;

#[derive(Clone, Debug, PartialEq, Eq)]
enum GooglePhotosLink {
    Album { id: String },
    Media { id: String },
}

#[derive(Debug, Deserialize)]
struct GooglePhotosMedia {
    id: String,
    #[serde(rename = "baseUrl")]
    base_url: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    filename: String,
}

#[derive(Debug, Deserialize)]
struct GooglePhotosSearchResponse {
    #[serde(default, rename = "mediaItems")]
    media_items: Vec<GooglePhotosMedia>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GooglePhotosHandle {
    media_id: String,
}

impl CloudProvider for GooglePhotosProvider {
    fn name(&self) -> &'static str {
        "google_photos"
    }

    fn can_handle(&self, url: &str) -> bool {
        parse_google_photos_link(url).is_ok()
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        true
    }

    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        let token = google_photos_token(auth)?;
        let client = photos_client()?;
        match parse_google_photos_link(url)? {
            GooglePhotosLink::Media { id } => {
                let media = fetch_media(&client, token, &id).await?;
                let record = media_record(&client, &media).await?;
                Ok(SourceManifest {
                    provider: "google_photos".to_string(),
                    url: url.to_string(),
                    kind: SourceKind::File,
                    name: record.name.clone(),
                    total_files: 1,
                    total_bytes: record.size,
                    files: vec![record],
                })
            }
            GooglePhotosLink::Album { id } => {
                let files = list_album(&client, token, &id).await?;
                let total_bytes = files.iter().map(|file| file.size).sum();
                Ok(SourceManifest {
                    provider: "google_photos".to_string(),
                    url: url.to_string(),
                    kind: SourceKind::Folder,
                    name: id,
                    total_files: files.len() as u64,
                    total_bytes,
                    files,
                })
            }
        }
    }

    async fn download_job(
        &self,
        mut manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        let token = google_photos_token(auth)?;
        let client = photos_client()?;
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
            let handle: GooglePhotosHandle = serde_json::from_str(
                file.handle
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("manifesto Google Photos sem handle"))?,
            )?;
            let media = fetch_media(&client, token, &handle.media_id).await?;
            let url = media_download_url(&media);
            let target = photos_target_path(&manifest, &file);

            if let Err(e) = download_file(
                &client,
                &url,
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

async fn fetch_media(client: &Client, token: &str, id: &str) -> anyhow::Result<GooglePhotosMedia> {
    let response = with_bearer(client.get(format!("{MEDIA_URL}/{id}")), token)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Google Photos mediaItems.get falhou: {}",
            response.status()
        ));
    }
    Ok(response.json().await?)
}

async fn list_album(
    client: &Client,
    token: &str,
    album_id: &str,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    let mut paths = HashSet::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut body = serde_json::json!({
            "albumId": album_id,
            "pageSize": 100
        });
        if let Some(page_token) = &page_token {
            body["pageToken"] = Value::String(page_token.clone());
        }
        let response = with_bearer(client.post(SEARCH_URL), token)
            .json(&body)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Google Photos mediaItems.search falhou: {}",
                response.status()
            ));
        }
        let page: GooglePhotosSearchResponse = response.json().await?;
        for media in page.media_items {
            let mut record = media_record(client, &media).await?;
            record.path = unique_photo_path(&mut paths, &record.path, &media.id);
            files.push(record);
        }
        let Some(next_page_token) = page.next_page_token else {
            break;
        };
        page_token = Some(next_page_token);
    }

    Ok(files)
}

async fn media_record(client: &Client, media: &GooglePhotosMedia) -> anyhow::Result<FileRecord> {
    let size = download_size(client, &media_download_url(media)).await?;
    Ok(FileRecord {
        path: safe_file_name(&media.filename),
        name: media.filename.clone(),
        size,
        handle: Some(serde_json::to_string(&GooglePhotosHandle {
            media_id: media.id.clone(),
        })?),
        done: false,
        error: None,
        integrity: None,
    })
}

// ponytail: Google Photos baseUrl expires, so the actual URL is rebuilt per download.
#[allow(clippy::too_many_arguments)]
async fn download_file(
    client: &Client,
    url: &str,
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

    let mut request = client.get(url);
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
            reason: "Google Photos respondeu Range invalido; reiniciando de zero".to_string(),
        });
        return download_from_zero(
            client,
            url,
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
    target: &Path,
    expected_size: u64,
    events: &EventSink,
    job_id: &str,
    event_path: &str,
) -> anyhow::Result<()> {
    let response = client.get(url).send().await?;
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
            "GET Google Photos falhou: {}",
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

async fn download_size(client: &Client, url: &str) -> anyhow::Result<u64> {
    if let Ok(response) = client.head(url).send().await {
        if let Some(size) = response_size(&response) {
            return Ok(size);
        }
    }
    let response = client.get(url).header(RANGE, "bytes=0-0").send().await?;
    response_size(&response).context("Google Photos nao retornou tamanho")
}

fn response_size(response: &Response) -> Option<u64> {
    if !response.status().is_success() {
        return None;
    }
    response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit_once('/')?.1.parse().ok())
        .or_else(|| {
            response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
}

fn parse_google_photos_link(url: &str) -> anyhow::Result<GooglePhotosLink> {
    let url = Url::parse(url)?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(anyhow::anyhow!(
            "URL Google Photos esperada: gphotos://album/ID ou gphotos://media/ID"
        ));
    }
    match (
        url.scheme(),
        url.host_str(),
        url.path().trim_start_matches('/'),
    ) {
        ("gphotos", Some("album"), id) if valid_id(id) => {
            Ok(GooglePhotosLink::Album { id: id.to_string() })
        }
        ("gphotos", Some("media"), id) if valid_id(id) => {
            Ok(GooglePhotosLink::Media { id: id.to_string() })
        }
        _ => Err(anyhow::anyhow!(
            "URL Google Photos esperada: gphotos://album/ID ou gphotos://media/ID"
        )),
    }
}

fn media_download_url(media: &GooglePhotosMedia) -> String {
    if media.mime_type.starts_with("video/") {
        format!("{}=dv", media.base_url)
    } else {
        format!("{}=d", media.base_url)
    }
}

fn valid_id(id: &str) -> bool {
    !id.trim().is_empty() && !id.contains(['/', '\\'])
}

fn unique_photo_path(paths: &mut HashSet<String>, path: &str, media_id: &str) -> String {
    if paths.insert(path.to_string()) {
        return path.to_string();
    }

    let (base, extension) = path
        .rsplit_once('.')
        .filter(|(base, _)| !base.is_empty())
        .map(|(base, extension)| (base, format!(".{extension}")))
        .unwrap_or((path, String::new()));
    let id = safe_file_name(media_id);
    let mut number = 1;
    loop {
        let suffix = if number == 1 {
            id.clone()
        } else {
            format!("{id}-{number}")
        };
        let candidate = format!("{base} ({suffix}){extension}");
        if paths.insert(candidate.clone()) {
            return candidate;
        }
        number += 1;
    }
}

fn google_photos_token(auth: &ProviderAuth) -> anyhow::Result<&str> {
    auth.google_photos_token
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!(""))
}

fn photos_target_path(manifest: &JobManifest, file: &FileRecord) -> PathBuf {
    match manifest.kind {
        SourceKind::File => PathBuf::from(&manifest.target_path),
        SourceKind::Folder => Path::new(&manifest.job_dir).join(safe_relative_path(&file.path)),
    }
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

fn photos_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_synthetic_google_photos_links() {
        assert_eq!(
            parse_google_photos_link("gphotos://album/abc_123").unwrap(),
            GooglePhotosLink::Album {
                id: "abc_123".to_string()
            }
        );
        assert_eq!(
            parse_google_photos_link("gphotos://media/xyz-789").unwrap(),
            GooglePhotosLink::Media {
                id: "xyz-789".to_string()
            }
        );
        assert!(parse_google_photos_link("https://photos.google.com/share/abc").is_err());
        assert!(parse_google_photos_link("gphotos://album/abc?x=1").is_err());
        assert!(parse_google_photos_link("gphotos://media/abc#fragment").is_err());
    }

    #[test]
    fn makes_duplicate_album_paths_unique() {
        let mut paths = HashSet::new();
        assert_eq!(unique_photo_path(&mut paths, "IMG.jpg", "id-1"), "IMG.jpg");
        assert_eq!(
            unique_photo_path(&mut paths, "IMG.jpg", "id-2"),
            "IMG (id-2).jpg"
        );
        assert_eq!(
            unique_photo_path(&mut paths, "IMG.jpg", "id-2"),
            "IMG (id-2-2).jpg"
        );
    }

    #[test]
    fn builds_download_urls_by_media_type() {
        let image = media("image/jpeg");
        let video = media("video/mp4");
        assert_eq!(
            media_download_url(&image),
            "https://lh3.googleusercontent.com/x=d"
        );
        assert_eq!(
            media_download_url(&video),
            "https://lh3.googleusercontent.com/x=dv"
        );
    }

    fn media(mime_type: &str) -> GooglePhotosMedia {
        GooglePhotosMedia {
            id: "id".to_string(),
            base_url: "https://lh3.googleusercontent.com/x".to_string(),
            mime_type: mime_type.to_string(),
            filename: "a.jpg".to_string(),
        }
    }
}
