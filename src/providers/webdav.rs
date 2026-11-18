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
use futures::StreamExt;
use reqwest::header::RANGE;
use reqwest::{Client, Method, Url};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug)]
pub struct WebDavProvider;

#[derive(Clone, Debug)]
struct WebDavEntry {
    url: String,
    relative_path: String,
    name: String,
    size: u64,
    is_dir: bool,
}

struct WebDavDownload<'a> {
    client: &'a Client,
    auth: &'a ProviderAuth,
    events: &'a EventSink,
    job_id: &'a str,
    url: &'a str,
    target: &'a Path,
    event_path: &'a str,
    expected_size: u64,
    expected_integrity: Option<&'a Integrity>,
}

impl CloudProvider for WebDavProvider {
    fn name(&self) -> &'static str {
        "webdav"
    }

    fn can_handle(&self, url: &str) -> bool {
        Url::parse(url)
            .map(|url| matches!(url.scheme(), "http" | "https"))
            .unwrap_or(false)
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn requires_auth(&self) -> bool {
        false
    }

    async fn probe(&self, url: &str, auth: &ProviderAuth) -> anyhow::Result<SourceManifest> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let root = Url::parse(url)?;
        let entries = propfind(&client, auth, &root, "1", &root).await?;
        let self_entry = entries
            .iter()
            .find(|entry| same_url_path(&entry.url, root.as_str()))
            .or_else(|| entries.first())
            .ok_or_else(|| anyhow::anyhow!("WebDAV nao retornou metadados"))?;

        if !self_entry.is_dir {
            return Ok(SourceManifest {
                provider: "webdav".to_string(),
                url: url.to_string(),
                kind: SourceKind::File,
                name: self_entry.name.clone(),
                total_files: 1,
                total_bytes: self_entry.size,
                files: vec![FileRecord {
                    path: self_entry.name.clone(),
                    name: self_entry.name.clone(),
                    size: self_entry.size,
                    handle: Some(url.to_string()),
                    done: false,
                    error: None,
                    integrity: None,
                }],
            });
        }

        let files = list_files_recursive(&client, auth, &root).await?;
        let total_bytes = files.iter().map(|file| file.size).sum();
        let name = self_entry
            .name
            .trim_matches('/')
            .split('/')
            .next_back()
            .unwrap_or("webdav")
            .to_string();
        Ok(SourceManifest {
            provider: "webdav".to_string(),
            url: url.to_string(),
            kind: SourceKind::Folder,
            name,
            total_files: files.len() as u64,
            total_bytes,
            files,
        })
    }

    async fn download_job(
        &self,
        mut manifest: JobManifest,
        auth: &ProviderAuth,
        options: &DownloadOptions,
    ) -> anyhow::Result<()> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let events = EventSink::new(options.json_events);
        events.emit(Event::Started {
            job_id: manifest.job_id.clone(),
            provider: manifest.provider.clone(),
            total_files: manifest.total_files,
            total_bytes: manifest.total_bytes,
        });

        let mut done_bytes = manifest
            .files
            .iter()
            .filter(|file| file.done)
            .map(|file| file.size)
            .sum();

        for index in 0..manifest.files.len() {
            if manifest.files[index].done {
                continue;
            }
            let file_url = manifest.files[index]
                .handle
                .clone()
                .ok_or_else(|| anyhow::anyhow!("manifesto WebDAV sem URL de arquivo"))?;
            let target = webdav_target_path(&manifest, &manifest.files[index]);
            if let Err(e) = (WebDavDownload {
                client: &client,
                auth,
                events: &events,
                job_id: &manifest.job_id,
                url: &file_url,
                target: &target,
                event_path: &manifest.files[index].path,
                expected_size: manifest.files[index].size,
                expected_integrity: manifest.files[index].integrity.as_ref(),
            })
            .run()
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

            done_bytes += manifest.files[index].size;
            manifest.files[index].done = true;
            manifest.files[index].error = None;
            save_job(&manifest)?;
            events.emit(Event::FileFinished {
                job_id: manifest.job_id.clone(),
                path: manifest.files[index].path.clone(),
                bytes: manifest.files[index].size,
            });
            events.emit(Event::Progress {
                job_id: manifest.job_id.clone(),
                path: manifest.files[index].path.clone(),
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

async fn propfind(
    client: &Client,
    auth: &ProviderAuth,
    url: &Url,
    depth: &str,
    root: &Url,
) -> anyhow::Result<Vec<WebDavEntry>> {
    let method = Method::from_bytes(b"PROPFIND")?;
    let body = r#"<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
    <d:getcontentlength/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>"#;
    let response = with_auth(
        client
            .request(method, url.clone())
            .header("Depth", depth)
            .header("Content-Type", "application/xml")
            .body(body),
        auth,
    )
    .send()
    .await?;
    if response.status().as_u16() != 207 && !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "PROPFIND {} falhou: {}",
            url,
            response.status()
        ));
    }
    Ok(parse_propfind(&response.text().await?, root))
}

async fn list_files_recursive(
    client: &Client,
    auth: &ProviderAuth,
    root: &Url,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    let mut stack = vec![root.clone()];

    while let Some(dir) = stack.pop() {
        if !seen.insert(dir.to_string()) {
            continue;
        }
        for entry in propfind(client, auth, &dir, "1", root).await? {
            if same_url_path(&entry.url, dir.as_str()) {
                continue;
            }
            if entry.is_dir {
                stack.push(Url::parse(&entry.url)?);
            } else {
                files.push(FileRecord {
                    path: entry.relative_path,
                    name: entry.name,
                    size: entry.size,
                    handle: Some(entry.url),
                    done: false,
                    error: None,
                    integrity: None,
                });
            }
        }
    }

    Ok(files)
}

impl WebDavDownload<'_> {
    async fn run(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if existing_target_is_complete(
            self.target,
            self.expected_size,
            self.expected_integrity,
            self.events,
            self.job_id,
            self.event_path,
        )? {
            return Ok(());
        }

        let part = part_path(self.target);
        let mut start = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        if start > self.expected_size {
            std::fs::remove_file(&part)?;
            start = 0;
        }

        let mut request = with_auth(self.client.get(self.url), self.auth);
        if start > 0 {
            request = request.header(RANGE, format!("bytes={start}-"));
        }
        let response = request.send().await?;
        if start > 0 && !resume_response_matches(&response, start, self.expected_size) {
            std::fs::remove_file(&part).ok();
            self.events.emit(Event::Retry {
                job_id: self.job_id.to_string(),
                path: self.event_path.to_string(),
                chunk: 0,
                attempt: 2,
                reason: "WebDAV respondeu Range invalido; reiniciando de zero".to_string(),
            });
            return self.run_from_zero().await;
        }
        self.write_response(part, start, response).await
    }

    async fn run_from_zero(&self) -> anyhow::Result<()> {
        let part = part_path(self.target);
        let response = with_auth(self.client.get(self.url), self.auth)
            .send()
            .await?;
        self.write_response(part, 0, response).await
    }

    async fn write_response(
        &self,
        part: PathBuf,
        start: u64,
        response: reqwest::Response,
    ) -> anyhow::Result<()> {
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "GET {} falhou: {}",
                self.url,
                response.status()
            ));
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(start > 0)
            .write(true)
            .truncate(start == 0)
            .open(&part)?;
        let mut done = start;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)?;
            done += chunk.len() as u64;
            self.events.emit(Event::Progress {
                job_id: self.job_id.to_string(),
                path: self.event_path.to_string(),
                bytes_done: done.min(self.expected_size),
                bytes_total: self.expected_size,
            });
        }
        drop(file);

        let actual = std::fs::metadata(&part)?.len();
        if actual != self.expected_size {
            return Err(anyhow::anyhow!(
                "Tamanho invalido em {}: esperado {}, obtido {}",
                self.event_path,
                self.expected_size,
                actual
            ));
        }
        if self.target.exists() {
            std::fs::remove_file(self.target)?;
        }
        std::fs::rename(&part, self.target)?;
        Ok(())
    }
}

fn with_auth(builder: reqwest::RequestBuilder, auth: &ProviderAuth) -> reqwest::RequestBuilder {
    if let Some(token) = &auth.bearer_token {
        builder.bearer_auth(token)
    } else if let Some(user) = &auth.user {
        builder.basic_auth(user, auth.pass.clone())
    } else {
        builder
    }
}

fn webdav_target_path(manifest: &JobManifest, file: &FileRecord) -> PathBuf {
    let base = Path::new(&manifest.job_dir);
    match manifest.kind {
        SourceKind::File => base.join(safe_file_name(&file.name)),
        SourceKind::Folder => base.join(safe_relative_path(&file.path)),
    }
}

fn part_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download.bin");
    target.with_file_name(format!("{file_name}.aditi.part"))
}

fn parse_propfind(xml: &str, root: &Url) -> Vec<WebDavEntry> {
    let xml = strip_xml_prefixes(xml);
    blocks(&xml, "response")
        .into_iter()
        .filter_map(|block| {
            let href = text(block, "href")?;
            let url = root.join(&href).or_else(|_| Url::parse(&href)).ok()?;
            let is_dir = block.contains("<collection")
                || text(block, "iscollection").as_deref() == Some("1");
            let name = text(block, "displayname")
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| last_url_segment(&url));
            let size = text(block, "getcontentlength")
                .and_then(|size| size.trim().parse().ok())
                .unwrap_or(0);
            Some(WebDavEntry {
                relative_path: relative_path(root, &url, &name),
                url: url.to_string(),
                name,
                size,
                is_dir,
            })
        })
        .collect()
}

fn strip_xml_prefixes(xml: &str) -> String {
    let bytes = xml.as_bytes();
    let mut out = String::with_capacity(xml.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        out.push('<');
        i += 1;
        if i < bytes.len() && bytes[i] == b'/' {
            out.push('/');
            i += 1;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b':' {
            i += 1;
        } else {
            out.push_str(&xml[start..i]);
        }
    }
    out
}

fn blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut rest = xml;
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    while let Some(start) = rest.find(&open) {
        let Some(open_end) = rest[start..].find('>') else {
            break;
        };
        let body_start = start + open_end + 1;
        let Some(end) = rest[body_start..].find(&close) else {
            break;
        };
        out.push(&rest[body_start..body_start + end]);
        rest = &rest[body_start + end + close.len()..];
    }
    out
}

fn text(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let start = block.find(&open)?;
    let open_end = block[start..].find('>')?;
    let body_start = start + open_end + 1;
    let end = block[body_start..].find(&close)?;
    Some(xml_unescape(block[body_start..body_start + end].trim()))
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn last_url_segment(url: &Url) -> String {
    url.path_segments()
        .and_then(|mut parts| parts.next_back())
        .filter(|part| !part.is_empty())
        .map(percent_decode)
        .unwrap_or_else(|| "webdav".to_string())
}

fn relative_path(root: &Url, child: &Url, fallback_name: &str) -> String {
    let mut root_path = percent_decode(root.path());
    if !root_path.ends_with('/') {
        root_path.push('/');
    }
    let child_path = percent_decode(child.path());
    child_path
        .strip_prefix(&root_path)
        .filter(|path| !path.is_empty())
        .unwrap_or(fallback_name)
        .trim_start_matches('/')
        .to_string()
}

fn same_url_path(a: &str, b: &str) -> bool {
    let Ok(a) = Url::parse(a) else {
        return false;
    };
    let Ok(b) = Url::parse(b) else {
        return false;
    };
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
        && a.path().trim_end_matches('/') == b.path().trim_end_matches('/')
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_propfind_response() {
        let root = Url::parse("https://example.com/dav/root/").unwrap();
        let xml = r#"
        <d:multistatus xmlns:d="DAV:">
          <d:response>
            <d:href>/dav/root/</d:href>
            <d:propstat><d:prop><d:displayname>root</d:displayname><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat>
          </d:response>
          <d:response>
            <d:href>/dav/root/a%20file.txt</d:href>
            <d:propstat><d:prop><d:displayname>a file.txt</d:displayname><d:getcontentlength>42</d:getcontentlength><d:resourcetype/></d:prop></d:propstat>
          </d:response>
        </d:multistatus>"#;
        let entries = parse_propfind(xml, &root);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].is_dir);
        assert_eq!(entries[1].relative_path, "a file.txt");
        assert_eq!(entries[1].size, 42);
    }
}
