use megalib::api::ApiClient;
use megalib::base64::base64url_decode;
use megalib::crypto::aes::aes128_cbc_decrypt;
use megalib::public::{open_folder, parse_folder_link, parse_mega_link};
use serde_json::Value;

use crate::core::manifest::{FileRecord, SourceKind, SourceManifest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MegaLinkKind {
    File,
    Folder,
}

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub handle: String,
    pub key: [u8; 32],
    pub download_url: String,
    pub public_url: Option<String>,
}

pub fn detect_mega_link(url: &str) -> anyhow::Result<MegaLinkKind> {
    if parse_mega_link(url).is_ok() {
        return Ok(MegaLinkKind::File);
    }
    if parse_folder_link(url).is_ok() {
        return Ok(MegaLinkKind::Folder);
    }
    Err(anyhow::anyhow!("URL MEGA nao suportada"))
}

pub async fn parse_and_fetch_node(url: &str) -> anyhow::Result<FileInfo> {
    tracing::info!("Parseando link: {}", url);

    let (handle, key_b64) = parse_mega_link(url).map_err(|e| anyhow::anyhow!(e.to_string()))?;

    // A chave do arquivo fica depois do # no link MEGA.
    let key_bytes = base64url_decode(&key_b64)?;
    if key_bytes.len() < 32 {
        return Err(anyhow::anyhow!("Chave MEGA invalida: menos de 32 bytes"));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&key_bytes[..32]);

    let mut api = ApiClient::new();
    let response = api
        .request(serde_json::json!({
            "a": "g",
            "g": 1,
            "p": handle
        }))
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let size = response.get("s").and_then(|v| v.as_u64()).unwrap_or(0);
    let download_url = response
        .get("g")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if download_url.is_empty() {
        return Err(anyhow::anyhow!("API MEGA nao retornou URL de download"));
    }
    let attrs_b64 = response.get("at").and_then(|v| v.as_str()).unwrap_or("");

    let encrypted_attrs = base64url_decode(attrs_b64)?;

    let mut aes_key = [0u8; 16];
    for i in 0..16 {
        aes_key[i] = key[i] ^ key[i + 16];
    }
    let decrypted = aes128_cbc_decrypt(&encrypted_attrs, &aes_key);
    let text = String::from_utf8_lossy(&decrypted);
    let json_str = text.trim_start_matches("MEGA").trim_end_matches('\0');
    let attrs: Value = serde_json::from_str(json_str).unwrap_or(serde_json::json!({}));
    let name = attrs
        .get("n")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown_file")
        .to_string();

    Ok(FileInfo {
        name,
        size,
        handle,
        key,
        download_url,
        public_url: Some(url.to_string()),
    })
}

pub async fn probe_source(url: &str) -> anyhow::Result<SourceManifest> {
    match detect_mega_link(url)? {
        MegaLinkKind::File => {
            let info = parse_and_fetch_node(url).await?;
            Ok(SourceManifest {
                provider: "mega".to_string(),
                url: url.to_string(),
                kind: SourceKind::File,
                name: info.name.clone(),
                total_files: 1,
                total_bytes: info.size,
                files: vec![FileRecord {
                    path: info.name.clone(),
                    name: info.name,
                    size: info.size,
                    handle: Some(info.handle),
                    done: false,
                    error: None,
                    integrity: None,
                }],
            })
        }
        MegaLinkKind::Folder => {
            let mut fetch_url = url.to_string();
            let mut subfolder_handle = None;
            if let Some(hash_idx) = url.find('#') {
                let key_part = &url[hash_idx + 1..];
                if let Some(sub_idx) = key_part.find("/folder/") {
                    subfolder_handle =
                        Some(key_part[sub_idx + 8..].trim_end_matches('/').to_string());
                    fetch_url = format!("{}#{}", &url[..hash_idx], &key_part[..sub_idx]);
                }
            }

            let folder = open_folder(&fetch_url)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let mut root_name = folder
                .nodes()
                .first()
                .map(|node| node.name.clone())
                .unwrap_or_else(|| "Shared Folder".to_string());

            let mut subfolder_path = None;
            if let Some(handle) = &subfolder_handle {
                if let Some(sub) = folder.nodes().iter().find(|n| n.handle == *handle) {
                    subfolder_path = sub.path().map(|s| s.trim_start_matches('/').to_string());
                    root_name = sub.name.clone();
                } else {
                    return Err(anyhow::anyhow!("Subfolder not found in the shared folder"));
                }
            }

            let files: Vec<_> = folder
                .nodes()
                .iter()
                .filter(|node| node.is_file())
                .filter_map(|node| {
                    let path = node
                        .path()
                        .map(|path| path.trim_start_matches('/').to_string())
                        .unwrap_or_else(|| node.name.clone());

                    if let Some(sub_path) = &subfolder_path {
                        if !path.starts_with(sub_path) {
                            return None;
                        }
                    }

                    Some(FileRecord {
                        path,
                        name: node.name.clone(),
                        size: node.size,
                        handle: Some(node.handle.clone()),
                        done: false,
                        error: None,
                        integrity: None,
                    })
                })
                .collect();
            let total_bytes = files.iter().map(|file| file.size).sum();

            Ok(SourceManifest {
                provider: "mega".to_string(),
                url: url.to_string(),
                kind: SourceKind::Folder,
                name: root_name,
                total_files: files.len() as u64,
                total_bytes,
                files,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_file_and_folder_links() {
        assert_eq!(
            detect_mega_link("https://mega.nz/file/ABC#KEY").unwrap(),
            MegaLinkKind::File
        );
        assert_eq!(
            detect_mega_link("https://mega.nz/folder/ABC#KEY").unwrap(),
            MegaLinkKind::Folder
        );
        assert!(detect_mega_link("https://example.com").is_err());
    }
}
