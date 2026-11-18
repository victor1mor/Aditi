use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::core::manifest::{read_json, write_json};

#[derive(Debug, Serialize, Deserialize)]
pub struct JobRegistryEntry {
    pub job_id: String,
    pub job_dir: String,
}

pub fn validate_job_id(job_id: &str) -> anyhow::Result<()> {
    if job_id.trim().is_empty() {
        anyhow::bail!("job-id nao pode ser vazio");
    }
    if job_id.contains(['/', '\\', ':']) || job_id.contains("..") {
        anyhow::bail!("job-id deve ser um nome simples, sem caminho");
    }
    if job_id.chars().any(is_windows_invalid_name_char) {
        anyhow::bail!("job-id contem caracteres invalidos para Windows");
    }
    if job_id != job_id.trim_end_matches([' ', '.']) || is_windows_reserved_name(job_id) {
        anyhow::bail!("job-id nao pode usar um nome reservado pelo Windows");
    }
    Ok(())
}

pub fn job_dir(out_dir: &Path, job_id: &str) -> PathBuf {
    out_dir.join(job_id)
}

/// Converte um nome vindo da origem num nome que o Windows aceita, sem deixar
/// dois nomes diferentes caindo no mesmo arquivo.
///
/// Um nome que ja e valido sai intacto — o caso comum mantem exatamente o nome
/// que a origem declarou. Um nome que precisou ser alterado recebe um sufixo
/// derivado do nome **original**, porque a alteracao e perdedora de informacao:
/// `foto:1.jpg` e `foto?1.jpg` viravam os dois `foto_1.jpg`, e o segundo
/// download sobrescrevia o primeiro em silencio. O mesmo valia para
/// `arquivo` contra `arquivo. `, e para `_CON.txt` contra `CON.txt`.
pub fn safe_file_name(name: &str) -> String {
    let cleaned = name
        .chars()
        .map(|c| {
            if is_windows_invalid_name_char(c) {
                '_'
            } else {
                c
            }
        })
        .collect::<String>();
    let cleaned = cleaned.trim_end_matches([' ', '.']);

    let candidate = if cleaned.is_empty() {
        "download.bin".to_string()
    } else if is_windows_reserved_name(cleaned) {
        format!("_{cleaned}")
    } else {
        cleaned.to_string()
    };

    if candidate == name {
        return candidate;
    }
    tag_with_origin(&candidate, name)
}

/// Costura no nome quatro bytes do SHA-256 do nome original, antes da extensao.
///
/// Vem do nome original, e nao de um contador nem do que ja existe no disco,
/// justamente para ser **estavel entre execucoes**: uma retomada precisa
/// recalcular o mesmo caminho que a primeira execucao usou, sem consultar
/// estado nenhum. Um contador ou uma checagem de "ja existe" mudariam o
/// destino conforme a ordem dos arquivos e quebrariam o resume.
///
/// A extensao fica preservada para o arquivo continuar abrindo no programa
/// certo.
fn tag_with_origin(candidate: &str, original: &str) -> String {
    let digest = Sha256::digest(original.as_bytes());
    let tag = format!(
        "{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    );
    match candidate.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => format!("{stem}~{tag}.{ext}"),
        _ => format!("{candidate}~{tag}"),
    }
}

fn is_windows_invalid_name_char(c: char) -> bool {
    matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') || c.is_control()
}

fn is_windows_reserved_name(name: &str) -> bool {
    matches!(
        name.split('.')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase()
            .as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

pub fn safe_relative_path(path: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.split(['/', '\\']) {
        let part = part.trim();
        if !part.is_empty() {
            out.push(safe_file_name(part));
        }
    }
    if out.as_os_str().is_empty() {
        out.push("download.bin");
    }
    out
}

pub fn registry_path(job_id: &str) -> anyhow::Result<PathBuf> {
    validate_job_id(job_id)?;
    Ok(std::env::current_dir()?
        .join(".aditi")
        .join("jobs")
        .join(format!("{job_id}.json")))
}

pub fn save_registry(job_id: &str, job_dir: &Path) -> anyhow::Result<()> {
    let path = registry_path(job_id)?;
    let entry = JobRegistryEntry {
        job_id: job_id.to_string(),
        job_dir: job_dir.to_string_lossy().to_string(),
    };
    write_json(&path, &entry)
}

pub fn load_registry(job_id: &str) -> anyhow::Result<JobRegistryEntry> {
    read_json(&registry_path(job_id)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_like_job_id() {
        assert!(validate_job_id("ok-123").is_ok());
        assert!(validate_job_id("..\\bad").is_err());
        assert!(validate_job_id("bad/name").is_err());
        assert!(validate_job_id("bad?name").is_err());
        assert!(validate_job_id("CON").is_err());
        assert!(validate_job_id("job.").is_err());
    }

    /// O caso comum nao pode mudar: um nome que o Windows ja aceita sai igual
    /// ao que a origem declarou, sem sufixo nenhum.
    #[test]
    fn keeps_valid_names_untouched() {
        for name in ["arquivo.zip", "foto 1.jpg", "sem-extensao", ".gitignore"] {
            assert_eq!(safe_file_name(name), name, "nome valido foi alterado");
        }
    }

    #[test]
    fn cleans_file_names_for_windows() {
        // A limpeza continua acontecendo: nada de caractere proibido sobrando.
        assert!(safe_file_name("a:b?.zip").starts_with("a_b_"));
        assert!(!safe_file_name("a:b?.zip").contains([':', '?']));
        assert!(safe_file_name("CON.txt").starts_with("_CON"));
        assert!(safe_file_name("..").starts_with("download"));
    }

    /// A regressao que importa. Antes, cada um destes pares caia no mesmo
    /// arquivo e o segundo download sobrescrevia o primeiro sem aviso.
    #[test]
    fn never_maps_two_names_to_the_same_file() {
        let colidiam = [
            "foto:1.jpg",
            "foto?1.jpg",
            "foto*1.jpg",
            "foto|1.jpg",
            "foto_1.jpg", // o nome literal, que ja era valido
            "CON.txt",
            "_CON.txt",
            "relatorio",
            "relatorio. ",
            "relatorio ",
        ];

        let mut vistos = std::collections::HashMap::new();
        for original in colidiam {
            let destino = safe_file_name(original);
            if let Some(anterior) = vistos.insert(destino.clone(), original) {
                panic!("'{anterior}' e '{original}' caem os dois em '{destino}'");
            }
        }
    }

    /// Sem isto o arquivo deixa de abrir no programa certo.
    #[test]
    fn keeps_the_extension_when_it_disambiguates() {
        let destino = safe_file_name("a:b.zip");
        assert!(destino.ends_with(".zip"), "extensao perdida: {destino}");
    }

    /// O resume depende disto: a segunda execucao precisa recalcular o mesmo
    /// caminho da primeira, sem olhar o disco.
    #[test]
    fn recomputes_the_same_name_every_time() {
        assert_eq!(safe_file_name("a:b?.zip"), safe_file_name("a:b?.zip"));
        assert_eq!(safe_relative_path("x:y/a:b"), safe_relative_path("x:y/a:b"));
    }

    #[test]
    fn cleans_relative_paths_piece_by_piece() {
        let destino = safe_relative_path("/root/a:b?.zip");
        let partes: Vec<_> = destino.iter().collect();
        assert_eq!(partes.len(), 2, "caminho inesperado: {destino:?}");
        assert_eq!(partes[0], "root", "parte valida foi alterada");
        assert!(destino.to_string_lossy().ends_with(".zip"));
    }

    /// Pastas diferentes tambem nao podem se fundir numa so.
    #[test]
    fn never_merges_two_directories_into_one() {
        assert_ne!(
            safe_relative_path("a:b/arquivo.zip"),
            safe_relative_path("a?b/arquivo.zip")
        );
    }
}
