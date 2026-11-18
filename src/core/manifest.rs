use crate::core::integrity::Integrity;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    File,
    Folder,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Finished,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    pub index: u64,
    pub start: u64,
    pub end: u64,
    pub done: bool,
}

impl ChunkRecord {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        self.end < self.start
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub handle: Option<String>,
    pub done: bool,
    pub error: Option<String>,
    /// Checksums que a origem declarou para este arquivo.
    ///
    /// `None` quando a origem nao oferece nenhum. Serializado apenas quando
    /// existe, para que manifests gravados por versoes anteriores continuem
    /// sendo lidos sem migracao.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<Integrity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceManifest {
    pub provider: String,
    pub url: String,
    pub kind: SourceKind,
    pub name: String,
    pub total_files: u64,
    pub total_bytes: u64,
    pub files: Vec<FileRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobManifest {
    pub version: u32,
    pub job_id: String,
    pub provider: String,
    pub url: String,
    pub kind: SourceKind,
    pub state: JobState,
    pub out_dir: String,
    pub job_dir: String,
    pub target_path: String,
    pub total_files: u64,
    pub total_bytes: u64,
    pub chunk_size: u64,
    pub chunks: Vec<ChunkRecord>,
    pub files: Vec<FileRecord>,
    pub error: Option<String>,
}

pub fn job_manifest_path(job_dir: &Path) -> PathBuf {
    job_dir.join("job.aditi.json")
}

pub fn source_manifest_path(job_dir: &Path) -> PathBuf {
    job_dir.join("source.aditi.json")
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> anyhow::Result<T> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

/// Grava JSON de forma atomica: escreve num temporario ao lado do alvo, forca o
/// conteudo ao disco e so entao renomeia por cima.
///
/// O manifesto e reescrito a cada chunk concluido. Com `fs::write` direto, uma
/// queda no meio da gravacao deixava um `job.aditi.json` truncado — e manifesto
/// truncado nao e um arquivo perdido, e a retomada de um download inteiro
/// perdida. Aqui uma queda deixa no maximo um `.tmp` orfao, e o manifesto
/// antigo continua valido.
///
/// `fs::rename` e `rename(2)` no Unix e `MoveFileExW` com queda para
/// `FileRenameInfoEx` no Windows: nos dois a troca do nome e atomica e
/// substitui um arquivo que ja exista.
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(value)?;
    let temp = temp_path_for(path);

    // O escopo fecha o arquivo antes do rename: o Windows recusa renomear um
    // arquivo que ainda esta aberto.
    let written = (|| -> std::io::Result<()> {
        let mut file = fs::File::create(&temp)?;
        file.write_all(text.as_bytes())?;
        // Sem o sync, o rename pode chegar ao disco antes do conteudo e a
        // queda deixa um manifesto de tamanho certo e miolo vazio.
        file.sync_all()
    })();

    if let Err(err) = written.and_then(|()| fs::rename(&temp, path)) {
        // Nao deixa lixo para tras quando a gravacao falha.
        let _ = fs::remove_file(&temp);
        return Err(err.into());
    }
    Ok(())
}

/// Temporario irmao do alvo, no mesmo diretorio — `rename` entre volumes
/// diferentes nao e atomico, e `/tmp` costuma ser outro volume.
///
/// O PID no nome evita que duas execucoes simultaneas disputem o mesmo arquivo
/// intermediario. Elas ainda podem se sobrescrever no destino (o ultimo a
/// renomear vence), mas nenhuma le um manifesto pela metade.
fn temp_path_for(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest.json");
    path.with_file_name(format!(".{name}.{}.tmp", std::process::id()))
}

pub fn load_job(job_dir: &Path) -> anyhow::Result<JobManifest> {
    read_json(&job_manifest_path(job_dir))
}

pub fn save_job(job: &JobManifest) -> anyhow::Result<()> {
    write_json(&job_manifest_path(Path::new(&job.job_dir)), job)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Diretorio proprio por teste. O projeto nao carrega `tempfile` e a regra
    /// de reducao de dependencias vale aqui tambem.
    fn scratch_dir(rotulo: &str) -> PathBuf {
        static CONTADOR: AtomicU32 = AtomicU32::new(0);
        let unico = CONTADOR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "aditi-teste-{}-{rotulo}-{unico}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn entradas(dir: &Path) -> Vec<String> {
        let mut nomes: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        nomes.sort();
        nomes
    }

    #[test]
    fn writes_and_reads_back() {
        let dir = scratch_dir("roundtrip");
        let alvo = dir.join("sub").join("job.aditi.json");
        write_json(&alvo, &vec![1u32, 2, 3]).unwrap();
        let volta: Vec<u32> = read_json(&alvo).unwrap();
        assert_eq!(volta, vec![1, 2, 3]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A gravacao substitui o manifesto anterior e nao deixa `.tmp` para tras.
    /// Um `.tmp` esquecido a cada chunk encheria o diretorio do job.
    #[test]
    fn replaces_the_previous_file_without_leaving_temporaries() {
        let dir = scratch_dir("replace");
        let alvo = dir.join("job.aditi.json");

        write_json(&alvo, &"primeiro").unwrap();
        write_json(&alvo, &"segundo").unwrap();

        let volta: String = read_json(&alvo).unwrap();
        assert_eq!(volta, "segundo");
        assert_eq!(
            entradas(&dir),
            vec!["job.aditi.json".to_string()],
            "sobrou arquivo intermediario"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// O ponto da mudanca: em nenhum instante o alvo existe com conteudo
    /// parcial. Ou tem o manifesto antigo inteiro, ou o novo inteiro.
    #[test]
    fn never_leaves_the_target_half_written() {
        let dir = scratch_dir("atomico");
        let alvo = dir.join("job.aditi.json");
        let grande = vec!["x".repeat(4096); 256];

        write_json(&alvo, &"antigo").unwrap();
        let antes = fs::read_to_string(&alvo).unwrap();
        assert_eq!(serde_json::from_str::<String>(&antes).unwrap(), "antigo");

        write_json(&alvo, &grande).unwrap();
        // Se o alvo tivesse sido escrito no lugar, um erro no meio deixaria um
        // JSON invalido. Aqui ele so troca de nome quando ja esta completo.
        let depois: Vec<String> = read_json(&alvo).unwrap();
        assert_eq!(depois.len(), 256);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Dois processos gravando ao mesmo tempo nao podem disputar o mesmo
    /// arquivo intermediario.
    #[test]
    fn temporary_is_a_sibling_and_carries_the_pid() {
        let alvo = Path::new("C:").join("jobs").join("job.aditi.json");
        let temp = temp_path_for(&alvo);
        assert_eq!(temp.parent(), alvo.parent(), "temporario mudou de volume");
        let nome = temp.file_name().unwrap().to_string_lossy().to_string();
        assert!(nome.contains(&std::process::id().to_string()), "{nome}");
        assert!(nome.ends_with(".tmp"), "{nome}");
    }

    /// Manifestos gravados antes do campo `integrity` precisam continuar sendo
    /// lidos sem migracao: e o que permite retomar um job antigo.
    #[test]
    fn reads_a_file_record_written_before_integrity_existed() {
        let antigo = r#"{
            "path": "a/b.stl",
            "name": "b.stl",
            "size": 42,
            "handle": "https://example.invalid/b.stl",
            "done": true,
            "error": null
        }"#;
        let record: FileRecord = serde_json::from_str(antigo).unwrap();
        assert_eq!(record.size, 42);
        assert!(record.integrity.is_none());
    }

    /// Sem checksum, o campo nao aparece no JSON — o manifesto de um provider
    /// que nao oferece integridade fica identico ao de antes.
    #[test]
    fn omits_integrity_from_json_when_absent() {
        let record = FileRecord {
            path: "a.stl".to_string(),
            name: "a.stl".to_string(),
            size: 1,
            handle: None,
            done: false,
            error: None,
            integrity: None,
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("integrity"), "json inesperado: {json}");
    }

    #[test]
    fn round_trips_integrity_when_present() {
        let record = FileRecord {
            path: "a.stl".to_string(),
            name: "a.stl".to_string(),
            size: 1,
            handle: None,
            done: false,
            error: None,
            integrity: Some(Integrity {
                md5: Some("900150983cd24fb0d6963f7d28e17f72".to_string()),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&record).unwrap();
        let volta: FileRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(volta.integrity, record.integrity);
        assert!(!json.contains("sha256"), "campo vazio vazou: {json}");
    }
}
