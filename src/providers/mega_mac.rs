//! Verificacao do meta-MAC que o MEGA embute na chave do arquivo.
//!
//! O MEGA nao publica checksum como o Google Drive faz. Em vez disso, a propria
//! chave do arquivo — a que chega no fragmento do link — carrega nos ultimos
//! oito bytes uma reducao do MAC de todo o conteudo.
//!
//! Conferir isso e mais forte que conferir um checksum publicado: a comparacao
//! esta amarrada a chave, entao nem quem serve os bytes consegue fabricar um
//! conteudo que passe sem ter a chave.
//!
//! A especificacao aqui nao foi deduzida: saiu da implementacao de upload do
//! proprio `megalib` (`fs/operations/upload.rs` e
//! `crypto/keys.rs::pack_node_key`), que e a contraparte exata deste caminho.
//! O que ela empacota ao subir e o que se confere ao baixar, e o teste de
//! ida-e-volta neste arquivo prende as duas pontas.

use megalib::crypto::aes::{chunk_mac_calculate, meta_mac_calculate};
use std::io::Read;
use std::path::Path;

/// A chave de 32 bytes do node, desdobrada nas tres partes que ela carrega.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MegaFileKey {
    /// Chave AES-128 do arquivo.
    pub key: [u8; 16],
    /// Nonce do CTR, usado tambem para montar o IV do MAC.
    pub nonce: [u8; 8],
    /// Meta-MAC reduzido, como veio na chave. E o valor esperado.
    pub meta_mac: [u8; 8],
}

impl MegaFileKey {
    /// Desdobra a chave do node.
    ///
    /// `pack_node_key` monta os 32 bytes de modo que a chave do arquivo saia do
    /// XOR das duas metades, e que nonce e meta-MAC apareçam em claro na
    /// segunda metade.
    pub fn from_node_key(node_key: &[u8; 32]) -> Self {
        let mut key = [0u8; 16];
        for index in 0..16 {
            key[index] = node_key[index] ^ node_key[index + 16];
        }
        let mut nonce = [0u8; 8];
        nonce.copy_from_slice(&node_key[16..24]);
        let mut meta_mac = [0u8; 8];
        meta_mac.copy_from_slice(&node_key[24..32]);
        Self {
            key,
            nonce,
            meta_mac,
        }
    }

    /// IV do MAC: o nonce repetido duas vezes.
    ///
    /// E o mesmo para todos os chunks. A doc do `chunk_mac_calculate` fala em
    /// IV que depende do offset, mas os dois lugares onde o `megalib` calcula
    /// MAC no upload montam o IV so com o nonce, sem offset. Vale o codigo, nao
    /// o comentario.
    fn mac_iv(&self) -> [u8; 16] {
        let mut iv = [0u8; 16];
        iv[..8].copy_from_slice(&self.nonce);
        iv[8..].copy_from_slice(&self.nonce);
        iv
    }
}

/// Resultado da conferencia.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MacCheck {
    Match,
    Mismatch { expected: String, actual: String },
}

/// Progressao de tamanho de chunk do MEGA.
///
/// Os oito primeiros crescem — 128 KB, 256 KB, ... ate 1 MB — e do nono em
/// diante ficam em 1 MB, sempre truncados no fim do arquivo.
///
/// Copiado de `get_chunk_size` do `megalib`, que e `pub(crate)` e por isso nao
/// pode ser reusado daqui. Errar esta progressao nao daria erro de compilacao,
/// daria MAC divergente em arquivo intacto — por isso ela tem teste proprio.
fn chunk_size(chunk_index: usize, offset: u64, total_size: u64) -> u64 {
    let size = if chunk_index < 8 {
        (chunk_index as u64 + 1) * 128 * 1024
    } else {
        1024 * 1024
    };
    if offset.saturating_add(size) > total_size {
        total_size.saturating_sub(offset)
    } else {
        size
    }
}

/// Reduz o meta-MAC de 16 bytes aos 8 que a chave carrega.
///
/// Mesma dobra de `pack_node_key`: `mm[i] ^ mm[4+i]` nos quatro primeiros e
/// `mm[8+i] ^ mm[12+i]` nos quatro seguintes.
fn fold_meta_mac(meta_mac: &[u8; 16]) -> [u8; 8] {
    let mut folded = [0u8; 8];
    for index in 0..4 {
        folded[index] = meta_mac[index] ^ meta_mac[4 + index];
        folded[4 + index] = meta_mac[8 + index] ^ meta_mac[12 + index];
    }
    folded
}

/// Calcula o meta-MAC reduzido do arquivo em `path`.
///
/// Le do disco em vez de acumular durante a transferencia de proposito: uma
/// retomada so tem em maos os bytes que ela mesma baixou, e o prefixo gravado
/// numa execucao anterior precisa entrar na conta. Esse prefixo so existe no
/// `.part`. Acumular no voo obrigaria a guardar MACs parciais no manifesto, ou
/// a desistir de retomar — e retomar download grande e o motivo deste motor
/// existir.
pub fn folded_meta_mac_of_file(
    path: &Path,
    file_key: &MegaFileKey,
    total_size: u64,
) -> anyhow::Result<[u8; 8]> {
    let mut file = std::fs::File::open(path)?;
    let mac_iv = file_key.mac_iv();

    let mut chunk_macs = Vec::new();
    let mut buffer = Vec::new();
    let mut offset = 0u64;
    let mut chunk_index = 0usize;

    while offset < total_size {
        let wanted = chunk_size(chunk_index, offset, total_size);
        buffer.clear();
        buffer.resize(wanted as usize, 0);
        file.read_exact(&mut buffer)?;
        chunk_macs.push(chunk_mac_calculate(&buffer, &file_key.key, &mac_iv));
        offset += wanted;
        chunk_index += 1;
    }

    Ok(fold_meta_mac(&meta_mac_calculate(
        &chunk_macs,
        &file_key.key,
    )))
}

/// Confere o conteudo de `path` contra o meta-MAC embutido na chave do node.
pub fn verify_file(path: &Path, node_key: &[u8; 32], total_size: u64) -> anyhow::Result<MacCheck> {
    let file_key = MegaFileKey::from_node_key(node_key);
    let calculado = folded_meta_mac_of_file(path, &file_key, total_size)?;
    if calculado == file_key.meta_mac {
        Ok(MacCheck::Match)
    } else {
        Ok(MacCheck::Mismatch {
            expected: hex(&file_key.meta_mac),
            actual: hex(&calculado),
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use megalib::crypto::keys::pack_node_key;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn arquivo_com(rotulo: &str, conteudo: &[u8]) -> PathBuf {
        static CONTADOR: AtomicU32 = AtomicU32::new(0);
        let unico = CONTADOR.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("aditi-mac-{}-{rotulo}-{unico}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let alvo = dir.join("arquivo.bin");
        std::fs::write(&alvo, conteudo).unwrap();
        alvo
    }

    /// Monta a chave de node exatamente como o upload do `megalib` montaria
    /// para este conteudo — usando as funcoes dele, nao as daqui.
    fn node_key_para(conteudo: &[u8], key: [u8; 16], nonce: [u8; 8]) -> [u8; 32] {
        let mut mac_iv = [0u8; 16];
        mac_iv[..8].copy_from_slice(&nonce);
        mac_iv[8..].copy_from_slice(&nonce);

        let mut chunk_macs = Vec::new();
        let mut offset = 0u64;
        let mut indice = 0usize;
        let total = conteudo.len() as u64;
        while offset < total {
            let tamanho = chunk_size(indice, offset, total);
            let fim = offset + tamanho;
            chunk_macs.push(chunk_mac_calculate(
                &conteudo[offset as usize..fim as usize],
                &key,
                &mac_iv,
            ));
            offset = fim;
            indice += 1;
        }
        let meta = meta_mac_calculate(&chunk_macs, &key);
        pack_node_key(&key, &nonce, &meta)
    }

    const CHAVE: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    const NONCE: [u8; 8] = [0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8];

    /// O teste que prende as duas pontas: o que o `pack_node_key` do `megalib`
    /// empacota ao subir, este modulo confere ao baixar.
    #[test]
    fn accepts_content_whose_mac_matches_the_packed_key() {
        for tamanho in [1usize, 15, 16, 17, 1000] {
            let conteudo: Vec<u8> = (0..tamanho).map(|i| (i % 251) as u8).collect();
            let alvo = arquivo_com("confere", &conteudo);
            let node_key = node_key_para(&conteudo, CHAVE, NONCE);
            assert_eq!(
                verify_file(&alvo, &node_key, conteudo.len() as u64).unwrap(),
                MacCheck::Match,
                "tamanho {tamanho} recusado"
            );
        }
    }

    /// A regressao. Um byte trocado no meio nao muda tamanho nenhum.
    #[test]
    fn rejects_content_with_a_single_flipped_byte() {
        let conteudo: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        let node_key = node_key_para(&conteudo, CHAVE, NONCE);

        let mut corrompido = conteudo.clone();
        corrompido[2500] ^= 0x01;
        assert_eq!(corrompido.len(), conteudo.len(), "o teste perdeu o sentido");

        let alvo = arquivo_com("corrompido", &corrompido);
        assert!(matches!(
            verify_file(&alvo, &node_key, conteudo.len() as u64).unwrap(),
            MacCheck::Mismatch { .. }
        ));
    }

    /// Arquivo grande o bastante para passar da oitava fronteira de chunk, onde
    /// a progressao troca de crescente para 1 MB fixo.
    #[test]
    fn covers_more_than_eight_chunks() {
        let total = 128 * 1024 * 40; // 5 MB: passa dos oito chunks crescentes
        let conteudo: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        let node_key = node_key_para(&conteudo, CHAVE, NONCE);
        let alvo = arquivo_com("grande", &conteudo);
        assert_eq!(
            verify_file(&alvo, &node_key, total as u64).unwrap(),
            MacCheck::Match
        );

        let mut corrompido = conteudo.clone();
        // Byte no ultimo chunk, justamente o trecho que uma retomada regravaria.
        let ultimo = total - 10;
        corrompido[ultimo] ^= 0xff;
        let alvo = arquivo_com("grande-corrompido", &corrompido);
        assert!(matches!(
            verify_file(&alvo, &node_key, total as u64).unwrap(),
            MacCheck::Mismatch { .. }
        ));
    }

    /// A progressao de chunk do MEGA, valor por valor. Errar aqui daria MAC
    /// divergente em arquivo intacto.
    #[test]
    fn follows_the_mega_chunk_progression() {
        let enorme = u64::MAX;
        for indice in 0..8usize {
            assert_eq!(
                chunk_size(indice, 0, enorme),
                (indice as u64 + 1) * 128 * 1024,
                "chunk {indice}"
            );
        }
        assert_eq!(chunk_size(8, 0, enorme), 1024 * 1024);
        assert_eq!(chunk_size(99, 0, enorme), 1024 * 1024);
        // Truncado no fim do arquivo.
        assert_eq!(chunk_size(0, 0, 100), 100);
        assert_eq!(chunk_size(0, 90, 100), 10);
    }

    /// Desdobrar a chave tem que devolver o que foi empacotado.
    #[test]
    fn unpacks_what_pack_node_key_packed() {
        let meta = [
            0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24, 0x31, 0x32, 0x33, 0x34, 0x41, 0x42,
            0x43, 0x44,
        ];
        let node_key = pack_node_key(&CHAVE, &NONCE, &meta);
        let desdobrada = MegaFileKey::from_node_key(&node_key);
        assert_eq!(desdobrada.key, CHAVE, "chave AES nao voltou");
        assert_eq!(desdobrada.nonce, NONCE, "nonce nao voltou");
        assert_eq!(
            desdobrada.meta_mac,
            fold_meta_mac(&meta),
            "meta-MAC reduzido nao voltou"
        );
    }
}
