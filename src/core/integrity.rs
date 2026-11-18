//! Integridade de conteudo declarada pela origem e calculada localmente.
//!
//! O motor nao decide o que fazer com uma divergencia: ele registra o que a
//! origem declarou, calcula o que chegou, e reporta. A politica fica com quem
//! orquestra.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Checksums de um arquivo.
///
/// Todo campo e opcional: nenhuma origem oferece todos, e algumas nao oferecem
/// nenhum. Um `Integrity` vazio significa "a origem nao declarou nada", nunca
/// "o arquivo esta errado".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Integrity {
    /// MD5 em hexadecimal minusculo. Google Drive: `md5Checksum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    /// SHA-1 em hexadecimal minusculo. Google Drive: `sha1Checksum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha1: Option<String>,
    /// SHA-256 em hexadecimal minusculo. Google Drive: `sha256Checksum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// CRC32C em base64, no formato do cabecalho `x-goog-hash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
}

/// Algoritmos que o motor sabe calcular, do mais forte para o mais fraco.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    Sha256,
    Sha1,
    Md5,
    Crc32c,
}

impl Algorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Sha1 => "sha1",
            Self::Md5 => "md5",
            Self::Crc32c => "crc32c",
        }
    }
}

/// Resultado de comparar o declarado com o calculado.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// A origem nao declarou nenhum checksum comparavel.
    NoReference,
    /// Todos os algoritmos declarados bateram. Traz o mais forte conferido.
    Match(Algorithm),
    /// Um algoritmo declarado divergiu.
    Mismatch {
        algorithm: Algorithm,
        expected: String,
        actual: String,
    },
}

impl Integrity {
    pub fn is_empty(&self) -> bool {
        self.md5.is_none() && self.sha1.is_none() && self.sha256.is_none() && self.crc32c.is_none()
    }

    /// O algoritmo mais forte que a origem declarou, se houver.
    pub fn strongest(&self) -> Option<(Algorithm, &str)> {
        if let Some(v) = self.sha256.as_deref() {
            return Some((Algorithm::Sha256, v));
        }
        if let Some(v) = self.sha1.as_deref() {
            return Some((Algorithm::Sha1, v));
        }
        if let Some(v) = self.md5.as_deref() {
            return Some((Algorithm::Md5, v));
        }
        self.crc32c.as_deref().map(|v| (Algorithm::Crc32c, v))
    }

    /// Compara este conjunto (o declarado) com o calculado localmente.
    ///
    /// Somente algoritmos presentes em `self` sao conferidos. O que a origem nao
    /// declarou nao pode divergir. Para na primeira divergencia, do mais forte
    /// para o mais fraco.
    pub fn check(&self, computed: &Self) -> Check {
        let pares = [
            (Algorithm::Sha256, &self.sha256, &computed.sha256),
            (Algorithm::Sha1, &self.sha1, &computed.sha1),
            (Algorithm::Md5, &self.md5, &computed.md5),
            (Algorithm::Crc32c, &self.crc32c, &computed.crc32c),
        ];
        let mut conferido = None;
        for (algoritmo, declarado, calculado) in pares {
            let (Some(declarado), Some(calculado)) = (declarado, calculado) else {
                continue;
            };
            if declarado.eq_ignore_ascii_case(calculado) {
                if conferido.is_none() {
                    conferido = Some(algoritmo);
                }
            } else {
                return Check::Mismatch {
                    algorithm: algoritmo,
                    expected: declarado.clone(),
                    actual: calculado.clone(),
                };
            }
        }
        match conferido {
            Some(algoritmo) => Check::Match(algoritmo),
            None => Check::NoReference,
        }
    }
}

/// Calcula checksums enquanto os bytes passam, sem reler o arquivo.
///
/// Liga somente o que foi pedido: hashear tres algoritmos num arquivo de 300 MB
/// custa CPU que nao se paga se a origem so declarou um.
pub struct Hasher {
    md5: Option<Md5>,
    sha1: Option<Sha1>,
    sha256: Option<Sha256>,
    crc32c: Option<u32>,
}

impl Hasher {
    /// Liga apenas os algoritmos que `reference` declarou.
    ///
    /// Se `reference` estiver vazio, nada e calculado e `finish` devolve vazio.
    pub fn for_reference(reference: &Integrity) -> Self {
        Self {
            md5: reference.md5.as_ref().map(|_| Md5::new()),
            sha1: reference.sha1.as_ref().map(|_| Sha1::new()),
            sha256: reference.sha256.as_ref().map(|_| Sha256::new()),
            crc32c: reference.crc32c.as_ref().map(|_| 0u32),
        }
    }

    /// Liga todos os algoritmos. Usado quando se quer inventariar um arquivo
    /// local sem referencia previa.
    pub fn all() -> Self {
        Self {
            md5: Some(Md5::new()),
            sha1: Some(Sha1::new()),
            sha256: Some(Sha256::new()),
            crc32c: Some(0),
        }
    }

    /// Verdadeiro quando nenhum algoritmo esta ligado, para o chamador poder
    /// pular o trabalho por completo.
    pub fn is_noop(&self) -> bool {
        self.md5.is_none() && self.sha1.is_none() && self.sha256.is_none() && self.crc32c.is_none()
    }

    pub fn update(&mut self, bytes: &[u8]) {
        if let Some(h) = self.md5.as_mut() {
            h.update(bytes);
        }
        if let Some(h) = self.sha1.as_mut() {
            h.update(bytes);
        }
        if let Some(h) = self.sha256.as_mut() {
            h.update(bytes);
        }
        if let Some(c) = self.crc32c.as_mut() {
            *c = crc32c::crc32c_append(*c, bytes);
        }
    }

    pub fn finish(self) -> Integrity {
        Integrity {
            md5: self.md5.map(|h| hex(&h.finalize())),
            sha1: self.sha1.map(|h| hex(&h.finalize())),
            sha256: self.sha256.map(|h| hex(&h.finalize())),
            crc32c: self.crc32c.map(|c| BASE64.encode(c.to_be_bytes())),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Le o cabecalho `x-goog-hash` e extrai o que ele declarar.
///
/// O Google manda algo como `crc32c=hQK9kg==` ou
/// `crc32c=hQK9kg==, md5=1B2M2Y8AsgTpgAmY7PhCfg==`. O md5 vem em base64 nesse
/// cabecalho, nao em hexadecimal como na API — por isso e reconvertido.
pub fn parse_goog_hash(value: &str) -> Integrity {
    let mut out = Integrity::default();
    for parte in value.split(',') {
        let parte = parte.trim();
        // o valor em base64 pode terminar com '=', por isso o corte e feito
        // pela primeira ocorrencia e o resto e tomado inteiro
        let Some((chave, _)) = parte.split_once('=') else {
            continue;
        };
        let valor = &parte[chave.len() + 1..];
        match chave.trim().to_ascii_lowercase().as_str() {
            "crc32c" => out.crc32c = Some(valor.trim().to_string()),
            "md5" => {
                if let Ok(bruto) = BASE64.decode(valor.trim()) {
                    out.md5 = Some(hex(&bruto));
                }
            }
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const VAZIO_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";
    const ABC_MD5: &str = "900150983cd24fb0d6963f7d28e17f72";
    const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn calcula_md5_e_sha256_conhecidos() {
        let mut h = Hasher::all();
        h.update(b"abc");
        let got = h.finish();
        assert_eq!(got.md5.as_deref(), Some(ABC_MD5));
        assert_eq!(got.sha256.as_deref(), Some(ABC_SHA256));
    }

    #[test]
    fn hasheia_em_pedacos_igual_a_uma_vez() {
        let mut inteiro = Hasher::all();
        inteiro.update(b"abc");
        let mut partido = Hasher::all();
        partido.update(b"a");
        partido.update(b"bc");
        assert_eq!(inteiro.finish(), partido.finish());
    }

    #[test]
    fn entrada_vazia_produz_o_md5_do_vazio() {
        assert_eq!(Hasher::all().finish().md5.as_deref(), Some(VAZIO_MD5));
    }

    #[test]
    fn liga_somente_o_que_a_origem_declarou() {
        let referencia = Integrity {
            md5: Some(ABC_MD5.to_string()),
            ..Default::default()
        };
        let mut h = Hasher::for_reference(&referencia);
        h.update(b"abc");
        let got = h.finish();
        assert_eq!(got.md5.as_deref(), Some(ABC_MD5));
        assert!(got.sha256.is_none(), "sha256 nao foi pedido");
        assert!(got.crc32c.is_none(), "crc32c nao foi pedido");
    }

    #[test]
    fn referencia_vazia_nao_calcula_nada() {
        let h = Hasher::for_reference(&Integrity::default());
        assert!(h.is_noop());
        assert!(h.finish().is_empty());
    }

    #[test]
    fn confere_quando_bate_e_reporta_o_mais_forte() {
        let referencia = Integrity {
            md5: Some(ABC_MD5.to_string()),
            sha256: Some(ABC_SHA256.to_string()),
            ..Default::default()
        };
        let mut h = Hasher::for_reference(&referencia);
        h.update(b"abc");
        assert_eq!(
            referencia.check(&h.finish()),
            Check::Match(Algorithm::Sha256)
        );
    }

    #[test]
    fn acusa_divergencia_com_o_valor_esperado_e_o_obtido() {
        let referencia = Integrity {
            md5: Some(ABC_MD5.to_string()),
            ..Default::default()
        };
        let mut h = Hasher::for_reference(&referencia);
        h.update(b"abd");
        match referencia.check(&h.finish()) {
            Check::Mismatch {
                algorithm,
                expected,
                actual,
            } => {
                assert_eq!(algorithm, Algorithm::Md5);
                assert_eq!(expected, ABC_MD5);
                assert_ne!(actual, ABC_MD5);
            }
            outro => panic!("esperava Mismatch, veio {outro:?}"),
        }
    }

    #[test]
    fn hexadecimal_compara_sem_diferenciar_caixa() {
        let referencia = Integrity {
            md5: Some(ABC_MD5.to_ascii_uppercase()),
            ..Default::default()
        };
        let mut h = Hasher::for_reference(&referencia);
        h.update(b"abc");
        assert_eq!(referencia.check(&h.finish()), Check::Match(Algorithm::Md5));
    }

    #[test]
    fn sem_referencia_nao_e_divergencia() {
        let calculado = Hasher::all().finish();
        assert_eq!(Integrity::default().check(&calculado), Check::NoReference);
    }

    #[test]
    fn strongest_prefere_sha256_e_cai_para_crc32c() {
        let cheio = Integrity {
            md5: Some("a".into()),
            sha1: Some("b".into()),
            sha256: Some("c".into()),
            crc32c: Some("d".into()),
        };
        assert_eq!(cheio.strongest(), Some((Algorithm::Sha256, "c")));
        let so_crc = Integrity {
            crc32c: Some("d".into()),
            ..Default::default()
        };
        assert_eq!(so_crc.strongest(), Some((Algorithm::Crc32c, "d")));
        assert_eq!(Integrity::default().strongest(), None);
    }

    #[test]
    fn le_o_cabecalho_x_goog_hash_com_crc32c_e_md5() {
        let got = parse_goog_hash("crc32c=hQK9kg==, md5=1B2M2Y8AsgTpgAmY7PhCfg==");
        assert_eq!(got.crc32c.as_deref(), Some("hQK9kg=="));
        assert_eq!(got.md5.as_deref(), Some(VAZIO_MD5));
    }

    #[test]
    fn le_o_cabecalho_com_apenas_crc32c() {
        let got = parse_goog_hash("crc32c=hQK9kg==");
        assert_eq!(got.crc32c.as_deref(), Some("hQK9kg=="));
        assert!(got.md5.is_none());
    }

    #[test]
    fn ignora_cabecalho_malformado_sem_entrar_em_panico() {
        assert!(parse_goog_hash("").is_empty());
        assert!(parse_goog_hash("lixo-sem-igual").is_empty());
        assert!(parse_goog_hash("sha999=abc").is_empty());
    }

    #[test]
    fn o_crc32c_calculado_casa_com_o_que_o_google_declara() {
        // Valor observado no cabecalho x-goog-hash do proprio Google para
        // conteudo vazio, garantindo que a codificacao base64 big-endian bate.
        let mut h = Hasher::all();
        h.update(b"");
        assert_eq!(h.finish().crc32c.as_deref(), Some("AAAAAA=="));
    }

    #[test]
    fn integrity_vazio_nao_aparece_no_json() {
        let json = serde_json::to_string(&Integrity::default()).unwrap();
        assert_eq!(json, "{}");
    }
}
