//! A2: ponta a ponta com o binário de verdade contra um servidor WebDAV local.
//! O servidor corta a conexão no meio do primeiro download; o `resume` continua do
//! ponto exato (Range) e um segundo `resume` não baixa nada de novo.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct Servidor {
    arquivos: HashMap<String, Vec<u8>>,
    pedidos: Mutex<Vec<String>>,
    cortar_primeiro: AtomicBool,
}

fn dados(tamanho: usize, semente: u8) -> Vec<u8> {
    (0..tamanho)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(semente))
        .collect()
}

fn propfind(servidor: &Servidor, caminho: &str) -> String {
    let mut respostas = vec![format!(
        "<d:response><d:href>{caminho}</d:href><d:propstat><d:prop><d:displayname>x</d:displayname>\
         <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>"
    )];
    let mut pastas = std::collections::BTreeSet::new();
    for (arquivo, conteudo) in &servidor.arquivos {
        let Some(resto) = arquivo.strip_prefix(caminho) else {
            continue;
        };
        match resto.split_once('/') {
            Some((pasta, _)) => {
                pastas.insert(pasta.to_string());
            }
            None => respostas.push(format!(
                "<d:response><d:href>{arquivo}</d:href><d:propstat><d:prop><d:displayname>{resto}</d:displayname>\
                 <d:getcontentlength>{}</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>",
                conteudo.len()
            )),
        }
    }
    for pasta in pastas {
        respostas.push(format!(
            "<d:response><d:href>{caminho}{pasta}/</d:href><d:propstat><d:prop><d:displayname>{pasta}</d:displayname>\
             <d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>"
        ));
    }
    format!(
        "<?xml version=\"1.0\"?><d:multistatus xmlns:d=\"DAV:\">{}</d:multistatus>",
        respostas.concat()
    )
}

fn atender(servidor: &Servidor, mut stream: TcpStream) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut leitor = BufReader::new(stream.try_clone().unwrap());
    let mut linha = String::new();
    if leitor.read_line(&mut linha).unwrap_or(0) == 0 {
        return;
    }
    let mut partes = linha.split_whitespace();
    let metodo = partes.next().unwrap_or("").to_string();
    let caminho = partes.next().unwrap_or("/").replace("%20", " ");
    let mut faixa = None;
    let mut corpo = 0;
    loop {
        let mut cabecalho = String::new();
        if leitor.read_line(&mut cabecalho).unwrap_or(0) == 0 || cabecalho == "\r\n" {
            break;
        }
        let minusculo = cabecalho.to_ascii_lowercase();
        if let Some(valor) = minusculo.strip_prefix("range: bytes=") {
            faixa = valor.trim().trim_end_matches('-').parse::<usize>().ok();
        }
        if let Some(valor) = minusculo.strip_prefix("content-length: ") {
            corpo = valor.trim().parse().unwrap_or(0);
        }
    }
    let mut descarte = vec![0; corpo];
    leitor.read_exact(&mut descarte).ok();
    servidor
        .pedidos
        .lock()
        .unwrap()
        .push(format!("{metodo} {caminho} {faixa:?}"));
    if metodo == "PROPFIND" {
        let xml = propfind(servidor, &caminho);
        let _ = write!(
            stream,
            "HTTP/1.1 207 Multi-Status\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{xml}",
            xml.len()
        );
        return;
    }
    let Some(conteudo) = servidor.arquivos.get(&caminho) else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        return;
    };
    match faixa {
        Some(inicio) => {
            let _ = write!(
                stream,
                "HTTP/1.1 206 Partial Content\r\ncontent-range: bytes {inicio}-{}/{}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                conteudo.len() - 1,
                conteudo.len(),
                conteudo.len() - inicio
            );
            let _ = stream.write_all(&conteudo[inicio..]);
        }
        None => {
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                conteudo.len()
            );
            if servidor.cortar_primeiro.swap(false, Ordering::SeqCst) {
                // Metade do arquivo e a conexão cai, como numa rede que falha.
                let _ = stream.write_all(&conteudo[..conteudo.len() / 2]);
                let _ = stream.flush();
                return;
            }
            let _ = stream.write_all(conteudo);
        }
    }
}

fn subir(servidor: Arc<Servidor>) -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let parar = Arc::new(AtomicBool::new(false));
    let parar_thread = Arc::clone(&parar);
    thread::spawn(move || {
        while !parar_thread.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    atender(&servidor, stream);
                }
                Err(_) => thread::sleep(Duration::from_millis(5)),
            }
        }
    });
    (base, parar)
}

fn aditi(cwd: &Path, args: &[&str]) -> Output {
    let inicio = Instant::now();
    let saida = Command::new(env!("CARGO_BIN_EXE_aditi"))
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        inicio.elapsed() < Duration::from_secs(60),
        "aditi demorou demais"
    );
    saida
}

fn arquivos_em(raiz: &Path) -> Vec<PathBuf> {
    let mut achados = Vec::new();
    let mut pilha = vec![raiz.to_path_buf()];
    while let Some(dir) = pilha.pop() {
        for entrada in std::fs::read_dir(&dir).unwrap() {
            let caminho = entrada.unwrap().path();
            if caminho.is_dir() {
                pilha.push(caminho);
            } else {
                achados.push(caminho);
            }
        }
    }
    achados
}

#[test]
fn a2_pasta_webdav_corta_retoma_e_confere() {
    let a = dados(300_000, 7);
    let b = dados(70_000, 91);
    let servidor = Arc::new(Servidor {
        arquivos: HashMap::from([
            ("/dav/pasta/a.bin".to_string(), a.clone()),
            ("/dav/pasta/sub/b c.bin".to_string(), b.clone()),
        ]),
        pedidos: Mutex::new(Vec::new()),
        cortar_primeiro: AtomicBool::new(true),
    });
    let (base, parar) = subir(Arc::clone(&servidor));
    let dir = std::env::temp_dir().join(format!("aditi-a2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("{base}/dav/pasta/");
    let out = dir.join("out");
    let out_txt = out.to_string_lossy().to_string();

    // 1. Primeiro download: a conexão cai no meio do primeiro arquivo.
    let primeira = aditi(
        &dir,
        &[
            "start",
            "--job-id",
            "a2",
            "--url",
            &url,
            "--out",
            &out_txt,
            "--json-events",
        ],
    );
    assert!(
        !primeira.status.success(),
        "o corte deveria fazer o start falhar"
    );

    // 2. Resume: continua do ponto exato do corte com Range.
    let antes = servidor.pedidos.lock().unwrap().len();
    let segunda = aditi(&dir, &["resume", "--job-id", "a2", "--json-events"]);
    assert!(
        segunda.status.success(),
        "resume falhou: {}",
        String::from_utf8_lossy(&segunda.stderr)
    );
    let pedidos_resume: Vec<String> = servidor.pedidos.lock().unwrap()[antes..].to_vec();
    assert!(
        pedidos_resume
            .iter()
            .any(|p| p == &format!("GET /dav/pasta/a.bin Some({})", a.len() / 2)),
        "o resume deveria pedir a.bin a partir de {}: {pedidos_resume:?}",
        a.len() / 2
    );
    let eventos = String::from_utf8_lossy(&segunda.stdout).to_string();
    assert!(
        eventos.contains("\"finished\""),
        "sem evento finished: {eventos}"
    );

    // 3. Conteúdo idêntico, byte a byte (SHA-256).
    let baixados = arquivos_em(&out.join("a2"));
    let mut conferidos = 0;
    for (nome, original) in [("a.bin", &a), ("b c.bin", &b)] {
        let caminho = baixados
            .iter()
            .find(|p| p.file_name().unwrap().to_string_lossy() == nome)
            .unwrap_or_else(|| panic!("{nome} não foi baixado: {baixados:?}"));
        assert_eq!(
            Sha256::digest(std::fs::read(caminho).unwrap()),
            Sha256::digest(original),
            "{nome} diferente do original"
        );
        conferidos += 1;
    }
    assert!(
        !baixados
            .iter()
            .any(|p| p.to_string_lossy().ends_with(".aditi.part")),
        "sobrou arquivo parcial: {baixados:?}"
    );

    // 4. Resume de novo: nada é baixado outra vez.
    let antes = servidor.pedidos.lock().unwrap().len();
    let terceira = aditi(&dir, &["resume", "--job-id", "a2", "--json-events"]);
    assert!(terceira.status.success());
    let gets: Vec<String> = servidor.pedidos.lock().unwrap()[antes..]
        .iter()
        .filter(|p| p.starts_with("GET"))
        .cloned()
        .collect();
    assert!(gets.is_empty(), "rebaixou arquivo já completo: {gets:?}");

    parar.store(true, Ordering::SeqCst);
    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "A2: corte no meio de a.bin, resume pediu a partir de {} bytes, {conferidos} arquivos com SHA-256 igual, \
         segundo resume sem nenhum GET",
        a.len() / 2
    );
}
