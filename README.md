# Aditi Engine

Aditi é um motor headless em Rust para adquirir arquivos de cloud drives. Ele preserva o download legado de arquivos MEGA e fornece `probe`, `start` e `resume` com manifests e eventos NDJSON para os providers suportados.

## Estado na v0.1.0

| Origem | Estado |
|---|---|
| MEGA (arquivo e pasta públicos) | estável |
| Google Drive (público ou com token) | estável |
| WebDAV (qualquer servidor HTTP(S) com PROPFIND) | estável; provado de ponta a ponta com servidor local, corte e retomada |
| OneDrive / SharePoint e Dropbox | **experimental**: funciona com token, sem teste contra o serviço real |
| Google Photos e TeraBox | bloqueados pela API dos próprios serviços |

O estado detalhado dos providers está em [PROVIDERS.md](PROVIDERS.md). TeraBox público permanece intencionalmente bloqueado: não há scraper nem fluxo baseado em cookie.

## Fluxos

- **MEGA legado**: download concorrente por chunks, descriptografia AES-CTR e rotação de proxy para HTTP 509.
- **Cloud drives**: `probe` descreve a origem; `start` grava `source.aditi.json`, `job.aditi.json` e o registry local; `resume` continua o job existente.

## Compilação

```bash
cargo build --release
```

O binário fica em `target/release/aditi`.

## CLI legada MEGA

```bash
aditi --url "https://mega.nz/file/ABCDEFGH#chaveDeAcesso" --proxies "proxies.txt" --threads 16 --out "C:\Downloads\meu_arquivo.zip"
```

- `-u, --url <URL>`: link público de arquivo MEGA.
- `-p, --proxies <FILE>`: arquivo de proxies, suportado somente para download de arquivo MEGA.
- `-o, --out <PATH>`: arquivo de destino; se omitido, Aditi grava no diretório de execução usando o nome remoto.
- `-t, --threads <NUMBER>`: chunks concorrentes; padrão `8`.
- `--force-proxy`: exige proxy em todos os chunks MEGA.
- `--json`: emite logs `tracing` em JSON no modo legado.

## CLI de providers

```bash
aditi probe --url "https://..." --json
aditi start --job-id fotos-01 --url "https://..." --out "C:\Downloads" --json-events
aditi resume --job-id fotos-01 --json-events
```

`start` cria `DIR/JOB_ID/source.aditi.json`, `DIR/JOB_ID/job.aditi.json` e `.aditi/jobs/JOB_ID.json`. `resume` reutiliza esse estado. Com `--json-events`, o stdout emite NDJSON de `started`, `progress`, `file_finished`, `verified`, `retry`, `error` e `finished`.

## Varredura de pastas

A listagem recursiva do Google Drive percorre a arvore **em largura, um nivel por rodada**, com as chamadas de cada nivel em paralelo (8 simultaneas). A versao anterior descia em profundidade e gastava uma ida e volta a API por pasta, o que fazia o tempo crescer com o numero total de pastas — arvores largas, como um acervo separado por marca ou por categoria, ficavam impraticaveis.

Toda chamada a `files.list` e `files.get` passa por recuo exponencial em `429` e `5xx`. Sem isso, paralelizar apenas trocaria lentidao por erro de cota: o limite do Google e por segundo. `401` e `403` nao sao repetidos — repetir nao resolve falta de permissao e so queima cota.

Pastas ja visitadas nao sao revisitadas, o que protege contra ciclo. A lista final sai ordenada por caminho, para que o manifesto seja estavel entre execucoes.

## Integridade de conteudo

Cada `FileRecord` do manifesto carrega um campo `integrity` opcional com os checksums que a **origem declarou**. Google Drive fornece `md5Checksum`, `sha1Checksum` e `sha256Checksum`; os tres sao pedidos na mesma chamada e o que vier e gravado.

Durante o download, o checksum e calculado em fluxo, sem reler o arquivo, e conferido no fim. Arquivo divergente **nao vira destino**: o `.part` e descartado e o erro nomeia o algoritmo, o valor esperado e o obtido. Quando confere, sai um evento `verified` com o algoritmo usado.

O cabecalho `x-goog-hash` da propria resposta tambem e lido, o que da um `crc32c` de graca mesmo em download publico sem token — util para pegar corrupcao em transito, que a verificacao por tamanho nao ve.

Ausencia de checksum nao e divergencia. Arquivos nativos do Google (Docs, Sheets) e atalhos nao tem conteudo binario no Drive e por isso nao tem checksum: nesses casos vale a verificacao por tamanho, que continua valendo para todos os providers.

O motor **reporta**; nao decide. Quem consome o NDJSON escolhe o que fazer com uma divergencia.

Para fontes autenticadas, passe o token pela flag do provider ou pela variável de ambiente indicada em [PROVIDERS.md](PROVIDERS.md): Google Drive, Google Photos, OneDrive/SharePoint ou Dropbox.

## OAuth nativo

Registre um aplicativo no provider e execute `aditi auth onedrive|google-drive|dropbox --client-id ID`. Para Google Drive, acrescente `--client-secret SECRET` emitido pela console; Aditi o usa somente nessa execução e não persiste credenciais. Google Photos compartilhado continua bloqueado pela API atual do provider.

## Integração

Aditi pode ser chamado por qualquer orquestrador como subprocesso. Consuma o stdout NDJSON de `start` ou `resume` para acompanhar progresso, arquivos concluídos, retries e erro final.

*Construído para performance, retomada segura e cloud drives sem wrapper de rclone.*
