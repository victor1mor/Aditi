# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/en/1.0.0/); versões seguem
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-18

Primeira versão publicada.

### Adicionado

- Download de cloud drives sem wrapper de rclone: MEGA (arquivo e pasta públicos), Google Drive (público ou com
  token) e WebDAV. OneDrive/SharePoint e Dropbox como experimentais.
- `probe`, `start` e `resume` com manifesto (`job.aditi.json`) e eventos NDJSON (`started`, `progress`,
  `file_finished`, `verified`, `retry`, `error`, `finished`).
- Retomada pelo arquivo `.aditi.part` com `Range`; arquivo já completo não é baixado de novo.
- Conferência de integridade: checksums declarados pela origem (Google Drive: MD5, SHA-1, SHA-256 e `x-goog-hash`),
  meta-MAC do MEGA e tamanho em todos os providers. Arquivo divergente não vira destino.
- Login OAuth nativo (`aditi auth onedrive|google-drive|dropbox`).

### Segurança

- Dependências sem alertas no `cargo audit` na data da versão. `megalib` fixo em 0.11.0 (a 0.11.1 embute SQLite
  com o RUSTSEC-2026-0459).
- Rust fixo em 1.99.1; CI em Linux, Windows e macOS com ações presas por SHA.

### Limites

- Testado de ponta a ponta contra um servidor WebDAV local. MEGA e Google Drive são cobertos por testes de
  unidade, não por teste automático contra os serviços reais.
- Google Photos compartilhado e links públicos do TeraBox não são suportados (limite das APIs dos serviços).
- Aditi só baixa; não envia arquivos.
