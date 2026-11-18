# Aditi provider plan

Ponytail rule: keep each provider useful before making it fancy. No rclone wrapper.

## Priority providers

1. MEGA
2. Google Drive
3. Microsoft OneDrive / SharePoint
4. Dropbox
5. Google Photos
6. TeraBox

## Current status

| Provider | Status | Auth | Notes |
|---|---|---|---|
| MEGA | active | public links | File/folder, chunk resume for file, manifests/events. |
| Google Drive | active | public file or `auth google-drive --client-id ID` | File/folder via Drive API, native Google docs export to PDF. |
| WebDAV | active | none, basic or bearer (`ADITI_WEBDAV_*`) | Any HTTP(S) server with PROPFIND; file and folder; resume with Range. |
| OneDrive / SharePoint | experimental | `auth onedrive --client-id ID` | Shared links via Microsoft Graph, file/folder probe and download. |
| Dropbox | experimental | `auth dropbox --client-id ID` | Shared links via Dropbox API, file/folder probe and download. |
| Google Photos | blocked upstream | Google Photos Picker redesign required | A API atual so permite midias criadas pelo proprio app; links/album compartilhados nao sao um fluxo suportado. |
| TeraBox | blocked for shared links | TeraBox Open Platform OAuth | Official API supports app-authorized files, but public share download requires `sekey` from the `BOXCLND` cookie; no cookie/scraping flow or WebDAV fallback. |

## TeraBox spike verdict

- The official [Open Platform](https://www.terabox.com/integrations/docs?lang=en) has OAuth, file listing, metadata and download APIs.
- It requires a registered `client_id`/`client_secret` and only exposes the application-assigned folder.
- Public `terabox.com/s/...` links require the `sekey` value from `BOXCLND`; keep them unsupported until TeraBox offers a token-only share flow.

## OAuth nativo

Registre um aplicativo no provider: Aditi nao embute nem persiste credenciais. O Google Drive recebe o `client_secret` emitido pela console somente na execucao; o comando abre o navegador, conclui o fluxo e imprime o `ADITI_*_TOKEN` para uso na sessao atual do PowerShell.

```powershell
aditi auth onedrive --client-id SEU_APP_ID
aditi auth google-drive --client-id SEU_CLIENT_ID --client-secret SEU_CLIENT_SECRET
aditi auth dropbox --client-id SEU_APP_KEY
```

Google Drive e Dropbox usam `http://127.0.0.1:53682/callback`; cadastre esse redirect URI no aplicativo Dropbox. Para outra porta, use `--redirect-port` e cadastre a mesma URI. O OneDrive abre o device code e copia o codigo para a area de transferencia, sem callback local.

Para links compartilhados do OneDrive com conta Microsoft pessoal, conceda a permissao **delegada** `Files.ReadWrite`; o Microsoft Graph nao aceita `Files.Read.All` nesse endpoint.

`--token-out CAMINHO` e opcional: use apenas para integrar outro processo e apague o arquivo depois, pois ele contem o token.

## Provider done checklist

- Parse common share URLs.
- Probe file/folder before download.
- Report `total_files` and `total_bytes`.
- Preserve folder tree.
- Start/resume with existing manifest model.
- Emit NDJSON events: `started`, `progress`, `file_finished`, `retry`, `error`, `finished`.
- Keep CLI old MEGA mode working.
- Leave one small parser/logic test.
