# RFID Print Service

## Overview
RFID Print Service is a Node.js/Express API that validates Entra bearer tokens, resolves lot and inventory data from Dataverse, sends label jobs to BarTender, and uploads supporting documents into SharePoint/OpDocs. The service keeps its station/template mappings in `mappings.json` under `CONFIG_DIR`, and it now exposes operational health and metrics endpoints for monitoring.

It also includes a local-only Emergency Offline Printing mode. During an internet or Dataverse outage, plant users can browse directly to the PrintSvc machine on the LAN and print RFID labels without Entra/MSAL, Dataverse, Graph, Cloudflare, or external CDN/assets. Offline printing is disabled by default and must be enabled by a local admin.

## Setup
1. Install dependencies:
   ```bash
   npm install
   ```
2. Copy `.env.example` to `.env` and populate the required values.
3. Ensure `PRINTSVC_CONFIG_DIR` contains `mappings.json`.
4. Ensure `BARTENDER_TEMPLATE_DIR` contains the referenced `.btw` templates.
5. For direct-ZPL emergency mode, install the ZPL templates under `C:\RFID\zpl`. After BarTender PRN-to-template visual changes, prefer `Deploy-ZPL-Templates.bat` from the repo root for template-only deployment.
6. Start the service with access to BarTender, Dataverse, and SharePoint.

## Required env vars
### Core service
- `PORT` - HTTP port to bind.
- `TENANT_ID` - Entra tenant used to validate incoming bearer tokens.
- `API_AUDIENCE` - Expected JWT audience.
- `REQUIRED_SCOPE` - Required delegated scope for protected endpoints.
- `BARTENDER_ACTIONS_URL` - BarTender Actions REST endpoint.
- `BUILD_TAG` - Build/version string returned by health and metrics endpoints.
- `PRINT_ENGINE` - `bartender` keeps the existing BarTender/Windows spooler path. `zpl` sends RFID labels directly to mapped Zebra printers over TCP 9100. Default: `bartender`.
- `DEBUG_ZPL` - Set to `true` only when troubleshooting rendered direct-ZPL output. Default: `false`.
- `DIRECT_ZPL_ENABLED_SCOPES` - Comma-separated direct-ZPL station/family scopes. Default: `P1:RAW`. Emergency RAW and FG scopes are available for `P1` through `P8` when explicitly listed.
- `ZPL_QUEUE_DIR` - Persistent direct-ZPL queue directory. Default: `C:\PrintSvc\queue`.
- `ZPL_STALE_SENDING_THRESHOLD_MS` - How long a persisted `sending` item can remain unresolved before startup/status recovery marks it safe/unknown. Default: `120000`.
- `ZPL_TCP_TIMEOUT_MS` - Direct-ZPL TCP send timeout. Default: `120000`.
- `ZPL_LABEL_SPACING_MS` - Delay between direct-ZPL RFID labels from the PrintSvc queue worker. Default: `8000`.
- `ZPL_CONNECT_RETRY_COUNT` - Retry count for failures before any bytes are written. Default: `0`.
- `ZPL_CONNECT_RETRY_DELAY_MS` - Delay before retrying pre-write TCP failures. Default: `3000`.
- `ZPL_DUPLICATE_POLICY` - Direct-ZPL duplicate handling. `skip_recent` skips/rejects recent duplicate RFID labels; `allow` permits intentional duplicate reprints for damaged labels. Default: `skip_recent`.
- `ZPL_SOCKET_MODE` - Direct-ZPL TCP socket mode. `per_label` opens/sends/closes one connection per label and remains the default. `persistent` reuses one socket per printer while its queue has work for controlled diagnostics. `batch` sends multiple rendered queued labels as one TCP stream for controlled diagnostics. Default: `per_label`.
- `ZPL_MAX_LABELS_PER_CONNECTION` - Maximum labels sent before a persistent ZPL socket is closed and reopened. Default: `50`.
- `ZPL_SOCKET_IDLE_CLOSE_MS` - Idle time before an unused persistent ZPL socket is closed. Default: `30000`.
- `ZPL_BATCH_MAX_LABELS` - Maximum labels included in one direct-ZPL batch stream. Default: `60`.
- `ZPL_BATCH_COLLECT_MS` - How long a batch-mode worker waits for related queued labels before sending. Default: `1500`.
- `ZPL_BATCH_INTER_BATCH_DELAY_MS` - Delay between batch TCP streams when more labels remain queued after a batch. Default: `0`.
- `ZPL_BATCH_MAX_BYTES` - Safety cap for one concatenated ZPL batch stream. Default: `524288`.
- `ZPL_PREVIEW_RENDERER_URL` - Optional local ZPL image renderer endpoint for Template Lab previews. If unset or unavailable, Template Lab still returns rendered ZPL and metadata.
- `ZPL_TEMPLATE_LAB_PROFILE_PATH` - Local JSON path for saved Template Lab tuning overrides. Default: `C:\PrintSvc\template-lab-profiles.json`.

### File and path configuration
- `PRINTSVC_CONFIG_DIR` - Directory containing `mappings.json`.
- `BARTENDER_TEMPLATE_DIR` - Directory containing BarTender `.btw` templates.

### BarTender auth
- `BT_REST_USER` - Username for BarTender REST basic auth.
- `BT_REST_PASSWORD` - Password for BarTender REST basic auth.

### Dataverse integration
- `DV_TENANT_ID`
- `DV_CLIENT_ID`
- `DV_CLIENT_SECRET`
- `DV_URL_DEV`
- `DV_URL_PROD`
- `DV_LOT_ENTITYSET`
- `DV_INVENTORY_ENTITYSET`
- `DV_PRINTLOG_ENTITYSET`
- `DV_LOTNUMBER_COL`
- `DV_INV_LOTLOOKUP_COL`
- `DV_INV_BOX_COL`
- `DV_INV_ID_COL`
- `DV_INV_RFID_COL`
- `DV_INV_WEIGHT_COL`
- `DV_PRINTLOG_PRINTEDBY_COL`
- `DV_PRINTLOG_PRINTEDON_COL`
- `DV_PRINTLOG_RESULT_COL`
- `DV_PRINTLOG_RFIDTEXT_COL`
- `DV_PRINTLOG_NOTES_COL`
- `DV_PRINTLOG_STATION_COL`
- `DV_PRINTLOG_LOT_NAV`
- `DV_PRINTLOG_INVENTORY_NAV`

### SharePoint / Microsoft Graph integration
- `SP_TENANT_ID`
- `SP_CLIENT_ID`
- `SP_CLIENT_SECRET`
- `SP_HOSTNAME`
- `SP_SITE_PATH`

### Emergency offline printing
- `OFFLINE_PRINT_ADMIN_PASSWORD` - Local admin password. Leave unset to disable admin login.
- `OFFLINE_PRINT_SESSION_SECRET` - Secret used to sign the short-lived offline admin cookie.
- `OFFLINE_PRINT_STATE_FILE` - Local JSON state file. Default: `C:\PrintAgent\offline-print-state.json`.
- `OFFLINE_PRINT_AUDIT_FILE` - Local NDJSON audit file. Default: `C:\PrintAgent\offline-print-audit.ndjson`.
- `OFFLINE_PRINT_MAX_LABELS` - Maximum labels per offline request. Default: `99`.
- `OFFLINE_PRINT_MAX_BOX_NUMBER` - Maximum allowed offline box number. Default: `99`.
- `OFFLINE_PRINT_ALLOWED_HOSTS` - Comma-separated extra local hostnames/IPs allowed for offline routes.

## Run instructions
### Development / local run
```bash
npm start
```

The service listens on `0.0.0.0:$PORT`.

## Endpoints
### `GET /health`
Basic liveness endpoint. Returns:
```json
{
  "ok": true,
  "build": "2026-05-05-transformation-label-printing",
  "printEngine": "bartender",
  "directZplPilotScopes": [{ "station": "P1", "family": "RAW" }],
  "zplQueueEnabled": true,
  "zplQueuePath": "C:\\PrintSvc\\queue",
  "zplDuplicatePolicy": "skip_recent",
  "zplSocketMode": "per_label",
  "zplMaxLabelsPerConnection": 50,
  "zplSocketIdleCloseMs": 30000,
  "zplBatchMaxLabels": 60,
  "zplBatchCollectMs": 1500,
  "zplBatchInterBatchDelayMs": 0,
  "zplBatchMaxBytes": 524288
}
```

### `GET /health/deep`
Deep health endpoint for monitors and dashboards. Returns build metadata, dependency checks, and the latest successful print timestamp when Dataverse print logs are available.

Example response:
```json
{
  "ok": true,
  "build": "2026-03-18-monitoring",
  "checks": {
    "server": "ok",
    "mappings": "ok",
    "bartender": "ok",
    "dataverse": "ok",
    "sharepoint": "ok"
  },
  "lastSuccessfulPrintUtc": "2026-03-18T14:02:11Z"
}
```

If a dependency check fails, the endpoint returns `ok: false`, marks the failing check as `fail`, and includes a concise top-level `errors` object.

### `GET /metrics/summary`
Read-only summary metrics endpoint backed by Dataverse print logs. Returns:
- `build`
- `serverTimeUtc`
- `lastPrintSuccessUtc`
- `lastPrintFailureUtc`
- `successCount15m`
- `successCount60m`
- `failureCount15m`
- `failureCount60m`
- `activePrintJobsCount`

### `POST /api/print`
Protected RFID print endpoint. Accepts a lot identifier/number, station, and box range, reads inventory rows from Dataverse, resolves the existing BarTender printer/template mapping, and then prints using the configured `PRINT_ENGINE`. Existing BarTender behavior is preserved when `PRINT_ENGINE` is missing or set to `bartender`, including dry-run mode, missing-box handling, Dataverse print logging, and lock protection against duplicate concurrent prints.

### `GET /offline`
Local-only Emergency Offline Printing page. The page uses only local HTML/CSS/JS assets served by PrintSvc. It shows whether offline printing is enabled, accepts station, family, lot, range, material/color/product fields, operator, reason, and reconciliation confirmation, and provides dry-run and print actions.

### `GET /offline/admin`
Local-only admin page used to log in and enable or disable emergency offline printing. Enabling requires a nonblank reason. State is persisted to `OFFLINE_PRINT_STATE_FILE`, and toggle events are written to the local audit log.

### `GET /offline/print-health`
Local-only Print Health / QHealth dashboard. It reuses `/health`, `/api/print/zpl-queue`, and `/api/print/logs` to show direct-ZPL runtime settings, enabled scopes, printer queue cards, batch state, paused queues, review-required labels, recent errors, and searchable logs. It is read-only and does not change print behavior.

### `GET /offline/template-lab`
Local-only Direct-ZPL Template Lab. It uses a two-column tuning workflow: controls and sample data on the left, preview image/metadata/debug/rendered ZPL on the right. It renders approved repo templates with sample data, shows QR/RFID/logo/payload/printer badges, exposes station/template calibration profiles, and provides browser controls for whole-label scale/offset, QR position/magnification, static-logo position/size, boxed field fit, product-description placement/fit, tolling behavior, and optional field anchor overrides. It can export/copy/download profile JSON, save lab-only profile overrides, download rendered ZPL, and send an explicitly confirmed proof print directly to a selected printer. Proof sends bypass the production direct-ZPL queue and do not mark queue items.

### `GET /api/offline/status`
Local-only status endpoint. Returns the offline enabled/disabled state, build tag, max labels, max box number, allowed stations, template families, and current emergency reason. It does not expose secrets.

### `POST /api/offline/print-labels`
Local-only offline RFID print endpoint. It does not require Entra and does not call Dataverse or Graph. It requires offline printing to be enabled, validates the box range and confirmation server-side, generates RFIDs as `{lotNumber}-B{two digit box number}`, and prints once per box through the configured RFID print engine unless `dryRun` is true.

Offline named data sources are intentionally limited to the existing online label fields: `lot`, `firstbox`, `RFID`, `pounds`, `po`, `prodname`, `color`, `type`, `tolling`, and `erp`.

### `GET /api/print/logs`
Local/admin-safe log tail endpoint for the Print Health dashboard. Query params are `tail` (default `500`, max `5000`), `event`, `level`, `station`, `family`, `lotNumber`, `printerIp`, and `search`. It reads the local PrintSvc out log, parses JSON log lines when possible, returns raw redacted lines otherwise, and redacts common secret/token/password/cookie fields.

### `GET /api/print/template-lab/catalog`
Local/admin-safe catalog endpoint for Template Lab. It returns the approved template list and calibration profiles from `lib/zplProfiles.js`.

### `POST /api/print/template-lab/profile`
Local/admin-safe save endpoint for Template Lab tuning overrides. It writes lab-only profile JSON to `ZPL_TEMPLATE_LAB_PROFILE_PATH` and does not change production queue rendering, mappings, scopes, or print execution behavior.

### `GET|POST /api/print/template-preview`
Local/admin-safe preview endpoint. It renders one approved direct-ZPL template using sample data and optional lab profile overrides, then returns rendered ZPL plus metadata: payload bytes, QR command and payload, RFID command presence, fitted-field debug, logo command presence, preview mode, unsupported approximate-preview commands, logo diagnostics, and the selected calibration profile. If `ZPL_PREVIEW_RENDERER_URL` is configured, PrintSvc attempts an external image preview request. If no external renderer is configured, or if the external renderer fails, PrintSvc returns a built-in approximate SVG preview for the ZPL subset used by the emergency templates.

### `POST /api/print/template-test-send`
Local/admin-safe proof-print endpoint. It requires `confirmTestPrint:true`, renders the selected template, and sends the rendered ZPL directly to `printerIp:port`. It does not enqueue production queue items and does not claim physical print confirmation.

## Notes
### Print engines
- `PRINT_ENGINE=bartender` is the normal rollback mode. It uses the existing BarTender Actions REST payload and the Windows printer/spooler path.
- `PRINT_ENGINE=zpl` bypasses BarTender runtime and the Windows print spooler only for scopes listed in `DIRECT_ZPL_ENABLED_SCOPES`. The default is confirmed `P1:RAW`; emergency RAW and FG scopes are mapped for P1-P8, and P3 sample scopes are mapped for extrusion sample labels, but all remain inactive until listed. Any unlisted station/family is rejected with `UNSUPPORTED_DIRECT_ZPL` and logged as `direct_zpl_unsupported_skipped`.
- Current RAW direct-ZPL mappings: `P1=192.168.50.239`, `P2=192.168.50.241`, `P3=192.168.50.223`, `P4=192.168.50.242`, `P5=192.168.50.244`, `P6=192.168.6.240`, `P7=192.168.8.200`, `P8=192.168.7.122`, port `9100`. RAW uses the shared emergency template `C:\RFID\zpl\RFID-RAW-P1.template.zpl` unless a station-specific ZPL template is configured.
- Current FG direct-ZPL mappings use the same station printer IPs as RAW. `P1` uses `C:\RFID\zpl\RFID-FG-P1.template.zpl`; `P3` uses `C:\RFID\zpl\RFID-FG-P3.template.zpl`; `P2/P4/P5/P6/P7/P8` use `C:\RFID\zpl\RFID-FG-P1.template.zpl` as the emergency generic FG template until station-specific templates are created.
- Current P3 sample direct-ZPL mappings use the Zebra ZT230 P3 EXT printer at `192.168.50.218:9100`: `P3:SAMPLE` maps to `C:\RFID\zpl\QCSample-P3.template.zpl`, `P3:RETAIN` maps to `C:\RFID\zpl\QCRetain-P3.template.zpl`, and `P3:SAMPLE_POUNDS` maps to `C:\RFID\zpl\QCSamplePounds-P3.template.zpl`. These sample/retain templates do not encode RFID unless explicitly added later; they are visible sample/retain labels only.
- Direct-ZPL stores one persistent JSON queue record per box under `ZPL_QUEUE_DIR`, defaulting to `C:\PrintSvc\queue`. A PrintSvc worker sends records FIFO through a per-printer queue keyed by printer IP/port, replacing Windows Spooler for enabled direct-ZPL emergency scopes.
- Direct-ZPL API responses return `ok:true`, `queued:true`, and `jobId`/`itemId` once a label is safely queued. That response does not mean the label physically printed.
- The queue worker bypasses Windows spooler and uses conservative pacing: `ZPL_LABEL_SPACING_MS` defaults to `8000` ms for RFID labels.
- Direct-ZPL transport defaults are `ZPL_TCP_TIMEOUT_MS=120000`, `ZPL_CONNECT_RETRY_COUNT=0`, and `ZPL_CONNECT_RETRY_DELAY_MS=3000`. Automatic retry is disabled by default for RFID labels.
- Direct-ZPL socket mode defaults to `ZPL_SOCKET_MODE=per_label`, preserving the existing connect/send/close behavior. `ZPL_SOCKET_MODE=persistent` is an opt-in diagnostic experiment that keeps one TCP 9100 socket open per printer queue, sends labels serially over that socket, and closes it when the queue drains, after `ZPL_SOCKET_IDLE_CLOSE_MS`, or after `ZPL_MAX_LABELS_PER_CONNECTION`.
- `ZPL_SOCKET_MODE=batch` is also opt-in. In batch mode the per-printer worker waits up to `ZPL_BATCH_COLLECT_MS`, renders up to `ZPL_BATCH_MAX_LABELS` queued labels for that printer, concatenates the ZPL, sends the batch as one TCP 9100 stream, and closes the connection. Batch mode does not wait `ZPL_LABEL_SPACING_MS` between labels inside a batch; it uses `ZPL_BATCH_INTER_BATCH_DELAY_MS` only between separate batch streams.
- Batch mode may help diagnose whether repeated raw-port connect/write/close cycles are contributing to printer TCP 9100 backpressure around label 8 or 15/16. A successful batch still means `sent_to_printer` only; it does not confirm physical printing or RFID encoding. Canceling a batch after bytes are accepted is printer-side behavior, not Windows Spooler behavior.
- Socket diagnostics log `zpl_socket_open`, `zpl_socket_reuse`, `zpl_socket_close`, `zpl_socket_error`, `zpl_send_timing`, plus batch-specific `zpl_batch_start`, `zpl_batch_item_included`, `zpl_batch_send_attempt`, `zpl_batch_send_success`, `zpl_batch_send_error`, and `zpl_batch_complete`. `/health` exposes socket and batch settings; `/api/print/zpl-queue` includes socket mode, phase, waiting state, active batch, and active socket state by printer key.
- The read-only `/offline/print-health` dashboard is the local QHealth view for direct-ZPL runtime status. It shows print engine, socket mode, duplicate policy, batch settings, enabled scopes, queue path, paused printer keys, printer queue cards, active/recent batches, review-required items, recent errors, and filterable logs.
- On startup/status, stale persisted `sending` items older than `ZPL_STALE_SENDING_THRESHOLD_MS` are recovered. If the record does not prove no write started, it becomes `unknown_after_send`, the printer queue pauses, and operators must verify the physical label before resuming.
- If a timeout happens after a TCP connection is established and bytes may have been written, the queue item is marked `unknown_after_send`, the printer queue pauses, and operators must verify the physical label before resuming. Use `GET /api/print/zpl-queue` from a local/admin-safe host to inspect state and `POST /api/print/zpl-queue/resume` after review.
- `failed_before_send` items with `writeStarted=false` and `bytesSent=0` are shown as `safeToRetry:true` and may be requeued with `POST /api/print/zpl-queue/retry-failed`. `unknown_after_send` is never safe to auto-retry.
- Accepted direct-ZPL sends are stored as `sent_to_printer`, not physical print confirmation. Success logs include `sendAccepted:true`, `physicalPrintConfirmed:false`, and the note that scanner/operator verification is required.
- Direct-ZPL duplicate behavior is controlled by `ZPL_DUPLICATE_POLICY`. The default `skip_recent` preserves the safety guard: if a same station/lot/box/RFID label is requested again within 10 minutes after TCP send acceptance, the normal print path returns `ok:true`, `skippedDuplicate:true`, and does not enqueue or print it, while unsafe/admin retry paths still reject duplicates as `DUPLICATE_RECENT_ZPL`. PRI operations may set `ZPL_DUPLICATE_POLICY=allow` when intentional duplicate reprints are acceptable for damaged labels; in that mode PrintSvc enqueues the requested label normally and logs `zpl_duplicate_allowed`.
- In `PRINT_ENGINE=zpl`, dry runs render the template without sending it and return a `zplPreview` summary with station, lot, box/label, printer, template path, and rendered byte count. RFID labels include RFID and RFID HEX in that preview; P3 sample labels do not encode RFID. Full rendered ZPL is logged only when `DEBUG_ZPL=true`.
- RFID/EPC source text must be exactly 12 printable ASCII characters before any direct-ZPL label is sent, or the label is rejected. Example: `PT000086-B52` becomes `50543030303038362D423532`.
- If the resolved RFID value is absent, PrintSvc derives it as `{lotNumber}-B{two digit box number}`, matching the current BarTender path.
- If `PRINT_ENGINE=zpl` is selected and the station/family is unsupported or has no direct-ZPL printer/template mapping, PrintSvc returns a clear error instead of silently routing to a different printer.
- Visible ZPL field values remove control characters plus `^` and `~`. To keep the emergency label bounded, visible fields are capped at: lot 24, box 8, RFID 12, pounds 12, material type 24, color 24, PO 32, product code 24, product name 48, product description 48, tolling 16, ERP 16, machine 32, company 48, label type 32, sample type 24, sample/frequency values 32, date 16, QR data 96. Boxed color/material/tolling fields use fitted direct-ZPL placeholders that render one centered line, shrink font by length, and truncate to 8 visible characters without adding hyphens or ellipses.
- BarTender remains useful for layout and template authoring even when emergency direct-ZPL mode is active.
- Emergency limitation: the current direct-ZPL templates are native-ZPL emergency layouts. RAW P1-P8 currently share the P1 emergency RAW template, and FG stations without station-specific templates use the P1 emergency FG template, so visual layout is generic and may not match each station's old BarTender template. P3 sample/retain templates are P3-only and based on available visual PRN proofs. Station-specific RAW, FG, and sample ZPL templates should be created later from printer-specific PRN proofs. The large BarTender PRN bitmap/graphics block is omitted, bitmap QR is not used, and a small static PRI logo is restored only in the RFID RAW/FG templates with inline `^GFA`. Native ZPL QR is included where practical and encodes the lot number only. Native barcodes from the FG PRNs are preserved where dynamic-safe; the old ERP barcode slot is used for the RFID text barcode in the emergency FG templates. FG-only fields that are not already resolved by PrintSvc render blank in the emergency template.
- Tolling blocks are conditional in direct-ZPL templates: blank tolling suppresses the black box and reverse text; nonblank tolling renders the existing black/reversed field.
- In dire need, operators may print small batches, but must watch for missing, delayed, duplicate, or out-of-order labels and verify RFID/EPC before retrying any unknown label.
- Template Lab calibration profiles live in `lib/zplProfiles.js`, with local lab-only saved overrides in `ZPL_TEMPLATE_LAB_PROFILE_PATH`. These profiles expose `scaleX`, `scaleY`, `offsetX`, `offsetY`, label dimensions, field-fit overrides, product-description placement/fit, QR settings, logo placement/size, and optional field position overrides for preview/proof work. Production print rendering does not automatically apply these lab overrides; use the profiles to tune and document printer-specific coordinates before converting template changes into deployed templates or mappings.
- The built-in approximate preview supports the emergency subset of ZPL: `^PW`, `^FO`, `^FT`, `^GB`, `^A0N`, `^FB`, `^FD/^FS`, `^BQN`, simple `^B3` barcode placeholders, `^FR`, and static `^GFA` logo placeholders. Unsupported commands are reported in preview metadata and ignored for preview only. Actual proof/production printing still sends the rendered ZPL.

### Direct-ZPL deployment
From the repository directory on the print server:

For template-only visual updates after converting BarTender Print-to-File PRNs into repo ZPL templates, run:

```cmd
Deploy-ZPL-Templates.bat
```

The script validates the six required repo templates, creates `C:\RFID\zpl` if needed, copies only `.\zpl\*.template.zpl`, leaves PRN proofs and `.btw` files alone, restarts `RFID-PrintSvc-BarTender` only after a successful copy, then displays `/health` and `/api/print/zpl-queue`.

For local template preview without touching the production queue or restarting the service:

```powershell
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52 -ProfileOverridesPath .\rendered\P1_RAW.template-lab-profile.json
```

To send a confirmed proof print through the Template Lab endpoint:

```powershell
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52 -PrinterIp 192.168.50.239 -Send
```

For RAW-only rollback within direct-ZPL mode, keep `DIRECT_ZPL_ENABLED_SCOPES=P1:RAW`. To enable all emergency RAW and Finished Goods stations plus P3 sample labels, use:

```text
P1:RAW,P2:RAW,P3:RAW,P4:RAW,P5:RAW,P6:RAW,P7:RAW,P8:RAW,P1:FG,P2:FG,P3:FG,P4:FG,P5:FG,P6:FG,P7:FG,P8:FG,P3:SAMPLE,P3:RETAIN,P3:SAMPLE_POUNDS
```

```powershell
Copy-Item C:\PrintSvc\queue "C:\PrintSvc\queue.backup-$(Get-Date -Format yyyyMMdd-HHmmss)" -Recurse -ErrorAction SilentlyContinue
npm test
New-Item -ItemType Directory -Force C:\RFID\zpl
New-Item -ItemType Directory -Force C:\PrintSvc\queue
Copy-Item .\zpl\RFID-RAW-P1.template.zpl C:\RFID\zpl\RFID-RAW-P1.template.zpl -Force
Copy-Item .\zpl\RFID-FG-P1.template.zpl C:\RFID\zpl\RFID-FG-P1.template.zpl -Force
Copy-Item .\zpl\RFID-FG-P3.template.zpl C:\RFID\zpl\RFID-FG-P3.template.zpl -Force
Copy-Item .\zpl\QCSample-P3.template.zpl C:\RFID\zpl\QCSample-P3.template.zpl -Force
Copy-Item .\zpl\QCRetain-P3.template.zpl C:\RFID\zpl\QCRetain-P3.template.zpl -Force
Copy-Item .\zpl\QCSamplePounds-P3.template.zpl C:\RFID\zpl\QCSamplePounds-P3.template.zpl -Force
Copy-Item .\mappings.json C:\PrintSvc\mappings.json -Force
[Environment]::SetEnvironmentVariable("PRINT_ENGINE", "zpl", "Machine")
[Environment]::SetEnvironmentVariable("DIRECT_ZPL_ENABLED_SCOPES", "P1:RAW,P2:RAW,P3:RAW,P4:RAW,P5:RAW,P6:RAW,P7:RAW,P8:RAW,P1:FG,P2:FG,P3:FG,P4:FG,P5:FG,P6:FG,P7:FG,P8:FG,P3:SAMPLE,P3:RETAIN,P3:SAMPLE_POUNDS", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_QUEUE_DIR", "C:\PrintSvc\queue", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_STALE_SENDING_THRESHOLD_MS", "120000", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_TCP_TIMEOUT_MS", "120000", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_LABEL_SPACING_MS", "8000", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_CONNECT_RETRY_COUNT", "0", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_CONNECT_RETRY_DELAY_MS", "3000", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_DUPLICATE_POLICY", "allow", "Machine")
Restart-Service RFID-PrintSvc-BarTender
```

Run one-label P1 dry runs first, then remove `dryRun` for the live test. `PT...` is RAW; `PL...` is FG by current lot-prefix mapping:

```powershell
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"lotNumber\":\"PT000086\",\"firstBox\":52,\"lastBox\":52,\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"lotNumber\":\"PT000086\",\"firstBox\":52,\"lastBox\":52}"
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"lotNumber\":\"PL123456\",\"firstBox\":1,\"lastBox\":1,\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"lotNumber\":\"PL123456\",\"firstBox\":1,\"lastBox\":1}"
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P3\",\"lotNumber\":\"PL123456\",\"firstBox\":1,\"lastBox\":1,\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/print -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P3\",\"lotNumber\":\"PL123456\",\"firstBox\":1,\"lastBox\":1}"
curl.exe -X POST http://localhost:7079/api/print/sample-labels -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P3\",\"lotNumber\":\"FF123456\",\"labelKind\":\"sample\",\"boxes\":[12],\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/print/sample-labels -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P3\",\"lotNumber\":\"FF123456\",\"labelKind\":\"retain\",\"boxes\":[12],\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/print/sample-labels -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d "{\"station\":\"P3\",\"lotNumber\":\"FF123456\",\"labelKind\":\"sample\",\"mode\":\"pounds\",\"poundLabels\":[\"5000\"],\"dryRun\":true}"
```

Queue inspection and operator resume after any `unknown_after_send`:

```powershell
curl.exe http://localhost:7079/api/print/zpl-queue
curl.exe -X POST http://localhost:7079/api/print/zpl-queue/resume -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"operator\":\"<name>\",\"note\":\"Verified physical label/RFID before resuming\"}"
curl.exe -X POST http://localhost:7079/api/print/zpl-queue/retry-failed -H "Content-Type: application/json" -d "{\"itemId\":\"<failed_before_send itemId>\",\"operator\":\"<name>\",\"note\":\"Confirmed bytesSent=0/writeStarted=false\"}"
```

During a suspected 3-minute printer pause, run the queue status command above from the PrintSvc server. A printer `phase` of `sending` means PrintSvc is waiting on TCP send/write completion; `waiting_between_labels` means PrintSvc is intentionally pacing before the next item; `failed_before_send` with `ETIMEDOUT`/`ECONNREFUSED` and `writeStarted=false` points to printer/network/port unavailability before send; `unknown_after_send` means bytes may have reached the printer and the operator must verify the physical output before resuming.

Optional persistent-socket comparison, without changing defaults:

```powershell
[Environment]::SetEnvironmentVariable("ZPL_SOCKET_MODE", "persistent", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_MAX_LABELS_PER_CONNECTION", "50", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_SOCKET_IDLE_CLOSE_MS", "30000", "Machine")
Restart-Service RFID-PrintSvc-BarTender
curl.exe http://localhost:7079/health
curl.exe http://localhost:7079/api/print/zpl-queue
```

Optional batch-stream comparison, without changing defaults:

```powershell
[Environment]::SetEnvironmentVariable("ZPL_SOCKET_MODE", "batch", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_BATCH_MAX_LABELS", "60", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_BATCH_COLLECT_MS", "1500", "Machine")
[Environment]::SetEnvironmentVariable("ZPL_BATCH_INTER_BATCH_DELAY_MS", "0", "Machine")
Restart-Service RFID-PrintSvc-BarTender
curl.exe http://localhost:7079/health
curl.exe http://localhost:7079/api/print/zpl-queue
```

Return to the default per-label socket behavior:

```powershell
[Environment]::SetEnvironmentVariable("ZPL_SOCKET_MODE", "per_label", "Machine")
Restart-Service RFID-PrintSvc-BarTender
```

Local emergency smoke test without ERP/Dataverse uses the local offline API after offline printing is intentionally enabled by an admin. Keep it to P1 RAW boxes 1-3:

```powershell
curl.exe -X POST http://localhost:7079/api/offline/print-labels -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"family\":\"RAW\",\"lotNumber\":\"PT000086\",\"firstBox\":1,\"lastBox\":3,\"material\":\"RAW\",\"color\":\"BLACK\",\"purchaseOrder\":\"SMOKE\",\"productDescription\":\"Direct ZPL smoke\",\"pounds\":\"_\",\"operator\":\"Smoke Test\",\"reason\":\"Direct ZPL smoke test\",\"confirmationAccepted\":true,\"dryRun\":true}"
curl.exe -X POST http://localhost:7079/api/offline/print-labels -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"family\":\"FG\",\"lotNumber\":\"PL123456\",\"firstBox\":1,\"lastBox\":1,\"material\":\"FG\",\"color\":\"BLACK\",\"purchaseOrder\":\"SMOKE\",\"productDescription\":\"Direct ZPL FG smoke\",\"pounds\":\"_\",\"operator\":\"Smoke Test\",\"reason\":\"Direct ZPL FG smoke test\",\"confirmationAccepted\":true,\"dryRun\":true}"
```

Rollback:

```powershell
[Environment]::SetEnvironmentVariable("PRINT_ENGINE", "bartender", "Machine")
Restart-Service RFID-PrintSvc-BarTender
```

### BarTender
- Print jobs are sent through `BARTENDER_ACTIONS_URL` using the existing BarTender Actions payload.
- `GET /health/deep` performs a lightweight reachability check that does not submit a print job.
- Template file names remain portable because configured mappings are resolved relative to `BARTENDER_TEMPLATE_DIR`.

### Dataverse logging
- Successful and failed prints continue to write to the Dataverse print log entity.
- `GET /health/deep` and `GET /metrics/summary` use those print logs to derive latest timestamps and recent success/failure counts.
- Structured JSON log events are also emitted to stdout/stderr for easier ingestion by external log processors.

### SharePoint integration
- Uploads still use app-only Graph auth with `Sites.Selected` access.
- `GET /health/deep` verifies SharePoint readiness by acquiring a Graph token and resolving the configured site.
- Document library resolution still honors explicit destination URLs before falling back to doc type/library-name matching.

### Emergency offline reconciliation
- Use `/offline/admin` to disable offline printing after the outage ends.
- Review `OFFLINE_PRINT_AUDIT_FILE`, defaulting to `C:\PrintAgent\offline-print-audit.ndjson`.
- Reconcile successful `offline_print_label` records back into Dataverse using lot number, box, RFID, station, operator, reason, and timestamp.
- Keep the audit file with plant records for traceability.

For the full runbook, see `docs/offline-emergency-printing.md`.

## Monitoring guidance
- External monitoring should call `GET /health/deep`.
- Alert if `GET /health` fails twice in a row or if `GET /health/deep` returns `ok=false`.
- Alert if there are no successful prints in 30 minutes during active operating hours.
- Alert if 3 or more print failures occur within 15 minutes.
