# Emergency Offline Printing

Emergency Offline Printing is a local-only fallback hosted by the PrintSvc server. It lets plant users browse to the PrintSvc machine on the LAN and print RFID labels when internet access, Dataverse, Entra/MSAL, Graph, Cloudflare, or other cloud dependencies are unavailable.

The offline path does not call Dataverse, Graph, Entra, Cloudflare, or external CDNs. It uses the local `mappings.json` station/template configuration and the configured RFID print engine. `PRINT_ENGINE=bartender` keeps the existing BarTender helper. `PRINT_ENGINE=zpl` uses the direct-ZPL mapping when one exists.

## URLs

- User print page: `http://<printsvc-host>:<port>/offline`
- Admin page: `http://<printsvc-host>:<port>/offline/admin`
- Print Health page: `http://<printsvc-host>:<port>/offline/print-health`
- Template Lab page: `http://<printsvc-host>:<port>/offline/template-lab`
- Status API: `GET /api/offline/status`
- Print API: `POST /api/offline/print-labels`

All `/offline` and `/api/offline` routes are enforced server-side as local/LAN only.

## Required Environment Variables

Add these values on the PrintSvc machine:

```env
OFFLINE_PRINT_ADMIN_PASSWORD=
OFFLINE_PRINT_SESSION_SECRET=
OFFLINE_PRINT_STATE_FILE=C:\PrintAgent\offline-print-state.json
OFFLINE_PRINT_AUDIT_FILE=C:\PrintAgent\offline-print-audit.ndjson
OFFLINE_PRINT_MAX_LABELS=99
OFFLINE_PRINT_MAX_BOX_NUMBER=99
OFFLINE_PRINT_ALLOWED_HOSTS=localhost,127.0.0.1
PRINT_ENGINE=bartender
ZPL_DUPLICATE_POLICY=skip_recent
ZPL_SOCKET_MODE=per_label
ZPL_MAX_LABELS_PER_CONNECTION=50
ZPL_SOCKET_IDLE_CLOSE_MS=30000
ZPL_BATCH_MAX_LABELS=60
ZPL_BATCH_COLLECT_MS=1500
ZPL_BATCH_INTER_BATCH_DELAY_MS=0
ZPL_BATCH_MAX_BYTES=524288
```

`OFFLINE_PRINT_ADMIN_PASSWORD` and `OFFLINE_PRINT_SESSION_SECRET` must be set before admin login will work. Do not commit real secrets.

`OFFLINE_PRINT_ALLOWED_HOSTS` is only for extra local hostnames or IPs that should be trusted. The default behavior already allows localhost and private IPv4 LAN clients. Public/Cloudflare host access is rejected unless explicitly allowed.

## Enable Or Disable

1. Open `/offline/admin` from the PrintSvc machine or plant LAN.
2. Log in with the offline admin password.
3. Enter the admin name.
4. Set the state to enabled or disabled.
5. When enabling, enter a nonblank emergency reason such as `Internet outage`.
6. Save the state.

Offline printing is disabled by default if the state file does not exist.

## Dry Run

1. Open `/offline`.
2. Enter station, family, lot number, box range, operator, reason, and confirmation.
3. Click `Dry Run`.

Dry run validates the payload, resolves the printer/template, and returns the first/last RFID preview without sending a print job. In `PRINT_ENGINE=zpl`, it also renders the template without sending it and returns a `zplPreview` summary instead of full ZPL.

## Printing

When enabled, the print page loops from `firstBox` to `lastBox` in ascending order. RFIDs are generated as:

```text
{lotNumber}-B{two digit box number}
```

Example:

```text
PL123456-B01
PL123456-B02
```

For direct-ZPL mode, the resolved RFID/EPC source value must be exactly 12 printable ASCII characters. Invalid RFIDs are rejected before printing and logged as `print_validation_error`.

The first version sends only the existing BarTender/direct-ZPL label fields:

```json
{
  "lot": "PL123456",
  "firstbox": "1",
  "RFID": "PL123456-B01",
  "pounds": "_",
  "po": "PO12345",
  "prodname": "ABS Regrind",
  "color": "BLACK",
  "type": "ABS",
  "tolling": "",
  "erp": "OFFLINE"
}
```

Direct-ZPL emergency mode is enabled only by `DIRECT_ZPL_ENABLED_SCOPES`. The default, confirmed production path is `P1:RAW`; emergency RAW and FG scopes are available for P1-P8 when explicitly listed. P3 extrusion sample scopes are also available when listed. Any unlisted station/family remains unsupported and fails clearly.

```text
RAW P1: 192.168.50.239:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl
RAW P2: 192.168.50.241:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P3: 192.168.50.223:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P4: 192.168.50.242:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P5: 192.168.50.244:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P6: 192.168.6.240:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P7: 192.168.8.200:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
RAW P8: 192.168.7.122:9100, C:\RFID\zpl\RFID-RAW-P1.template.zpl unless station-specific template is configured
FG P1: 192.168.50.239:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl
FG P2: 192.168.50.241:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
FG P3: 192.168.50.223:9100, C:\RFID\zpl\RFID-FG-P3.template.zpl
FG P4: 192.168.50.242:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
FG P5: 192.168.50.244:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
FG P6: 192.168.6.240:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
FG P7: 192.168.8.200:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
FG P8: 192.168.7.122:9100, C:\RFID\zpl\RFID-FG-P1.template.zpl emergency generic template
P3 SAMPLE: Zebra ZT230 P3 EXT, 192.168.50.218:9100, C:\RFID\zpl\QCSample-P3.template.zpl, no RFID encoding
P3 RETAIN: Zebra ZT230 P3 EXT, 192.168.50.218:9100, C:\RFID\zpl\QCRetain-P3.template.zpl, no RFID encoding
P3 SAMPLE_POUNDS: Zebra ZT230 P3 EXT, 192.168.50.218:9100, C:\RFID\zpl\QCSamplePounds-P3.template.zpl, no RFID encoding
```

Enable all RAW, FG, and P3 sample labels only when needed:

```powershell
[Environment]::SetEnvironmentVariable("DIRECT_ZPL_ENABLED_SCOPES", "P1:RAW,P2:RAW,P3:RAW,P4:RAW,P5:RAW,P6:RAW,P7:RAW,P8:RAW,P1:FG,P2:FG,P3:FG,P4:FG,P5:FG,P6:FG,P7:FG,P8:FG,P3:SAMPLE,P3:RETAIN,P3:SAMPLE_POUNDS", "Machine")
Restart-Service RFID-PrintSvc-BarTender
```

The direct-ZPL templates are intentionally native-ZPL emergency layouts. P1/P3 FG use station-specific visual PRN proofs; P2/P4/P5/P6/P7/P8 FG use the P1 emergency FG template until station-specific PRN proofs are converted. P3 sample/retain templates use the available P3 visual PRN proofs, are P3-only, print to the Zebra ZT230 P3 EXT printer, and are visible sample/retain labels only with no RFID encoding unless explicitly added later. Large BarTender bitmap graphics are omitted, bitmap QR is not used, and a small static PRI logo is restored only in the RFID RAW/FG templates with inline `^GFA`. Native ZPL QR is included where practical and encodes the lot number only. Native barcode structures from the FG PRNs are preserved where dynamic-safe; the old ERP barcode slot is used for the RFID text barcode in the emergency FG templates. RAW P1-P8 currently share the P1 emergency RAW template, so visual layout is generic and may not match each station's old BarTender template. Station-specific RAW, FG, and sample ZPL templates should be created later. FG-only fields that PrintSvc does not already resolve render blank in the emergency template.

After any BarTender PRN-to-template visual conversion, prefer the repo-root `Deploy-ZPL-Templates.bat` script for template-only deployment. It validates the required `.\zpl\*.template.zpl` files, creates `C:\RFID\zpl`, copies only ZPL template files, leaves PRN proof files and BarTender `.btw` files untouched, restarts `RFID-PrintSvc-BarTender` only after a successful copy, then displays `/health` and `/api/print/zpl-queue`.

Use `/offline/template-lab` for template preview, browser-based tuning, and proof printing. The page is arranged with controls/sample data/profile tuning on the left and preview image, QR/RFID/logo/payload/printer badges, fitted-field debug, rendered ZPL, and status output on the right. The browser controls can tune whole-label scale/offset, QR position/magnification, static-logo position/size, boxed color/material/tolling font and box settings, product-description placement/fit, and optional field anchor positions. Profile JSON can be exported, copied, downloaded, or saved as lab-only overrides under `ZPL_TEMPLATE_LAB_PROFILE_PATH`. Proof sends require `confirmTestPrint:true`, go through `/api/print/template-test-send`, bypass the production queue, and do not claim physical print confirmation.

When `ZPL_PREVIEW_RENDERER_URL` is not configured, Template Lab uses a built-in approximate SVG preview so operators still get a visual label canvas. The fallback preview supports the emergency subset of ZPL used by these templates: `^PW`, `^FO`, `^FT`, `^GB`, `^A0N`, `^FB`, `^FD/^FS`, `^BQN`, simple `^B3` barcode placeholders, `^FR`, and static `^GFA` logo placeholders. Unsupported commands are listed in preview metadata and ignored for preview only; proof/production printing still sends the actual rendered ZPL.

For scriptable preview without touching the production queue:

```powershell
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52 -ProfileOverridesPath .\rendered\P1_RAW.template-lab-profile.json
Tools\Preview-ZPL-Template.ps1 -Template RFID-RAW-P1.template.zpl -ProfileKey P1:RAW -LotNumber PT000086 -BoxNumber 52 -PrinterIp 192.168.50.239 -Send
```

Calibration profile tuning lives in `lib/zplProfiles.js`, with lab-only saved overrides in `ZPL_TEMPLATE_LAB_PROFILE_PATH` (default `C:\PrintSvc\template-lab-profiles.json`). Profiles expose label dots, DPI, scale/offset placeholders, field-fit overrides, product-description fit/position settings, QR settings, logo placement/size, and optional field position overrides for preview/test rendering. Production print execution does not automatically apply these lab overrides. Use the exported JSON or saved lab profile as the working notes for a later template deployment.

Tolling is conditional in the direct-ZPL templates. If the tolling field is blank, the black background box and reverse/white tolling text are omitted. If tolling has a value, the existing black/reversed field is rendered.

Direct-ZPL bypasses Windows spooler and uses a persistent PrintSvc queue under `ZPL_QUEUE_DIR`, defaulting to `C:\PrintSvc\queue`. ERP or offline requests may submit one box at a time or a range; PrintSvc queues one JSON item per label and one worker per printer sends enabled direct-ZPL labels FIFO.

Defaults are `ZPL_TCP_TIMEOUT_MS=120000`, `ZPL_LABEL_SPACING_MS=8000`, `ZPL_CONNECT_RETRY_COUNT=0`, `ZPL_CONNECT_RETRY_DELAY_MS=3000`, `ZPL_STALE_SENDING_THRESHOLD_MS=120000`, `ZPL_DUPLICATE_POLICY=skip_recent`, `ZPL_SOCKET_MODE=per_label`, `ZPL_MAX_LABELS_PER_CONNECTION=50`, `ZPL_SOCKET_IDLE_CLOSE_MS=30000`, `ZPL_BATCH_MAX_LABELS=60`, `ZPL_BATCH_COLLECT_MS=1500`, `ZPL_BATCH_INTER_BATCH_DELAY_MS=0`, and `ZPL_BATCH_MAX_BYTES=524288`. If a timeout happens after a TCP connection is established and bytes may have been written, or if startup recovers a stale `sending` item without proof that no write started, the queue item is marked `unknown_after_send`, the printer queue pauses, and operators must verify the physical label before resuming.

`ZPL_SOCKET_MODE=per_label` preserves the existing direct-ZPL transport: one TCP 9100 connection per label. `ZPL_SOCKET_MODE=persistent` is an opt-in diagnostic mode for investigating printer backpressure; it keeps one socket open per printer queue, sends one label at a time, waits `ZPL_LABEL_SPACING_MS`, reuses the socket until the queue drains, then closes it after `ZPL_SOCKET_IDLE_CLOSE_MS` or after `ZPL_MAX_LABELS_PER_CONNECTION`. Persistent mode does not change RFID validation, duplicate policy, queue ordering, or BarTender rollback.

`ZPL_SOCKET_MODE=batch` is an opt-in diagnostic mode for sending multiple rendered labels as one TCP 9100 stream. In batch mode PrintSvc waits up to `ZPL_BATCH_COLLECT_MS`, collects FIFO queued labels for the same printer up to `ZPL_BATCH_MAX_LABELS`, concatenates the rendered ZPL, opens one TCP connection, sends the full stream, and closes it. Batch mode does not wait `ZPL_LABEL_SPACING_MS` between labels inside a batch; `ZPL_BATCH_INTER_BATCH_DELAY_MS` applies only between separate batch streams. This may help determine whether repeated raw-port connections contribute to the observed label 8 and 15/16 pauses. `sent_to_printer` still means TCP acceptance only, and canceling an accepted batch is printer-side behavior, not Windows Spooler behavior.

Direct-ZPL duplicate behavior is configurable. The default `skip_recent` policy skips a same station/lot/box/RFID label requested within 10 minutes after TCP send acceptance with `ok:true` and `skippedDuplicate:true`; unsafe/admin retry paths still reject duplicates. PRI operations may set `ZPL_DUPLICATE_POLICY=allow` when duplicate reprints are acceptable for damaged labels and duplicate EPC scans are handled operationally. In `allow` mode, PrintSvc enqueues the requested label normally and logs `zpl_duplicate_allowed`.

Use local/admin-safe queue endpoints for review and resume:

```powershell
curl.exe http://localhost:7079/api/print/zpl-queue
curl.exe -X POST http://localhost:7079/api/print/zpl-queue/resume -H "Content-Type: application/json" -d "{\"station\":\"P1\",\"operator\":\"<name>\",\"note\":\"Verified physical label/RFID before resuming\"}"
curl.exe -X POST http://localhost:7079/api/print/zpl-queue/retry-failed -H "Content-Type: application/json" -d "{\"itemId\":\"<failed_before_send itemId>\",\"operator\":\"<name>\",\"note\":\"Confirmed bytesSent=0/writeStarted=false\"}"
```

During a suspected 3-minute printer pause, run `curl.exe http://localhost:7079/api/print/zpl-queue` from the PrintSvc server. Queue status shows each printer as `sending`, `waiting_between_labels`, `failed_before_send`, `unknown_after_send`, `paused`, or `idle`; it also includes socket mode and active socket state when available. `ETIMEDOUT` or `ECONNREFUSED` before write points to printer/network/raw-port unavailability before send. `unknown_after_send` means bytes may have reached the printer, so operators must verify physical output before resuming.

To compare per-label, persistent, and batch TCP behavior on a known-good printer and a problem printer, change only `ZPL_SOCKET_MODE`, restart PrintSvc, run the same small controlled batch, and inspect `zpl_socket_open`, `zpl_socket_reuse`, `zpl_socket_close`, `zpl_socket_error`, `zpl_send_timing`, `zpl_batch_start`, `zpl_batch_send_success`, and `zpl_batch_send_error` logs. Keep `per_label` as the default unless an experiment is intentionally active.

The read-only Print Health page at `/offline/print-health` is the local QHealth dashboard for direct-ZPL operations. It uses `/health`, `/api/print/zpl-queue`, and `/api/print/logs` to show print engine, socket mode, duplicate policy, batch settings, enabled scopes, queue path, paused printer keys, printer queue cards, active/recent batches, review-required items, recent errors, and filterable logs. `/api/print/logs` accepts `tail`, `event`, `level`, `station`, `family`, `lotNumber`, `printerIp`, and `search` query parameters and redacts common secret/token/password/cookie fields.

`sent_to_printer` means TCP send was accepted by the printer connection. It is not proof that the label physically printed or encoded.

Operators must verify missing or unknown labels before retrying. In dire need, print small batches and watch for missing, delayed, duplicate, or out-of-order labels.

Visible ZPL field values remove control characters plus `^` and `~`. Emergency label caps are: lot 24, box 8, RFID 12, pounds 12, material type 24, color 24, PO 32, product code 24, product name 48, product description 48, tolling 16, ERP 16, QR data 96. Boxed color/material/tolling fields use fitted placeholders that render one centered line, shrink the font by length, and truncate to 8 visible characters without hyphens or ellipses.

## Reconciliation

After internet and Dataverse access return:

1. Open the audit log at `OFFLINE_PRINT_AUDIT_FILE`.
2. Review successful `offline_print_label` records.
3. Match each `lotNumber`, `station`, `firstBox`/`lastBox`, `box`, `rfid`, operator, and reason against the physical labels printed.
4. Enter or correct the corresponding Dataverse inventory and print-log records according to the plant reconciliation process.
5. Disable offline printing from `/offline/admin` when emergency use is complete.

## Audit Log

The default audit file is:

```text
C:\PrintAgent\offline-print-audit.ndjson
```

It writes one JSON object per line. Print attempts include timestamp, event type, source IP, host, operator, reason, station, family, printer, template, lot number, box range, requested count, printed count, named data source values, success/failure, and error details when applicable.

Admin password and signed cookies are never logged.

## Deployment And Restart Notes

- Deploy the code normally when ready.
- Set the offline environment variables on the PrintSvc machine.
- Restart PrintSvc so the server picks up the new environment.
- Confirm `GET /health` still works.
- Confirm `/api/offline/status` is reachable from localhost or the LAN and blocked from public/Cloudflare access.
- Keep the state file and audit file on a local path that survives service restarts.
