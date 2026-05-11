# Emergency Offline Printing

Emergency Offline Printing is a local-only fallback hosted by the PrintSvc server. It lets plant users browse to the PrintSvc machine on the LAN and print RFID labels when internet access, Dataverse, Entra/MSAL, Graph, Cloudflare, or other cloud dependencies are unavailable.

The offline path does not call Dataverse, Graph, Entra, Cloudflare, or external CDNs. It uses the local `mappings.json` station/template configuration and the existing BarTender print helper.

## URLs

- User print page: `http://<printsvc-host>:<port>/offline`
- Admin page: `http://<printsvc-host>:<port>/offline/admin`
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

Dry run validates the payload, resolves the printer/template, and returns the first/last RFID preview without calling BarTender.

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

The first version sends only the existing BarTender named data source fields:

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
