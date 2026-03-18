# RFID Print Service

## Overview
RFID Print Service is a Node.js/Express API that validates Entra bearer tokens, resolves lot and inventory data from Dataverse, sends label jobs to BarTender, and uploads supporting documents into SharePoint/OpDocs. The service keeps its station/template mappings in `mappings.json` under `CONFIG_DIR`, and it now exposes operational health and metrics endpoints for monitoring.

## Setup
1. Install dependencies:
   ```bash
   npm install
   ```
2. Copy `.env.example` to `.env` and populate the required values.
3. Ensure `PRINTSVC_CONFIG_DIR` contains `mappings.json`.
4. Ensure `BARTENDER_TEMPLATE_DIR` contains the referenced `.btw` templates.
5. Start the service with access to BarTender, Dataverse, and SharePoint.

## Required env vars
### Core service
- `PORT` - HTTP port to bind.
- `TENANT_ID` - Entra tenant used to validate incoming bearer tokens.
- `API_AUDIENCE` - Expected JWT audience.
- `REQUIRED_SCOPE` - Required delegated scope for protected endpoints.
- `BARTENDER_ACTIONS_URL` - BarTender Actions REST endpoint.
- `BUILD_TAG` - Build/version string returned by health and metrics endpoints.

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
{ "ok": true }
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
Protected RFID print endpoint. Accepts a lot identifier/number, station, and box range, reads inventory rows from Dataverse, resolves the BarTender template from `mappings.json`, and sends label jobs to BarTender. Existing print behavior is preserved, including dry-run mode, missing-box handling, Dataverse print logging, and lock protection against duplicate concurrent prints.

## Notes
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

## Monitoring guidance
- External monitoring should call `GET /health/deep`.
- Alert if `GET /health` fails twice in a row or if `GET /health/deep` returns `ok=false`.
- Alert if there are no successful prints in 30 minutes during active operating hours.
- Alert if 3 or more print failures occur within 15 minutes.
