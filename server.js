require("dotenv").config();

const BUILD_TAG = process.env.BUILD_TAG || "2026-03-05-sharepoint-drive-fix";

function isoNow() {
  return new Date().toISOString();
}

function writeStructuredLog(level, event, details = {}, message) {
  const payload = {
    timestamp: isoNow(),
    build: BUILD_TAG,
    level,
    event,
    ...details
  };

  if (message) {
    const fn = level === "error" ? console.error : level === "warn" ? console.warn : console.log;
    fn(message);
  }

  const serialized = JSON.stringify(payload);
  if (level === "error") console.error(serialized);
  else if (level === "warn") console.warn(serialized);
  else console.log(serialized);

  return payload;
}

function logInfo(event, details = {}, message) {
  return writeStructuredLog("info", event, details, message);
}

function logWarn(event, details = {}, message) {
  return writeStructuredLog("warn", event, details, message);
}

function logError(event, details = {}, message) {
  return writeStructuredLog("error", event, details, message);
}

function logEvent(event, details = {}, message) {
  return logInfo(event, details, message);
}

logInfo("service_start", { port: process.env.PORT || null }, `PrintSvc build: ${BUILD_TAG}`);

const express = require("express");
const axios = require("axios");
const jwt = require("jsonwebtoken");
const jwksClient = require("jwks-rsa");
const fs = require("fs");
const path = require("path");
const multer = require("multer");

const CONFIG_DIR = process.env.PRINTSVC_CONFIG_DIR || "C:\\PrintSvc";
const TEMPLATE_DIR = process.env.BARTENDER_TEMPLATE_DIR || "C:\\RFID";
const mappingsPath = path.join(CONFIG_DIR, "mappings.json");

const GRAPH_DRIVE_CACHE_MS = 6 * 60 * 60 * 1000; // 6 hours
const SMALL_UPLOAD_MAX = 4 * 1024 * 1024; // 4 MB or whatever threshold you want

const DV_INV_NOWEIGHT_COL = process.env.DV_INV_NOWEIGHT_COL || "rm_noweightmode";
const PRINT_JOB_SPACING_MS = Number(process.env.PRINT_JOB_SPACING_MS || 1500);

/**
 * =========================
 * Environment Configuration
 * =========================
 */
const {
  PORT,
  TENANT_ID,
  API_AUDIENCE,
  REQUIRED_SCOPE,
  BARTENDER_ACTIONS_URL,

  // Dataverse (server-to-server) auth config (same tenant/app creds for both envs)
  DV_TENANT_ID,
  DV_CLIENT_ID,
  DV_CLIENT_SECRET,

  // Dataverse base URLs (per environment)
  DV_URL_DEV,
  DV_URL_PROD,

  // SharePoint / Graph (app-only) ? Sites.Selected assignment required on target site
  SP_TENANT_ID,
  SP_CLIENT_ID,
  SP_CLIENT_SECRET,
  SP_HOSTNAME,
  SP_SITE_PATH
} = process.env;

if (!PORT || !TENANT_ID || !API_AUDIENCE || !REQUIRED_SCOPE || !BARTENDER_ACTIONS_URL) {
  throw new Error(`Missing required env vars. Check your .env configuration. CONFIG_DIR=${CONFIG_DIR}`);
}

if (!DV_TENANT_ID || !DV_CLIENT_ID || !DV_CLIENT_SECRET || !DV_URL_DEV || !DV_URL_PROD) {
  console.warn("[WARN] Dataverse env vars missing. /print/lot + logging will fail until DV_* vars are configured.");
}

if (!SP_TENANT_ID || !SP_CLIENT_ID || !SP_CLIENT_SECRET || !SP_HOSTNAME || !SP_SITE_PATH) {
  console.warn("[WARN] SharePoint/Graph env vars missing. /api/uploadDocument will fail until SP_* vars are configured.");
}

/**
 * Decide which Dataverse org to use based on request Origin.
 */
function getDvUrlForRequest(req) {
  const origin = String(req.headers.origin || "").toLowerCase();

  if (origin === "https://pridev.crm.dynamics.com") return DV_URL_DEV;
  if (origin === "https://datastream.crm.dynamics.com") return DV_URL_PROD;

  return DV_URL_PROD;
}

/**
 * =========================
 * Mappings (station -> printer/template)
 * =========================
 */
function loadMappingsFile() {
  return JSON.parse(fs.readFileSync(mappingsPath, "utf8"));
}

const mappings = loadMappingsFile();

function getLotFamily(lotNumber) {
  const prefix = (lotNumber || "").trim().substring(0, 2).toUpperCase();
  return mappings.rules?.lotPrefixToLabelFamily?.[prefix] || "RAW";
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function resolveTemplatePath(templateValue) {
  const raw = String(templateValue || "").trim();
  if (!raw) throw new Error("Template mapping is blank.");

  // If mapping already contains a full path, keep only the filename
  // and rebuild using TEMPLATE_DIR so deployment stays portable.
  const fileName = path.basename(raw);

  return path.join(TEMPLATE_DIR, fileName);
}

function resolvePrinterAndTemplate({ station, lotNumber }) {
  const fam = getLotFamily(lotNumber);
  const st = String(station || "").toUpperCase();

  const printer = mappings.stations?.[st]?.printer;
  const templateValue = mappings.templates?.[fam]?.[st];

  if (!printer) throw new Error(`Unknown station/printer mapping for station='${st}'`);
  if (!templateValue) throw new Error(`No template for family='${fam}' station='${st}'`);

  const template = resolveTemplatePath(templateValue);

  return { family: fam, printer, template };
}

/**
 * =========================
 * Tiny throttle helper (future-proof)
 * =========================
 */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * =========================
 * Entra JWT validation for callers
 * =========================
 */
const jwksUri = `https://login.microsoftonline.com/${TENANT_ID}/discovery/v2.0/keys`;
const client = jwksClient({ jwksUri, cache: true, cacheMaxEntries: 5, cacheMaxAge: 10 * 60 * 1000 });

function getKey(header, callback) {
  client.getSigningKey(header.kid, function (err, key) {
    if (err) return callback(err);
    callback(null, key.getPublicKey());
  });
}

function requireBearerToken(req, res, next) {
  const auth = req.headers.authorization || "";
  const match = auth.match(/^Bearer (.+)$/i);
  if (!match) return res.status(401).json({ error: "Missing Bearer token" });
  req.token = match[1];
  next();
}

function requireValidToken(req, res, next) {
  const allowedIssuers = new Set([
    `https://login.microsoftonline.com/${TENANT_ID}/v2.0`,
    `https://login.microsoftonline.com/${TENANT_ID}/v2.0/`,
    `https://sts.windows.net/${TENANT_ID}/`,
    `https://sts.windows.net/${TENANT_ID}`
  ]);

  jwt.verify(
    req.token,
    getKey,
    {
      audience: API_AUDIENCE,
      algorithms: ["RS256"]
    },
    (err, decoded) => {
      if (err) return res.status(401).json({ error: "Invalid token", details: err.message });

      const iss = decoded?.iss || "";
      if (!allowedIssuers.has(iss)) {
        return res.status(401).json({
          error: "Invalid token",
          details: `jwt issuer invalid. got: ${iss}`
        });
      }

      const scp = decoded.scp || "";
      const scopes = scp.split(" ").filter(Boolean);

      if (!scopes.includes(REQUIRED_SCOPE)) {
        return res.status(403).json({
          error: "Insufficient scope",
          required: REQUIRED_SCOPE,
          got: scopes
        });
      }

      req.user = decoded;
      next();
    }
  );
}

/**
 * =========================
 * Dataverse (server-to-server) helpers
 * =========================
 */
const dvTokenCacheByUrl = new Map();

async function getDataverseAccessToken(baseUrl) {
  if (!DV_TENANT_ID || !DV_CLIENT_ID || !DV_CLIENT_SECRET || !baseUrl) {
    throw new Error("Dataverse env vars missing. Set DV_TENANT_ID, DV_CLIENT_ID, DV_CLIENT_SECRET, DV_URL_DEV, DV_URL_PROD.");
  }

  const now = Date.now();
  const cached = dvTokenCacheByUrl.get(baseUrl);
  if (cached?.accessToken && now < cached.expiresAt - 60_000) {
    return cached.accessToken;
  }

  const tokenUrl = `https://login.microsoftonline.com/${DV_TENANT_ID}/oauth2/v2.0/token`;
  const params = new URLSearchParams();
  params.append("client_id", DV_CLIENT_ID);
  params.append("client_secret", DV_CLIENT_SECRET);
  params.append("grant_type", "client_credentials");
  params.append("scope", `${baseUrl}/.default`);

  const r = await axios.post(tokenUrl, params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 20000
  });

  const accessToken = r.data.access_token;
  const expiresIn = Number(r.data.expires_in || 3600);

  dvTokenCacheByUrl.set(baseUrl, {
    accessToken,
    expiresAt: Date.now() + expiresIn * 1000
  });

  return accessToken;
}

async function dvGet(baseUrl, path, extraConfig = {}) {
  const token = await getDataverseAccessToken(baseUrl);
  const url = `${baseUrl}${path}`;
  const r = await axios.get(url, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json", ...(extraConfig.headers || {}) },
    timeout: 30000,
    ...extraConfig
  });
  return r.data;
}

async function dvPost(baseUrl, path, body) {
  const token = await getDataverseAccessToken(baseUrl);
  const url = `${baseUrl}${path}`;
  const r = await axios.post(url, body, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json", "Content-Type": "application/json" },
    timeout: 30000
  });
  return r.data;
}

// =========================
// Entity set names + columns
// =========================
const DV_LOT_ENTITYSET = process.env.DV_LOT_ENTITYSET || "rm_lots";
const DV_INVENTORY_ENTITYSET = process.env.DV_INVENTORY_ENTITYSET || "rm_inventories";
const DV_PRINTLOG_ENTITYSET = process.env.DV_PRINTLOG_ENTITYSET || "rm_printlogs";
const DV_PRODUCT_ENTITYSET = process.env.DV_PRODUCT_ENTITYSET || "rm_products";

const DV_LOTNUMBER_COL = process.env.DV_LOTNUMBER_COL || "rm_lotnumber";
const DV_LOT_PURCHASEORDER_COL = process.env.DV_LOT_PURCHASEORDER_COL || "rm_purchaseorder";
const DV_LOT_PRODUCTLOOKUP_COL = process.env.DV_LOT_PRODUCTLOOKUP_COL || "rm_product";
const DV_LOT_COLORTEXT_COL = process.env.DV_LOT_COLORTEXT_COL || "crb9d_colortext";
const DV_LOT_MATERIALSHORTTEXT_COL = process.env.DV_LOT_MATERIALSHORTTEXT_COL || "rm_materialshorttext";
const DV_LOT_TOLLING_COL = process.env.DV_LOT_TOLLING_COL || "rm_tolling";
const DV_PRODUCT_NAME_COL = process.env.DV_PRODUCT_NAME_COL || "rm_productname";
const DV_PRODUCT_CODE_COL = process.env.DV_PRODUCT_CODE_COL || "rm_productcode";
const DV_PRODUCT_LABELDESCRIPTION_COL = process.env.DV_PRODUCT_LABELDESCRIPTION_COL || "rm_productlabeldescription";

const DV_INV_LOTLOOKUP_COL = process.env.DV_INV_LOTLOOKUP_COL || "rm_lot";
const DV_INV_BOX_COL = process.env.DV_INV_BOX_COL || "rm_box";
const DV_INV_ID_COL = process.env.DV_INV_ID_COL || "rm_inventoryid";
const DV_INV_RFID_COL = process.env.DV_INV_RFID_COL || "rm_rfid";
const DV_INV_WEIGHT_COL = process.env.DV_INV_WEIGHT_COL || "rm_netweight";

function normalizeGuid(id) {
  return String(id || "").replace(/[{}]/g, "").toLowerCase();
}

async function getLotIdByLotNumber(baseUrl, lotNumber) {
  const ln = String(lotNumber || "").trim().replace(/'/g, "''");

  const path =
    `/api/data/v9.2/${DV_LOT_ENTITYSET}` +
    `?$select=rm_lotid,${DV_LOTNUMBER_COL}` +
    `&$filter=${encodeURIComponent(`${DV_LOTNUMBER_COL} eq '${ln}'`)}` +
    `&$top=1`;

  const data = await dvGet(baseUrl, path);
  const row = data?.value?.[0];
  if (!row) throw new Error(`Lot not found for lotNumber='${lotNumber}'`);

  const id = row.rm_lotid || row.rm_lotid?.toString?.();
  if (!id) throw new Error(`Lot found but id missing for lotNumber='${lotNumber}'`);

  return id;
}

async function getLotNumberById(baseUrl, lotId) {
  const id = normalizeGuid(lotId);
  if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error(`Invalid lotId GUID: ${lotId}`);

  const data = await dvGet(baseUrl, `/api/data/v9.2/${DV_LOT_ENTITYSET}(${id})?$select=${DV_LOTNUMBER_COL}`);
  const lotNumber = data?.[DV_LOTNUMBER_COL];
  if (!lotNumber) throw new Error(`Lot found but ${DV_LOTNUMBER_COL} is empty for lotId=${id}`);

  return String(lotNumber);
}

function toPrintString(value) {
  if (value == null) return "";
  return String(value).trim();
}

function isTruthyDataverseBoolean(value) {
  if (value === true || value === 1) return true;
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "true" || normalized === "yes" || normalized === "1";
}

async function getLotLabelData(baseUrl, lotId) {
  const id = normalizeGuid(lotId);
  if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error(`Invalid lotId GUID: ${lotId}`);

  const lotProductLookupValueCol = `_${DV_LOT_PRODUCTLOOKUP_COL}_value`;
  const selectCols = [
    DV_LOT_PURCHASEORDER_COL,
    DV_LOT_COLORTEXT_COL,
    DV_LOT_MATERIALSHORTTEXT_COL,
    DV_LOT_TOLLING_COL,
    lotProductLookupValueCol
  ].join(",");

  const lot = await dvGet(baseUrl, `/api/data/v9.2/${DV_LOT_ENTITYSET}(${id})?$select=${selectCols}`);

  const productId = lot?.[lotProductLookupValueCol];
  let productCode = "";
  let productLabelDescription = "";

  if (productId) {
    const productSelectCols = [
      DV_PRODUCT_CODE_COL,
      DV_PRODUCT_LABELDESCRIPTION_COL
    ].join(",");

    const product = await dvGet(
      baseUrl,
      `/api/data/v9.2/${DV_PRODUCT_ENTITYSET}(${normalizeGuid(productId)})?$select=${productSelectCols}`
    );

    productCode = product?.[DV_PRODUCT_CODE_COL] || "";
    productLabelDescription = product?.[DV_PRODUCT_LABELDESCRIPTION_COL] || "";
  }

  const materialShortText = toPrintString(lot?.[DV_LOT_MATERIALSHORTTEXT_COL]);

  return {
    po: toPrintString(lot?.[DV_LOT_PURCHASEORDER_COL]),
    prodname: toPrintString(productLabelDescription),
    proddesc: toPrintString(productLabelDescription),
    prodnum: toPrintString(productCode),
    product: materialShortText,
    color: toPrintString(lot?.[DV_LOT_COLORTEXT_COL]),
    type: materialShortText,
    tolling: isTruthyDataverseBoolean(lot?.[DV_LOT_TOLLING_COL]) ? "Tolling" : ""
  };
}

async function getInventoryRowsForLotRange(baseUrl, lotId, firstBox, lastBox) {
  const id = normalizeGuid(lotId);
  const lotLookupValueCol = `_${DV_INV_LOTLOOKUP_COL}_value`;

  const selectCols = [
    DV_INV_ID_COL,
    DV_INV_BOX_COL,
    DV_INV_RFID_COL,
    DV_INV_WEIGHT_COL,
    DV_INV_NOWEIGHT_COL
  ].join(",");

  const filter = [
    `${lotLookupValueCol} eq ${id}`,
    `${DV_INV_BOX_COL} ge ${firstBox}`,
    `${DV_INV_BOX_COL} le ${lastBox}`
  ].join(" and ");

  const path =
    `/api/data/v9.2/${DV_INVENTORY_ENTITYSET}` +
    `?$select=${selectCols}` +
    `&$filter=${encodeURIComponent(filter)}` +
    `&$orderby=${DV_INV_BOX_COL} asc`;

  const data = await dvGet(baseUrl, path);
  const rows = Array.isArray(data?.value) ? data.value : [];

  // Defensive numeric sort. The OData $orderby is retained, but the print path
  // must not depend on Dataverse/API response ordering.
  rows.sort((a, b) => Number(a?.[DV_INV_BOX_COL] || 0) - Number(b?.[DV_INV_BOX_COL] || 0));

  return rows;
}

// Station choice mapping
const STATION_CODE_TO_VALUE = {
  P1: 126190000,
  P2: 126190001,
  P3: 126190002,
  P4: 126190004,
  P5: 126190003,
  P6: 126190005,
  P7: 126190006,
  P8: 126190007
};

// Reverse map + normalizer
const STATION_VALUE_TO_CODE = Object.fromEntries(
  Object.entries(STATION_CODE_TO_VALUE).map(([k, v]) => [String(v), k])
);

function normalizeStation(stationRaw) {
  const s = String(stationRaw ?? "").trim();
  if (!s) return "";

  if (/^P[1-8]$/i.test(s)) return s.toUpperCase();
  if (/^\d+$/.test(s)) return STATION_VALUE_TO_CODE[s] || s;

  return s.toUpperCase();
}

// Print log column names
const LOG_PRINTEDBY_COL = process.env.DV_PRINTLOG_PRINTEDBY_COL || "rm_printedby";
const LOG_PRINTEDON_COL = process.env.DV_PRINTLOG_PRINTEDON_COL || "rm_printedon";
const LOG_RESULT_COL = process.env.DV_PRINTLOG_RESULT_COL || "rm_result";
const LOG_RFIDTEXT_COL = process.env.DV_PRINTLOG_RFIDTEXT_COL || "rm_rfidtext";
const LOG_NOTES_COL = process.env.DV_PRINTLOG_NOTES_COL || "rm_notes";
const LOG_STATION_COL = process.env.DV_PRINTLOG_STATION_COL || "rm_station";

// NAV property (schema) names for @odata.bind
const LOG_LOT_NAV_PROP = process.env.DV_PRINTLOG_LOT_NAV || "rm_Lot";
const LOG_INVENTORY_NAV_PROP = process.env.DV_PRINTLOG_INVENTORY_NAV || "rm_Inventory";

function formatErrorDetail(error) {
  if (!error) return "Unknown error";
  if (error.response?.data) {
    try {
      return JSON.stringify(error.response.data);
    } catch {
      return String(error.response.data);
    }
  }
  return error.message || String(error);
}

function escapeODataString(value) {
  return String(value || "").replace(/'/g, "''");
}

function normalizeResultLabel(value) {
  return String(value || "").trim().toLowerCase();
}

async function getLatestPrintLogByResult(baseUrl, resultLabel) {
  const filter = `${LOG_RESULT_COL} eq '${escapeODataString(resultLabel)}'`;
  const path = `/api/data/v9.2/${DV_PRINTLOG_ENTITYSET}?$select=${LOG_PRINTEDON_COL},${LOG_RESULT_COL}&$filter=${encodeURIComponent(filter)}&$orderby=${LOG_PRINTEDON_COL} desc&$top=1`;
  const data = await dvGet(baseUrl, path);
  const row = data?.value?.[0];
  return row?.[LOG_PRINTEDON_COL] || null;
}

async function getPrintLogCountSince(baseUrl, resultLabel, sinceIso) {
  const filter = `${LOG_RESULT_COL} eq '${escapeODataString(resultLabel)}' and ${LOG_PRINTEDON_COL} ge ${sinceIso}`;
  const path = `/api/data/v9.2/${DV_PRINTLOG_ENTITYSET}?$select=${LOG_PRINTEDON_COL}&$filter=${encodeURIComponent(filter)}&$count=true&$top=1`;
  const data = await dvGet(baseUrl, path, { headers: { Prefer: 'odata.include-annotations="*"' } });
  return Number(data?.['@odata.count'] || 0);
}

async function getPrintMetricsSummary(baseUrl) {
  const now = new Date();
  const since15 = new Date(now.getTime() - 15 * 60 * 1000).toISOString();
  const since60 = new Date(now.getTime() - 60 * 60 * 1000).toISOString();

  const [lastPrintSuccessUtc, lastPrintFailureUtc, successCount15m, successCount60m, failureCount15m, failureCount60m] = await Promise.all([
    getLatestPrintLogByResult(baseUrl, 'Success'),
    getLatestPrintLogByResult(baseUrl, 'Failed'),
    getPrintLogCountSince(baseUrl, 'Success', since15),
    getPrintLogCountSince(baseUrl, 'Success', since60),
    getPrintLogCountSince(baseUrl, 'Failed', since15),
    getPrintLogCountSince(baseUrl, 'Failed', since60)
  ]);

  return {
    build: BUILD_TAG,
    serverTimeUtc: now.toISOString(),
    lastPrintSuccessUtc,
    lastPrintFailureUtc,
    successCount15m,
    successCount60m,
    failureCount15m,
    failureCount60m,
    activePrintJobsCount: activePrintJobs.size
  };
}

async function probeSharePointHealth() {
  requireSpConfig();

  const token = await getGraphAppToken();
  if (!token) throw new Error("Graph token acquisition returned an empty access token.");

  const sitePath = String(SP_SITE_PATH || "").startsWith("/") ? SP_SITE_PATH : `/${SP_SITE_PATH}`;
  const site = await graphGet(`https://graph.microsoft.com/v1.0/sites/${SP_HOSTNAME}:${sitePath}?$select=id,webUrl`);

  if (!site?.id) {
    throw new Error("Graph site probe succeeded but site.id was missing.");
  }

  graphSiteCache.siteId = site.id;
  graphSiteCache.checkedAt = Date.now();

  return { siteId: site.id, webUrl: site.webUrl || null };
}

async function runDeepHealthChecks(baseUrl) {
  const checks = { server: 'ok', mappings: 'ok', bartender: 'ok', dataverse: 'ok', sharepoint: 'ok' };
  const errors = {};
  let sharepoint = null;

  try {
    loadMappingsFile();
  } catch (error) {
    checks.mappings = 'fail';
    errors.mappings = formatErrorDetail(error);
  }

  try {
    const authString = Buffer.from(`${process.env.BT_REST_USER || ''}:${process.env.BT_REST_PASSWORD || ''}`, 'utf8').toString('base64');
    const response = await axios.options(BARTENDER_ACTIONS_URL, {
      headers: authString ? { Authorization: `Basic ${authString}` } : {},
      timeout: 10000,
      validateStatus: () => true
    });
    if (response.status >= 500 || response.status === 0) {
      throw new Error(`Unexpected status ${response.status}`);
    }
  } catch (error) {
    checks.bartender = 'fail';
    errors.bartender = formatErrorDetail(error);
  }

  try {
    await getDataverseAccessToken(baseUrl);
    await dvGet(baseUrl, '/api/data/v9.2/WhoAmI()');
  } catch (error) {
    checks.dataverse = 'fail';
    errors.dataverse = formatErrorDetail(error);
  }

  try {
    sharepoint = await probeSharePointHealth();
  } catch (error) {
    checks.sharepoint = 'fail';
    errors.sharepoint = formatErrorDetail(error);
  }

  let lastSuccessfulPrintUtc = null;
  if (checks.dataverse === 'ok') {
    try {
      lastSuccessfulPrintUtc = await getLatestPrintLogByResult(baseUrl, 'Success');
    } catch (error) {
      errors.lastSuccessfulPrintUtc = formatErrorDetail(error);
    }
  }

  const ok = Object.values(checks).every((value) => value === 'ok');
  return {
    ok,
    build: BUILD_TAG,
    checks,
    ...(Object.keys(errors).length ? { errors } : {}),
    ...(sharepoint ? { sharepoint } : {}),
    lastSuccessfulPrintUtc
  };
}

async function writePrintLog(baseUrl, { lotId, inventoryId, rfid, station, printedBy, result, notes }) {
  try {
    const stationVal = STATION_CODE_TO_VALUE[String(station || "").toUpperCase()] ?? null;

    const body = {};
    body[LOG_PRINTEDBY_COL] = printedBy || "";
    body[LOG_PRINTEDON_COL] = new Date().toISOString();
    body[LOG_RESULT_COL] = result || "";
    body[LOG_RFIDTEXT_COL] = rfid || "";
    body[LOG_NOTES_COL] = notes || "";

    if (stationVal !== null) body[LOG_STATION_COL] = stationVal;
    if (lotId) body[`${LOG_LOT_NAV_PROP}@odata.bind`] = `/${DV_LOT_ENTITYSET}(${normalizeGuid(lotId)})`;
    if (inventoryId) body[`${LOG_INVENTORY_NAV_PROP}@odata.bind`] = `/${DV_INVENTORY_ENTITYSET}(${normalizeGuid(inventoryId)})`;

    await dvPost(baseUrl, `/api/data/v9.2/${DV_PRINTLOG_ENTITYSET}`, body);
  } catch (e) {
    const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
    logWarn("print_log_write_failed", { message: msg }, `[PrintLog] Failed to write print log: ${msg}`);
  }
}

/**
 * =========================
 * BarTender REST print helper
 * =========================
 */
async function bartenderPrintBTW({ documentPath, printerName, namedDataSources, copies }) {
  const payload = {
    PrintBTWAction: {
      Document: documentPath,
      Printer: printerName,
      NamedDataSources: namedDataSources || {},
      SaveAfterPrint: false,
      VerifyPrintJobIsComplete: true,
      ReturnPrintSummary: true
    }
  };

  if (copies && Number(copies) > 1) {
    payload.PrintBTWAction.IdenticalCopiesOfLabel = Number(copies);
  }

  const authString = Buffer.from(`${process.env.BT_REST_USER}:${process.env.BT_REST_PASSWORD}`, "utf8").toString("base64");

  const r = await axios.post(BARTENDER_ACTIONS_URL, payload, {
    headers: { "Content-Type": "application/json", Authorization: `Basic ${authString}` },
    timeout: 60000
  });

  return r.data;
}

/**
 * =========================
 * SharePoint / Graph (APP-ONLY) helpers
 * =========================
 */
const graphTokenCache = { accessToken: null, expiresAt: 0 };
const graphSiteCache = { siteId: null, checkedAt: 0 };
const graphDriveCache = new Map(); // driveName -> { id, checkedAt }
const graphDriveByDestCache = new Map(); // destRootNorm -> { id, name, webUrl, checkedAt }
const graphDriveListCache = new Map();

function requireSpConfig() {
  if (!SP_TENANT_ID || !SP_CLIENT_ID || !SP_CLIENT_SECRET || !SP_HOSTNAME || !SP_SITE_PATH) {
    throw new Error("SharePoint/Graph env vars missing. Set SP_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET, SP_HOSTNAME, SP_SITE_PATH.");
  }
}

async function getGraphAppToken() {
  requireSpConfig();

  const now = Date.now();
  if (graphTokenCache.accessToken && now < graphTokenCache.expiresAt - 60_000) {
    return graphTokenCache.accessToken;
  }

  const tokenUrl = `https://login.microsoftonline.com/${SP_TENANT_ID}/oauth2/v2.0/token`;
  const params = new URLSearchParams();
  params.append("client_id", SP_CLIENT_ID);
  params.append("client_secret", SP_CLIENT_SECRET);
  params.append("grant_type", "client_credentials");
  params.append("scope", "https://graph.microsoft.com/.default");

  const r = await axios.post(tokenUrl, params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 20000
  });

  const accessToken = r.data.access_token;
  const expiresIn = Number(r.data.expires_in || 3600);

  graphTokenCache.accessToken = accessToken;
  graphTokenCache.expiresAt = Date.now() + expiresIn * 1000;

  return accessToken;
}

async function graphGet(url) {
  const token = await getGraphAppToken();
  const r = await axios.get(url, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    timeout: 30000
  });
  return r.data;
}

async function graphPut(url, buffer, contentType) {
  const token = await getGraphAppToken();
  const r = await axios.put(url, buffer, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": contentType || "application/octet-stream"
    },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    timeout: 120000
  });
  return r.data;
}

async function getOpDocsSiteId() {
  requireSpConfig();

  const now = Date.now();
  // refresh site id occasionally (in case of changes)
  if (graphSiteCache.siteId && (now - graphSiteCache.checkedAt) < 6 * 60 * 60 * 1000) {
    return graphSiteCache.siteId;
  }

  const sitePath = String(SP_SITE_PATH || "").startsWith("/") ? SP_SITE_PATH : `/${SP_SITE_PATH}`;
  const url = `https://graph.microsoft.com/v1.0/sites/${SP_HOSTNAME}:${sitePath}`;
  const site = await graphGet(url);

  if (!site?.id) throw new Error("Graph site lookup succeeded but site.id missing. Check SP_HOSTNAME / SP_SITE_PATH.");
  graphSiteCache.siteId = site.id;
  graphSiteCache.checkedAt = now;

  return site.id;
}

function sanitizeFilename(name) {
  const base = String(name || "").trim() || `upload_${Date.now()}`;
  // strip path separators + illegal-ish chars
  const cleaned = base
    .replace(/[\/\\]/g, "_")
    .replace(/[:*?"<>|]/g, "_")
    .replace(/\s+/g, " ")
    .trim();
  // keep it sane
  return cleaned.slice(0, 180);
}

function folderForDocType(docType) {
  const dt = String(docType || "").trim();
  // These are DOCUMENT LIBRARY display names on the OpDocs site
  const MAP = {
    BOL: "BOL",
    ScaleTicket: "Scale Ticket",
    PackingList: "Packing List",
    PurchaseOrder: "Purchase orders",
    Other: "Misc"
  };
  return MAP[dt] || "Misc";
}

function encodeGraphPath(pathStr) {
  // Encode each segment but keep slashes
  return String(pathStr || "")
    .split("/")
    .filter(Boolean)
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}

function safeDecodeURIComponent(s) {
  try {
    return decodeURIComponent(String(s));
  } catch {
    return String(s);
  }
}

function normalizeDriveNameForCompare(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normalizeSpUrlForCompare(urlStr) {
  return safeDecodeURIComponent(String(urlStr || ""))
    .trim()
    .replace(/\/+$/, "")
    .toLowerCase();
}

/**
 * SharePoint destination links come in a few shapes:
 *  - Library root: https://.../sites/OpDocs/BOL
 *  - Library view: https://.../sites/OpDocs/BOL/Forms/AllItems.aspx
 *  - Folder view:  https://.../sites/OpDocs/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FOpDocs%2FShared%20Documents%2FBOL
 *
 * We accept ANY of these and try multiple candidate "library root" URLs to match against drive.webUrl.
 */
function extractCandidateLibraryRootUrls(destinationUrl) {
  const out = [];
  try {
    const u = new URL(String(destinationUrl || ""));
    const origin = `${u.protocol}//${u.host}`;

    // 1) Strip /Forms/... from pathname (library view URLs)
    const p0 = u.pathname || "";
    const formsIdx = p0.toLowerCase().indexOf("/forms/");
    const p1 = (formsIdx !== -1 ? p0.slice(0, formsIdx) : p0).replace(/\/+$/, "");
    if (p1) out.push(`${origin}${p1}`);

    // 2) Raw pathname (no query)
    const p2 = (u.pathname || "").replace(/\/+$/, "");
    if (p2) out.push(`${origin}${p2}`);

    // 3) "id" or "RootFolder" query param sometimes contains a server-relative path
    const idParam = u.searchParams.get("id") || u.searchParams.get("RootFolder");
    if (idParam) {
      const rel = safeDecodeURIComponent(idParam).trim();
      if (rel.startsWith("/")) out.push(`${origin}${rel.replace(/\/+$/, "")}`);
    }
  } catch {
    // ignore
  }

  // de-dupe
  const uniq = [];
  const seen = new Set();
  for (const v of out) {
    const n = normalizeSpUrlForCompare(v);
    if (!n) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    uniq.push(v);
  }
  return uniq;
}

async function listDrivesOnSite(siteId) {
  const now = Date.now();
  const cached = graphDriveListCache.get(siteId);
  if (cached && now - cached.checkedAt < GRAPH_DRIVE_CACHE_MS && Array.isArray(cached.drives)) {
    return cached.drives;
  }

  const r = await graphGet(`https://graph.microsoft.com/v1.0/sites/${siteId}/drives?$select=id,name,webUrl`);
  const drives = (r?.value || []).map((d) => ({
    id: d?.id,
    name: d?.name,
    webUrl: d?.webUrl,
  }));

  graphDriveListCache.set(siteId, { drives, checkedAt: now });
  return drives;
}

async function getDriveByDestinationUrl(siteId, destinationUrl) {
  if (!destinationUrl) return null;

  const roots = extractCandidateLibraryRootUrls(destinationUrl);
  if (!roots.length) return null;

  const now = Date.now();

  // Check cache for any candidate root
  for (const rootUrl of roots) {
    const rootNorm = normalizeSpUrlForCompare(rootUrl);
    const cached = graphDriveByDestCache.get(rootNorm);
    if (cached && now - cached.checkedAt < GRAPH_DRIVE_CACHE_MS && cached.drive?.id) {
      return cached.drive;
    }
  }

  const drives = await listDrivesOnSite(siteId);

  // Exact match against drive.webUrl
  for (const rootUrl of roots) {
    const rootNorm = normalizeSpUrlForCompare(rootUrl);
    const drive = drives.find((d) => normalizeSpUrlForCompare(String(d?.webUrl || "")) === rootNorm);
    if (drive?.id) {
      graphDriveByDestCache.set(rootNorm, { drive, checkedAt: now });
      return drive;
    }
  }

  // Fuzzy match: root is inside the library (folder links)
  for (const rootUrl of roots) {
    const rootNorm = normalizeSpUrlForCompare(rootUrl);
    const drive = drives.find((d) => {
      const dNorm = normalizeSpUrlForCompare(String(d?.webUrl || ""));
      return dNorm && (rootNorm.startsWith(dNorm + "/") || dNorm.startsWith(rootNorm + "/"));
    });
    if (drive?.id) {
      graphDriveByDestCache.set(rootNorm, { drive, checkedAt: now });
      return drive;
    }
  }

  return null;
}

function normalizeDocTypeKey(docType) {
  const raw = String(docType || "").trim();
  const k = raw.toLowerCase().replace(/[^a-z0-9]/g, ""); // collapse spaces, dashes, underscores, etc.

  if (k === "bol" || k === "billoflading") return "BOL";
  if (k === "scaleticket" || k === "scale") return "ScaleTicket";
  if (k === "packinglist" || k === "packing") return "PackingList";
  if (k === "purchaseorder" || k === "purchaseorders" || k === "po") return "PurchaseOrder";
  if (k === "misc" || k === "other" || k === "generic") return "Other";

  // Unknown docTypes route to Misc (Other) unless destinationUrl resolves a drive.
  return raw || "Other";
}

/**
 * This is the **document library URL segment** on the OpDocs site.
 * It corresponds to drive.webUrl ending in "/<segment>" for document libraries:
 *   https://plasticrecycling.sharepoint.com/sites/OpDocs/<segment>
 */
function librarySegmentForDocType(docTypeKey) {
  const dtKey = normalizeDocTypeKey(docTypeKey);
  const MAP = {
    BOL: "BOL",
    ScaleTicket: "Scale Ticket",
    PackingList: "Packing List",
    PurchaseOrder: "Purchase orders",
    Other: "Misc",
  };
  return MAP[dtKey] || "Misc";
}

function driveEndsWithLibrarySegment(driveWebUrl, segment) {
  try {
    const u = new URL(String(driveWebUrl || ""));
    const segNorm = normalizeSpUrlForCompare(String(segment || ""));
    if (!segNorm) return false;

    // Compare decoded path ending: "/<segment>"
    const path = normalizeSpUrlForCompare(`${u.origin}${u.pathname}`);
    return path.endsWith("/" + segNorm);
  } catch {
    return false;
  }
}

async function getDriveByLibrarySegment(siteId, segment) {
  const seg = String(segment || "").trim();
  if (!seg) return null;

  const drives = await listDrivesOnSite(siteId);
  const found = drives.find((d) => driveEndsWithLibrarySegment(String(d?.webUrl || ""), seg));
  return found?.id ? found : null;
}

async function resolveOpDocsDriveForUpload(siteId, docType, destinationUrl) {
  const dtKey = normalizeDocTypeKey(docType);
  const expectedSegment = librarySegmentForDocType(dtKey);

  // 1) Prefer explicit destination URL match (most reliable)
  const byDest = await getDriveByDestinationUrl(siteId, destinationUrl);
  if (byDest?.id) {
    return { drive: byDest, resolvedBy: "destinationUrl", docTypeKey: dtKey, expectedSegment };
  }

  // 2) Resolve by expected library URL segment (matches drive.webUrl)
  const bySeg = await getDriveByLibrarySegment(siteId, expectedSegment);
  if (bySeg?.id) {
    return { drive: bySeg, resolvedBy: "docTypeSegment", docTypeKey: dtKey, expectedSegment };
  }

  // 3) Fallback to drive display name
  const driveName = folderForDocType(dtKey);
  const nameKey = normalizeDriveNameForCompare(driveName);
  const drives = await listDrivesOnSite(siteId);
  const byName = drives.find((d) => normalizeDriveNameForCompare(String(d?.name || "")) === nameKey);
  if (byName?.id) {
    return { drive: byName, resolvedBy: "docTypeName", docTypeKey: dtKey, expectedSegment };
  }

  // 4) Give up with useful diagnostics
  const available = (drives || [])
    .map((d) => {
      const n = d?.name ? String(d.name) : "(no name)";
      const w = d?.webUrl ? String(d.webUrl) : "";
      return w ? `${n} (${w})` : n;
    })
    .join(", ");

  throw new Error(
    `Could not resolve OpDocs document library for docType='${docType}' (normalized='${dtKey}'). ` +
      `Tried destinationUrl='${destinationUrl || ""}', expectedSegment='${expectedSegment}', driveName='${driveName}'. ` +
      `Available drives: ${available}`
  );
}

async function uploadToOpDocsAppOnly({ docType, filename, buffer, contentType, sharePointDestinationUrl }) {
  const siteId = await getOpDocsSiteId();

  const safeName = sanitizeFilename(filename);

  const { drive, resolvedBy, docTypeKey, expectedSegment } = await resolveOpDocsDriveForUpload(
    siteId,
    docType,
    sharePointDestinationUrl
  );

  const driveId = drive.id;
  const driveName = drive.name || expectedSegment || folderForDocType(docTypeKey);
  const driveWebUrl = drive.webUrl || "";

  // NOTE: If this log still shows driveWebUrl ending with "/Shared Documents",
  // you're uploading into the default Documents library, not the dedicated libraries.
  logInfo("sharepoint_upload_resolved", { docType: docTypeKey, destinationUrl: sharePointDestinationUrl || null, library: driveName, resolvedBy, driveId, driveWebUrl, fileName: safeName, size: buffer.length }, `[UploadDocument] docType='${docTypeKey}' dest='${sharePointDestinationUrl || ""}' -> library='${driveName}' (${resolvedBy}) driveId='${driveId}' driveWebUrl='${driveWebUrl}' file='${safeName}' size=${buffer.length}`);

  // We still prefer small PUT for small files, but retry with an upload session on Graph 500/generalException.
  try {
    if (buffer.length <= SMALL_UPLOAD_MAX) {
      return await uploadSmallToDrive({
        driveId,
        pathInDrive: safeName,
        buffer: buffer,
        contentType,
      });
    }
    return await uploadLargeToDrive({
      driveId,
      pathInDrive: safeName,
      buffer: buffer,
      contentType,
    });
  } catch (err) {
    const msg = String(err?.response?.data?.error?.code || err?.message || err || "");
    const status = err?.response?.status || null;

    // If the small PUT hit a transient Graph "generalException", retry via upload session.
    const shouldRetryWithSession =
      buffer.length <= SMALL_UPLOAD_MAX &&
      (String(msg).toLowerCase().includes("generalexception") || status === 500 || status === 503);

    if (shouldRetryWithSession) {
      logWarn("sharepoint_upload_retry", { status, message: msg || String(status || "") }, `[OpDocsUpload] Small upload failed with '${msg || status}'. Retrying via upload session...`);
      return await uploadLargeToDrive({
        driveId,
        pathInDrive: safeName,
        buffer: buffer,
        contentType,
      });
    }

    throw err;
  }
}

async function graphPost(url, body) {
  const token = await getGraphAppToken();
  const r = await axios.post(url, body, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json", "Content-Type": "application/json" },
    timeout: 30000
  });
  return r.data;
}

async function getDriveIdByName(siteId, driveName) {
  const key = String(driveName || "").trim();
  if (!key) throw new Error("Drive name is blank");

  const cached = graphDriveCache.get(key);
  const now = Date.now();
  if (cached?.id && (now - cached.checkedAt) < 6 * 60 * 60 * 1000) {
    return cached.id;
  }

  const drives = await listDrivesOnSite(siteId);

  const keyNorm = normalizeDriveNameForCompare(key);
  const drive = (drives || []).find((d) => normalizeDriveNameForCompare(d?.name) === keyNorm);

  if (!drive?.id) {
    const names = (drives || [])
      .map((d) => {
        const n = d?.name ? String(d.name) : "";
        const w = d?.webUrl ? String(d.webUrl) : "";
        return w ? `${n} (${w})` : n;
      })
      .filter(Boolean);

    throw new Error(`Drive not found on OpDocs site: '${key}'. Available drives: ${names.join(", ")}`);
  }

  graphDriveCache.set(key, { id: drive.id, checkedAt: now });
  return drive.id;
}

async function uploadSmallToDrive({ driveId, pathInDrive, buffer, contentType }) {
  const rel = encodeGraphPath(pathInDrive);
  const url = `https://graph.microsoft.com/v1.0/drives/${driveId}/root:/${rel}:/content`;
  return await graphPut(url, buffer, contentType);
}

async function uploadLargeToDrive({ driveId, pathInDrive, buffer, contentType }) {
  const rel = encodeGraphPath(pathInDrive);

  // Even for small files, upload sessions can be more reliable than simple PUT when Graph returns 500/generalException.
  // We'll retry the entire session once on transient failures.
  const maxAttempts = 2;
  let lastErr = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const session = await graphPost(
        `https://graph.microsoft.com/v1.0/drives/${driveId}/root:/${rel}:/createUploadSession`,
        {
          item: {
            "@microsoft.graph.conflictBehavior": "replace",
            name: pathInDrive,
          },
        }
      );

      const uploadUrl = session?.uploadUrl;
      if (!uploadUrl) throw new Error("createUploadSession response missing uploadUrl");

      const chunkSize = 5 * 1024 * 1024; // 5MB
      let offset = 0;

      while (offset < buffer.length) {
        const end = Math.min(offset + chunkSize, buffer.length);
        const chunk = buffer.slice(offset, end);

        const contentRange = `bytes ${offset}-${end - 1}/${buffer.length}`;

        const r = await axios.put(uploadUrl, chunk, {
          headers: {
            "Content-Length": chunk.length,
            "Content-Range": contentRange,
            "Content-Type": contentType || "application/octet-stream",
          },
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
          timeout: 120000,
        });

        // Final chunk returns the DriveItem
        if (r.status === 200 || r.status === 201) {
          return r.data;
        }

        // Intermediate chunks: 202 Accepted
        offset = end;
      }

      throw new Error("Upload session completed without returning a DriveItem.");
    } catch (err) {
      lastErr = err;
      const status = err?.response?.status || null;
      const code = err?.response?.data?.error?.code || null;
      const msg = String(code || err?.message || err || "");

      const transient =
        status === 500 ||
        status === 502 ||
        status === 503 ||
        status === 504 ||
        String(msg).toLowerCase().includes("generalexception");

      if (attempt < maxAttempts && transient) {
        logWarn("sharepoint_upload_session_retry", { attempt, maxAttempts, status, message: msg }, `[OpDocsUpload] Upload session failed (attempt ${attempt}/${maxAttempts}) with ${status || ""} ${msg}. Retrying...`);
        await new Promise((r) => setTimeout(r, 600));
        continue;
      }

      throw err;
    }
  }

  throw lastErr;
}

/**
 * =========================
 * Express app
 * =========================
 */
const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "2mb" })); // printing + normal JSON; uploads go via multer

// Multer for multipart/form-data file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 } // 25MB
});

// ===== CORS + Private Network Access (PNA) for Dataverse custom pages =====
const ALLOWED_ORIGINS = new Set([
  "https://datastream.crm.dynamics.com",
  "https://pridev.crm.dynamics.com",
  "https://pritest.crm.dynamics.com"
]);

const PUBLIC_MONITORING_ROUTES = new Set([
  "GET /health",
  "GET /health/deep",
  "GET /metrics/summary"
]);

function isPublicMonitoringRoute(req) {
  return PUBLIC_MONITORING_ROUTES.has(`${req.method.toUpperCase()} ${req.path}`);
}

app.use((req, res, next) => {
  const origin = String(req.headers.origin || "");
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  const reqHeaders = req.headers["access-control-request-headers"];
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", reqHeaders ? String(reqHeaders) : "authorization,content-type");

  if (req.headers["access-control-request-private-network"] === "true") {
    res.setHeader("Access-Control-Allow-Private-Network", "true");
  }

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use((req, res, next) => {
  if (isPublicMonitoringRoute(req)) {
    res.setHeader("Cache-Control", "no-store");
  }
  next();
});

app.get("/health", (req, res) => res.json({ ok: true, build: BUILD_TAG }));

app.get("/health/deep", async (req, res) => {
  const baseUrl = getDvUrlForRequest(req);
  try {
    const payload = await runDeepHealthChecks(baseUrl);
    logInfo("health_deep", { ok: payload.ok, checks: payload.checks, errors: payload.errors || null });
    return res.status(payload.ok ? 200 : 503).json(payload);
  } catch (error) {
    const detail = formatErrorDetail(error);
    logError("health_deep_failed", { message: detail });
    return res.status(503).json({ ok: false, build: BUILD_TAG, checks: { server: "ok" }, errors: { server: detail }, lastSuccessfulPrintUtc: null });
  }
});

app.get("/metrics/summary", async (req, res) => {
  const baseUrl = getDvUrlForRequest(req);
  try {
    const payload = await getPrintMetricsSummary(baseUrl);
    logInfo("metrics_summary", { activePrintJobsCount: payload.activePrintJobsCount });
    return res.json(payload);
  } catch (error) {
    const detail = formatErrorDetail(error);
    logError("metrics_summary_failed", { message: detail });
    return res.status(503).json({ ok: false, build: BUILD_TAG, message: detail });
  }
});

/**
 * =========================
 * Secure routes (Entra protected)
 * =========================
 */
const activePrintJobs = new Map(); // key -> startedAtMs for in-flight print requests
const PRINT_LOCK_TTL_MS = 2 * 60 * 1000; // 2 minutes (tweak if you want)
const printerQueues = new Map(); // printerName -> promise chain so whole-lot label runs do not interleave

function getSafePrintJobSpacingMs() {
  return Number.isFinite(PRINT_JOB_SPACING_MS) && PRINT_JOB_SPACING_MS >= 0 ? PRINT_JOB_SPACING_MS : 1500;
}

function enqueuePrinterWork(printerName, work) {
  const key = String(printerName || "UNKNOWN_PRINTER").trim() || "UNKNOWN_PRINTER";
  const previous = printerQueues.get(key) || Promise.resolve();

  const run = previous
    .catch(() => {
      // Keep the queue alive even if the previous print run failed.
    })
    .then(work);

  printerQueues.set(key, run);

  run.finally(() => {
    if (printerQueues.get(key) === run) {
      printerQueues.delete(key);
    }
  }).catch(() => {
    // The caller handles the real error; this prevents an unhandled rejection
    // from the cleanup branch.
  });

  return run;
}

app.post("/api/print", requireBearerToken, requireValidToken, handlePrintLot);
app.post("/print/lot", requireBearerToken, requireValidToken, handlePrintLot);

// ? Server-side SharePoint upload (app-only, Sites.Selected)
app.post(
  "/api/uploadDocument",
  requireBearerToken,
  requireValidToken,
  upload.single("file"),
  handleUploadDocument
);

async function handleUploadDocument(req, res) {
  try {
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ ok: false, error: "Missing file (multipart field name must be 'file')" });
    }

    const docType = req.body?.docType || "Other";
    const model = req.body?.model || "";
    const lotNumber = req.body?.lotNumber || "";
    const lotId = req.body?.lotId || "";
    const preferredFilename = req.body?.preferredFilename || req.file.originalname || `upload_${Date.now()}`;
    const sharePointDestinationUrl = req.body?.sharePointDestinationUrl || "";

    const uploaded = await uploadToOpDocsAppOnly({
      docType,
      filename: preferredFilename,
      buffer: req.file.buffer,
      contentType: req.file.mimetype || "application/octet-stream",
      sharePointDestinationUrl
    });

    // uploaded.webUrl is typically present
    const webUrl = uploaded?.webUrl || null;

    return res.json({
      ok: true,
      docType,
      model,
      lotNumber,
      lotId,
      name: uploaded?.name || sanitizeFilename(preferredFilename),
      id: uploaded?.id || null,
      webUrl,
      url: webUrl
    });
  } catch (e) {
    const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
    logError("upload_document_failed", { message: msg }, `[UploadDocument] Failed: ${msg}`);
    return res.status(500).json({ ok: false, error: "UPLOAD_FAILED", message: msg });
  }
}

async function handlePrintLot(req, res) {
  let lockKey = null;

  try {
    const body = req.body || {};

    const stationRaw =
      body.station ??
      body.printStation ??
      body.stationCode ??
      body.stationId ??
      null;

    const station = normalizeStation(stationRaw);

    const lotIdFromBody =
      body.lotId ??
      body.LotId ??
      body.lotid ??
      body.lot?.id ??
      null;

    const lotNumberFromBody =
      body.lotNumber ??
      body.LotNumber ??
      body.lot ??
      body.lotRef ??
      null;

    const dryRun = body.dryRun === true;
    const allowMissing = body.allowMissing === true;

    // ---- Box range handling (supports Receiving single-box payloads) ----
    let firstBoxRaw = body.firstBox ?? body.FirstBox ?? body.firstbox ?? null;
    let lastBoxRaw = body.lastBox ?? body.LastBox ?? body.lastbox ?? null;

    const singleBoxRaw =
      body.box ??
      body.boxNumber ??
      body.Box ??
      body.BoxNumber ??
      body.rm_box ??
      null;

    if ((firstBoxRaw == null || firstBoxRaw === "") && (lastBoxRaw == null || lastBoxRaw === "") && singleBoxRaw != null) {
      firstBoxRaw = singleBoxRaw;
      lastBoxRaw = singleBoxRaw;
    }

    if (firstBoxRaw != null && firstBoxRaw !== "" && (lastBoxRaw == null || lastBoxRaw === "")) {
      lastBoxRaw = firstBoxRaw;
    }

    const fb = Number(firstBoxRaw);
    const lb = Number(lastBoxRaw);

    if (!station || (!lotIdFromBody && !lotNumberFromBody)) {
      return res.status(400).json({
        error: "lotId and station are required",
        got: { stationRaw, stationNormalized: station, lotId: lotIdFromBody, lotNumber: lotNumberFromBody }
      });
    }

    if (!Number.isInteger(fb) || !Number.isInteger(lb) || fb < 1 || lb > 99 || fb > lb) {
      return res.status(400).json({
        error: "firstBox/lastBox must be integers 1?99 and firstBox <= lastBox",
        got: { firstBox: firstBoxRaw, lastBox: lastBoxRaw, singleBox: singleBoxRaw }
      });
    }
    // -------------------------------------------------------------------

    const baseUrl = getDvUrlForRequest(req);

    let effectiveLotId = lotIdFromBody;
    if (!effectiveLotId && lotNumberFromBody) {
      effectiveLotId = await getLotIdByLotNumber(baseUrl, lotNumberFromBody);
    }

    // Acquire lock ONLY for real prints (not dryRun)
    lockKey = `${station}|${normalizeGuid(effectiveLotId)}`;

    const existing = activePrintJobs.get(lockKey);
    const now = Date.now();

    // Auto-expire stale lock
    if (existing && (now - existing) > PRINT_LOCK_TTL_MS) {
      logWarn("print_lock_expired", { lockKey, ageMs: now - existing }, `[PrintSvc] Expiring stale print lock for ${lockKey} (ageMs=${now - existing})`);
      activePrintJobs.delete(lockKey);
    }

    if (activePrintJobs.has(lockKey)) {
      return res.status(409).json({
        ok: false,
        code: "PRINT_IN_PROGRESS",
        message: "A print job is already running for this station and lot. Please wait a moment and try again.",
        station,
        lotId: normalizeGuid(effectiveLotId)
      });
    }

    activePrintJobs.set(lockKey, now);

    const lotNumber = await getLotNumberById(baseUrl, effectiveLotId);
    const { family, printer, template } = resolvePrinterAndTemplate({ station, lotNumber });

    const rows = await getInventoryRowsForLotRange(baseUrl, effectiveLotId, fb, lb);

    const byBox = new Map();
    for (const r of rows) {
      const b = Number(r[DV_INV_BOX_COL]);
      if (!Number.isInteger(b)) continue;

      // Keep the first row for a box number and log duplicates instead of
      // allowing duplicate inventory rows to make the sequence unpredictable.
      if (byBox.has(b)) {
        logWarn(
          "print_duplicate_box_number",
          { station, lotId: normalizeGuid(effectiveLotId), box: b },
          `[PrintSvc] Duplicate inventory row for box=${b}; using first row in print sequence`
        );
        continue;
      }

      byBox.set(b, r);
    }

    const requestedBoxes = [];
    for (let b = fb; b <= lb; b++) requestedBoxes.push(b);

    // Single source of truth for print order. Everything below walks this array,
    // so labels are submitted 1, 2, 3 ... regardless of Dataverse/page ordering.
    requestedBoxes.sort((a, b) => a - b);

    const missingBoxes = requestedBoxes.filter((b) => !byBox.has(b));

    if (dryRun === true) {
      logInfo("print_dry_run", { station, lotNumber, missingBoxesCount: missingBoxes.length, firstBox: fb, lastBox: lb }, `[PrintSvc] DRYRUN station=${station} lot=${lotNumber} missing=${missingBoxes.length}`);
      return res.json({
        ok: true,
        dryRun: true,
        baseUrl,
        lotId: normalizeGuid(effectiveLotId),
        lotNumber,
        station,
        family,
        printer,
        template,
        firstBox: fb,
        lastBox: lb,
        requestedCount: requestedBoxes.length,
        foundCount: rows.length,
        missingBoxes
      });
    }

    if (missingBoxes.length > 0 && allowMissing !== true) {
      logWarn("print_missing_boxes", { station, lotNumber, missingBoxes, firstBox: fb, lastBox: lb }, `[PrintSvc] ABORT missing boxes station=${station} lot=${lotNumber} missing=${missingBoxes.join(",")}`);
      return res.status(409).json({
        ok: false,
        code: "MISSING_BOXES",
        message: "Some boxes in the range were not found in Inventory. Confirm to continue or adjust the range.",
        lotNumber,
        station,
        firstBox: fb,
        lastBox: lb,
        missingBoxes
      });
    }

    const lotLabelData = await getLotLabelData(baseUrl, effectiveLotId);
    const printedBy = req.user?.preferred_username || req.user?.upn || "";
    const results = [];
    const printJobSpacingMs = getSafePrintJobSpacingMs();

    logInfo(
      "print_sequence_resolved",
      { station, lotNumber, printer, firstBox: fb, lastBox: lb, requestedBoxes, printJobSpacingMs },
      `[PrintSvc] Print sequence resolved station=${station} lot=${lotNumber}: ${requestedBoxes.join(",")}`
    );

    await enqueuePrinterWork(printer, async () => {
      for (const box of requestedBoxes) {
        const row = byBox.get(box);

        if (!row) {
          await writePrintLog(baseUrl, {
            lotId: effectiveLotId,
            inventoryId: null,
            rfid: `${lotNumber}-B${pad2(box)}`,
            station,
            printedBy,
            result: "Skipped",
            notes: "Inventory row missing for this box number"
          });
          continue;
        }

        const inventoryId = row[DV_INV_ID_COL];
        const rfid = row[DV_INV_RFID_COL] || `${lotNumber}-B${pad2(box)}`;
        const poundsVal = row[DV_INV_WEIGHT_COL];
        const isNoWeight = isTruthyDataverseBoolean(row[DV_INV_NOWEIGHT_COL]);

        const named = {
          lot: lotNumber,
          firstbox: String(box),
          RFID: String(rfid),
          pounds: isNoWeight ? "_" : (poundsVal == null ? "" : String(poundsVal)),
          po: lotLabelData.po,
          prodname: lotLabelData.prodname,
          proddesc: lotLabelData.proddesc,
          prodnum: lotLabelData.prodnum,
          product: lotLabelData.product,
          color: lotLabelData.color,
          type: lotLabelData.type,
          tolling: lotLabelData.tolling,
          erp: ""
        };

        try {
          logEvent("print_attempt", { station, lotNumber, box, rfid, printer, template }, `[PrintSvc] -> BarTender PRINT box=${box} rfid=${rfid} printer="${printer}" template="${template}"`);

          const action = await bartenderPrintBTW({
            documentPath: template,
            printerName: printer,
            namedDataSources: named,
            copies: 1
          });

          const actionId = action?.Id || null;
          const status = action?.Status || null;

          logInfo("print_success", { station, lotNumber, box, rfid, printer, template, actionId, status }, `[PrintSvc] <- BarTender actionId=${actionId} status=${status} box=${box}`);

          results.push({ box, rfid, pounds: named.pounds, actionId, status });

          await writePrintLog(baseUrl, {
            lotId: effectiveLotId,
            inventoryId,
            rfid,
            station,
            printedBy,
            result: "Success",
            notes: ""
          });
        } catch (e) {
          const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
          logError("print_failure", { station, lotNumber, box, rfid, printer, template, message: msg }, `[PrintSvc] FAILED box=${box} lot=${lotNumber} station=${station}: ${msg}`);

          await writePrintLog(baseUrl, {
            lotId: effectiveLotId,
            inventoryId,
            rfid,
            station,
            printedBy,
            result: "Failed",
            notes: msg
          });

          throw e;
        }

        await sleep(printJobSpacingMs);
      }
    });

    return res.json({
      ok: true,
      dryRun: false,
      baseUrl,
      lotId: normalizeGuid(effectiveLotId),
      lotNumber,
      station,
      family,
      printer,
      template,
      firstBox: fb,
      lastBox: lb,
      requestedCount: requestedBoxes.length,
      printedCount: results.length,
      missingBoxes,
      results
    });
  } catch (e) {
    logError("print_request_failed", { message: e.message, bartender: e.response?.data || null, lockKey });
    return res.status(500).json({ ok: false, message: e.message, bartender: e.response?.data || null });
  } finally {
    if (lockKey) activePrintJobs.delete(lockKey);
  }
}

app.listen(Number(PORT), "0.0.0.0", () => {
  logInfo("service_listening", { port: Number(PORT), host: "0.0.0.0" }, `PrintSvc listening on http://0.0.0.0:${PORT}`);
});