require("dotenv").config();

const BUILD_TAG = process.env.BUILD_TAG || "2026-05-05-transformation-label-printing";

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
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const { appendOfflineAuditEvent, readLatestOfflineAuditEvents } = require("./lib/offlineAudit");
const { readOfflineState, writeOfflineState } = require("./lib/offlineState");
const {
  getSourceIp,
  requireOfflineAdminCookie,
  requireOfflineLocalAccess,
  setAdminCookie
} = require("./lib/offlineSecurity");

const CONFIG_DIR = process.env.PRINTSVC_CONFIG_DIR || "C:\\PrintSvc";
const TEMPLATE_DIR = process.env.BARTENDER_TEMPLATE_DIR || "C:\\RFID";
const mappingsPath = path.join(CONFIG_DIR, "mappings.json");
const OFFLINE_PUBLIC_DIR = path.join(__dirname, "public", "offline");
const OFFLINE_ASSETS_DIR = path.join(OFFLINE_PUBLIC_DIR, "assets");

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
const QC_SAMPLE_POUNDS_TEMPLATE_FILENAME = process.env.QC_SAMPLE_POUNDS_TEMPLATE_FILENAME || process.env.QC_SAMPLE_POUNDS_TEMPLATE || "QCSamplePounds-P3.btw";
const QC_SAMPLE_POUNDS_DEFAULT_LABELS = ["5000", "15000", "25000", "35000", "Last Box"];

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

function getMappedPrinterForStation(station, usage, labelKind) {
  const st = usage === "sample" ? normalizeSampleStation(station) : String(station || "").toUpperCase();
  const kind = labelKind ? normalizeSampleLabelKind(labelKind) : "";

  // RFID labels and QC/retain labels may use different physical printers at
  // the same production station. RFID uses mappings.stations / mappings.rfidStations.
  // QC/Retain uses mappings.sampleStations / mappings.sampleLabelStations.
  // Do not fall back to the RFID printer for sample labels; that should fail fast
  // if the QC printer is not explicitly mapped.
  const stationMaps = usage === "sample"
    ? [
        kind ? mappings.sampleLabelStations?.[kind] : null,
        mappings.sampleLabelStations,
        kind ? mappings.sampleStations?.[kind] : null,
        mappings.sampleStations,
        kind ? mappings.qcStations?.[kind] : null,
        mappings.qcStations
      ]
    : [mappings.rfidStations, mappings.stations];

  for (const stationMap of stationMaps) {
    const printer = stationMap?.[st]?.printer;
    if (printer) return printer;
  }

  return "";
}

function resolvePrinterAndTemplate({ station, lotNumber }) {
  const fam = getLotFamily(lotNumber);
  const st = String(station || "").toUpperCase();

  const printer = getMappedPrinterForStation(st, "rfid");
  const templateValue = mappings.templates?.[fam]?.[st];

  if (!printer) throw new Error(`Unknown RFID station/printer mapping for station='${st}'`);
  if (!templateValue) throw new Error(`No RFID template for family='${fam}' station='${st}'`);

  const template = resolveTemplatePath(templateValue);

  return { family: fam, printer, template };
}

function normalizeOfflineFamily(familyRaw) {
  const family = String(familyRaw || "AUTO").trim().toUpperCase();
  if (!family || family === "AUTO") return "AUTO";
  if (family === "RAW" || family === "FG") return family;
  throw new Error(`Unknown offline label family '${familyRaw}'. Expected AUTO, RAW, or FG.`);
}

function resolvePrinterAndTemplateForFamily({ station, lotNumber, family }) {
  const requestedFamily = normalizeOfflineFamily(family);
  const fam = requestedFamily === "AUTO" ? getLotFamily(lotNumber) : requestedFamily;
  const st = normalizeStation(station);

  const printer = getMappedPrinterForStation(st, "rfid");
  const templateValue = mappings.templates?.[fam]?.[st];

  if (!printer) throw new Error(`Unknown RFID station/printer mapping for station='${st}'`);
  if (!templateValue) throw new Error(`No RFID template for family='${fam}' station='${st}'`);

  const template = resolveTemplatePath(templateValue);

  return { requestedFamily, family: fam, printer, template };
}

function normalizeSampleLabelKind(labelKindRaw) {
  const raw = String(labelKindRaw || "").trim().toLowerCase().replace(/[\s_\-]/g, "");

  if (["qcsample", "sample", "qc"].includes(raw)) return "QCSample";
  if (["qcretain", "retain", "retainsample"].includes(raw)) return "QCRetain";

  throw new Error(`Unknown sample label kind '${labelKindRaw}'. Expected QCSample or QCRetain.`);
}

function normalizeSampleStation(stationRaw) {
  const raw = String(stationRaw || "").trim().toUpperCase();
  if (!raw) return "";

  const aliases = mappings.rules?.sampleStationAliases || {};
  return String(aliases[raw] || raw).trim().toUpperCase();
}

function resolvePrinterAndSampleTemplate({ station, labelKind, byPounds = false }) {
  const st = normalizeSampleStation(station);
  const kind = normalizeSampleLabelKind(labelKind);

  if (byPounds && kind !== "QCSample") {
    throw new Error("By-pounds sample labels are only supported for QCSample.");
  }

  const printer = getMappedPrinterForStation(st, "sample", kind);
  const templateValue = byPounds
    ? QC_SAMPLE_POUNDS_TEMPLATE_FILENAME
    : mappings.templates?.[kind]?.[st];

  if (!printer) throw new Error(`No QC/Retain printer mapping for labelKind='${kind}' station='${st}'. Add mappings.sampleStations.${st}.printer in mappings.json.`);
  if (!templateValue) throw new Error(`No sample-label template for labelKind='${kind}' station='${st}'`);

  const template = resolveTemplatePath(templateValue);
  return { labelKind: kind, printer, template };
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
const DV_CUSTOMER_ENTITYSET = process.env.DV_CUSTOMER_ENTITYSET || "rm_customers";
const DV_MACHINE_ENTITYSET = process.env.DV_MACHINE_ENTITYSET || "rm_machines";

const DV_LOTNUMBER_COL = process.env.DV_LOTNUMBER_COL || "rm_lotnumber";
const DV_LOT_PURCHASEORDER_COL = process.env.DV_LOT_PURCHASEORDER_COL || "rm_purchaseorder";
const DV_LOT_PRODUCTLOOKUP_COL = process.env.DV_LOT_PRODUCTLOOKUP_COL || "rm_product";
const DV_LOT_CUSTOMERLOOKUP_COL = process.env.DV_LOT_CUSTOMERLOOKUP_COL || "rm_customer";
const DV_LOT_MACHINELOOKUP_COL = process.env.DV_LOT_MACHINELOOKUP_COL || "rm_machine";
const DV_LOT_COLORTEXT_COL = process.env.DV_LOT_COLORTEXT_COL || "crb9d_colortext";
const DV_LOT_MATERIALSHORTTEXT_COL = process.env.DV_LOT_MATERIALSHORTTEXT_COL || "rm_materialshorttext";
const DV_LOT_TOLLING_COL = process.env.DV_LOT_TOLLING_COL || "rm_tolling";
const DV_PRODUCT_NAME_COL = process.env.DV_PRODUCT_NAME_COL || "rm_productname";
const DV_PRODUCT_CODE_COL = process.env.DV_PRODUCT_CODE_COL || "rm_productcode";
const DV_PRODUCT_LABELDESCRIPTION_COL = process.env.DV_PRODUCT_LABELDESCRIPTION_COL || "rm_productlabeldescription";
const DV_CUSTOMER_NAME_COL = process.env.DV_CUSTOMER_NAME_COL || "rm_customername";
const DV_MACHINE_NAME_COL = process.env.DV_MACHINE_NAME_COL || "rm_machinename";

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

async function getLotLabelData(baseUrl, lotId, options = {}) {
  const id = normalizeGuid(lotId);
  if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error(`Invalid lotId GUID: ${lotId}`);

  const includeMachine = options?.includeMachine === true;
  const includeCompany = options?.includeCompany === true;
  const lotProductLookupValueCol = `_${DV_LOT_PRODUCTLOOKUP_COL}_value`;
  const lotCustomerLookupValueCol = `_${DV_LOT_CUSTOMERLOOKUP_COL}_value`;
  const lotMachineLookupValueCol = `_${DV_LOT_MACHINELOOKUP_COL}_value`;
  const selectCols = [
    DV_LOT_PURCHASEORDER_COL,
    DV_LOT_COLORTEXT_COL,
    DV_LOT_MATERIALSHORTTEXT_COL,
    DV_LOT_TOLLING_COL,
    lotProductLookupValueCol,
    ...(includeCompany ? [lotCustomerLookupValueCol] : []),
    ...(includeMachine ? [lotMachineLookupValueCol] : [])
  ].join(",");

  const lot = await dvGet(baseUrl, `/api/data/v9.2/${DV_LOT_ENTITYSET}(${id})?$select=${selectCols}`);

  const productId = lot?.[lotProductLookupValueCol];
  const customerId = includeCompany ? lot?.[lotCustomerLookupValueCol] : null;
  const machineId = includeMachine ? lot?.[lotMachineLookupValueCol] : null;
  let productCode = "";
  let productLabelDescription = "";
  let companyName = "";
  let machineName = "";

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

  if (customerId) {
    const customer = await dvGet(
      baseUrl,
      `/api/data/v9.2/${DV_CUSTOMER_ENTITYSET}(${normalizeGuid(customerId)})?$select=${DV_CUSTOMER_NAME_COL}`
    );

    companyName = customer?.[DV_CUSTOMER_NAME_COL] || "";
  }

  if (machineId) {
    const machine = await dvGet(
      baseUrl,
      `/api/data/v9.2/${DV_MACHINE_ENTITYSET}(${normalizeGuid(machineId)})?$select=${DV_MACHINE_NAME_COL}`
    );

    machineName = machine?.[DV_MACHINE_NAME_COL] || "";
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
    tolling: isTruthyDataverseBoolean(lot?.[DV_LOT_TOLLING_COL]) ? "Tolling" : "",
    company: toPrintString(companyName),
    machine: toPrintString(machineName)
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

// Optional QC-label choice values from the Dataverse Print Station global choice.
// The current Transformation Label Printing page filters these choices out and
// does not need them, but the server tolerates them if an older build sends one.
const QC_LABEL_STATION_VALUE_TO_CODE = {
  126190008: "P3",
  126190009: "P4",
  126190010: "P5"
};

// Reverse map + normalizer
const STATION_VALUE_TO_CODE = {
  ...Object.fromEntries(Object.entries(STATION_CODE_TO_VALUE).map(([k, v]) => [String(v), k])),
  ...Object.fromEntries(Object.entries(QC_LABEL_STATION_VALUE_TO_CODE).map(([k, v]) => [String(k), v]))
};

function normalizeStation(stationRaw) {
  const s = String(stationRaw ?? "").trim();
  if (!s) return "";

  if (/^P[1-8]$/i.test(s)) return s.toUpperCase();
  if (/^\d+$/.test(s)) return STATION_VALUE_TO_CODE[s] || s;

  const qcMatch = s.toUpperCase().match(/QC[^P]*P([1-8])/);
  if (qcMatch) return `P${qcMatch[1]}`;

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
app.use(["/offline", "/api/offline"], requireOfflineLocalAccess);
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

/**
 * =========================
 * Emergency Offline Printing (local-only)
 * =========================
 */
function getPositiveIntegerEnv(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function getOfflineMaxLabels() {
  return getPositiveIntegerEnv("OFFLINE_PRINT_MAX_LABELS", 99);
}

function getOfflineMaxBoxNumber() {
  return getPositiveIntegerEnv("OFFLINE_PRINT_MAX_BOX_NUMBER", 99);
}

function sortStations(stations) {
  return stations.sort((a, b) => {
    const left = Number(String(a).replace(/^P/i, ""));
    const right = Number(String(b).replace(/^P/i, ""));
    if (Number.isInteger(left) && Number.isInteger(right)) return left - right;
    return String(a).localeCompare(String(b));
  });
}

function getOfflineAllowedStations() {
  return sortStations(Array.from(new Set([
    ...Object.keys(mappings.stations || {}),
    ...Object.keys(mappings.rfidStations || {})
  ])).map((station) => String(station).toUpperCase()));
}

const OFFLINE_STATION_DESCRIPTION_FALLBACKS = {
  P1: "Engineering",
  P2: "Receiving",
  P3: "Extrusions",
  P4: "Grinding",
  P5: "Blending",
  P6: "Penn",
  P7: "South Carolina",
  P8: "State"
};

const OFFLINE_PRINTER_CODE_DESCRIPTIONS = {
  ENGR: "Engineering",
  REC: "Receiving",
  EXT: "Extrusions",
  GRD: "Grinding",
  BLD: "Blending",
  PENN: "Penn",
  SC: "South Carolina",
  STATE: "State"
};

function getMappedStationConfig(station) {
  return mappings.rfidStations?.[station] || mappings.stations?.[station] || {};
}

function getStationDescriptionFromMapping(station) {
  const config = getMappedStationConfig(station);
  const explicit =
    config.description ||
    config.department ||
    config.displayName ||
    config.label ||
    config.name;

  if (explicit) return String(explicit).trim();

  const printer = String(config.printer || "").trim();
  const parts = printer.split(/\s+/).filter(Boolean);
  const lastToken = String(parts[parts.length - 1] || "").toUpperCase();

  return OFFLINE_PRINTER_CODE_DESCRIPTIONS[lastToken] || OFFLINE_STATION_DESCRIPTION_FALLBACKS[station] || "";
}

function getOfflineStationOptions() {
  return getOfflineAllowedStations().map((code) => {
    const description = getStationDescriptionFromMapping(code);
    return {
      code,
      description,
      label: description ? `${code} - ${description}` : code
    };
  });
}

function getOfflineTemplateFamilies() {
  const templateKeys = Object.keys(mappings.templates || {});
  const preferred = ["RAW", "FG"].filter((family) => templateKeys.includes(family));
  const extra = templateKeys.filter((family) => !preferred.includes(family)).sort();
  return preferred.length ? preferred : extra;
}

function buildOfflineStatusPayload() {
  const state = readOfflineState();
  const templateFamilies = getOfflineTemplateFamilies();

  return {
    ok: true,
    build: BUILD_TAG,
    enabled: state.enabled,
    reason: state.reason,
    state,
    maxLabels: getOfflineMaxLabels(),
    maxBoxNumber: getOfflineMaxBoxNumber(),
    allowedStations: getOfflineAllowedStations(),
    stationOptions: getOfflineStationOptions(),
    templateFamilies,
    familyOptions: ["AUTO", ...templateFamilies]
  };
}

function httpError(status, code, message, details = {}) {
  const error = new Error(message);
  error.statusCode = status;
  error.code = code;
  error.details = details;
  return error;
}

function trimString(value) {
  return String(value ?? "").trim();
}

function parseIntegerField(value) {
  if (typeof value === "number" && Number.isInteger(value)) return value;
  const text = trimString(value);
  if (!/^\d+$/.test(text)) return NaN;
  return Number(text);
}

function requireNonBlankString(value, fieldName) {
  const text = trimString(value);
  if (!text) throw httpError(400, "VALIDATION_ERROR", `${fieldName} is required.`);
  return text;
}

function generateOfflineRfid(lotNumber, box) {
  return `${lotNumber}-B${pad2(box)}`;
}

function buildOfflineNamedDataSources(body, lotNumber, box) {
  const generatedRfid = generateOfflineRfid(lotNumber, box);

  return {
    lot: lotNumber,
    firstbox: String(box),
    RFID: generatedRfid,
    pounds: trimString(body.pounds) || "_",
    po: trimString(body.purchaseOrder),
    prodname: trimString(body.productDescription) || trimString(body.productName) || trimString(body.material),
    color: trimString(body.color),
    type: trimString(body.material),
    tolling: isTruthyDataverseBoolean(body.tolling) ? "Tolling" : "",
    erp: "OFFLINE"
  };
}

function validateOfflinePrintPayload(body) {
  const lotNumber = requireNonBlankString(body.lotNumber, "lotNumber");
  const station = normalizeStation(body.station);
  if (!station) throw httpError(400, "VALIDATION_ERROR", "station is required.");

  const allowedStations = getOfflineAllowedStations();
  if (!allowedStations.includes(station)) {
    throw httpError(400, "VALIDATION_ERROR", `station must exist in mappings.json. Got '${station}'.`, {
      allowedStations
    });
  }

  let requestedFamily;
  try {
    requestedFamily = normalizeOfflineFamily(body.family);
  } catch (error) {
    throw httpError(400, "VALIDATION_ERROR", error.message);
  }

  const firstBox = parseIntegerField(body.firstBox);
  const lastBox = parseIntegerField(body.lastBox);
  const maxBoxNumber = getOfflineMaxBoxNumber();
  const maxLabels = getOfflineMaxLabels();

  if (!Number.isInteger(firstBox) || !Number.isInteger(lastBox)) {
    throw httpError(400, "VALIDATION_ERROR", "firstBox and lastBox must be integers.");
  }

  if (firstBox < 1) throw httpError(400, "VALIDATION_ERROR", "firstBox must be at least 1.");
  if (lastBox > maxBoxNumber) {
    throw httpError(400, "VALIDATION_ERROR", `lastBox must be less than or equal to ${maxBoxNumber}.`);
  }
  if (firstBox > lastBox) throw httpError(400, "VALIDATION_ERROR", "firstBox must be less than or equal to lastBox.");

  const requestedCount = lastBox - firstBox + 1;
  if (requestedCount > maxLabels) {
    throw httpError(400, "VALIDATION_ERROR", `requested label count must be less than or equal to ${maxLabels}.`, {
      requestedCount,
      maxLabels
    });
  }

  const operator = requireNonBlankString(body.operator, "operator");
  const reason = requireNonBlankString(body.reason, "reason");

  if (body.confirmationAccepted !== true) {
    throw httpError(400, "VALIDATION_ERROR", "confirmationAccepted must be true.");
  }

  const resolved = resolvePrinterAndTemplateForFamily({
    station,
    lotNumber,
    family: requestedFamily
  });

  return {
    lotNumber,
    station,
    requestedFamily,
    family: resolved.family,
    printer: resolved.printer,
    template: resolved.template,
    firstBox,
    lastBox,
    requestedCount,
    operator,
    reason,
    dryRun: body.dryRun === true
  };
}

function writeOfflineAudit(eventType, req, details = {}) {
  try {
    return appendOfflineAuditEvent({
      eventType,
      sourceIp: getSourceIp(req),
      host: String(req.headers.host || ""),
      ...details
    });
  } catch (error) {
    logWarn("offline_audit_write_failed", { message: error.message }, `[OfflinePrint] Failed to write audit event: ${error.message}`);
    return null;
  }
}

function safeCompareSecret(actual, expected) {
  const left = crypto.createHash("sha256").update(String(actual || "")).digest();
  const right = crypto.createHash("sha256").update(String(expected || "")).digest();
  return crypto.timingSafeEqual(left, right);
}

function offlinePreview(validated) {
  return {
    firstRfid: generateOfflineRfid(validated.lotNumber, validated.firstBox),
    lastRfid: generateOfflineRfid(validated.lotNumber, validated.lastBox),
    count: validated.requestedCount
  };
}

function getAuditFamily(validated, body) {
  if (validated?.family) return validated.family;
  try {
    return normalizeOfflineFamily(body?.family || "AUTO");
  } catch {
    return trimString(body?.family);
  }
}

function buildOfflinePrintAuditDetails(validated, body, overrides = {}) {
  return {
    operator: validated?.operator || trimString(body?.operator),
    reason: validated?.reason || trimString(body?.reason),
    station: validated?.station || normalizeStation(body?.station),
    family: getAuditFamily(validated, body),
    printer: validated?.printer || "",
    template: validated?.template || "",
    lotNumber: validated?.lotNumber || trimString(body?.lotNumber),
    firstBox: validated?.firstBox ?? parseIntegerField(body?.firstBox),
    lastBox: validated?.lastBox ?? parseIntegerField(body?.lastBox),
    requestedCount: validated?.requestedCount || 0,
    printedCount: 0,
    ok: false,
    ...overrides
  };
}

app.get("/offline/assets/pri-exterior.jpg", (req, res, next) => {
  const jpgPath = path.join(OFFLINE_ASSETS_DIR, "pri-exterior.jpg");
  const pngPath = path.join(OFFLINE_ASSETS_DIR, "pri-exterior.png");

  if (fs.existsSync(jpgPath)) {
    return res.sendFile(jpgPath);
  }

  if (fs.existsSync(pngPath)) {
    return res.sendFile(pngPath);
  }

  return next();
});

app.use(
  "/offline/assets",
  express.static(path.join(__dirname, "public", "offline", "assets"))
);

app.get("/offline", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(path.join(OFFLINE_PUBLIC_DIR, "index.html"));
});

app.get("/offline/admin", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(path.join(OFFLINE_PUBLIC_DIR, "admin.html"));
});

app.use("/offline", express.static(OFFLINE_PUBLIC_DIR, {
  index: false,
  setHeaders(res) {
    res.setHeader("Cache-Control", "no-store");
  }
}));

app.get("/api/offline/status", (req, res) => {
  try {
    return res.json(buildOfflineStatusPayload());
  } catch (error) {
    return res.status(500).json({ ok: false, error: "OFFLINE_STATUS_ERROR", message: error.message });
  }
});

app.post("/api/offline/admin/login", (req, res) => {
  try {
    const configuredPassword = process.env.OFFLINE_PRINT_ADMIN_PASSWORD;
    if (!configuredPassword) {
      return res.status(500).json({
        ok: false,
        error: "OFFLINE_ADMIN_CONFIG_ERROR",
        message: "OFFLINE_PRINT_ADMIN_PASSWORD is not configured. Offline admin login is disabled."
      });
    }

    if (!process.env.OFFLINE_PRINT_SESSION_SECRET) {
      return res.status(500).json({
        ok: false,
        error: "OFFLINE_ADMIN_CONFIG_ERROR",
        message: "OFFLINE_PRINT_SESSION_SECRET is not configured. Offline admin login is disabled."
      });
    }

    const password = String(req.body?.password || "");
    if (!safeCompareSecret(password, configuredPassword)) {
      return res.status(401).json({ ok: false, error: "INVALID_PASSWORD", message: "Invalid offline admin password." });
    }

    setAdminCookie(res, { adminName: trimString(req.body?.adminName) });

    return res.json({
      ok: true,
      message: "Offline admin login successful.",
      expiresInSeconds: 30 * 60
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: "OFFLINE_ADMIN_LOGIN_ERROR", message: error.message });
  }
});

app.post("/api/offline/admin/toggle", requireOfflineAdminCookie, (req, res) => {
  try {
    const enabled = req.body?.enabled;
    if (typeof enabled !== "boolean") {
      return res.status(400).json({ ok: false, error: "VALIDATION_ERROR", message: "enabled must be true or false." });
    }

    const adminName = requireNonBlankString(req.body?.adminName, "adminName");
    const reason = trimString(req.body?.reason);
    if (enabled && !reason) {
      return res.status(400).json({ ok: false, error: "VALIDATION_ERROR", message: "reason is required when enabling emergency offline printing." });
    }

    const current = readOfflineState();
    const now = isoNow();
    const next = {
      ...current,
      enabled,
      reason: enabled ? reason : "",
      updatedOn: now
    };

    if (enabled) {
      next.enabledBy = adminName;
      next.enabledOn = now;
    } else {
      next.disabledBy = adminName;
      next.disabledOn = now;
    }

    const state = writeOfflineState(next);
    writeOfflineAudit("offline_admin_toggle", req, {
      adminName,
      reason,
      enabled: state.enabled,
      ok: true
    });

    return res.json({ ok: true, state });
  } catch (error) {
    writeOfflineAudit("offline_admin_toggle", req, {
      adminName: trimString(req.body?.adminName),
      reason: trimString(req.body?.reason),
      enabled: req.body?.enabled === true,
      ok: false,
      error: error.message
    });
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "OFFLINE_TOGGLE_ERROR", message: error.message });
  }
});

app.get("/api/offline/admin/audit", requireOfflineAdminCookie, (req, res) => {
  try {
    return res.json({
      ok: true,
      records: readLatestOfflineAuditEvents(25)
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: "OFFLINE_AUDIT_READ_ERROR", message: error.message });
  }
});

app.post("/api/offline/print-labels", async (req, res) => {
  let validated = null;
  let lockKey = null;

  try {
    const state = readOfflineState();
    if (state.enabled !== true) {
      throw httpError(403, "OFFLINE_PRINTING_DISABLED", "Emergency offline printing is currently disabled.");
    }

    validated = validateOfflinePrintPayload(req.body || {});
    lockKey = validated.dryRun ? null : `offline|${validated.station}|${validated.lotNumber}`;

    if (lockKey) {
      const existing = activePrintJobs.get(lockKey);
      const now = Date.now();

      if (existing && (now - existing) > PRINT_LOCK_TTL_MS) {
        logWarn("offline_print_lock_expired", { lockKey, ageMs: now - existing }, `[OfflinePrint] Expiring stale print lock for ${lockKey} (ageMs=${now - existing})`);
        activePrintJobs.delete(lockKey);
      }

      if (activePrintJobs.has(lockKey)) {
        throw httpError(409, "PRINT_IN_PROGRESS", "An offline print job is already running for this station and lot.");
      }

      activePrintJobs.set(lockKey, now);
    }

    const preview = offlinePreview(validated);

    if (validated.dryRun) {
      writeOfflineAudit("offline_print_dry_run", req, buildOfflinePrintAuditDetails(validated, req.body, {
        printedCount: 0,
        preview,
        ok: true
      }));

      return res.json({
        ok: true,
        dryRun: true,
        station: validated.station,
        requestedFamily: validated.requestedFamily,
        family: validated.family,
        printer: validated.printer,
        template: validated.template,
        lotNumber: validated.lotNumber,
        firstBox: validated.firstBox,
        lastBox: validated.lastBox,
        requestedCount: validated.requestedCount,
        preview
      });
    }

    const results = [];
    let printedCount = 0;
    const printJobSpacingMs = getSafePrintJobSpacingMs();

    await enqueuePrinterWork(validated.printer, async () => {
      for (let box = validated.firstBox; box <= validated.lastBox; box++) {
        const namedDataSources = buildOfflineNamedDataSources(req.body || {}, validated.lotNumber, box);
        const rfid = namedDataSources.RFID;

        try {
          logEvent(
            "offline_print_attempt",
            { station: validated.station, lotNumber: validated.lotNumber, box, rfid, printer: validated.printer, template: validated.template },
            `[OfflinePrint] -> BarTender PRINT box=${box} rfid=${rfid} printer="${validated.printer}" template="${validated.template}"`
          );

          const action = await bartenderPrintBTW({
            documentPath: validated.template,
            printerName: validated.printer,
            namedDataSources,
            copies: 1
          });

          printedCount += 1;
          const result = {
            box,
            rfid,
            actionId: action?.Id || null,
            status: action?.Status || null
          };
          results.push(result);

          writeOfflineAudit("offline_print_label", req, buildOfflinePrintAuditDetails(validated, req.body, {
            box,
            rfid,
            printedCount: 1,
            namedDataSources,
            ok: true
          }));
        } catch (error) {
          writeOfflineAudit("offline_print_label", req, buildOfflinePrintAuditDetails(validated, req.body, {
            box,
            rfid,
            printedCount: 0,
            namedDataSources,
            ok: false,
            error: formatErrorDetail(error)
          }));
          throw error;
        }

        await sleep(printJobSpacingMs);
      }
    });

    writeOfflineAudit("offline_print_success", req, buildOfflinePrintAuditDetails(validated, req.body, {
      printedCount,
      preview,
      ok: true
    }));

    return res.json({
      ok: true,
      dryRun: false,
      station: validated.station,
      requestedFamily: validated.requestedFamily,
      family: validated.family,
      printer: validated.printer,
      template: validated.template,
      lotNumber: validated.lotNumber,
      firstBox: validated.firstBox,
      lastBox: validated.lastBox,
      requestedCount: validated.requestedCount,
      printedCount,
      preview,
      results
    });
  } catch (error) {
    if (error.code !== "PRINT_IN_PROGRESS") {
      writeOfflineAudit("offline_print_failure", req, buildOfflinePrintAuditDetails(validated, req.body || {}, {
        ok: false,
        error: formatErrorDetail(error)
      }));
    }

    return res.status(error.statusCode || 500).json({
      ok: false,
      error: error.code || "OFFLINE_PRINT_FAILED",
      message: error.message,
      details: error.details || undefined,
      bartender: error.response?.data || undefined
    });
  } finally {
    if (lockKey) activePrintJobs.delete(lockKey);
  }
});

app.post("/api/print", requireBearerToken, requireValidToken, handlePrintLot);
app.post("/print/lot", requireBearerToken, requireValidToken, handlePrintLot);
app.post("/api/print/sample-labels", requireBearerToken, requireValidToken, handlePrintSampleLabels);
app.post("/print/sample-labels", requireBearerToken, requireValidToken, handlePrintSampleLabels);

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

function normalizeRequestedBoxesFromBody(body) {
  if (Array.isArray(body?.boxes)) {
    return Array.from(new Set(
      body.boxes
        .map((v) => Number(v))
        .filter((n) => Number.isInteger(n) && n > 0 && n <= 9999)
    )).sort((a, b) => a - b);
  }

  const singleBoxRaw = body?.box ?? body?.boxNumber ?? body?.Box ?? body?.BoxNumber ?? body?.rm_box ?? null;
  if (singleBoxRaw != null && singleBoxRaw !== "") {
    const n = Number(singleBoxRaw);
    return Number.isInteger(n) && n > 0 && n <= 9999 ? [n] : [];
  }

  const firstBoxRaw = body?.firstBox ?? body?.FirstBox ?? body?.firstbox ?? null;
  const lastBoxRaw = body?.lastBox ?? body?.LastBox ?? body?.lastbox ?? firstBoxRaw;
  const firstBox = Number(firstBoxRaw);
  const lastBox = Number(lastBoxRaw);

  if (!Number.isInteger(firstBox) || !Number.isInteger(lastBox) || firstBox < 1 || lastBox > 9999 || firstBox > lastBox) return [];

  const out = [];
  for (let b = firstBox; b <= lastBox; b++) out.push(b);
  return out;
}

function isSampleByPoundsMode(body) {
  const rawMode = String(body?.sampleMode ?? body?.mode ?? "").trim().toLowerCase();
  return body?.byPounds === true || body?.sampleByPounds === true || rawMode === "pounds" || rawMode === "bypounds" || rawMode === "by-pounds";
}

function normalizeSamplePoundLabelValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";

  if (/^last\s*box$/i.test(raw)) return "Last Box";

  const numericText = raw.replace(/[,\s]/g, "");
  if (/^\d+$/.test(numericText)) {
    const n = Number(numericText);
    if (Number.isInteger(n) && n > 0) return String(n);
  }

  return raw.slice(0, 64);
}

function normalizeRequestedPoundSampleLabelsFromBody(body) {
  const rawLabels = Array.isArray(body?.poundLabels)
    ? body.poundLabels
    : Array.isArray(body?.samplePoundLabels)
      ? body.samplePoundLabels
      : Array.isArray(body?.poundMilestones)
        ? body.poundMilestones
        : [];

  const source = rawLabels.length ? rawLabels : QC_SAMPLE_POUNDS_DEFAULT_LABELS;
  const seen = new Set();
  const out = [];

  for (const value of source) {
    const normalized = normalizeSamplePoundLabelValue(value);
    const key = normalized.toLowerCase();
    if (!normalized || seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
  }

  return out;
}

async function handlePrintSampleLabels(req, res) {
  let lockKey = null;

  try {
    const body = req.body || {};

    const stationRaw =
      body.station ??
      body.printStation ??
      body.stationCode ??
      body.stationId ??
      null;

    const station = normalizeSampleStation(normalizeStation(stationRaw));
    const labelKind = normalizeSampleLabelKind(body.labelKind ?? body.labelType ?? body.templateType ?? body.type);

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

    const byPounds = isSampleByPoundsMode(body);
    const requestedPoundLabels = byPounds ? normalizeRequestedPoundSampleLabelsFromBody(body) : [];
    const requestedBoxes = byPounds ? [] : normalizeRequestedBoxesFromBody(body);
    const allowMissing = body.allowMissing === true;
    const dryRun = body.dryRun === true;

    if (byPounds && labelKind !== "QCSample") {
      return res.status(400).json({
        ok: false,
        error: "By-pounds sample labels are only supported for QCSample.",
        got: { labelKind, byPounds }
      });
    }

    if (!station || (!lotIdFromBody && !lotNumberFromBody) || (!byPounds && !requestedBoxes.length) || (byPounds && !requestedPoundLabels.length)) {
      return res.status(400).json({
        ok: false,
        error: byPounds
          ? "station, lotId/lotNumber, labelKind, and poundLabels are required"
          : "station, lotId/lotNumber, labelKind, and boxes are required",
        got: { stationRaw, stationNormalized: station, lotId: lotIdFromBody, lotNumber: lotNumberFromBody, labelKind, boxes: body.boxes, byPounds, poundLabels: requestedPoundLabels }
      });
    }

    const baseUrl = getDvUrlForRequest(req);

    let effectiveLotId = lotIdFromBody;
    if (!effectiveLotId && lotNumberFromBody) {
      effectiveLotId = await getLotIdByLotNumber(baseUrl, lotNumberFromBody);
    }

    const lotNumber = await getLotNumberById(baseUrl, effectiveLotId);
    const { printer, template } = resolvePrinterAndSampleTemplate({ station, labelKind, byPounds });

    lockKey = `${station}|${normalizeGuid(effectiveLotId)}|${labelKind}${byPounds ? "|pounds" : ""}`;
    const existing = activePrintJobs.get(lockKey);
    const now = Date.now();

    if (existing && (now - existing) > PRINT_LOCK_TTL_MS) {
      logWarn("sample_print_lock_expired", { lockKey, ageMs: now - existing }, `[PrintSvc] Expiring stale sample-label print lock for ${lockKey} (ageMs=${now - existing})`);
      activePrintJobs.delete(lockKey);
    }

    if (activePrintJobs.has(lockKey)) {
      return res.status(409).json({
        ok: false,
        code: "PRINT_IN_PROGRESS",
        message: "A sample-label print job is already running for this station and lot. Please wait a moment and try again.",
        station,
        lotId: normalizeGuid(effectiveLotId),
        labelKind
      });
    }

    if (!dryRun) activePrintJobs.set(lockKey, now);

    if (byPounds) {
      if (dryRun === true) {
        return res.json({
          ok: true,
          dryRun: true,
          baseUrl,
          lotId: normalizeGuid(effectiveLotId),
          lotNumber,
          station,
          labelKind,
          byPounds: true,
          printer,
          template,
          requestedPoundLabels,
          requestedCount: requestedPoundLabels.length,
          missingBoxes: []
        });
      }

      const lotLabelData = await getLotLabelData(baseUrl, effectiveLotId, { includeMachine: true, includeCompany: true });
      const printedBy = req.user?.preferred_username || req.user?.upn || "";
      const results = [];
      const printJobSpacingMs = getSafePrintJobSpacingMs();

      logInfo(
        "sample_print_pounds_sequence_resolved",
        { station, lotNumber, labelKind, printer, template, requestedPoundLabels, printJobSpacingMs },
        `[PrintSvc] Sample-label by-pounds sequence resolved station=${station} lot=${lotNumber} kind=${labelKind}: ${requestedPoundLabels.join(",")}`
      );

      await enqueuePrinterWork(printer, async () => {
        for (const poundLabel of requestedPoundLabels) {
          const labelValue = String(poundLabel);
          const logRfid = `${lotNumber}-${labelValue.replace(/\s+/g, "")}`;

          const named = {
            lot: lotNumber,
            firstbox: labelValue,
            box: labelValue,
            Box: labelValue,
            RFID: "",
            rfid: "",
            pounds: labelValue,
            po: lotLabelData.po,
            prodname: lotLabelData.prodname,
            proddesc: lotLabelData.proddesc,
            prodnum: lotLabelData.prodnum,
            product: lotLabelData.product,
            color: lotLabelData.color,
            type: lotLabelData.type,
            tolling: lotLabelData.tolling,
            company: lotLabelData.company,
            machine: lotLabelData.machine,
            labeltype: "QC Sample",
            sampletype: "QC",
            erp: ""
          };

          try {
            logEvent("sample_print_pounds_attempt", { station, lotNumber, labelKind, poundLabel: labelValue, printer, template }, `[PrintSvc] -> BarTender SAMPLE POUNDS PRINT kind=${labelKind} label=${labelValue} printer="${printer}" template="${template}"`);

            const action = await bartenderPrintBTW({
              documentPath: template,
              printerName: printer,
              namedDataSources: named,
              copies: 1
            });

            const actionId = action?.Id || null;
            const status = action?.Status || null;

            logInfo("sample_print_pounds_success", { station, lotNumber, labelKind, poundLabel: labelValue, printer, template, actionId, status }, `[PrintSvc] <- BarTender sample-pounds actionId=${actionId} status=${status} label=${labelValue}`);
            results.push({ box: labelValue, poundLabel: labelValue, rfid: "", pounds: labelValue, actionId, status });

            await writePrintLog(baseUrl, {
              lotId: effectiveLotId,
              inventoryId: null,
              rfid: logRfid,
              station,
              printedBy,
              result: `Success-${labelKind}-Pounds`,
              notes: `By-pounds sample label: ${labelValue}`
            });
          } catch (e) {
            const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
            logError("sample_print_pounds_failure", { station, lotNumber, labelKind, poundLabel: labelValue, printer, template, message: msg }, `[PrintSvc] FAILED sample pounds label kind=${labelKind} label=${labelValue} lot=${lotNumber} station=${station}: ${msg}`);

            await writePrintLog(baseUrl, {
              lotId: effectiveLotId,
              inventoryId: null,
              rfid: logRfid,
              station,
              printedBy,
              result: `Failed-${labelKind}-Pounds`,
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
        labelKind,
        byPounds: true,
        printer,
        template,
        requestedPoundLabels,
        requestedCount: requestedPoundLabels.length,
        printedCount: results.length,
        missingBoxes: [],
        results
      });
    }

    const firstBox = Math.min(...requestedBoxes);
    const lastBox = Math.max(...requestedBoxes);
    const rows = await getInventoryRowsForLotRange(baseUrl, effectiveLotId, firstBox, lastBox);

    const byBox = new Map();
    for (const r of rows) {
      const b = Number(r[DV_INV_BOX_COL]);
      if (!Number.isInteger(b)) continue;
      if (!byBox.has(b)) byBox.set(b, r);
    }

    const missingBoxes = requestedBoxes.filter((b) => !byBox.has(b));

    if (dryRun === true) {
      return res.json({
        ok: true,
        dryRun: true,
        baseUrl,
        lotId: normalizeGuid(effectiveLotId),
        lotNumber,
        station,
        labelKind,
        printer,
        template,
        requestedBoxes,
        requestedCount: requestedBoxes.length,
        foundCount: rows.length,
        missingBoxes
      });
    }

    if (missingBoxes.length > 0 && allowMissing !== true) {
      logWarn("sample_print_missing_boxes", { station, lotNumber, labelKind, missingBoxes }, `[PrintSvc] ABORT sample-label missing boxes station=${station} lot=${lotNumber} kind=${labelKind} missing=${missingBoxes.join(",")}`);
      return res.status(409).json({
        ok: false,
        code: "MISSING_BOXES",
        message: "Some selected boxes were not found in Inventory. Adjust the selection and try again.",
        lotNumber,
        station,
        labelKind,
        missingBoxes
      });
    }

    const lotLabelData = await getLotLabelData(baseUrl, effectiveLotId, { includeMachine: true, includeCompany: true });
    const printedBy = req.user?.preferred_username || req.user?.upn || "";
    const results = [];
    const printJobSpacingMs = getSafePrintJobSpacingMs();

    logInfo(
      "sample_print_sequence_resolved",
      { station, lotNumber, labelKind, printer, template, requestedBoxes, printJobSpacingMs },
      `[PrintSvc] Sample-label sequence resolved station=${station} lot=${lotNumber} kind=${labelKind}: ${requestedBoxes.join(",")}`
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
            result: `Skipped-${labelKind}`,
            notes: "Inventory row missing for this sample-label box number"
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
          box: String(box),
          Box: String(box),
          RFID: String(rfid),
          rfid: String(rfid),
          pounds: isNoWeight ? "_" : (poundsVal == null ? "" : String(poundsVal)),
          po: lotLabelData.po,
          prodname: lotLabelData.prodname,
          proddesc: lotLabelData.proddesc,
          prodnum: lotLabelData.prodnum,
          product: lotLabelData.product,
          color: lotLabelData.color,
          type: lotLabelData.type,
          tolling: lotLabelData.tolling,
          company: lotLabelData.company,
          machine: lotLabelData.machine,
          labeltype: labelKind === "QCRetain" ? "Retain Sample" : "QC Sample",
          sampletype: labelKind === "QCRetain" ? "Retain" : "QC",
          erp: ""
        };

        try {
          logEvent("sample_print_attempt", { station, lotNumber, labelKind, box, rfid, printer, template }, `[PrintSvc] -> BarTender SAMPLE PRINT kind=${labelKind} box=${box} rfid=${rfid} printer="${printer}" template="${template}"`);

          const action = await bartenderPrintBTW({
            documentPath: template,
            printerName: printer,
            namedDataSources: named,
            copies: 1
          });

          const actionId = action?.Id || null;
          const status = action?.Status || null;

          logInfo("sample_print_success", { station, lotNumber, labelKind, box, rfid, printer, template, actionId, status }, `[PrintSvc] <- BarTender sample actionId=${actionId} status=${status} box=${box}`);
          results.push({ box, rfid, pounds: named.pounds, actionId, status });

          await writePrintLog(baseUrl, {
            lotId: effectiveLotId,
            inventoryId,
            rfid,
            station,
            printedBy,
            result: `Success-${labelKind}`,
            notes: ""
          });
        } catch (e) {
          const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
          logError("sample_print_failure", { station, lotNumber, labelKind, box, rfid, printer, template, message: msg }, `[PrintSvc] FAILED sample label kind=${labelKind} box=${box} lot=${lotNumber} station=${station}: ${msg}`);

          await writePrintLog(baseUrl, {
            lotId: effectiveLotId,
            inventoryId,
            rfid,
            station,
            printedBy,
            result: `Failed-${labelKind}`,
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
      labelKind,
      printer,
      template,
      requestedBoxes,
      requestedCount: requestedBoxes.length,
      printedCount: results.length,
      missingBoxes,
      results
    });
  } catch (e) {
    logError("sample_print_request_failed", { message: e.message, bartender: e.response?.data || null, lockKey });
    return res.status(500).json({ ok: false, message: e.message, bartender: e.response?.data || null });
  } finally {
    if (lockKey) activePrintJobs.delete(lockKey);
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

if (require.main === module) {
  app.listen(Number(PORT), "0.0.0.0", () => {
    logInfo("service_listening", { port: Number(PORT), host: "0.0.0.0" }, `PrintSvc listening on http://0.0.0.0:${PORT}`);
  });
}

module.exports = {
  app,
  buildOfflineNamedDataSources,
  generateOfflineRfid,
  normalizeOfflineFamily,
  resolvePrinterAndTemplate,
  resolvePrinterAndTemplateForFamily
};
