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
const net = require("net");
const path = require("path");
const multer = require("multer");
const zlib = require("zlib");
const { appendOfflineAuditEvent, readLatestOfflineAuditEvents } = require("./lib/offlineAudit");
const { readOfflineState, writeOfflineState } = require("./lib/offlineState");
const {
  getSourceIp,
  requireOfflineAdminCookie,
  requireOfflineLocalAccess,
  setAdminCookie
} = require("./lib/offlineSecurity");
const {
  FIELD_FIT_DEFINITIONS_COMMENT_PREFIX,
  loadZplTemplate,
  getFittedFieldDefinitions,
  renderZplTemplateFile,
  renderZplTemplateWithMetadata,
  renderZplTemplateFileWithoutRfid,
  renderZplTemplateWithoutRfidWithMetadata,
  rfidTextToHex,
  sendZplOverTcp
} = require("./lib/zplPrinter");
const {
  FG_STATIONS,
  QC_STATIONS,
  RAW_STATIONS,
  fgTemplateForStation,
  getStationProfile,
  getTemplateDefinition,
  qcRetainTemplateForStation,
  qcSamplePoundsTemplateForStation,
  qcSampleTemplateForStation,
  rawTemplateForStation,
  listStationProfiles,
  listTemplateLabTemplates
} = require("./lib/zplProfiles");

const CONFIG_DIR = process.env.PRINTSVC_CONFIG_DIR || "C:\\PrintSvc";
const TEMPLATE_DIR = process.env.BARTENDER_TEMPLATE_DIR || "C:\\RFID";
const mappingsPath = path.join(CONFIG_DIR, "mappings.json");
const OFFLINE_PUBLIC_DIR = path.join(__dirname, "public", "offline");
const OFFLINE_ASSETS_DIR = path.join(OFFLINE_PUBLIC_DIR, "assets");
const ZPL_TEMPLATE_SOURCE_DIR = process.env.ZPL_TEMPLATE_SOURCE_DIR || process.env.ZPL_TEMPLATE_DIR || "C:\\RFID\\zpl";
const ZPL_TEMPLATE_LAB_PROFILE_PATH = process.env.ZPL_TEMPLATE_LAB_PROFILE_PATH || "C:\\PrintSvc\\template-lab-profiles.json";
const ZPL_TEMPLATE_LAB_ASSET_DIR = process.env.ZPL_TEMPLATE_LAB_ASSET_DIR || path.join(ZPL_TEMPLATE_SOURCE_DIR, "assets");
const PRINTSVC_LOG_PATH = process.env.PRINTSVC_LOG_PATH || path.join(CONFIG_DIR, "logs", "printsvc-out.log");
const PRINTSVC_LOG_TAIL_DEFAULT = 500;
const PRINTSVC_LOG_TAIL_MAX = 5000;
const PRINTSVC_LOG_READ_MAX_BYTES = 16 * 1024 * 1024;

const GRAPH_DRIVE_CACHE_MS = 6 * 60 * 60 * 1000; // 6 hours
const SMALL_UPLOAD_MAX = 4 * 1024 * 1024; // 4 MB or whatever threshold you want

const DV_INV_NOWEIGHT_COL = process.env.DV_INV_NOWEIGHT_COL || "rm_noweightmode";
const PRINT_JOB_SPACING_MS = Number(process.env.PRINT_JOB_SPACING_MS || 1500);
const ZPL_TCP_TIMEOUT_DEFAULT_MS = 120000;
const ZPL_LABEL_SPACING_DEFAULT_MS = 8000;
const ZPL_CONNECT_RETRY_COUNT_DEFAULT = 0;
const ZPL_CONNECT_RETRY_DELAY_DEFAULT_MS = 3000;
const ZPL_DUPLICATE_GUARD_TTL_MS = 10 * 60 * 1000;
const ZPL_DUPLICATE_POLICY_DEFAULT = "skip_recent";
const ZPL_DUPLICATE_POLICIES = Object.freeze(["skip_recent", "allow"]);
const ZPL_SOCKET_MODE_DEFAULT = "per_label";
const ZPL_SOCKET_MODES = Object.freeze(["per_label", "persistent", "batch"]);
const ZPL_MAX_LABELS_PER_CONNECTION_DEFAULT = 50;
const ZPL_SOCKET_IDLE_CLOSE_DEFAULT_MS = 30000;
const ZPL_BATCH_MAX_LABELS_DEFAULT = 60;
const ZPL_BATCH_COLLECT_DEFAULT_MS = 1500;
const ZPL_BATCH_INTER_BATCH_DELAY_DEFAULT_MS = 0;
const ZPL_BATCH_MAX_BYTES_DEFAULT = 512 * 1024;
const ZPL_QUEUE_DIR = process.env.ZPL_QUEUE_DIR || path.join(CONFIG_DIR, "queue");
const ZPL_STALE_SENDING_THRESHOLD_DEFAULT_MS = 2 * 60 * 1000;
const DEFAULT_DIRECT_ZPL_ENABLED_SCOPES = "P1:RAW";
const DEFAULT_DIRECT_ZPL_RAW_TEMPLATE_PATHS = Object.freeze(Object.fromEntries(
  RAW_STATIONS.map((station) => [station, path.join(ZPL_TEMPLATE_SOURCE_DIR, rawTemplateForStation(station))])
));
const DEFAULT_DIRECT_ZPL_FG_TEMPLATE_PATHS = Object.freeze(Object.fromEntries(
  FG_STATIONS.map((station) => [station, path.join(ZPL_TEMPLATE_SOURCE_DIR, fgTemplateForStation(station))])
));
const DEFAULT_DIRECT_ZPL_SAMPLE_TEMPLATE_PATHS = Object.freeze({
  SAMPLE: Object.freeze(Object.fromEntries(
    QC_STATIONS.map((station) => [station, path.join(ZPL_TEMPLATE_SOURCE_DIR, qcSampleTemplateForStation(station))])
  )),
  RETAIN: Object.freeze(Object.fromEntries(
    QC_STATIONS.map((station) => [station, path.join(ZPL_TEMPLATE_SOURCE_DIR, qcRetainTemplateForStation(station))])
  )),
  SAMPLE_POUNDS: Object.freeze(Object.fromEntries(
    QC_STATIONS.map((station) => [station, path.join(ZPL_TEMPLATE_SOURCE_DIR, qcSamplePoundsTemplateForStation(station))])
  ))
});
const DIRECT_ZPL_P3_SAMPLE_PRINTER_DEFAULT = Object.freeze({
  ip: "192.168.50.218",
  port: 9100,
  printer: "Zebra ZT230 P3 EXT"
});
const DIRECT_ZPL_P8_SAMPLE_PRINTER_DEFAULT = Object.freeze({
  ip: "192.168.50.214",
  port: 9100,
  printer: "Zebra ZT230 P8 State"
});
const DIRECT_ZPL_SAMPLE_PRINTER_DEFAULTS = Object.freeze({
  SAMPLE: Object.freeze({
    P3: DIRECT_ZPL_P3_SAMPLE_PRINTER_DEFAULT,
    P8: DIRECT_ZPL_P8_SAMPLE_PRINTER_DEFAULT
  }),
  RETAIN: Object.freeze({
    P3: DIRECT_ZPL_P3_SAMPLE_PRINTER_DEFAULT,
    P8: DIRECT_ZPL_P8_SAMPLE_PRINTER_DEFAULT
  }),
  SAMPLE_POUNDS: Object.freeze({
    P3: DIRECT_ZPL_P3_SAMPLE_PRINTER_DEFAULT,
    P8: DIRECT_ZPL_P8_SAMPLE_PRINTER_DEFAULT
  })
});
const DIRECT_ZPL_QUEUE_STATUSES = Object.freeze([
  "queued",
  "sending",
  "sent_to_printer",
  "unknown_after_send",
  "failed_before_send",
  "rejected"
]);
const DUPLICATE_RECENT_ZPL_SKIP_MESSAGE = "Label was already accepted recently and was skipped to prevent duplicate RFID.";
const DIRECT_ZPL_RAW_PRINTER_DEFAULTS = Object.freeze({
  P1: Object.freeze({ ip: "192.168.50.239", port: 9100, templateFamily: "RAW" }),
  P2: Object.freeze({ ip: "192.168.50.241", port: 9100, templateFamily: "RAW" }),
  P3: Object.freeze({ ip: "192.168.50.223", port: 9100, templateFamily: "RAW" }),
  P4: Object.freeze({ ip: "192.168.50.242", port: 9100, templateFamily: "RAW" }),
  P5: Object.freeze({ ip: "192.168.50.244", port: 9100, templateFamily: "RAW" }),
  P6: Object.freeze({ ip: "192.168.6.240", port: 9100, templateFamily: "RAW" }),
  P7: Object.freeze({ ip: "192.168.8.200", port: 9100, templateFamily: "RAW" }),
  P8: Object.freeze({ ip: "192.168.7.122", port: 9100, templateFamily: "RAW" })
});
const DIRECT_ZPL_SUPPORTED_PILOT_SCOPES = Object.freeze({
  P1: Object.freeze(["RAW", "FG"]),
  P2: Object.freeze(["RAW", "FG"]),
  P3: Object.freeze(["RAW", "FG", "SAMPLE", "RETAIN", "SAMPLE_POUNDS"]),
  P4: Object.freeze(["RAW", "FG"]),
  P5: Object.freeze(["RAW", "FG"]),
  P6: Object.freeze(["RAW", "FG"]),
  P7: Object.freeze(["RAW", "FG"]),
  P8: Object.freeze(["RAW", "FG", "SAMPLE", "RETAIN", "SAMPLE_POUNDS"])
});

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

function normalizePrintEngine(value) {
  const engine = String(value || "bartender").trim().toLowerCase();
  if (engine === "bartender" || engine === "zpl") return engine;

  const error = new Error("Invalid PRINT_ENGINE. Expected 'bartender' or 'zpl'.");
  error.code = "INVALID_PRINT_ENGINE";
  error.statusCode = 500;
  error.details = { printEngine: value };
  throw error;
}

function getConfiguredPrintEngine() {
  return normalizePrintEngine(process.env.PRINT_ENGINE);
}

function getPrintEngineHealth() {
  try {
    return { ok: true, printEngine: getConfiguredPrintEngine() };
  } catch (error) {
    return {
      ok: false,
      printEngine: "invalid",
      message: error.message
    };
  }
}

function getNonNegativeIntegerEnv(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isInteger(value) && value >= 0 ? value : fallback;
}

function getPositiveIntegerEnvValue(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function getZplTcpTimeoutMs() {
  return getPositiveIntegerEnvValue("ZPL_TCP_TIMEOUT_MS", ZPL_TCP_TIMEOUT_DEFAULT_MS);
}

function getZplLabelSpacingMs() {
  return getNonNegativeIntegerEnv("ZPL_LABEL_SPACING_MS", ZPL_LABEL_SPACING_DEFAULT_MS);
}

function getZplConnectRetryCount() {
  return getNonNegativeIntegerEnv("ZPL_CONNECT_RETRY_COUNT", ZPL_CONNECT_RETRY_COUNT_DEFAULT);
}

function getZplConnectRetryDelayMs() {
  return getNonNegativeIntegerEnv("ZPL_CONNECT_RETRY_DELAY_MS", ZPL_CONNECT_RETRY_DELAY_DEFAULT_MS);
}

function getZplStaleSendingThresholdMs() {
  return getPositiveIntegerEnvValue("ZPL_STALE_SENDING_THRESHOLD_MS", ZPL_STALE_SENDING_THRESHOLD_DEFAULT_MS);
}

function normalizeZplDuplicatePolicy(value) {
  const policy = String(value || ZPL_DUPLICATE_POLICY_DEFAULT).trim().toLowerCase();
  if (ZPL_DUPLICATE_POLICIES.includes(policy)) return policy;

  const error = new Error("Invalid ZPL_DUPLICATE_POLICY. Expected 'skip_recent' or 'allow'.");
  error.code = "INVALID_ZPL_DUPLICATE_POLICY";
  error.statusCode = 500;
  error.details = { zplDuplicatePolicy: value };
  throw error;
}

function getZplDuplicatePolicy() {
  return normalizeZplDuplicatePolicy(process.env.ZPL_DUPLICATE_POLICY);
}

function getZplDuplicatePolicyHealth() {
  try {
    return { ok: true, zplDuplicatePolicy: getZplDuplicatePolicy() };
  } catch (error) {
    return {
      ok: false,
      zplDuplicatePolicy: "invalid",
      message: error.message
    };
  }
}

function normalizeZplSocketMode(value) {
  const mode = String(value || ZPL_SOCKET_MODE_DEFAULT).trim().toLowerCase();
  if (ZPL_SOCKET_MODES.includes(mode)) return mode;

  const error = new Error("Invalid ZPL_SOCKET_MODE. Expected 'per_label', 'persistent', or 'batch'.");
  error.code = "INVALID_ZPL_SOCKET_MODE";
  error.statusCode = 500;
  error.details = { zplSocketMode: value };
  throw error;
}

function getZplSocketMode() {
  return normalizeZplSocketMode(process.env.ZPL_SOCKET_MODE);
}

function getZplSocketModeHealth() {
  try {
    return { ok: true, zplSocketMode: getZplSocketMode() };
  } catch (error) {
    return {
      ok: false,
      zplSocketMode: "invalid",
      message: error.message
    };
  }
}

function getZplMaxLabelsPerConnection() {
  return getPositiveIntegerEnvValue("ZPL_MAX_LABELS_PER_CONNECTION", ZPL_MAX_LABELS_PER_CONNECTION_DEFAULT);
}

function getZplSocketIdleCloseMs() {
  return getPositiveIntegerEnvValue("ZPL_SOCKET_IDLE_CLOSE_MS", ZPL_SOCKET_IDLE_CLOSE_DEFAULT_MS);
}

function getZplBatchMaxLabels() {
  return getPositiveIntegerEnvValue("ZPL_BATCH_MAX_LABELS", ZPL_BATCH_MAX_LABELS_DEFAULT);
}

function getZplBatchCollectMs() {
  return getNonNegativeIntegerEnv("ZPL_BATCH_COLLECT_MS", ZPL_BATCH_COLLECT_DEFAULT_MS);
}

function getZplBatchInterBatchDelayMs() {
  return getNonNegativeIntegerEnv("ZPL_BATCH_INTER_BATCH_DELAY_MS", ZPL_BATCH_INTER_BATCH_DELAY_DEFAULT_MS);
}

function getZplBatchMaxBytes() {
  return getPositiveIntegerEnvValue("ZPL_BATCH_MAX_BYTES", ZPL_BATCH_MAX_BYTES_DEFAULT);
}

function getZplTransportSettings() {
  return {
    tcpTimeoutMs: getZplTcpTimeoutMs(),
    labelSpacingMs: getZplLabelSpacingMs(),
    connectRetryCount: getZplConnectRetryCount(),
    connectRetryDelayMs: getZplConnectRetryDelayMs(),
    socketMode: getZplSocketMode(),
    maxLabelsPerConnection: getZplMaxLabelsPerConnection(),
    socketIdleCloseMs: getZplSocketIdleCloseMs(),
    batchMaxLabels: getZplBatchMaxLabels(),
    batchCollectMs: getZplBatchCollectMs(),
    batchInterBatchDelayMs: getZplBatchInterBatchDelayMs(),
    batchMaxBytes: getZplBatchMaxBytes()
  };
}

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

function resolveZplTemplatePath(templateValue) {
  const raw = String(templateValue || "").trim();
  if (!raw) throw new Error("ZPL template mapping is blank.");

  const fileName = path.basename(path.win32.basename(raw));
  if (!fileName) throw new Error("ZPL template mapping does not include a filename.");
  return path.join(ZPL_TEMPLATE_SOURCE_DIR, fileName);
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

function zplMappingError(message, details = {}) {
  const error = new Error(message);
  error.code = "ZPL_MAPPING_NOT_FOUND";
  error.statusCode = 400;
  error.details = details;
  return error;
}

function unsupportedDirectZplError(message, details = {}) {
  const error = new Error(message);
  error.code = "UNSUPPORTED_DIRECT_ZPL";
  error.statusCode = 400;
  error.details = {
    supportedScopes: getDirectZplEnabledScopes(),
    ...details
  };
  return error;
}

function getDirectZplConfig() {
  return mappings.directZpl || mappings.zpl || {};
}

function normalizeDirectZplScopeFamily(value) {
  const raw = String(value || "RAW").trim().toUpperCase().replace(/[\s\-]/g, "_");
  const compact = raw.replace(/_/g, "");

  if (raw === "RAW" || raw === "FG") return raw;
  if (["SAMPLE", "QCSAMPLE", "QC"].includes(compact)) return "SAMPLE";
  if (["RETAIN", "QCRETAIN", "RETAINSAMPLE"].includes(compact)) return "RETAIN";
  if (["SAMPLEPOUNDS", "QCSAMPLEPOUNDS", "POUNDS", "BYPOUNDS", "SAMPLEBYPOUNDS"].includes(compact)) return "SAMPLE_POUNDS";

  return raw;
}

function parseDirectZplEnabledScopes(value = process.env.DIRECT_ZPL_ENABLED_SCOPES || DEFAULT_DIRECT_ZPL_ENABLED_SCOPES) {
  return String(value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [stationRaw, familyRaw = "RAW"] = entry.split(":");
      return {
        station: String(stationRaw || "").trim().toUpperCase(),
        family: normalizeDirectZplScopeFamily(familyRaw)
      };
    })
    .filter((scope) => scope.station && scope.family);
}

function getDirectZplEnabledScopes() {
  return parseDirectZplEnabledScopes();
}

function isKnownDirectZplPilotScope({ station, family }) {
  const st = String(station || "").trim().toUpperCase();
  const fam = normalizeDirectZplScopeFamily(family);
  return (DIRECT_ZPL_SUPPORTED_PILOT_SCOPES[st] || []).includes(fam);
}

function isDirectZplPilotSupported({ station, family }) {
  const st = String(station || "").trim().toUpperCase();
  const fam = normalizeDirectZplScopeFamily(family);
  if (!isKnownDirectZplPilotScope({ station: st, family: fam })) return false;
  return getDirectZplEnabledScopes().some((scope) => scope.station === st && scope.family === fam);
}

function getDirectZplPrinterConfig(directZpl, station, family = "") {
  const st = String(station || "").trim().toUpperCase();
  const fam = normalizeDirectZplScopeFamily(family);
  const familyPrinter = fam ? (
    directZpl.familyPrinters?.[fam]?.[st] ||
    directZpl.printersByFamily?.[fam]?.[st] ||
    directZpl.printerOverrides?.[fam]?.[st] ||
    DIRECT_ZPL_SAMPLE_PRINTER_DEFAULTS[fam]?.[st]
  ) : null;

  return familyPrinter ||
    directZpl.printers?.[st] ||
    directZpl.stations?.[st] ||
    mappings.zplPrinters?.[st] ||
    DIRECT_ZPL_RAW_PRINTER_DEFAULTS[st] ||
    null;
}

function getDirectZplTemplateValue(directZpl, family, station, printerConfig = {}) {
  const fam = normalizeDirectZplScopeFamily(family);
  const st = String(station || "").trim().toUpperCase();
  const genericPrinterTemplate = printerConfig.templatePath || printerConfig.template || "";

  return directZpl.templates?.[fam]?.[st] ||
    mappings.zplTemplates?.[fam]?.[st] ||
    printerConfig.templates?.[fam] ||
    (fam === "RAW" ? genericPrinterTemplate : "") ||
    (fam === "RAW" ? directZpl.templates?.RAW?.P1 : "") ||
    (fam === "RAW" ? DEFAULT_DIRECT_ZPL_RAW_TEMPLATE_PATHS[st] : "") ||
    (fam === "FG" ? DEFAULT_DIRECT_ZPL_FG_TEMPLATE_PATHS[st] : "") ||
    (DEFAULT_DIRECT_ZPL_SAMPLE_TEMPLATE_PATHS[fam]?.[st] || "");
}

function getDirectZplPilotMappingsForLog() {
  const directZpl = getDirectZplConfig();
  return getDirectZplEnabledScopes().map((scope) => {
    const printerConfig = getDirectZplPrinterConfig(directZpl, scope.station, scope.family) || {};
    const templateValue = getDirectZplTemplateValue(directZpl, scope.family, scope.station, printerConfig);
    return {
      station: scope.station,
      family: scope.family,
      printerName: String(printerConfig.printer || printerConfig.name || printerConfig.displayName || ""),
      printerIp: String(printerConfig.ip || printerConfig.printerIp || printerConfig.host || ""),
      port: Number(printerConfig.port || 9100),
      templatePath: templateValue ? resolveZplTemplatePath(templateValue) : ""
    };
  });
}

function logUnsupportedDirectZpl({ station, family, reason }) {
  logWarn(
    "direct_zpl_unsupported_skipped",
    { station, family, reason, supportedScopes: getDirectZplEnabledScopes() },
    `[PrintSvc] Direct ZPL skipped for unsupported station/family station=${station} family=${family}: ${reason}`
  );
}

function resolveZplPrinterAndTemplate({ station, family }) {
  const st = String(station || "").trim().toUpperCase();
  const fam = normalizeDirectZplScopeFamily(family);
  const directZpl = getDirectZplConfig();

  if (!isDirectZplPilotSupported({ station: st, family: fam })) {
    const reason = "Emergency direct-ZPL mode is enabled only for configured supported DIRECT_ZPL_ENABLED_SCOPES.";
    logUnsupportedDirectZpl({ station: st, family: fam, reason });
    throw unsupportedDirectZplError(reason, { station: st, family: fam });
  }

  const printerConfig = getDirectZplPrinterConfig(directZpl, st, fam);

  if (!printerConfig) {
    throw zplMappingError(`No direct-ZPL printer mapping for station='${st}'.`, {
      station: st,
      family: fam
    });
  }

  const templateValue = getDirectZplTemplateValue(directZpl, fam, st, printerConfig);

  if (!templateValue) {
    throw zplMappingError(`No direct-ZPL template mapping for family='${fam}' station='${st}'.`, {
      station: st,
      family: fam
    });
  }

  const printerIp = String(printerConfig.ip || printerConfig.printerIp || printerConfig.host || "").trim();
  const port = Number(printerConfig.port || 9100);

  if (!printerIp) {
    throw zplMappingError(`No direct-ZPL printer IP for station='${st}'.`, {
      station: st,
      family: fam
    });
  }

  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw zplMappingError(`Invalid direct-ZPL printer port for station='${st}'.`, {
      station: st,
      family: fam,
      port: printerConfig.port
    });
  }

  return {
    station: st,
    family: fam,
    printerName: String(printerConfig.printer || printerConfig.name || printerConfig.displayName || ""),
    printerIp,
    port,
    templatePath: resolveZplTemplatePath(templateValue),
    templateFamily: fam
  };
}

function resolveRfidPrintTarget({ station, lotNumber }) {
  const bartender = resolvePrinterAndTemplate({ station, lotNumber });
  const printEngine = getConfiguredPrintEngine();

  if (printEngine === "bartender") {
    return {
      printEngine,
      family: bartender.family,
      printer: bartender.printer,
      template: bartender.template,
      zpl: null
    };
  }

  return {
    printEngine,
    family: bartender.family,
    printer: bartender.printer,
    template: bartender.template,
    zpl: resolveZplPrinterAndTemplate({ station, family: bartender.family })
  };
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

function resolveRfidPrintTargetForFamily({ station, lotNumber, family }) {
  const bartender = resolvePrinterAndTemplateForFamily({ station, lotNumber, family });
  const printEngine = getConfiguredPrintEngine();

  if (printEngine === "bartender") {
    return {
      ...bartender,
      printEngine,
      zpl: null
    };
  }

  return {
    ...bartender,
    printEngine,
    zpl: resolveZplPrinterAndTemplate({ station: bartender.station || station, family: bartender.family })
  };
}

const startupPrintEngineHealth = getPrintEngineHealth();
logInfo(
  "print_engine_config",
  {
    printEngine: startupPrintEngineHealth.printEngine,
    printEngineOk: startupPrintEngineHealth.ok,
    directZplPilotScopes: getDirectZplEnabledScopes(),
    directZplEnabledScopes: getDirectZplEnabledScopes(),
    directZplPilotMappings: getDirectZplPilotMappingsForLog(),
    zplTransportSettings: getZplTransportSettings(),
    zplDuplicatePolicy: getZplDuplicatePolicyHealth().zplDuplicatePolicy,
    zplStaleSendingThresholdMs: getZplStaleSendingThresholdMs(),
    directZplLimitation: "RAW and FG P1-P8 plus P3/P8 sample, retain, and sample-by-pounds labels; stations controlled by DIRECT_ZPL_ENABLED_SCOPES"
  },
  `[PrintSvc] Print engine=${startupPrintEngineHealth.printEngine}; direct-ZPL scopes=${getDirectZplEnabledScopes().map((scope) => `${scope.station}:${scope.family}`).join(",")}`
);

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
  ? (
      mappings.templates?.QCSamplePounds?.[st] ||
      mappings.templates?.SAMPLE_POUNDS?.[st] ||
      QC_SAMPLE_POUNDS_TEMPLATE_FILENAME
    )
  : mappings.templates?.[kind]?.[st];

  if (!printer) throw new Error(`No QC/Retain printer mapping for labelKind='${kind}' station='${st}'. Add mappings.sampleStations.${st}.printer in mappings.json.`);
  if (!templateValue) throw new Error(`No sample-label template for labelKind='${kind}' station='${st}'`);

  const template = resolveTemplatePath(templateValue);
  return { labelKind: kind, printer, template };
}

function getDirectZplFamilyForSample({ labelKind, byPounds = false }) {
  const kind = normalizeSampleLabelKind(labelKind);
  if (byPounds) return "SAMPLE_POUNDS";
  return kind === "QCRetain" ? "RETAIN" : "SAMPLE";
}

function resolveSamplePrintTarget({ station, labelKind, byPounds = false }) {
  const st = normalizeSampleStation(station);
  const bartender = resolvePrinterAndSampleTemplate({ station: st, labelKind, byPounds });
  const printEngine = getConfiguredPrintEngine();
  const directZplFamily = getDirectZplFamilyForSample({ labelKind: bartender.labelKind, byPounds });

  if (printEngine === "bartender") {
    return {
      ...bartender,
      station: st,
      byPounds,
      printEngine,
      directZplFamily,
      zpl: null
    };
  }

  return {
    ...bartender,
    station: st,
    byPounds,
    printEngine,
    directZplFamily,
    zpl: resolveZplPrinterAndTemplate({ station: st, family: directZplFamily })
  };
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

app.get("/health", (req, res) => {
  const printEngineHealth = getPrintEngineHealth();
  const duplicatePolicyHealth = getZplDuplicatePolicyHealth();
  const socketModeHealth = getZplSocketModeHealth();
  return res.json({
    ok: printEngineHealth.ok && socketModeHealth.ok,
    build: BUILD_TAG,
    printEngine: printEngineHealth.printEngine,
    printEngineError: printEngineHealth.ok ? undefined : printEngineHealth.message,
    zplDuplicatePolicy: duplicatePolicyHealth.zplDuplicatePolicy,
    zplDuplicatePolicyError: duplicatePolicyHealth.ok ? undefined : duplicatePolicyHealth.message,
    zplSocketMode: socketModeHealth.zplSocketMode,
    zplSocketModeError: socketModeHealth.ok ? undefined : socketModeHealth.message,
    zplMaxLabelsPerConnection: getZplMaxLabelsPerConnection(),
    zplSocketIdleCloseMs: getZplSocketIdleCloseMs(),
    zplBatchMaxLabels: getZplBatchMaxLabels(),
    zplBatchCollectMs: getZplBatchCollectMs(),
    zplBatchInterBatchDelayMs: getZplBatchInterBatchDelayMs(),
    zplBatchMaxBytes: getZplBatchMaxBytes(),
    directZplPilotScopes: getDirectZplEnabledScopes(),
    directZplEnabledScopes: getDirectZplEnabledScopes(),
    zplQueueEnabled: true,
    zplQueuePath: ZPL_QUEUE_DIR,
    zplLabelSpacingMs: getZplLabelSpacingMs(),
    zplTcpTimeoutMs: getZplTcpTimeoutMs(),
    zplStaleSendingThresholdMs: getZplStaleSendingThresholdMs(),
    zplTransportSettings: getZplTransportSettings()
  });
});

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
const recentZplSendAccepted = new Map(); // station|lot|box|rfid -> acceptedAtMs
const zplPersistentSockets = new Map(); // printerKey -> persistent TCP socket state
const zplPrinterLastSendStartedAt = new Map(); // printerKey -> epoch ms
let zplSocketFactoryForTests = null;
let templateTestSendFunctionForTests = null;

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

function getZplQueueKey(zpl) {
  return `zpl:${zpl.printerIp}:${zpl.port}`;
}

function zplTransportError(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  return error;
}

function makeZplSocket() {
  return zplSocketFactoryForTests ? zplSocketFactoryForTests() : new net.Socket();
}

function getZplPersistentSocketState(printerKey) {
  const key = String(printerKey || "").trim();
  if (!key) return null;
  if (!zplPersistentSockets.has(key)) {
    zplPersistentSockets.set(key, {
      printerKey: key,
      socket: null,
      connected: false,
      connectingPromise: null,
      labelsSent: 0,
      openedAt: null,
      lastUsedAt: null,
      idleTimer: null,
      idleCloseAt: null,
      activeSend: null,
      lastError: null,
      closing: false
    });
  }
  return zplPersistentSockets.get(key);
}

function clearZplPersistentSocketIdleTimer(state) {
  if (state?.idleTimer) {
    clearTimeout(state.idleTimer);
    state.idleTimer = null;
    state.idleCloseAt = null;
  }
}

function closeZplPersistentSocket(printerKey, reason = "close") {
  const state = zplPersistentSockets.get(printerKey);
  if (!state) return false;

  clearZplPersistentSocketIdleTimer(state);
  const socket = state.socket;
  const hadSocket = Boolean(socket);
  const labelsSent = Number(state.labelsSent || 0);
  const openedAt = state.openedAt;
  const lastUsedAt = state.lastUsedAt;

  state.closing = true;
  state.connected = false;
  state.connectingPromise = null;
  state.socket = null;
  state.activeSend = null;
  zplPersistentSockets.delete(printerKey);

  if (socket) {
    try {
      if (typeof socket.removeAllListeners === "function") socket.removeAllListeners();
      if (typeof socket.end === "function" && socket.destroyed !== true) socket.end();
      if (typeof socket.destroy === "function" && socket.destroyed !== true) socket.destroy();
    } catch {
      // Best effort cleanup only.
    }
  }

  if (hadSocket) {
    logInfo(
      "zpl_socket_close",
      { printerKey, reason, labelsSent, openedAt, lastUsedAt, socketMode: "persistent" },
      `[PrintSvc] Direct ZPL persistent socket closed printerKey=${printerKey} reason=${reason} labelsSent=${labelsSent}`
    );
  }

  return hadSocket;
}

function scheduleZplPersistentSocketIdleClose(printerKey) {
  if (getZplSocketMode() !== "persistent") return;
  const state = zplPersistentSockets.get(printerKey);
  if (!state?.socket || state.activeSend) return;

  clearZplPersistentSocketIdleTimer(state);
  const delayMs = getZplSocketIdleCloseMs();
  state.idleCloseAt = new Date(Date.now() + delayMs).toISOString();
  state.idleTimer = setTimeout(() => {
    closeZplPersistentSocket(printerKey, "idle_timeout");
  }, delayMs);
  if (typeof state.idleTimer.unref === "function") state.idleTimer.unref();
}

function getZplPersistentSocketStatus(printerKey) {
  const state = zplPersistentSockets.get(printerKey);
  if (!state) return null;
  return {
    printerKey,
    connected: state.connected === true,
    connecting: Boolean(state.connectingPromise),
    labelsSent: Number(state.labelsSent || 0),
    openedAt: state.openedAt || null,
    lastUsedAt: state.lastUsedAt || null,
    idleCloseAt: state.idleCloseAt || null,
    activeSend: state.activeSend ? { ...state.activeSend } : null,
    lastError: state.lastError || null
  };
}

function getZplPersistentSocketStatusForAll() {
  return Object.fromEntries(
    Array.from(zplPersistentSockets.keys()).map((printerKey) => [printerKey, getZplPersistentSocketStatus(printerKey)])
  );
}

function openZplPersistentSocket({ printerKey, printerIp, port, timeoutMs }) {
  const targetHost = String(printerIp || "").trim();
  const targetPort = Number(port || 9100);
  const state = getZplPersistentSocketState(printerKey);

  if (!targetHost) {
    return Promise.reject(zplTransportError("ZPL_PRINTER_IP_MISSING", "ZPL printer IP/host is required."));
  }
  if (!Number.isInteger(targetPort) || targetPort <= 0 || targetPort > 65535) {
    return Promise.reject(zplTransportError("ZPL_PRINTER_PORT_INVALID", "ZPL printer port must be a valid TCP port.", { port }));
  }

  if (state.socket && state.connected && Number(state.labelsSent || 0) < getZplMaxLabelsPerConnection()) {
    clearZplPersistentSocketIdleTimer(state);
    logInfo(
      "zpl_socket_reuse",
      { printerKey, printerIp: targetHost, port: targetPort, labelsSent: state.labelsSent, socketMode: "persistent" },
      `[PrintSvc] Direct ZPL persistent socket reused printerKey=${printerKey} labelsSent=${state.labelsSent}`
    );
    return Promise.resolve(state);
  }

  if (state.connectingPromise) return state.connectingPromise;
  if (state.socket) closeZplPersistentSocket(printerKey, "max_labels_or_reopen");

  const next = getZplPersistentSocketState(printerKey);
  const startedAt = Date.now();
  const socket = makeZplSocket();
  next.socket = socket;
  next.connected = false;
  next.labelsSent = 0;
  next.openedAt = null;
  next.lastError = null;
  next.closing = false;
  clearZplPersistentSocketIdleTimer(next);

  next.connectingPromise = new Promise((resolve, reject) => {
    let settled = false;
    let timeout = null;

    function cleanup() {
      if (timeout) clearTimeout(timeout);
      timeout = null;
      socket.removeListener?.("connect", onConnect);
      socket.removeListener?.("error", onError);
      socket.removeListener?.("close", onClose);
      next.connectingPromise = null;
    }

    function fail(error) {
      if (settled) return;
      settled = true;
      cleanup();
      error.details = {
        ...(error.details || {}),
        printerIp: targetHost,
        port: targetPort,
        durationMs: Date.now() - startedAt,
        connected: false,
        writeStarted: false,
        writeCompleted: false,
        socketClosed: true,
        bytesAttempted: 0,
        bytesSent: 0
      };
      next.lastError = { code: error.code || null, message: error.message, details: error.details };
      logError(
        "zpl_socket_error",
        { printerKey, printerIp: targetHost, port: targetPort, socketMode: "persistent", code: error.code || null, message: error.message, durationMs: error.details.durationMs },
        `[PrintSvc] Direct ZPL persistent socket open failed printerKey=${printerKey}: ${error.message}`
      );
      closeZplPersistentSocket(printerKey, "open_error");
      reject(error);
    }

    function onConnect() {
      if (settled) return;
      settled = true;
      cleanup();
      next.connected = true;
      next.openedAt = isoNow();
      next.lastUsedAt = null;
      logInfo(
        "zpl_socket_open",
        { printerKey, printerIp: targetHost, port: targetPort, socketMode: "persistent", durationMs: Date.now() - startedAt },
        `[PrintSvc] Direct ZPL persistent socket opened printerKey=${printerKey} printer=${targetHost}:${targetPort}`
      );

      socket.on?.("error", (error) => {
        if (next.activeSend) return;
        next.lastError = { code: error.code || null, message: error.message };
        logError(
          "zpl_socket_error",
          { printerKey, printerIp: targetHost, port: targetPort, socketMode: "persistent", code: error.code || null, message: error.message },
          `[PrintSvc] Direct ZPL persistent socket error printerKey=${printerKey}: ${error.message}`
        );
        closeZplPersistentSocket(printerKey, "socket_error");
      });

      socket.on?.("close", () => {
        if (next.activeSend || next.closing) return;
        logInfo("zpl_socket_close", { printerKey, printerIp: targetHost, port: targetPort, socketMode: "persistent", reason: "remote_close", labelsSent: next.labelsSent });
        zplPersistentSockets.delete(printerKey);
      });

      resolve(next);
    }

    function onError(error) {
      fail(error);
    }

    function onClose() {
      if (!settled) {
        fail(zplTransportError("ZPL_SOCKET_CLOSED", `Persistent ZPL socket closed before connecting to ${targetHost}:${targetPort}.`));
      }
    }

    timeout = setTimeout(() => {
      fail(zplTransportError("ZPL_TCP_TIMEOUT", `Timed out opening persistent ZPL socket to ${targetHost}:${targetPort}.`, {
        timeoutMs: Number(timeoutMs) || getZplTcpTimeoutMs()
      }));
    }, Number(timeoutMs) || getZplTcpTimeoutMs());
    if (typeof timeout.unref === "function") timeout.unref();

    socket.once?.("connect", onConnect);
    socket.once?.("error", onError);
    socket.once?.("close", onClose);
    socket.connect(targetPort, targetHost);
  });

  return next.connectingPromise;
}

function sendZplOverPersistentSocket({ printerKey, printerIp, port = 9100, zpl, timeoutMs = getZplTcpTimeoutMs(), queueDepth = null }) {
  const payload = String(zpl ?? "");
  const bytesSent = Buffer.byteLength(payload, "utf8");
  const targetHost = String(printerIp || "").trim();
  const targetPort = Number(port || 9100);

  if (!payload) {
    return Promise.reject(zplTransportError("ZPL_PAYLOAD_EMPTY", "Rendered ZPL payload is empty."));
  }

  return openZplPersistentSocket({ printerKey, printerIp: targetHost, port: targetPort, timeoutMs })
    .then((state) => new Promise((resolve, reject) => {
      const socket = state.socket;
      const startedAt = Date.now();
      let settled = false;
      let writeStarted = false;
      let writeCompleted = false;
      let timeout = null;

      state.activeSend = {
        startedAt: new Date(startedAt).toISOString(),
        bytesAttempted: bytesSent,
        queueDepth
      };

      function cleanup() {
        if (timeout) clearTimeout(timeout);
        timeout = null;
        socket.removeListener?.("error", onError);
        socket.removeListener?.("close", onClose);
        state.activeSend = null;
      }

      function finish(error) {
        if (settled) return;
        settled = true;
        cleanup();

        if (error) {
          error.details = {
            ...(error.details || {}),
            printerIp: targetHost,
            port: targetPort,
            durationMs: Date.now() - startedAt,
            connected: state.connected === true,
            writeStarted,
            writeCompleted,
            endCompleted: false,
            socketClosed: false,
            bytesAttempted: writeStarted ? bytesSent : 0,
            bytesSent: writeCompleted ? bytesSent : 0
          };
          state.lastError = { code: error.code || null, message: error.message, details: error.details };
          logError(
            "zpl_socket_error",
            { printerKey, printerIp: targetHost, port: targetPort, socketMode: "persistent", code: error.code || null, message: error.message, writeStarted, writeCompleted },
            `[PrintSvc] Direct ZPL persistent socket send error printerKey=${printerKey}: ${error.message}`
          );
          closeZplPersistentSocket(printerKey, "send_error");
          reject(error);
          return;
        }

        state.labelsSent = Number(state.labelsSent || 0) + 1;
        state.lastUsedAt = isoNow();
        state.lastError = null;

        const result = {
          durationMs: Date.now() - startedAt,
          bytesSent,
          socketClosed: false,
          connected: true,
          writeStarted: true,
          writeCompleted: true,
          endCompleted: false,
          socketMode: "persistent",
          labelsSentOnConnection: state.labelsSent
        };

        if (state.labelsSent >= getZplMaxLabelsPerConnection()) {
          closeZplPersistentSocket(printerKey, "max_labels_per_connection");
        } else if (!Number.isFinite(Number(queueDepth)) || Number(queueDepth) <= 0) {
          scheduleZplPersistentSocketIdleClose(printerKey);
        }

        resolve(result);
      }

      function onError(error) {
        finish(error);
      }

      function onClose() {
        finish(zplTransportError("ZPL_SOCKET_CLOSED", `Persistent ZPL socket closed while sending to ${targetHost}:${targetPort}.`));
      }

      timeout = setTimeout(() => {
        finish(zplTransportError("ZPL_TCP_TIMEOUT", `Timed out sending ZPL over persistent socket to ${targetHost}:${targetPort}.`, {
          timeoutMs: Number(timeoutMs) || getZplTcpTimeoutMs()
        }));
      }, Number(timeoutMs) || getZplTcpTimeoutMs());
      if (typeof timeout.unref === "function") timeout.unref();

      socket.once?.("error", onError);
      socket.once?.("close", onClose);

      try {
        writeStarted = true;
        socket.write(payload, "utf8", (error) => {
          if (error) return finish(error);
          writeCompleted = true;
          finish(null);
        });
      } catch (error) {
        finish(error);
      }
    }));
}

function logZplSendTiming({ printerKey, station, lotNumber, box, socketMode, queueDepth }) {
  const now = Date.now();
  const previous = zplPrinterLastSendStartedAt.get(printerKey);
  const elapsedMsSincePreviousSendOnPrinter = Number.isFinite(previous) ? now - previous : null;
  zplPrinterLastSendStartedAt.set(printerKey, now);
  logInfo(
    "zpl_send_timing",
    { printerKey, station, lotNumber, box, elapsedMsSincePreviousSendOnPrinter, socketMode, queueDepth: queueDepth ?? null },
    `[PrintSvc] Direct ZPL send timing printerKey=${printerKey} station=${station} lot=${lotNumber} box=${box} mode=${socketMode} elapsedMs=${elapsedMsSincePreviousSendOnPrinter ?? "n/a"}`
  );
}

function isDebugZplEnabled() {
  return String(process.env.DEBUG_ZPL || "").trim().toLowerCase() === "true";
}

function isRetryableZplTcpError(error) {
  const code = String(error?.code || error?.details?.code || "").toUpperCase();
  return [
    "ZPL_TCP_TIMEOUT",
    "ETIMEDOUT",
    "ECONNRESET",
    "ECONNREFUSED",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "EPIPE"
  ].includes(code);
}

function zplSendMayHaveReachedPrinter(error) {
  const details = error?.details || {};
  return details.connected === true && (
    details.writeStarted === true ||
    details.writeCompleted === true ||
    Number(details.bytesAttempted || 0) > 0 ||
    Number(details.bytesSent || 0) > 0
  );
}

function toZplSendUnknownError(error, { box }) {
  const unknown = new Error(`Box ${box} may or may not have printed. Verify before resuming.`);
  unknown.code = "ZPL_SEND_UNKNOWN";
  unknown.statusCode = 500;
  unknown.retryable = false;
  unknown.operatorAction = "Verify whether the label physically printed before retrying.";
  unknown.cause = error;
  unknown.details = {
    ...(error.details || {}),
    originalCode: error.code || null,
    originalMessage: error.message
  };
  return unknown;
}

function getZplRetryDelayMs() {
  return Math.max(getZplConnectRetryDelayMs(), getZplLabelSpacingMs());
}

function getRequestScopeFromCount(count) {
  return Number(count) === 1 ? "single-box" : "multi-box";
}

function decorateZplPartialFailure(error, { results, failedBox }) {
  const acceptedBoxes = Array.isArray(results) ? results.map((result) => result.box).filter((box) => box != null) : [];
  if (error.code === "ZPL_SEND_UNKNOWN") {
    error.partialPrint = {
      acceptedBoxes,
      unknownBox: failedBox,
      failedBox: null,
      retryable: false,
      operatorAction: error.operatorAction || "Verify whether the label physically printed before retrying."
    };
  } else {
    error.partialPrint = {
      acceptedBoxes,
      printedBoxes: acceptedBoxes,
      failedBox,
      retryable: error.retryable === false ? false : isRetryableZplTcpError(error)
    };
  }
  return error;
}

function buildErrorResponsePayload(error, fallbackError = "PRINT_FAILED") {
  return {
    ok: false,
    error: error.code || fallbackError,
    message: error.message,
    details: error.details || undefined,
    acceptedBoxes: error.partialPrint?.acceptedBoxes,
    printedBoxes: error.partialPrint?.printedBoxes,
    unknownBox: error.partialPrint?.unknownBox,
    failedBox: error.partialPrint?.failedBox,
    retryable: error.partialPrint?.retryable,
    operatorAction: error.partialPrint?.operatorAction || error.operatorAction,
    bartender: error.response?.data || null
  };
}

function getZplDuplicateGuardKey({ station, lotNumber, box, rfid }) {
  return [
    String(station || "").trim().toUpperCase(),
    String(lotNumber || "").trim().toUpperCase(),
    String(box || "").trim(),
    String(rfid || "").trim().toUpperCase()
  ].join("|");
}

function pruneRecentZplSendAccepted(now = Date.now()) {
  for (const [key, record] of recentZplSendAccepted.entries()) {
    if (!record?.acceptedAtMs || now - record.acceptedAtMs > ZPL_DUPLICATE_GUARD_TTL_MS) {
      recentZplSendAccepted.delete(key);
    }
  }
}

function markRecentZplSendAccepted({ station, lotNumber, box, rfid }, now = Date.now()) {
  if (String(rfid || "").trim() === "") return;
  pruneRecentZplSendAccepted(now);
  recentZplSendAccepted.set(getZplDuplicateGuardKey({ station, lotNumber, box, rfid }), {
    station,
    lotNumber,
    box,
    rfid,
    acceptedAtMs: now
  });
}

function duplicateRecentZplError(record, { station, lotNumber, box, rfid }, now = Date.now()) {
  const error = new Error(`Direct-ZPL label was already accepted recently for station=${station} lot=${lotNumber} box=${box} rfid=${rfid}.`);
  error.code = "DUPLICATE_RECENT_ZPL";
  error.statusCode = 409;
  error.retryable = false;
  error.details = {
    station,
    lotNumber,
    box,
    rfid,
    acceptedAtUtc: new Date(record.acceptedAtMs).toISOString(),
    expiresAtUtc: new Date(record.acceptedAtMs + ZPL_DUPLICATE_GUARD_TTL_MS).toISOString(),
    ageMs: now - record.acceptedAtMs
  };
  return error;
}

function isZplDuplicatePolicyAllow() {
  return getZplDuplicatePolicy() === "allow";
}

function logZplDuplicateAllowed(details) {
  logInfo(
    "zpl_duplicate_allowed",
    details,
    `[PrintSvc] Duplicate recent direct-ZPL label allowed station=${details.station} lot=${details.lotNumber} box=${details.box} rfid=${details.rfid}`
  );
}

function assertNoRecentZplDuplicate({ station, lotNumber, box, rfid }, now = Date.now()) {
  pruneRecentZplSendAccepted(now);
  const key = getZplDuplicateGuardKey({ station, lotNumber, box, rfid });
  const record = recentZplSendAccepted.get(key);
  if (!record) return;

  if (isZplDuplicatePolicyAllow()) {
    logZplDuplicateAllowed({ station, lotNumber, box, rfid });
    return;
  }

  const error = duplicateRecentZplError(record, { station, lotNumber, box, rfid }, now);
  logWarn(
    "duplicate_recent_zpl_rejected",
    error.details,
    `[PrintSvc] Duplicate recent direct-ZPL label rejected station=${station} lot=${lotNumber} box=${box} rfid=${rfid}`
  );
  throw error;
}

function clearRecentZplDuplicateGuard() {
  recentZplSendAccepted.clear();
}

const zplQueueWorkers = new Map(); // printerKey -> { running, paused, activeItem, lastError }
const zplStaleSendingRecoveryTimers = new Map(); // itemId -> timeout
let directZplQueueSendFunction = sendDirectZplQueueItem;
let zplQueueSequence = Date.now();

function ensureZplQueueDir() {
  fs.mkdirSync(ZPL_QUEUE_DIR, { recursive: true });
}

function getZplQueueItemPath(itemId) {
  return path.join(ZPL_QUEUE_DIR, `${itemId}.json`);
}

function safeJsonRead(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    logWarn("zpl_queue_item_read_failed", { filePath, message: error.message }, `[PrintSvc] Failed to read ZPL queue item ${filePath}: ${error.message}`);
    return null;
  }
}

function writeZplQueueItem(item) {
  ensureZplQueueDir();
  const next = {
    ...item,
    updatedAt: isoNow()
  };
  const target = getZplQueueItemPath(next.itemId);
  const temp = `${target}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(next, null, 2), "utf8");
  fs.renameSync(temp, target);
  return next;
}

function nextZplQueueSequence() {
  zplQueueSequence += 1;
  return zplQueueSequence;
}

function listZplQueueItems() {
  ensureZplQueueDir();
  return fs.readdirSync(ZPL_QUEUE_DIR)
    .filter((name) => name.toLowerCase().endsWith(".json"))
    .map((name) => safeJsonRead(path.join(ZPL_QUEUE_DIR, name)))
    .filter(Boolean)
    .sort((a, b) => {
      const byCreated = String(a.createdAt || "").localeCompare(String(b.createdAt || ""));
      if (byCreated !== 0) return byCreated;
      const aSequence = Number(a.queueSequence);
      const bSequence = Number(b.queueSequence);
      if (Number.isFinite(aSequence) && Number.isFinite(bSequence) && aSequence !== bSequence) {
        return aSequence - bSequence;
      }
      return String(a.itemId || "").localeCompare(String(b.itemId || ""));
    });
}

function getOrCreateZplWorkerState(printerKey) {
  const key = String(printerKey || "").trim();
  if (!zplQueueWorkers.has(key)) {
    zplQueueWorkers.set(key, {
      running: false,
      paused: false,
      phase: "idle",
      waitingUntil: null,
      activeItem: null,
      activeBatch: null,
      lastBatchDurationMs: null,
      lastError: null
    });
  }
  return zplQueueWorkers.get(key);
}

function isCurrentZplWorkerState(printerKey, state) {
  return zplQueueWorkers.get(printerKey) === state;
}

function makeZplJobId() {
  return `zpljob-${Date.now()}-${crypto.randomUUID()}`;
}

function makeZplItemId() {
  return `zplitem-${Date.now()}-${crypto.randomUUID()}`;
}

function buildZplQueueItem({
  jobId,
  station,
  family,
  lotNumber,
  box,
  rfid,
  zpl,
  namedDataSources,
  printLog = {},
  requiresRfidEncoding = true,
  labelKind = "",
  sampleByPounds = false
}) {
  const now = isoNow();
  return {
    jobId,
    itemId: makeZplItemId(),
    station,
    family,
    lotNumber,
    box,
    rfid: String(rfid ?? ""),
    requiresRfidEncoding: requiresRfidEncoding !== false,
    labelKind,
    sampleByPounds: sampleByPounds === true,
    printerIp: zpl.printerIp,
    printerPort: zpl.port,
    printerKey: getZplQueueKey(zpl),
    templatePath: zpl.templatePath,
    status: "queued",
    createdAt: now,
    updatedAt: now,
    attempts: 0,
    lastError: null,
    namedDataSources,
    printLog
  };
}

function isRecentAcceptedQueueItem(item, now = Date.now()) {
  if (!["sent_to_printer", "unknown_after_send"].includes(item?.status)) return false;
  const when = Date.parse(item.sentAt || item.unknownAt || item.updatedAt || item.createdAt || "");
  return Number.isFinite(when) && now - when <= ZPL_DUPLICATE_GUARD_TTL_MS;
}

function findRecentAcceptedZplItem({ station, lotNumber, box, rfid }, { excludeItemId = null, now = Date.now() } = {}) {
  const key = getZplDuplicateGuardKey({ station, lotNumber, box, rfid });
  const memoryRecord = recentZplSendAccepted.get(key);
  if (memoryRecord && now - memoryRecord.acceptedAtMs <= ZPL_DUPLICATE_GUARD_TTL_MS) {
    return {
      station: memoryRecord.station,
      lotNumber: memoryRecord.lotNumber,
      box: memoryRecord.box,
      rfid: memoryRecord.rfid,
      acceptedAtMs: memoryRecord.acceptedAtMs,
      source: "memory"
    };
  }

  return listZplQueueItems().find((item) => {
    if (excludeItemId && item.itemId === excludeItemId) return false;
    if (!isRecentAcceptedQueueItem(item, now)) return false;
    return getZplDuplicateGuardKey(item) === key;
  }) || null;
}

function queueItemUsesDuplicateGuard(item = {}) {
  return item.requiresRfidEncoding !== false && String(item.rfid || "").trim() !== "";
}

function assertNoRecentZplDuplicatePersistent({ station, lotNumber, box, rfid }, options = {}) {
  if (options.requiresRfidEncoding === false || String(rfid || "").trim() === "") return;

  const now = options.now || Date.now();
  pruneRecentZplSendAccepted(now);
  const record = findRecentAcceptedZplItem({ station, lotNumber, box, rfid }, { excludeItemId: options.excludeItemId, now });
  if (!record) return;

  if (isZplDuplicatePolicyAllow()) {
    logZplDuplicateAllowed({ station, lotNumber, box, rfid });
    return;
  }

  const acceptedAtMs = record.acceptedAtMs || Date.parse(record.sentAt || record.unknownAt || record.updatedAt || record.createdAt || "");
  const error = duplicateRecentZplError({ acceptedAtMs }, { station, lotNumber, box, rfid }, now);
  error.details.itemId = record.itemId || null;
  error.details.jobId = record.jobId || null;
  error.details.status = record.status || null;
  logWarn(
    "duplicate_recent_zpl_rejected",
    error.details,
    `[PrintSvc] Duplicate recent direct-ZPL label rejected station=${station} lot=${lotNumber} box=${box} rfid=${rfid}`
  );
  throw error;
}

function getRecentZplDuplicateSkipDetails({ station, lotNumber, box, rfid }, options = {}) {
  if (options.requiresRfidEncoding === false || String(rfid || "").trim() === "") return null;
  if (isZplDuplicatePolicyAllow()) return null;

  const now = options.now || Date.now();
  pruneRecentZplSendAccepted(now);
  const record = findRecentAcceptedZplItem({ station, lotNumber, box, rfid }, { excludeItemId: options.excludeItemId, now });
  if (!record) return null;

  const acceptedAtMs = record.acceptedAtMs || Date.parse(record.sentAt || record.unknownAt || record.updatedAt || record.createdAt || "");
  if (!Number.isFinite(acceptedAtMs)) return null;

  return {
    station,
    lotNumber,
    box,
    rfid,
    acceptedAtUtc: new Date(acceptedAtMs).toISOString(),
    expiresAtUtc: new Date(acceptedAtMs + ZPL_DUPLICATE_GUARD_TTL_MS).toISOString(),
    ageMs: now - acceptedAtMs,
    itemId: record.itemId || null,
    jobId: record.jobId || null,
    status: record.status || null,
    source: record.source || "queue"
  };
}

function logDuplicateRecentZplSkipped(details) {
  logWarn(
    "duplicate_recent_zpl_skipped",
    details,
    `[PrintSvc] Duplicate recent direct-ZPL label skipped station=${details.station} lot=${details.lotNumber} box=${details.box} rfid=${details.rfid}`
  );
}

function enqueueNormalDirectZplQueueItems(items) {
  const itemsToQueue = [];
  const skippedDuplicates = [];

  for (const item of items) {
    const duplicate = getRecentZplDuplicateSkipDetails(item, { requiresRfidEncoding: item.requiresRfidEncoding });
    if (duplicate) {
      logDuplicateRecentZplSkipped(duplicate);
      skippedDuplicates.push(duplicate);
    } else {
      itemsToQueue.push(item);
    }
  }

  let queuedItems = [];
  try {
    queuedItems = itemsToQueue.length ? enqueueDirectZplQueueItems(itemsToQueue, { persistRejectedDuplicates: false }) : [];
  } catch (error) {
    if (error.code === "DUPLICATE_RECENT_ZPL") {
      const details = {
        station: error.details?.station,
        lotNumber: error.details?.lotNumber,
        box: error.details?.box,
        rfid: error.details?.rfid,
        acceptedAtUtc: error.details?.acceptedAtUtc,
        expiresAtUtc: error.details?.expiresAtUtc,
        ageMs: error.details?.ageMs,
        itemId: error.details?.itemId || null,
        jobId: error.details?.jobId || null,
        status: error.details?.status || null,
        source: "enqueue"
      };
      logDuplicateRecentZplSkipped(details);
      return { queuedItems: [], skippedDuplicates: [details] };
    }
    throw error;
  }

  return { queuedItems, skippedDuplicates };
}

function buildDirectZplQueueResponse({
  jobId,
  station,
  requestedFamily,
  family,
  lotNumber,
  requestedBoxes = [],
  queuedItems = [],
  skippedDuplicates = [],
  firstBox,
  lastBox,
  requestedCount,
  missingBoxes,
  printerIp,
  printerPort,
  templatePath,
  extra = {}
}) {
  const allSkipped = queuedItems.length === 0 && skippedDuplicates.length > 0;
  const firstSkipped = skippedDuplicates[0] || null;

  if (allSkipped) {
    return {
      ok: true,
      queued: false,
      skippedDuplicate: true,
      dryRun: false,
      jobId,
      station,
      requestedFamily,
      family,
      lotNumber,
      requestedBoxes,
      box: firstSkipped.box,
      rfid: firstSkipped.rfid,
      acceptedAtUtc: firstSkipped.acceptedAtUtc,
      expiresAtUtc: firstSkipped.expiresAtUtc,
      skippedDuplicates,
      skippedDuplicateCount: skippedDuplicates.length,
      firstBox,
      lastBox,
      requestedCount: requestedCount ?? requestedBoxes.length,
      queuedCount: 0,
      missingBoxes,
      printerIp,
      printerPort,
      templatePath,
      message: DUPLICATE_RECENT_ZPL_SKIP_MESSAGE,
      ...extra
    };
  }

  return {
    ok: true,
    queued: true,
    skippedDuplicate: skippedDuplicates.length > 0,
    dryRun: false,
    jobId,
    itemId: queuedItems.length === 1 ? queuedItems[0].itemId : undefined,
    itemIds: queuedItems.map((item) => item.itemId),
    station,
    requestedFamily,
    family,
    lotNumber,
    requestedBoxes,
    queuedBoxes: queuedItems.map((item) => item.box),
    skippedDuplicates,
    skippedDuplicateCount: skippedDuplicates.length,
    firstBox,
    lastBox,
    requestedCount: requestedCount ?? requestedBoxes.length,
    queuedCount: queuedItems.length,
    missingBoxes,
    printerIp,
    printerPort,
    templatePath,
    message: skippedDuplicates.length > 0
      ? "Direct-ZPL labels queued; recent duplicates were skipped to prevent duplicate RFID."
      : "Direct-ZPL label queued for printer.",
    ...extra
  };
}

function persistRejectedZplQueueItem(baseItem, error) {
  const item = {
    ...baseItem,
    status: "rejected",
    attempts: 0,
    lastError: {
      code: error.code || "REJECTED",
      message: error.message,
      details: error.details || null
    },
    rejectedAt: isoNow()
  };
  return writeZplQueueItem(item);
}

function enqueueDirectZplQueueItems(items, options = {}) {
  const queuedItems = items.map((item) => ({
    ...item,
    queuedAt: isoNow(),
    queueSequence: nextZplQueueSequence()
  }));
  const written = [];
  const printerKeys = new Set();

  for (const queuedItem of queuedItems) {
    try {
      assertNoRecentZplDuplicatePersistent(queuedItem, { requiresRfidEncoding: queuedItem.requiresRfidEncoding });
    } catch (error) {
      if (error.code === "DUPLICATE_RECENT_ZPL" && options.persistRejectedDuplicates !== false) {
        const rejected = persistRejectedZplQueueItem(queuedItem, error);
        error.details = { ...(error.details || {}), itemId: rejected.itemId, jobId: rejected.jobId };
      }
      throw error;
    }
  }

  for (const queuedItem of queuedItems) {
    const saved = writeZplQueueItem(queuedItem);
    written.push(saved);
    printerKeys.add(saved.printerKey);
    logInfo(
      "zpl_queue_item_enqueued",
      { station: saved.station, lotNumber: saved.lotNumber, box: saved.box, rfid: saved.rfid, printerIp: saved.printerIp, printerPort: saved.printerPort, itemId: saved.itemId, jobId: saved.jobId },
      `[PrintSvc] Direct ZPL queue item enqueued itemId=${saved.itemId} station=${saved.station} lot=${saved.lotNumber} box=${saved.box} printer=${saved.printerIp}:${saved.printerPort}`
    );
  }

  for (const printerKey of printerKeys) {
    setImmediate(() => startZplQueueWorkerForPrinter(printerKey));
  }

  return written;
}

async function writeQueueItemPrintLog(item, result, notes) {
  const printLog = item.printLog || {};
  if (!printLog.baseUrl || !printLog.lotId) return;
  const resultKey = String(result || "").toLowerCase();
  const resultText = printLog.resultMap?.[result] || printLog[`${resultKey}Result`] || result;
  const notesText = printLog.notesMap?.[result] || printLog[`${resultKey}Notes`] || notes;

  await writePrintLog(printLog.baseUrl, {
    lotId: printLog.lotId,
    inventoryId: printLog.inventoryId || null,
    rfid: item.rfid,
    station: item.station,
    printedBy: printLog.printedBy || "",
    result: resultText,
    notes: notesText
  });
}

function serializeQueueError(error) {
  return {
    code: error.code || "ZPL_QUEUE_ERROR",
    message: error.message,
    details: error.details || null,
    retryable: error.retryable === true
  };
}

function parseUtcMs(value) {
  const ms = Date.parse(value || "");
  return Number.isFinite(ms) ? ms : null;
}

function getQueueItemStartedAtMs(item) {
  return parseUtcMs(item.sendingStartedAt) ||
    parseUtcMs(item.updatedAt) ||
    parseUtcMs(item.createdAt) ||
    0;
}

function getQueueItemSendDetails(item = {}) {
  return item.lastError?.details ||
    item.sendResult ||
    item.sendDetails ||
    {};
}

function getQueueItemBytesSent(item = {}) {
  const details = getQueueItemSendDetails(item);
  const value = details.bytesSent ?? item.bytesSent ?? 0;
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function getQueueItemBytesAttempted(item = {}) {
  const details = getQueueItemSendDetails(item);
  const value = details.bytesAttempted ?? details.bytesSent ?? item.bytesAttempted ?? item.bytesSent ?? 0;
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function queueItemProvesNoBytesWritten(item = {}) {
  const details = getQueueItemSendDetails(item);
  return details.writeStarted === false &&
    details.writeCompleted !== true &&
    getQueueItemBytesAttempted(item) === 0 &&
    getQueueItemBytesSent(item) === 0;
}

function isQueueItemSafeToRetry(item = {}) {
  return item.status === "failed_before_send" && queueItemProvesNoBytesWritten(item);
}

function queueStatusCounts() {
  return DIRECT_ZPL_QUEUE_STATUSES.reduce((counts, status) => {
    counts[status] = 0;
    return counts;
  }, {});
}

function summarizeZplQueueItem(item = {}) {
  return {
    itemId: item.itemId,
    jobId: item.jobId,
    status: item.status,
    station: item.station,
    family: item.family,
    lotNumber: item.lotNumber,
    box: item.box,
    rfid: item.rfid,
    printerIp: item.printerIp,
    printerPort: item.printerPort,
    printerKey: item.printerKey,
    templatePath: item.templatePath,
    attempts: Number(item.attempts || 0),
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    sendingStartedAt: item.sendingStartedAt || null,
    sentAt: item.sentAt || null,
    unknownAt: item.unknownAt || null,
    failedAt: item.failedAt || null,
    rejectedAt: item.rejectedAt || null,
    recoveredAt: item.recoveredAt || null,
    recoveredFromStatus: item.recoveredFromStatus || null,
    recoveryReason: item.recoveryReason || null,
    operatorAction: item.operatorAction || null,
    operatorReviewedAt: item.operatorReviewedAt || null,
    lastError: item.lastError || null,
    bytesSent: getQueueItemBytesSent(item),
    writeStarted: getQueueItemSendDetails(item).writeStarted,
    safeToRetry: isQueueItemSafeToRetry(item)
  };
}

function getNextQueuedZplItem(printerKey) {
  return listZplQueueItems().find((item) => item.printerKey === printerKey && item.status === "queued") || null;
}

function getQueuedZplItemDepth(printerKey) {
  return listZplQueueItems().filter((item) => item.printerKey === printerKey && item.status === "queued").length;
}

function getQueuedZplItemsForPrinter(printerKey) {
  return listZplQueueItems().filter((item) => item.printerKey === printerKey && item.status === "queued");
}

function summarizeZplBatchItems(items = []) {
  return {
    itemIds: items.map((item) => item.itemId),
    jobIds: Array.from(new Set(items.map((item) => item.jobId).filter(Boolean))),
    stations: Array.from(new Set(items.map((item) => item.station).filter(Boolean))),
    families: Array.from(new Set(items.map((item) => item.family).filter(Boolean))),
    lotNumbers: Array.from(new Set(items.map((item) => item.lotNumber).filter(Boolean))),
    boxes: items.map((item) => item.box),
    rfids: items.map((item) => item.rfid).filter(Boolean)
  };
}

function getBatchPrinterTarget(items = []) {
  const first = items[0] || {};
  return {
    printerIp: first.printerIp,
    printerPort: first.printerPort || 9100,
    printerKey: first.printerKey
  };
}

function renderZplForQueueItem(item = {}) {
  const zpl = {
    printerIp: item.printerIp,
    port: item.printerPort,
    templatePath: item.templatePath
  };
  const data = buildZplRenderDataFromNamed({
    lotNumber: item.lotNumber,
    box: item.box,
    rfid: item.rfid,
    namedDataSources: item.namedDataSources || {}
  });

  try {
    const renderedZpl = item.requiresRfidEncoding === false
      ? renderZplTemplateFileWithoutRfid(zpl.templatePath, data)
      : renderZplTemplateFile(zpl.templatePath, data);
    return {
      item,
      renderedZpl,
      bytes: Buffer.byteLength(renderedZpl, "utf8")
    };
  } catch (error) {
    if (error.code === "INVALID_RFID") {
      logError(
        "print_validation_error",
        { station: item.station, lotNumber: item.lotNumber, box: item.box, invalidRfid: item.rfid, reason: error.message },
        `[PrintSvc] Direct ZPL validation failed station=${item.station} lot=${item.lotNumber} box=${item.box} invalid rfid="${item.rfid}": ${error.message}`
      );
    } else {
      logError(
        "zpl_print_error",
        { station: item.station, lotNumber: item.lotNumber, box: item.box, rfid: item.rfid, printerIp: zpl.printerIp, port: zpl.port, attemptNumber: 0, durationMs: 0, code: error.code || null, message: error.message },
        `[PrintSvc] Direct ZPL render failed box=${item.box} rfid=${item.rfid} printer=${zpl.printerIp}:${zpl.port}: ${error.message}`
      );
    }
    throw error;
  }
}

function markZplQueueItemSending(item, { batchId = null } = {}) {
  return writeZplQueueItem({
    ...item,
    status: "sending",
    attempts: Number(item.attempts || 0) + 1,
    lastError: null,
    sendingStartedAt: isoNow(),
    activeBatchId: batchId
  });
}

async function markZplBatchItemSent(item, result, notes = "Direct ZPL batch sent_to_printer; physical print not confirmed") {
  const next = writeZplQueueItem({
    ...item,
    status: "sent_to_printer",
    sentAt: isoNow(),
    sendResult: result,
    lastError: null,
    activeBatchId: null
  });
  if (queueItemUsesDuplicateGuard(next)) markRecentZplSendAccepted(next);
  await writeQueueItemPrintLog(next, "Success", notes);
  logInfo(
    "zpl_queue_item_sent_to_printer",
    { station: next.station, lotNumber: next.lotNumber, box: next.box, rfid: next.rfid, printerIp: next.printerIp, printerPort: next.printerPort, itemId: next.itemId, jobId: next.jobId, batchId: result.batchId || null },
    `[PrintSvc] Direct ZPL queue item sent_to_printer itemId=${next.itemId} station=${next.station} lot=${next.lotNumber} box=${next.box}`
  );
  return next;
}

async function markZplBatchItemFailed(item, error, { rejected = false } = {}) {
  const next = writeZplQueueItem({
    ...item,
    status: rejected ? "rejected" : "failed_before_send",
    failedAt: isoNow(),
    rejectedAt: rejected ? isoNow() : item.rejectedAt || null,
    lastError: serializeQueueError(error),
    activeBatchId: null
  });
  await writeQueueItemPrintLog(next, rejected ? "Rejected" : "Failed", error.message);
  logError(
    rejected ? "duplicate_recent_zpl_rejected" : "zpl_queue_item_failed_before_send",
    { station: next.station, lotNumber: next.lotNumber, box: next.box, rfid: next.rfid, printerIp: next.printerIp, printerPort: next.printerPort, itemId: next.itemId, jobId: next.jobId, error: next.lastError },
    `[PrintSvc] Direct ZPL queue item ${next.status} itemId=${next.itemId} station=${next.station} lot=${next.lotNumber} box=${next.box}: ${error.message}`
  );
  return next;
}

async function markZplBatchItemUnknown(item, error) {
  const next = writeZplQueueItem({
    ...item,
    status: "unknown_after_send",
    unknownAt: isoNow(),
    lastError: serializeQueueError(error),
    operatorAction: error.operatorAction || "Verify whether the label physically printed before retrying.",
    activeBatchId: null
  });
  if (queueItemUsesDuplicateGuard(next)) markRecentZplSendAccepted(next);
  await writeQueueItemPrintLog(next, "Unknown", next.operatorAction);
  logError(
    "zpl_queue_item_unknown_after_send",
    { station: next.station, lotNumber: next.lotNumber, box: next.box, rfid: next.rfid, printerIp: next.printerIp, printerPort: next.printerPort, itemId: next.itemId, jobId: next.jobId, operatorAction: next.operatorAction, error: next.lastError },
    `[PrintSvc] Direct ZPL queue item unknown_after_send itemId=${next.itemId} station=${next.station} lot=${next.lotNumber} box=${next.box}`
  );
  return next;
}

function toZplBatchSendUnknownError(error, { boxes = [] } = {}) {
  const unknown = new Error(`Batch boxes ${boxes.join(",")} may or may not have printed. Verify before resuming.`);
  unknown.code = "ZPL_SEND_UNKNOWN";
  unknown.statusCode = 500;
  unknown.retryable = false;
  unknown.operatorAction = "Verify whether the label physically printed before retrying.";
  unknown.cause = error;
  unknown.details = {
    ...(error.details || {}),
    boxes,
    originalCode: error.code || null,
    originalMessage: error.message
  };
  return unknown;
}

async function processNextZplBatchForPrinter(printerKey, state) {
  const collectMs = getZplBatchCollectMs();
  if (collectMs > 0) {
    state.phase = "collecting_batch";
    state.waitingUntil = new Date(Date.now() + collectMs).toISOString();
    await sleep(collectMs);
    state.waitingUntil = null;
    if (!isCurrentZplWorkerState(printerKey, state) || state.paused) return { didWork: false, didSend: false };
  }

  const candidates = getQueuedZplItemsForPrinter(printerKey).slice(0, getZplBatchMaxLabels());
  if (candidates.length === 0) return { didWork: false, didSend: false };

  const batchId = `zplbatch-${Date.now()}-${crypto.randomUUID()}`;
  const startedAt = Date.now();
  const maxBytes = getZplBatchMaxBytes();
  const included = [];
  let batchBytes = 0;

  logInfo(
    "zpl_batch_start",
    { printerKey, batchId, candidateCount: candidates.length, maxLabels: getZplBatchMaxLabels(), maxBytes, collectMs },
    `[PrintSvc] Direct ZPL batch start printerKey=${printerKey} candidates=${candidates.length}`
  );

  state.phase = "rendering_batch";
  state.activeBatch = {
    batchId,
    printerKey,
    batchStartedAt: new Date(startedAt).toISOString(),
    batchLabelCount: 0,
    batchBoxes: [],
    batchBytes: 0,
    itemIds: []
  };

  for (const candidate of candidates) {
    try {
      assertNoRecentZplDuplicatePersistent(candidate, {
        excludeItemId: candidate.itemId,
        requiresRfidEncoding: candidate.requiresRfidEncoding
      });
      const rendered = renderZplForQueueItem(candidate);
      if (included.length > 0 && batchBytes + rendered.bytes > maxBytes) break;

      const sending = markZplQueueItemSending(candidate, { batchId });
      const entry = { item: sending, renderedZpl: rendered.renderedZpl, bytes: rendered.bytes };
      included.push(entry);
      batchBytes += rendered.bytes;

      state.activeBatch = {
        ...state.activeBatch,
        batchLabelCount: included.length,
        batchBoxes: included.map((item) => item.item.box),
        batchBytes,
        itemIds: included.map((item) => item.item.itemId)
      };

      logInfo(
        "zpl_batch_item_included",
        { printerKey, batchId, station: sending.station, family: sending.family, lotNumber: sending.lotNumber, box: sending.box, rfid: sending.rfid, itemId: sending.itemId, jobId: sending.jobId, itemBytes: rendered.bytes, batchBytes },
        `[PrintSvc] Direct ZPL batch included itemId=${sending.itemId} station=${sending.station} lot=${sending.lotNumber} box=${sending.box}`
      );
    } catch (error) {
      await markZplBatchItemFailed(candidate, error, { rejected: error.code === "DUPLICATE_RECENT_ZPL" });
      state.lastError = serializeQueueError(error);
    }
  }

  if (included.length === 0) {
    state.activeBatch = null;
    logWarn(
      "zpl_batch_complete",
      { printerKey, batchId, batchLabelCount: 0, durationMs: Date.now() - startedAt, status: "empty" },
      `[PrintSvc] Direct ZPL batch complete printerKey=${printerKey} batchId=${batchId} empty`
    );
    return { didWork: true, didSend: false };
  }

  const items = included.map((entry) => entry.item);
  const target = getBatchPrinterTarget(items);
  const summary = summarizeZplBatchItems(items);
  const payload = included.map((entry) => entry.renderedZpl).join("");

  state.phase = "sending_batch";
  state.activeItem = null;
  state.activeBatch = {
    ...state.activeBatch,
    printerIp: target.printerIp,
    printerPort: target.printerPort
  };

  logInfo(
    "zpl_batch_send_attempt",
    { printerKey, batchId, printerIp: target.printerIp, printerPort: target.printerPort, ...summary, batchLabelCount: included.length, batchBytes },
    `[PrintSvc] -> Direct ZPL BATCH labels=${included.length} bytes=${batchBytes} printer=${target.printerIp}:${target.printerPort}`
  );

  try {
    const sendResult = await sendZplOverTcp({
      printerIp: target.printerIp,
      port: target.printerPort,
      zpl: payload,
      timeoutMs: getZplTcpTimeoutMs(),
      socketFactory: zplSocketFactoryForTests || undefined
    });
    const durationMs = sendResult.durationMs ?? (Date.now() - startedAt);
    state.lastBatchDurationMs = durationMs;

    logInfo(
      "zpl_batch_send_success",
      { printerKey, batchId, printerIp: target.printerIp, printerPort: target.printerPort, ...summary, batchLabelCount: included.length, batchBytes, durationMs, bytesSent: sendResult.bytesSent, socketClosed: sendResult.socketClosed === true, sendAccepted: true, physicalPrintConfirmed: false },
      `[PrintSvc] <- Direct ZPL BATCH TCP send accepted labels=${included.length} bytes=${batchBytes} printer=${target.printerIp}:${target.printerPort} durationMs=${durationMs}`
    );

    for (const entry of included) {
      await markZplBatchItemSent(entry.item, {
        ...sendResult,
        batchId,
        batchLabelCount: included.length,
        batchBytes,
        itemBytes: entry.bytes,
        socketMode: "batch",
        sendAccepted: true,
        physicalPrintConfirmed: false
      });
    }

    logInfo(
      "zpl_batch_complete",
      { printerKey, batchId, printerIp: target.printerIp, printerPort: target.printerPort, ...summary, batchLabelCount: included.length, batchBytes, durationMs, status: "sent_to_printer" },
      `[PrintSvc] Direct ZPL batch complete printerKey=${printerKey} batchId=${batchId} labels=${included.length}`
    );

    return { didWork: true, didSend: true };
  } catch (error) {
    const durationMs = error.details?.durationMs ?? (Date.now() - startedAt);
    const unknown = zplSendMayHaveReachedPrinter(error);
    const batchError = unknown ? toZplBatchSendUnknownError(error, { boxes: summary.boxes }) : error;
    batchError.details = {
      ...(batchError.details || {}),
      printerKey,
      batchId,
      batchLabelCount: included.length,
      batchBytes,
      itemIds: summary.itemIds,
      boxes: summary.boxes
    };

    logError(
      "zpl_batch_send_error",
      { printerKey, batchId, printerIp: target.printerIp, printerPort: target.printerPort, ...summary, batchLabelCount: included.length, batchBytes, durationMs, code: batchError.code || null, message: batchError.message, unknownAfterSend: unknown },
      `[PrintSvc] Direct ZPL batch send error printerKey=${printerKey} batchId=${batchId}: ${batchError.message}`
    );

    if (unknown) {
      for (const entry of included) await markZplBatchItemUnknown(entry.item, batchError);
      state.paused = true;
      state.phase = "paused";
      state.lastError = serializeQueueError(batchError);
      logWarn(
        "zpl_queue_worker_paused",
        { printerKey, batchId, operatorAction: batchError.operatorAction, itemIds: summary.itemIds, boxes: summary.boxes },
        `[PrintSvc] Direct ZPL worker paused printerKey=${printerKey}; batch operator verification required`
      );
    } else {
      batchError.retryable = isRetryableZplTcpError(error);
      for (const entry of included) await markZplBatchItemFailed(entry.item, batchError);
      state.lastError = serializeQueueError(batchError);
    }

    logWarn(
      "zpl_batch_complete",
      { printerKey, batchId, printerIp: target.printerIp, printerPort: target.printerPort, ...summary, batchLabelCount: included.length, batchBytes, durationMs, status: unknown ? "unknown_after_send" : "failed_before_send" },
      `[PrintSvc] Direct ZPL batch complete printerKey=${printerKey} batchId=${batchId} status=${unknown ? "unknown_after_send" : "failed_before_send"}`
    );

    return { didWork: true, didSend: false };
  } finally {
    if (isCurrentZplWorkerState(printerKey, state) && !state.paused) {
      state.activeBatch = null;
    }
  }
}

async function processZplQueueForPrinter(printerKey) {
  const state = getOrCreateZplWorkerState(printerKey);
  if (state.paused) {
    state.running = false;
    state.phase = "paused";
    return;
  }

  if (!state.running) state.running = true;
  state.phase = "running";
  state.waitingUntil = null;
  logInfo("zpl_queue_worker_start", { printerKey }, `[PrintSvc] Direct ZPL queue worker start printerKey=${printerKey}`);

  try {
    while (isCurrentZplWorkerState(printerKey, state) && !state.paused) {
      if (getZplSocketMode() === "batch") {
        const outcome = await processNextZplBatchForPrinter(printerKey, state);
        if (!outcome.didWork) break;
        if (outcome.didSend) {
          const delayMs = getZplBatchInterBatchDelayMs();
          state.phase = delayMs > 0 ? "waiting_between_batches" : "running";
          state.waitingUntil = delayMs > 0 ? new Date(Date.now() + delayMs).toISOString() : null;
          await sleep(delayMs);
          state.waitingUntil = null;
        }
        continue;
      }

      let item = getNextQueuedZplItem(printerKey);
      if (!item) break;

      state.activeItem = item;
      state.phase = "sending";
      state.waitingUntil = null;
      item.status = "sending";
      item.attempts = Number(item.attempts || 0) + 1;
      item.lastError = null;
      item.sendingStartedAt = isoNow();
      item = writeZplQueueItem(item);

      logInfo(
        "zpl_queue_item_sending",
        { station: item.station, lotNumber: item.lotNumber, box: item.box, rfid: item.rfid, printerIp: item.printerIp, printerPort: item.printerPort, itemId: item.itemId, jobId: item.jobId },
        `[PrintSvc] Direct ZPL queue item sending itemId=${item.itemId} station=${item.station} lot=${item.lotNumber} box=${item.box}`
      );

      let didSendToPrinter = false;
      try {
        assertNoRecentZplDuplicatePersistent(item, {
          excludeItemId: item.itemId,
          requiresRfidEncoding: item.requiresRfidEncoding
        });

        const result = await directZplQueueSendFunction({
          zpl: {
            printerIp: item.printerIp,
            port: item.printerPort,
            templatePath: item.templatePath
          },
          station: item.station,
          lotNumber: item.lotNumber,
          box: item.box,
          rfid: item.rfid,
          namedDataSources: item.namedDataSources || {},
          requiresRfidEncoding: item.requiresRfidEncoding,
          item,
          queueDepth: getQueuedZplItemDepth(printerKey)
        });

        didSendToPrinter = true;
        item = {
          ...item,
          status: "sent_to_printer",
          sentAt: isoNow(),
          sendResult: result,
          lastError: null
        };
        item = writeZplQueueItem(item);
        if (queueItemUsesDuplicateGuard(item)) markRecentZplSendAccepted(item);

        await writeQueueItemPrintLog(item, "Success", "Direct ZPL sent_to_printer; physical print not confirmed");

        logInfo(
          "zpl_queue_item_sent_to_printer",
          { station: item.station, lotNumber: item.lotNumber, box: item.box, rfid: item.rfid, printerIp: item.printerIp, printerPort: item.printerPort, itemId: item.itemId, jobId: item.jobId },
          `[PrintSvc] Direct ZPL queue item sent_to_printer itemId=${item.itemId} station=${item.station} lot=${item.lotNumber} box=${item.box}`
        );
      } catch (error) {
        if (error.code === "ZPL_SEND_UNKNOWN") {
          item = {
            ...item,
            status: "unknown_after_send",
            unknownAt: isoNow(),
            lastError: serializeQueueError(error),
            operatorAction: error.operatorAction || "Verify whether the label physically printed before retrying."
          };
          item = writeZplQueueItem(item);
          if (queueItemUsesDuplicateGuard(item)) markRecentZplSendAccepted(item);
          state.paused = true;
          state.phase = "paused";
          state.lastError = item.lastError;

          await writeQueueItemPrintLog(item, "Unknown", item.operatorAction);

          logError(
            "zpl_queue_item_unknown_after_send",
            { station: item.station, lotNumber: item.lotNumber, box: item.box, rfid: item.rfid, printerIp: item.printerIp, printerPort: item.printerPort, itemId: item.itemId, jobId: item.jobId, operatorAction: item.operatorAction, error: item.lastError },
            `[PrintSvc] Direct ZPL queue item unknown_after_send itemId=${item.itemId} station=${item.station} lot=${item.lotNumber} box=${item.box}`
          );
          logWarn(
            "zpl_queue_worker_paused",
            { printerKey, itemId: item.itemId, jobId: item.jobId, operatorAction: item.operatorAction },
            `[PrintSvc] Direct ZPL worker paused printerKey=${printerKey}; operator verification required`
          );
          break;
        }

        item = {
          ...item,
          status: error.code === "DUPLICATE_RECENT_ZPL" ? "rejected" : "failed_before_send",
          failedAt: isoNow(),
          lastError: serializeQueueError(error)
        };
        item = writeZplQueueItem(item);
        state.lastError = item.lastError;

        await writeQueueItemPrintLog(item, item.status === "rejected" ? "Rejected" : "Failed", error.message);

        const eventName = item.status === "rejected" ? "duplicate_recent_zpl_rejected" : "zpl_queue_item_failed_before_send";
        logError(
          eventName,
          { station: item.station, lotNumber: item.lotNumber, box: item.box, rfid: item.rfid, printerIp: item.printerIp, printerPort: item.printerPort, itemId: item.itemId, jobId: item.jobId, error: item.lastError },
          `[PrintSvc] Direct ZPL queue item ${item.status} itemId=${item.itemId} station=${item.station} lot=${item.lotNumber} box=${item.box}: ${error.message}`
        );
      } finally {
        state.activeItem = null;
      }

      if (didSendToPrinter) {
        const spacingMs = getZplLabelSpacingMs();
        state.phase = spacingMs > 0 ? "waiting_between_labels" : "running";
        state.waitingUntil = spacingMs > 0 ? new Date(Date.now() + spacingMs).toISOString() : null;
        await sleep(spacingMs);
        state.waitingUntil = null;
      }
    }
  } finally {
    if (!isCurrentZplWorkerState(printerKey, state)) return;

    state.running = false;
    state.activeItem = null;
    if (!state.paused) state.activeBatch = null;
    state.phase = state.paused ? "paused" : "idle";
    state.waitingUntil = null;
    if (!state.paused && getZplSocketMode() === "persistent" && !getNextQueuedZplItem(printerKey)) {
      scheduleZplPersistentSocketIdleClose(printerKey);
    }
    if (!state.paused && getNextQueuedZplItem(printerKey)) {
      setImmediate(() => startZplQueueWorkerForPrinter(printerKey));
    }
  }
}

function startZplQueueWorkerForPrinter(printerKey) {
  const state = getOrCreateZplWorkerState(printerKey);
  if (state.running || state.paused) return;
  state.running = true;
  processZplQueueForPrinter(printerKey).catch((error) => {
    state.running = false;
    state.lastError = serializeQueueError(error);
    logError("zpl_queue_worker_error", { printerKey, message: error.message, code: error.code || null }, `[PrintSvc] Direct ZPL queue worker error printerKey=${printerKey}: ${error.message}`);
  });
}

function recoverStaleSendingItem(item, { nowMs = Date.now(), reason = "stale sending item recovered" } = {}) {
  if (!item || item.status !== "sending") return null;

  const noBytesWritten = queueItemProvesNoBytesWritten(item);
  const operatorAction = "Verify whether this label physically printed before resuming.";
  const nextStatus = noBytesWritten ? "failed_before_send" : "unknown_after_send";
  const lastError = noBytesWritten
    ? {
        code: "ZPL_FAILED_BEFORE_SEND",
        message: "PrintSvc recovered a stale sending queue item that proved no TCP write started.",
        details: getQueueItemSendDetails(item),
        retryable: true
      }
    : {
        code: "ZPL_SEND_UNKNOWN",
        message: "PrintSvc recovered a stale sending queue item after restart or worker interruption.",
        retryable: false
      };

  const next = writeZplQueueItem({
    ...item,
    status: nextStatus,
    recoveredAt: new Date(nowMs).toISOString(),
    recoveredFromStatus: item.status,
    recoveryReason: reason,
    unknownAt: nextStatus === "unknown_after_send" ? new Date(nowMs).toISOString() : item.unknownAt,
    failedAt: nextStatus === "failed_before_send" ? new Date(nowMs).toISOString() : item.failedAt,
    lastError,
    operatorAction: nextStatus === "unknown_after_send" ? operatorAction : item.operatorAction || null
  });

  if (nextStatus === "unknown_after_send") {
    markRecentZplSendAccepted(next, nowMs);
    const state = getOrCreateZplWorkerState(next.printerKey);
    state.paused = true;
    state.lastError = next.lastError;
  } else {
    const state = getOrCreateZplWorkerState(next.printerKey);
    state.paused = false;
    state.lastError = next.lastError;
  }

  logWarn(
    "zpl_queue_recovered_stale_sending",
    {
      itemId: next.itemId,
      jobId: next.jobId,
      station: next.station,
      lotNumber: next.lotNumber,
      box: next.box,
      rfid: next.rfid,
      printerIp: next.printerIp,
      printerPort: next.printerPort,
      oldStatus: "sending",
      newStatus: next.status,
      operatorAction: next.operatorAction || null,
      safeToRetry: isQueueItemSafeToRetry(next)
    },
    `[PrintSvc] Recovered stale ZPL sending item itemId=${next.itemId} status=${next.status} station=${next.station} lot=${next.lotNumber} box=${next.box}`
  );

  return next;
}

function scheduleStaleSendingRecovery(item, nowMs = Date.now()) {
  if (!item?.itemId || zplStaleSendingRecoveryTimers.has(item.itemId)) return;
  const startedAtMs = getQueueItemStartedAtMs(item);
  const thresholdMs = getZplStaleSendingThresholdMs();
  const delayMs = Math.max(1, startedAtMs + thresholdMs - nowMs);

  const timer = setTimeout(() => {
    zplStaleSendingRecoveryTimers.delete(item.itemId);
    recoverStaleSendingItems({ nowMs: Date.now(), reason: "stale sending threshold elapsed" });
  }, delayMs);
  if (typeof timer.unref === "function") timer.unref();
  zplStaleSendingRecoveryTimers.set(item.itemId, timer);
}

function recoverStaleSendingItems({ nowMs = Date.now(), reason = "startup stale sending recovery" } = {}) {
  const recovered = [];
  const thresholdMs = getZplStaleSendingThresholdMs();

  for (const item of listZplQueueItems()) {
    if (item.status !== "sending") continue;

    const state = getOrCreateZplWorkerState(item.printerKey);
    const isActiveItem = state.activeItem?.itemId === item.itemId;
    const isActiveBatchItem = Array.isArray(state.activeBatch?.itemIds) && state.activeBatch.itemIds.includes(item.itemId);
    if (state.running && (isActiveItem || isActiveBatchItem)) {
      continue;
    }

    state.paused = true;
    state.lastError = item.lastError || {
      code: "ZPL_QUEUE_SENDING_RECOVERY_PENDING",
      message: "Queue item was found in sending status; waiting for stale threshold before recovery.",
      retryable: false
    };

    const ageMs = nowMs - getQueueItemStartedAtMs(item);
    if (ageMs >= thresholdMs) {
      const timer = zplStaleSendingRecoveryTimers.get(item.itemId);
      if (timer) clearTimeout(timer);
      zplStaleSendingRecoveryTimers.delete(item.itemId);
      const next = recoverStaleSendingItem(item, { nowMs, reason });
      if (next) recovered.push(next);
    } else {
      scheduleStaleSendingRecovery(item, nowMs);
    }
  }

  return recovered;
}

function startAllZplQueueWorkers() {
  recoverStaleSendingItems({ reason: "startup stale sending recovery" });
  const items = listZplQueueItems();
  const queued = items.filter((item) => item.status === "queued");
  const keys = new Set(queued.map((item) => item.printerKey).filter(Boolean));

  for (const item of items) {
    if (item.status === "sending") {
      const state = getOrCreateZplWorkerState(item.printerKey);
      state.paused = true;
      state.lastError = state.lastError || {
        code: "ZPL_QUEUE_SENDING_RECOVERY_PENDING",
        message: "Queue item is still within the stale sending threshold.",
        retryable: false
      };
      continue;
    }

    if (item.status === "unknown_after_send" && !item.operatorReviewedAt) {
      const state = getOrCreateZplWorkerState(item.printerKey);
      state.paused = true;
      state.lastError = item.lastError || null;
    }
  }

  for (const key of keys) {
    startZplQueueWorkerForPrinter(key);
  }
}

function getZplQueueStatusPayload() {
  recoverStaleSendingItems({ reason: "status stale sending recovery" });
  const items = listZplQueueItems();
  const byPrinter = {};

  for (const item of items) {
    const key = item.printerKey || `${item.printerIp}:${item.printerPort}`;
    if (!byPrinter[key]) {
      byPrinter[key] = {
        printerKey: key,
        printerIp: item.printerIp,
        printerPort: item.printerPort,
        socketMode: getZplSocketMode(),
        socketState: getZplPersistentSocketStatus(key),
        counts: queueStatusCounts(),
        itemsByStatus: DIRECT_ZPL_QUEUE_STATUSES.reduce((acc, status) => {
          acc[status] = [];
          return acc;
        }, {}),
        activeItem: null,
        activeBatch: null,
        lastBatchDurationMs: null,
        paused: false,
        lastError: null,
        staleItems: [],
        recoveredItems: [],
        reviewRequiredItems: [],
        safeRetryItems: [],
        recent: []
      };
    }

    byPrinter[key].counts[item.status] = (byPrinter[key].counts[item.status] || 0) + 1;
    byPrinter[key].queueDepth = getQueuedZplItemDepth(key);
    const summary = summarizeZplQueueItem(item);
    if (byPrinter[key].itemsByStatus[item.status]) {
      byPrinter[key].itemsByStatus[item.status].push(summary);
    }
    if (item.status === "sending" && Date.now() - getQueueItemStartedAtMs(item) >= getZplStaleSendingThresholdMs()) {
      byPrinter[key].staleItems.push(summary);
    }
    if (item.recoveredAt) {
      byPrinter[key].recoveredItems.push(summary);
    }
    if (item.status === "unknown_after_send" && !item.operatorReviewedAt) {
      const state = getOrCreateZplWorkerState(key);
      state.paused = true;
      state.lastError = item.lastError || state.lastError || null;
      byPrinter[key].reviewRequiredItems.push(summary);
    }
    if (isQueueItemSafeToRetry(item)) {
      byPrinter[key].safeRetryItems.push(summary);
    }
    if (["sent_to_printer", "unknown_after_send"].includes(item.status)) {
      byPrinter[key].recent.push(summary);
    }
  }

  for (const [key, state] of zplQueueWorkers.entries()) {
    if (!byPrinter[key]) {
      byPrinter[key] = {
        printerKey: key,
        socketMode: getZplSocketMode(),
        socketState: getZplPersistentSocketStatus(key),
        counts: queueStatusCounts(),
        itemsByStatus: DIRECT_ZPL_QUEUE_STATUSES.reduce((acc, status) => {
          acc[status] = [];
          return acc;
        }, {}),
        activeItem: null,
        activeBatch: null,
        lastBatchDurationMs: null,
        paused: false,
        lastError: null,
        staleItems: [],
        recoveredItems: [],
        reviewRequiredItems: [],
        safeRetryItems: [],
        recent: []
      };
    }
    byPrinter[key].activeItem = state.activeItem ? summarizeZplQueueItem(state.activeItem) : null;
    byPrinter[key].activeBatch = state.activeBatch || null;
    byPrinter[key].lastBatchDurationMs = state.lastBatchDurationMs ?? null;
    byPrinter[key].paused = state.paused === true;
    byPrinter[key].running = state.running === true;
    byPrinter[key].phase = state.paused ? "paused" : (state.phase || (state.running ? "running" : "idle"));
    byPrinter[key].waitingUntil = state.waitingUntil || null;
    byPrinter[key].lastError = state.lastError || null;
    byPrinter[key].socketMode = getZplSocketMode();
    byPrinter[key].socketState = getZplPersistentSocketStatus(key);
  }

  for (const printer of Object.values(byPrinter)) {
    if (!printer.phase) {
      if (printer.paused) printer.phase = "paused";
      else if (printer.counts.sending > 0) printer.phase = "sending";
      else if (printer.counts.queued > 0) printer.phase = "idle";
      else printer.phase = "idle";
    }
    if (!Object.prototype.hasOwnProperty.call(printer, "queueDepth")) {
      printer.queueDepth = getQueuedZplItemDepth(printer.printerKey);
    }
    printer.socketMode = getZplSocketMode();
    printer.socketState = getZplPersistentSocketStatus(printer.printerKey);
    printer.recent.sort((a, b) => String(b.updatedAt || "").localeCompare(String(a.updatedAt || "")));
    printer.recent = printer.recent.slice(0, 20);
    printer.recoveredItems.sort((a, b) => String(b.recoveredAt || "").localeCompare(String(a.recoveredAt || "")));
    printer.safeRetryItems.sort((a, b) => String(a.updatedAt || "").localeCompare(String(b.updatedAt || "")));
  }

  return {
    ok: true,
    printEngine: getPrintEngineHealth().printEngine,
    directZplEnabledScopes: getDirectZplEnabledScopes(),
    zplQueueEnabled: true,
    zplQueuePath: ZPL_QUEUE_DIR,
    zplStaleSendingThresholdMs: getZplStaleSendingThresholdMs(),
    socketMode: getZplSocketMode(),
    zplSocketMode: getZplSocketMode(),
    zplMaxLabelsPerConnection: getZplMaxLabelsPerConnection(),
    zplSocketIdleCloseMs: getZplSocketIdleCloseMs(),
    zplBatchMaxLabels: getZplBatchMaxLabels(),
    zplBatchCollectMs: getZplBatchCollectMs(),
    zplBatchInterBatchDelayMs: getZplBatchInterBatchDelayMs(),
    zplBatchMaxBytes: getZplBatchMaxBytes(),
    activeSockets: getZplPersistentSocketStatusForAll(),
    pausedPrinterKeys: Object.entries(byPrinter).filter(([, value]) => value.paused).map(([key]) => key),
    printers: byPrinter
  };
}

function getPrintSvcLogPath() {
  return process.env.PRINTSVC_LOG_PATH || PRINTSVC_LOG_PATH;
}

function normalizeLogTail(value) {
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) return PRINTSVC_LOG_TAIL_DEFAULT;
  return Math.min(number, PRINTSVC_LOG_TAIL_MAX);
}

function readTailLogLines(filePath, tail) {
  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      return { exists: false, filePath, readBytes: 0, lines: [] };
    }
    throw error;
  }

  if (!stat.isFile()) {
    throw httpError(400, "PRINT_LOG_PATH_INVALID", "PrintSvc log path is not a file.");
  }

  const desiredBytes = Math.min(
    stat.size,
    PRINTSVC_LOG_READ_MAX_BYTES,
    Math.max(64 * 1024, tail * 2048)
  );
  const start = Math.max(0, stat.size - desiredBytes);
  const buffer = Buffer.alloc(desiredBytes);
  const fd = fs.openSync(filePath, "r");
  let bytesRead = 0;
  try {
    bytesRead = fs.readSync(fd, buffer, 0, desiredBytes, start);
  } finally {
    fs.closeSync(fd);
  }

  let text = buffer.slice(0, bytesRead).toString("utf8");
  if (start > 0) {
    const firstNewline = text.search(/\r?\n/);
    text = firstNewline >= 0 ? text.slice(firstNewline + (text[firstNewline] === "\r" && text[firstNewline + 1] === "\n" ? 2 : 1)) : "";
  }

  const lines = text.split(/\r?\n/).filter((line) => line.trim()).slice(-tail);
  return { exists: true, filePath, readBytes: bytesRead, lines };
}

function redactLogValue(value) {
  if (Array.isArray(value)) return value.map(redactLogValue);
  if (!value || typeof value !== "object") return value;

  const output = {};
  for (const [key, child] of Object.entries(value)) {
    if (/password|secret|token|authorization|cookie/i.test(key)) {
      output[key] = "[redacted]";
    } else {
      output[key] = redactLogValue(child);
    }
  }
  return output;
}

function redactRawLogLine(line) {
  return String(line || "")
    .replace(/(Bearer\s+)[A-Za-z0-9._~+/=-]+/gi, "$1[redacted]")
    .replace(/((?:password|secret|token|authorization|cookie)\s*[:=]\s*)("[^"]*"|'[^']*'|\S+)/gi, "$1[redacted]");
}

function logRecordFieldMatches(record, field, expected) {
  const value = record?.[field];
  if (value === undefined || value === null) return false;
  const expectedText = String(expected).toLowerCase();
  if (Array.isArray(value)) {
    return value.some((entry) => String(entry ?? "").toLowerCase() === expectedText);
  }
  return String(value).toLowerCase() === expectedText;
}

function parsePrintLogLine(line, index) {
  const raw = redactRawLogLine(line);
  const trimmed = raw.trim();
  try {
    const record = redactLogValue(JSON.parse(trimmed));
    return {
      index,
      parsed: true,
      timestamp: record.timestamp || null,
      level: record.level || null,
      event: record.event || null,
      station: record.station || null,
      family: record.family || null,
      lotNumber: record.lotNumber || null,
      printerIp: record.printerIp || null,
      record
    };
  } catch {
    return {
      index,
      parsed: false,
      timestamp: null,
      level: null,
      event: null,
      raw
    };
  }
}

function logEntryMatchesFilters(entry, filters) {
  const record = entry.record || {};
  const rawSearchText = `${entry.raw || ""} ${entry.parsed ? JSON.stringify(record) : ""}`.toLowerCase();

  for (const field of ["event", "level", "station", "family", "lotNumber", "printerIp"]) {
    const expected = filters[field];
    if (!expected) continue;
    if (!entry.parsed || !logRecordFieldMatches(record, field, expected)) return false;
  }

  if (filters.search && !rawSearchText.includes(filters.search.toLowerCase())) return false;
  return true;
}

function getPrintLogsPayload(query = {}) {
  const tail = normalizeLogTail(query.tail);
  const filters = {
    event: trimString(query.event),
    level: trimString(query.level),
    station: trimString(query.station),
    family: trimString(query.family),
    lotNumber: trimString(query.lotNumber),
    printerIp: trimString(query.printerIp),
    search: trimString(query.search)
  };
  const source = readTailLogLines(getPrintSvcLogPath(), tail);
  const entries = source.lines
    .map((line, index) => parsePrintLogLine(line, index))
    .filter((entry) => logEntryMatchesFilters(entry, filters));

  return {
    ok: true,
    logPath: source.filePath,
    exists: source.exists,
    tail,
    readBytes: source.readBytes,
    filters,
    count: entries.length,
    lines: entries
  };
}

function deepCloneJson(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function deepMergePlainObjects(base = {}, override = {}) {
  const output = { ...(base || {}) };
  for (const [key, value] of Object.entries(override || {})) {
    if (isPlainObject(value) && isPlainObject(output[key])) {
      output[key] = deepMergePlainObjects(output[key], value);
    } else if (isPlainObject(value)) {
      output[key] = deepMergePlainObjects({}, value);
    } else if (Array.isArray(value)) {
      output[key] = value.map((entry) => (isPlainObject(entry) ? deepMergePlainObjects({}, entry) : entry));
    } else if (value !== undefined) {
      output[key] = value;
    }
  }
  return output;
}

function readTemplateLabProfileConfig() {
  try {
    if (!fs.existsSync(ZPL_TEMPLATE_LAB_PROFILE_PATH)) {
      return { profiles: {} };
    }
    const parsed = JSON.parse(fs.readFileSync(ZPL_TEMPLATE_LAB_PROFILE_PATH, "utf8"));
    return isPlainObject(parsed) ? { profiles: isPlainObject(parsed.profiles) ? parsed.profiles : {} } : { profiles: {} };
  } catch (error) {
    logWarn("template_lab_profile_config_read_failed", {
      profilePath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
      message: error.message
    });
    return { profiles: {} };
  }
}

function writeTemplateLabProfileConfig(config) {
  fs.mkdirSync(path.dirname(ZPL_TEMPLATE_LAB_PROFILE_PATH), { recursive: true });
  fs.writeFileSync(ZPL_TEMPLATE_LAB_PROFILE_PATH, `${JSON.stringify(config, null, 2)}\n`, "utf8");
}

function parseJsonObjectField(value, fieldName) {
  if (value === undefined || value === null || value === "") return {};
  if (isPlainObject(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (isPlainObject(parsed)) return parsed;
    } catch (error) {
      throw httpError(400, "VALIDATION_ERROR", `${fieldName} must be valid JSON.`);
    }
  }
  throw httpError(400, "VALIDATION_ERROR", `${fieldName} must be an object.`);
}

function numberFromInput(value, options = {}) {
  if (value === undefined || value === null || value === "") return undefined;
  const number = Number(value);
  if (!Number.isFinite(number)) return undefined;
  if (Number.isFinite(options.min) && number < options.min) return undefined;
  if (Number.isFinite(options.max) && number > options.max) return undefined;
  return options.integer ? Math.round(number) : number;
}

function setIfNumber(target, key, value, options = {}) {
  const number = numberFromInput(value, options);
  if (number !== undefined) target[key] = number;
}

function booleanFromInput(value) {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "off"].includes(normalized)) return false;
  return undefined;
}

function setIfBoolean(target, key, value) {
  const bool = booleanFromInput(value);
  if (bool !== undefined) target[key] = bool;
}

function buildFieldFitOverrideFromInput(input = {}, prefix) {
  const output = {};
  setIfNumber(output, "boxWidth", input[`${prefix}BoxWidth`], { min: 1, integer: true });
  setIfNumber(output, "boxHeight", input[`${prefix}BoxHeight`], { min: 1, integer: true });
  setIfNumber(output, "fontHeight", input[`${prefix}FontHeight`], { min: 1, integer: true });
  setIfNumber(output, "fontWidth", input[`${prefix}FontWidth`], { min: 1, integer: true });
  setIfNumber(output, "maxChars", input[`${prefix}MaxChars`], { min: 1, integer: true });
  setIfNumber(output, "maxLines", input[`${prefix}MaxLines`], { min: 1, max: 6, integer: true });
  setIfNumber(output, "borderThickness", input[`${prefix}BorderThickness`], { min: 0, max: 20, integer: true });
  const alignment = trimString(input[`${prefix}Alignment`]).toUpperCase();
  if (["L", "C", "R", "J"].includes(alignment)) output.alignment = alignment;
  for (const tier of ["large", "medium", "small", "min"]) {
    const tierOutput = {};
    const inputPrefix = `${prefix}${tier.charAt(0).toUpperCase()}${tier.slice(1)}`;
    setIfNumber(tierOutput, "fontH", input[`${inputPrefix}FontH`], { min: 1, integer: true });
    setIfNumber(tierOutput, "fontW", input[`${inputPrefix}FontW`], { min: 1, integer: true });
    if (Object.keys(tierOutput).length) output[tier] = tierOutput;
  }
  return output;
}

function buildProfileOverridesFromInput(input = {}) {
  const overrides = deepCloneJson(parseJsonObjectField(input.profileOverrides, "profileOverrides"));
  setIfNumber(overrides, "labelWidthDots", input.labelWidthDots, { min: 1, max: 10000, integer: true });
  setIfNumber(overrides, "labelHeightDots", input.labelHeightDots, { min: 1, max: 10000, integer: true });
  setIfNumber(overrides, "globalScaleX", input.globalScaleX ?? input.scaleX, { min: 0.1, max: 5 });
  setIfNumber(overrides, "globalScaleY", input.globalScaleY ?? input.scaleY, { min: 0.1, max: 5 });
  setIfNumber(overrides, "globalOffsetX", input.globalOffsetX ?? input.offsetX, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "globalOffsetY", input.globalOffsetY ?? input.offsetY, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "scaleX", input.scaleX, { min: 0.1, max: 5 });
  setIfNumber(overrides, "scaleY", input.scaleY, { min: 0.1, max: 5 });
  setIfNumber(overrides, "offsetX", input.offsetX, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "offsetY", input.offsetY, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "labelHomeX", input.labelHomeX, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "labelHomeY", input.labelHomeY, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "labelShiftX", input.labelShiftX, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "labelShiftY", input.labelShiftY, { min: -5000, max: 5000, integer: true });
  setIfNumber(overrides, "borderThickness", input.borderThickness, { min: 0, max: 20, integer: true });
  setIfBoolean(overrides, "scaleBorderThickness", input.scaleBorderThickness);

  const qr = {};
  setIfNumber(qr, "x", input.qrX, { min: -5000, max: 5000, integer: true });
  setIfNumber(qr, "y", input.qrY, { min: -5000, max: 5000, integer: true });
  setIfNumber(qr, "magnification", input.qrMagnification, { min: 1, max: 20, integer: true });
  if (Object.keys(qr).length) overrides.qr = deepMergePlainObjects(overrides.qr || {}, qr);

  const logo = {};
  setIfNumber(logo, "x", input.logoX, { min: -5000, max: 5000, integer: true });
  setIfNumber(logo, "y", input.logoY, { min: -5000, max: 5000, integer: true });
  setIfNumber(logo, "scale", input.logoScale, { min: 0.25, max: 6 });
  setIfNumber(logo, "widthDots", input.logoWidthDots, { min: 1, max: 2000, integer: true });
  setIfNumber(logo, "heightDots", input.logoHeightDots, { min: 1, max: 2000, integer: true });
  setIfNumber(logo, "threshold", input.logoThreshold, { min: 0, max: 255, integer: true });
  const logoDithering = trimString(input.logoDithering).toLowerCase();
  if (["none", "ordered"].includes(logoDithering)) logo.dithering = logoDithering;
  const logoAssetName = trimString(input.logoAssetName || input.logoAsset);
  if (logoAssetName) logo.assetName = logoAssetName;
  const logoGfa = trimString(input.logoGfa || input.logoGFA);
  if (logoGfa && /^\^GFA,/i.test(logoGfa)) logo.gfa = logoGfa;
  if (Object.keys(logo).length) overrides.logo = deepMergePlainObjects(overrides.logo || {}, logo);

  const fieldFitDefinitions = {};
  for (const prefix of ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"]) {
    const fieldOverride = buildFieldFitOverrideFromInput(input, prefix);
    if (Object.keys(fieldOverride).length) fieldFitDefinitions[prefix] = fieldOverride;
  }
  if (Object.keys(fieldFitDefinitions).length) {
    overrides.fieldFitDefinitions = deepMergePlainObjects(overrides.fieldFitDefinitions || {}, fieldFitDefinitions);
  }

  const fieldPositionOverrides = {};
  for (const prefix of ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"]) {
    const position = {};
    setIfNumber(position, "x", input[`${prefix}X`], { min: -5000, max: 5000, integer: true });
    setIfNumber(position, "y", input[`${prefix}Y`], { min: -5000, max: 5000, integer: true });
    if (Object.keys(position).length) fieldPositionOverrides[prefix] = position;
  }
  if (Object.keys(fieldPositionOverrides).length) {
    overrides.fieldPositionOverrides = deepMergePlainObjects(overrides.fieldPositionOverrides || {}, fieldPositionOverrides);
  }

  const fieldGeometryOverrides = parseJsonObjectField(input.fieldGeometryOverrides, "fieldGeometryOverrides");
  if (Object.keys(fieldGeometryOverrides).length) {
    overrides.fieldGeometryOverrides = deepMergePlainObjects(overrides.fieldGeometryOverrides || {}, fieldGeometryOverrides);
  }

  const bottomGrid = {};
  setIfNumber(bottomGrid, "x", input.bottomGridX, { min: -5000, max: 10000, integer: true });
  setIfNumber(bottomGrid, "y", input.bottomGridY, { min: -5000, max: 10000, integer: true });
  setIfNumber(bottomGrid, "width", input.bottomGridWidth, { min: 1, max: 10000, integer: true });
  setIfNumber(bottomGrid, "height", input.bottomGridHeight, { min: 1, max: 10000, integer: true });
  setIfNumber(bottomGrid, "borderThickness", input.bottomGridBorderThickness, { min: 0, max: 20, integer: true });
  setIfNumber(bottomGrid, "columnCount", input.bottomGridColumnCount, { min: 1, max: 24, integer: true });
  setIfNumber(bottomGrid, "columnLineThickness", input.bottomGridColumnLineThickness, { min: 0, max: 20, integer: true });
  if (Object.keys(bottomGrid).length) {
    overrides.bottomGrid = deepMergePlainObjects(overrides.bottomGrid || {}, bottomGrid);
  }

  return overrides;
}

function getSavedTemplateLabProfileOverrides(profileKey) {
  const config = readTemplateLabProfileConfig();
  return deepCloneJson(config.profiles?.[String(profileKey || "").toUpperCase()] || {});
}

function normalizeTemplateLabProfileGeometry(profile = {}) {
  const normalized = { ...profile };
  const scaleX = numberFromInput(normalized.globalScaleX, { min: 0.1, max: 5 }) ??
    numberFromInput(normalized.scaleX, { min: 0.1, max: 5 }) ?? 1;
  const scaleY = numberFromInput(normalized.globalScaleY, { min: 0.1, max: 5 }) ??
    numberFromInput(normalized.scaleY, { min: 0.1, max: 5 }) ?? 1;
  const offsetX = numberFromInput(normalized.globalOffsetX, { min: -5000, max: 5000, integer: true }) ??
    numberFromInput(normalized.offsetX, { min: -5000, max: 5000, integer: true }) ?? 0;
  const offsetY = numberFromInput(normalized.globalOffsetY, { min: -5000, max: 5000, integer: true }) ??
    numberFromInput(normalized.offsetY, { min: -5000, max: 5000, integer: true }) ?? 0;

  normalized.globalScaleX = scaleX;
  normalized.globalScaleY = scaleY;
  normalized.globalOffsetX = offsetX;
  normalized.globalOffsetY = offsetY;
  normalized.scaleX = scaleX;
  normalized.scaleY = scaleY;
  normalized.offsetX = offsetX;
  normalized.offsetY = offsetY;
  normalized.labelWidthDots = numberFromInput(normalized.labelWidthDots, { min: 1, max: 10000, integer: true }) || 812;
  normalized.labelHeightDots = numberFromInput(normalized.labelHeightDots, { min: 1, max: 10000, integer: true }) || 1218;
  normalized.labelHomeX = numberFromInput(normalized.labelHomeX, { min: -5000, max: 5000, integer: true });
  normalized.labelHomeY = numberFromInput(normalized.labelHomeY, { min: -5000, max: 5000, integer: true });
  normalized.labelShiftX = numberFromInput(normalized.labelShiftX, { min: -5000, max: 5000, integer: true });
  normalized.labelShiftY = numberFromInput(normalized.labelShiftY, { min: -5000, max: 5000, integer: true });
  normalized.borderThickness = numberFromInput(normalized.borderThickness, { min: 0, max: 20, integer: true });
  normalized.scaleBorderThickness = Boolean(normalized.scaleBorderThickness);
  normalized.bottomGrid = normalizeBottomGridProfile(normalized.bottomGrid || {});
  return normalized;
}

function buildEffectiveTemplateLabProfile(profileKey, fallbackProfileKey, inlineOverrides = {}, options = {}) {
  const key = String(profileKey || fallbackProfileKey || "").trim().toUpperCase();
  const base = getStationProfile(key) || getStationProfile(fallbackProfileKey);
  if (!base) return null;

  const savedOverrides = getSavedTemplateLabProfileOverrides(base.key);
  const profile = options.includeSaved === true ? deepMergePlainObjects(base, savedOverrides) : deepMergePlainObjects({}, base);
  const merged = deepMergePlainObjects(profile, inlineOverrides);
  merged.key = base.key;
  merged.labOnly = true;
  merged.savedOverrides = savedOverrides;
  merged.inlineOverrides = inlineOverrides;
  Object.assign(merged, normalizeTemplateLabProfileGeometry(merged));
  merged.effectiveFieldFitDefinitions = getFittedFieldDefinitions(merged.fieldFitDefinitions || {});
  return merged;
}

function getTemplateLabCatalogPayload() {
  const savedConfig = readTemplateLabProfileConfig();
  const profiles = listStationProfiles().map((profile) => {
    const effective = buildEffectiveTemplateLabProfile(profile.key, profile.key, {});
    return {
      ...effective,
      profileConfigPath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
      savedOverrides: savedConfig.profiles?.[profile.key] || {}
    };
  });

  return {
    ok: true,
    templates: listTemplateLabTemplates(),
    profiles,
    templateSourceDir: ZPL_TEMPLATE_SOURCE_DIR,
    profileConfigPath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
    logoAssetDir: ZPL_TEMPLATE_LAB_ASSET_DIR,
    previewRendererConfigured: Boolean(trimString(process.env.ZPL_PREVIEW_RENDERER_URL))
  };
}

function normalizeTemplateLabTemplateName(value) {
  const raw = trimString(value);
  const name = path.basename(path.win32.basename(raw));
  if (!name) throw httpError(400, "VALIDATION_ERROR", "template is required.");
  const definition = getTemplateDefinition(name);
  if (!definition) {
    throw httpError(400, "UNSUPPORTED_TEMPLATE", "Template Lab can only render approved direct-ZPL templates.", {
      template: name,
      supportedTemplates: listTemplateLabTemplates().map((template) => template.name)
    });
  }
  return { name, definition, templatePath: path.join(ZPL_TEMPLATE_SOURCE_DIR, name) };
}

function parseTemplateLabFieldFitDefinitionsFromSource(source) {
  const pattern = templateLabFieldFitCommentPattern();
  const matches = Array.from(String(source || "").matchAll(pattern));
  if (!matches.length) return {};

  const lastMatch = matches[matches.length - 1][0];
  const encoded = lastMatch
    .replace(new RegExp(`^\\^FX\\s*${FIELD_FIT_DEFINITIONS_COMMENT_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`), "")
    .trim();
  try {
    const parsed = JSON.parse(Buffer.from(encoded, "base64").toString("utf8"));
    return isPlainObject(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function parseZplTemplateCommandsWithPositions(zpl) {
  const text = String(zpl || "");
  const commands = [];
  let index = 0;
  while (index < text.length) {
    const start = text.slice(index).search(/[\^~]/);
    if (start < 0) break;
    const absoluteStart = index + start;
    const prefix = text[absoluteStart];
    const code = text.slice(absoluteStart + 1, absoluteStart + 3);
    if (code.length < 2) break;
    let next = absoluteStart + 3;
    while (next < text.length && text[next] !== "^" && text[next] !== "~") next += 1;
    const paramsStart = absoluteStart + 3;
    const raw = text.slice(absoluteStart, next);
    commands.push({
      prefix,
      code,
      params: text.slice(paramsStart, next).replace(/\r?\n/g, ""),
      raw,
      start: absoluteStart,
      end: next,
      paramsStart,
      paramsEnd: next
    });
    index = next;
  }
  return commands;
}

function numberOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function zplParamParts(value) {
  return String(value || "").split(",").map((part) => part.trim());
}

function parseFontCommand(command) {
  if (!command || command.code !== "A0") return null;
  const parts = zplParamParts(command.params);
  return {
    command: `^${command.code}`,
    orientation: parts[0] || "N",
    fontHeight: numberOrNull(parts[1]),
    fontWidth: numberOrNull(parts[2]),
    fontHeightRaw: parts[1] || "",
    fontWidthRaw: parts[2] || "",
    commandStart: command.start,
    commandEnd: command.end,
    raw: command.raw
  };
}

function parseFieldBlockCommand(command) {
  if (!command || command.code !== "FB") return null;
  const parts = zplParamParts(command.params);
  return {
    width: numberOrNull(parts[0]),
    maxLines: numberOrNull(parts[1]),
    lineSpacing: numberOrNull(parts[2]),
    alignment: (parts[3] || "L").toUpperCase(),
    hangingIndent: numberOrNull(parts[4]),
    widthRaw: parts[0] || "",
    maxLinesRaw: parts[1] || "",
    lineSpacingRaw: parts[2] || "",
    alignmentRaw: parts[3] || "",
    hangingIndentRaw: parts[4] || "",
    commandStart: command.start,
    commandEnd: command.end,
    raw: command.raw
  };
}

function parsePositionCommand(command) {
  if (!command || !["FO", "FT"].includes(command.code)) return null;
  const parts = zplParamParts(command.params);
  return {
    command: command.code,
    x: numberOrNull(parts[0]),
    y: numberOrNull(parts[1]),
    xRaw: parts[0] || "",
    yRaw: parts[1] || "",
    commandStart: command.start,
    commandEnd: command.end,
    raw: command.raw
  };
}

function parseGraphicBoxCommand(command, position) {
  if (!command || command.code !== "GB") return null;
  const parts = zplParamParts(command.params);
  return {
    x: position?.x ?? null,
    y: position?.y ?? null,
    originCommand: position?.command || null,
    width: numberOrNull(parts[0]),
    height: numberOrNull(parts[1]),
    thickness: numberOrNull(parts[2]),
    color: parts[3] || "",
    rounding: numberOrNull(parts[4]),
    commandStart: command.start,
    commandEnd: command.end,
    raw: command.raw
  };
}

function normalizeBottomGridProfile(value = {}) {
  const input = isPlainObject(value) ? value : {};
  const output = {};
  setIfNumber(output, "x", input.x ?? input.bottomGridX, { min: -5000, max: 10000, integer: true });
  setIfNumber(output, "y", input.y ?? input.bottomGridY, { min: -5000, max: 10000, integer: true });
  setIfNumber(output, "width", input.width ?? input.bottomGridWidth, { min: 1, max: 10000, integer: true });
  setIfNumber(output, "height", input.height ?? input.bottomGridHeight, { min: 1, max: 10000, integer: true });
  setIfNumber(output, "borderThickness", input.borderThickness ?? input.thickness ?? input.bottomGridBorderThickness, { min: 0, max: 20, integer: true });
  setIfNumber(output, "columnCount", input.columnCount ?? input.columns ?? input.bottomGridColumnCount, { min: 1, max: 24, integer: true });
  setIfNumber(output, "columnLineThickness", input.columnLineThickness ?? input.bottomGridColumnLineThickness, { min: 0, max: 20, integer: true });
  return output;
}

function detectBottomGridFromBorders(borders = [], label = {}) {
  const labelHeight = Number(label?.labelHeightDots) || 0;
  const minimumY = labelHeight > 0 ? labelHeight * 0.62 : 650;
  const candidates = (borders || []).filter((border) => {
    const x = Number(border.x);
    const y = Number(border.y);
    const width = Math.abs(Number(border.width) || 0);
    const height = Math.abs(Number(border.height) || 0);
    if (!Number.isFinite(x) || !Number.isFinite(y) || y < minimumY) return false;
    return (width >= 180 && height >= 30) || (width >= 180 && height === 0) || (width === 0 && height >= 45);
  });
  if (!candidates.length) return null;
  const maxTopY = Math.max(...candidates.map((border) => Number(border.y)).filter(Number.isFinite));
  const bottomCandidates = candidates.filter((border) => Number(border.y) >= maxTopY - 180);

  const rectangle = bottomCandidates
    .filter((border) => Math.abs(Number(border.width) || 0) >= 180 && Math.abs(Number(border.height) || 0) >= 30)
    .sort((a, b) => Number(b.y || 0) - Number(a.y || 0))[0];

  let x;
  let y;
  let width;
  let height;
  let thickness;
  if (rectangle) {
    x = Number(rectangle.x);
    y = Number(rectangle.y);
    width = Math.abs(Number(rectangle.width) || 0);
    height = Math.abs(Number(rectangle.height) || 0);
    thickness = Number(rectangle.thickness);
  } else {
    const xs = [];
    const ys = [];
    bottomCandidates.forEach((border) => {
      const bx = Number(border.x);
      const by = Number(border.y);
      const bw = Number(border.width) || 0;
      const bh = Number(border.height) || 0;
      if (Number.isFinite(bx)) {
        xs.push(bx);
        xs.push(bx + bw);
      }
      if (Number.isFinite(by)) {
        ys.push(by);
        ys.push(by + bh);
      }
    });
    x = Math.min(...xs);
    y = Math.min(...ys);
    width = Math.max(...xs) - x;
    height = Math.max(...ys) - y;
    thickness = Number(bottomCandidates.find((border) => Number.isFinite(Number(border.thickness)))?.thickness);
  }

  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) return null;
  const internalVerticals = bottomCandidates.filter((border) => {
    const bx = Number(border.x);
    const bw = Math.abs(Number(border.width) || 0);
    const bh = Math.abs(Number(border.height) || 0);
    return bw === 0 && bh >= Math.max(20, height * 0.45) && bx > x + 8 && bx < x + width - 8;
  });
  const columnCount = Math.max(1, internalVerticals.length + 1);
  const start = Math.min(...bottomCandidates.map((border) => Number(border.commandStart)).filter(Number.isFinite));
  const end = Math.max(...bottomCandidates.map((border) => Number(border.commandEnd)).filter(Number.isFinite));
  return {
    x: Math.round(x),
    y: Math.round(y),
    width: Math.round(width),
    height: Math.round(height),
    borderThickness: clampStrokeThickness(Number.isFinite(thickness) ? thickness : 4),
    columnCount,
    columnLineThickness: clampStrokeThickness(Number.isFinite(thickness) ? thickness : 4),
    commandStart: Number.isFinite(start) ? start : null,
    commandEnd: Number.isFinite(end) ? end : null
  };
}

function conditionalRangesForTemplate(source) {
  const ranges = [];
  const stack = [];
  const pattern = /{{\s*(#if\s+([A-Za-z][A-Za-z0-9_]*)|\/if)\s*}}/g;
  for (const match of String(source || "").matchAll(pattern)) {
    const full = match[1] || "";
    if (full.startsWith("#if")) {
      stack.push({ field: match[2], start: match.index, open: match[0] });
    } else {
      const opened = stack.pop();
      if (opened) ranges.push({ ...opened, end: match.index + match[0].length, close: match[0] });
    }
  }
  return ranges;
}

function conditionalForIndex(ranges, index) {
  const containing = ranges
    .filter((range) => range.start <= index && range.end >= index)
    .sort((a, b) => (b.end - b.start) - (a.end - a.start));
  return containing[0] || null;
}

function tokenNameFromFieldToken(value) {
  const match = String(value || "").match(/{{\s*([A-Za-z][A-Za-z0-9_]*)\s*}}/);
  return match ? match[1] : "";
}

function fittedFieldKeyFromToken(tokenName) {
  return String(tokenName || "").endsWith("Text")
    ? String(tokenName).slice(0, -"Text".length)
    : "";
}

function resolveFieldFitNumeric(fieldFitDefinitions, fieldKey, property, fallback) {
  if (!fieldKey) return fallback;
  const definition = fieldFitDefinitions?.[fieldKey] || {};
  if (Number.isFinite(definition[property])) return definition[property];
  if (property === "fontHeight") {
    return Number(definition.small?.fontH ?? definition.medium?.fontH ?? definition.large?.fontH ?? definition.min?.fontH) || fallback;
  }
  if (property === "fontWidth") {
    return Number(definition.small?.fontW ?? definition.medium?.fontW ?? definition.large?.fontW ?? definition.min?.fontW) || fallback;
  }
  if (property === "fieldWidth") return Number(definition.boxWidth) || fallback;
  if (property === "maxLines") return Number(definition.maxLines) || fallback;
  return fallback;
}

function resolveFieldFitAlignment(fieldFitDefinitions, fieldKey, fallback) {
  if (!fieldKey) return fallback;
  return fieldFitDefinitions?.[fieldKey]?.alignment || fallback;
}

function mergeStagedGeometryIntoParsedGeometry(parsedGeometry, savedProfileOverrides = {}) {
  const staged = savedProfileOverrides.fieldGeometryOverrides || {};
  const output = deepCloneJson(parsedGeometry);
  output.fields = output.fields.map((field) => ({
    ...field,
    staged: isPlainObject(staged[field.tokenName]) ? staged[field.tokenName] : null
  }));
  output.qr = { ...(output.qr || {}), staged: savedProfileOverrides.qr || null };
  output.logo = { ...(output.logo || {}), staged: savedProfileOverrides.logo || null };
  output.label = { ...(output.label || {}), staged: {
    labelWidthDots: savedProfileOverrides.labelWidthDots,
    labelHeightDots: savedProfileOverrides.labelHeightDots,
    labelHomeX: savedProfileOverrides.labelHomeX,
    labelHomeY: savedProfileOverrides.labelHomeY
  }};
  output.bottomGrid = { ...(output.bottomGrid || {}), staged: savedProfileOverrides.bottomGrid || null };
  return output;
}

function parseTemplateLabSourceGeometry(sourceTemplate, options = {}) {
  const source = String(sourceTemplate || "");
  const commands = parseZplTemplateCommandsWithPositions(source);
  const conditionalRanges = conditionalRangesForTemplate(source);
  const sourceFieldFitDefinitions = getFittedFieldDefinitions(parseTemplateLabFieldFitDefinitionsFromSource(source));
  const warnings = [];
  const fields = [];
  const borders = [];
  const label = {
    labelWidthDots: null,
    labelHeightDots: null,
    labelHomeX: null,
    labelHomeY: null,
    labelShiftX: null,
    labelShiftY: null,
    printQuantity: null
  };
  let activePosition = null;
  let activeFont = null;
  let activeFieldBlock = null;
  let qr = null;
  let logo = null;

  for (let index = 0; index < commands.length; index += 1) {
    const command = commands[index];
    if (command.code === "PW") label.labelWidthDots = numberOrNull(command.params);
    else if (command.code === "LL") label.labelHeightDots = numberOrNull(command.params);
    else if (command.code === "LH") {
      const parts = zplParamParts(command.params);
      label.labelHomeX = numberOrNull(parts[0]);
      label.labelHomeY = numberOrNull(parts[1]);
    } else if (command.code === "LS") {
      label.labelShiftX = numberOrNull(command.params);
    } else if (command.code === "LT") {
      label.labelShiftY = numberOrNull(command.params);
    } else if (command.code === "PQ") {
      label.printQuantity = numberOrNull(zplParamParts(command.params)[0]);
    } else if (["FO", "FT"].includes(command.code)) {
      activePosition = parsePositionCommand(command);
    } else if (command.code === "A0") {
      activeFont = parseFontCommand(command);
    } else if (command.code === "FB") {
      activeFieldBlock = parseFieldBlockCommand(command);
    } else if (command.code === "GB") {
      const border = parseGraphicBoxCommand(command, activePosition);
      if (border) borders.push(border);
    } else if (command.code === "BQ") {
      const nextFd = commands.slice(index + 1).find((item) => item.code === "FD" || item.code === "FS");
      const parts = zplParamParts(command.params);
      if (nextFd?.code === "FD" && /{{\s*lotNumber\s*}}|LA,\s*{{/i.test(nextFd.params)) {
        qr = {
          x: activePosition?.x ?? null,
          y: activePosition?.y ?? null,
          originCommand: activePosition?.command || "FO",
          magnification: numberOrNull(parts[2]),
          model: parts[1] || "",
          payload: nextFd.params,
          commandStart: activePosition?.commandStart ?? command.start,
          commandEnd: nextFd.end,
          raw: source.slice(activePosition?.commandStart ?? command.start, nextFd.end)
        };
      }
    } else if (command.code === "GF") {
      const metrics = getGfaCommandMetrics(command.raw);
      logo = {
        x: activePosition?.x ?? null,
        y: activePosition?.y ?? null,
        originCommand: activePosition?.command || "FO",
        widthDots: metrics?.width || null,
        heightDots: metrics?.height || null,
        payloadBytes: metrics?.payloadBytes || null,
        commandStart: activePosition?.commandStart ?? command.start,
        commandEnd: command.end,
        raw: source.slice(activePosition?.commandStart ?? command.start, command.end)
      };
    } else if (command.code === "FD") {
      const tokenMatches = Array.from(command.params.matchAll(/{{\s*([A-Za-z][A-Za-z0-9_]*)\s*}}/g));
      for (const tokenMatch of tokenMatches) {
        const tokenName = tokenMatch[1];
        const fieldKey = fittedFieldKeyFromToken(tokenName);
        const conditional = conditionalForIndex(conditionalRanges, command.paramsStart + tokenMatch.index);
        const fontHeight = activeFont?.fontHeight ?? resolveFieldFitNumeric(sourceFieldFitDefinitions, fieldKey, "fontHeight", null);
        const fontWidth = activeFont?.fontWidth ?? resolveFieldFitNumeric(sourceFieldFitDefinitions, fieldKey, "fontWidth", null);
        const fieldWidth = activeFieldBlock?.width ?? resolveFieldFitNumeric(sourceFieldFitDefinitions, fieldKey, "fieldWidth", null);
        const maxLines = activeFieldBlock?.maxLines ?? resolveFieldFitNumeric(sourceFieldFitDefinitions, fieldKey, "maxLines", null);
        const alignment = activeFieldBlock?.alignment || resolveFieldFitAlignment(sourceFieldFitDefinitions, fieldKey, null);
        const field = {
          fieldKey: fieldKey || tokenName,
          tokenName,
          token: tokenMatch[0],
          originCommand: activePosition?.command || null,
          x: activePosition?.x ?? null,
          y: activePosition?.y ?? null,
          fontHeight,
          fontWidth,
          fontHeightRaw: activeFont?.fontHeightRaw || "",
          fontWidthRaw: activeFont?.fontWidthRaw || "",
          fieldWidth,
          maxLines,
          lineSpacing: activeFieldBlock?.lineSpacing ?? null,
          alignment,
          fieldWidthRaw: activeFieldBlock?.widthRaw || "",
          maxLinesRaw: activeFieldBlock?.maxLinesRaw || "",
          conditional: conditional ? { field: conditional.field, start: conditional.start, end: conditional.end } : null,
          commandStart: activePosition?.commandStart ?? command.start,
          commandEnd: command.end,
          rawSegment: source.slice(Math.max(0, (activePosition?.commandStart ?? command.start) - 80), Math.min(source.length, command.end + 120)),
          source: "template"
        };
        if (!activePosition) warnings.push({ tokenName, message: `No preceding ^FO/^FT found for {{${tokenName}}}.` });
        if (!activeFont) warnings.push({ tokenName, message: `No active ^A0 font found for {{${tokenName}}}.` });
        fields.push(field);
      }
    } else if (command.code === "FS") {
      activeFieldBlock = null;
    }
  }

  const bottomGrid = detectBottomGridFromBorders(borders, label);
  const bottomGridStart = Number(bottomGrid?.commandStart);
  const bottomGridEnd = Number(bottomGrid?.commandEnd);
  for (const field of fields) {
    const related = borders
      .filter((border) => {
        const commandStart = Number(border.commandStart);
        if (Number.isFinite(bottomGridStart) && Number.isFinite(bottomGridEnd) && commandStart >= bottomGridStart && commandStart <= bottomGridEnd) return false;
        return Math.abs(border.commandStart - field.commandStart) <= 260 || Math.abs(border.commandStart - field.commandEnd) <= 260;
      })
      .sort((a, b) => Math.min(Math.abs(a.commandStart - field.commandStart), Math.abs(a.commandStart - field.commandEnd)) -
        Math.min(Math.abs(b.commandStart - field.commandStart), Math.abs(b.commandStart - field.commandEnd)))[0];
    field.border = related ? {
      x: related.x,
      y: related.y,
      width: related.width,
      height: related.height,
      thickness: related.thickness,
      commandStart: related.commandStart,
      raw: related.raw
    } : null;
  }

  return {
    label,
    fields,
    qr,
    logo,
    borders,
    bottomGrid,
    warnings,
    sourceFieldFitDefinitions,
    parsedAt: isoNow(),
    parser: "template-source-v1",
    ...options
  };
}

function getTemplateLabTemplateGeometryPayload(input = {}) {
  const selected = normalizeTemplateLabTemplateName(input.template || input.templateName);
  const profileKey = trimString(input.profileKey || input.profile || selected.definition.defaultProfileKey).toUpperCase();
  const sourceTemplate = loadZplTemplate(selected.templatePath);
  const savedProfileOverrides = getSavedTemplateLabProfileOverrides(profileKey);
  const parsed = parseTemplateLabSourceGeometry(sourceTemplate);
  const merged = mergeStagedGeometryIntoParsedGeometry(parsed, savedProfileOverrides);
  return {
    ok: true,
    templateName: selected.name,
    templatePath: selected.templatePath,
    profileKey,
    parsedAt: parsed.parsedAt,
    fields: merged.fields,
    qr: merged.qr,
    logo: merged.logo,
    borders: merged.borders,
    bottomGrid: merged.bottomGrid,
    label: merged.label,
    savedProfileOverrides,
    warnings: merged.warnings,
    sourceFieldFitDefinitions: merged.sourceFieldFitDefinitions
  };
}

function buildTemplateLabData(input = {}, templateDefinition = {}) {
  const lotNumber = trimString(input.lotNumber) || "PT000086";
  const boxNumber = trimString(input.boxNumber || input.box || input.firstBox) || "52";
  const resolvedRfid = trimString(input.rfid) || `${lotNumber}-B${pad2(boxNumber)}`;

  return {
    lotNumber,
    boxNumber,
    rfid: resolvedRfid,
    pounds: trimString(input.pounds) || "1200",
    materialType: trimString(input.materialType || input.material || input.type) || "PP",
    color: trimString(input.color) || "Black",
    po: trimString(input.po || input.purchaseOrder) || "PO12345",
    productCode: trimString(input.productCode || input.prodnum) || "PROD001",
    productName: trimString(input.productName || input.product) || "Template Lab Product",
    productDescription: trimString(input.productDescription || input.prodname || input.product) || "Template Lab Product",
    tolling: trimString(input.tolling) || "Tolling",
    erp: trimString(input.erp) || "LAB",
    qrData: lotNumber,
    machine: trimString(input.machine) || "P3 EXT",
    company: trimString(input.company),
    labelType: trimString(input.labelType),
    sampleType: trimString(input.sampleType),
    sampleTime: trimString(input.sampleTime || input.sampleLabel || input.box) || boxNumber,
    sampleLabel: trimString(input.sampleLabel || input.box) || boxNumber,
    frequencyCheck: trimString(input.frequencyCheck || input.pounds) || "5000",
    printedDate: trimString(input.printedDate) || new Date().toLocaleDateString("en-US")
  };
}

function replaceLastCoordinateBeforeToken(source, token, position = {}) {
  const text = String(source || "");
  const tokenIndex = text.indexOf(token);
  if (tokenIndex < 0) return text;
  const searchStart = Math.max(0, tokenIndex - 260);
  const beforeToken = text.slice(searchStart, tokenIndex);
  const matches = Array.from(beforeToken.matchAll(/\^(FO|FT)(-?\d+),(-?\d+)/g));
  if (!matches.length) return text;
  const match = matches[matches.length - 1];
  const absoluteStart = searchStart + match.index;
  const absoluteEnd = absoluteStart + match[0].length;
  const x = Number.isFinite(position.x) ? position.x : Number(match[2]);
  const y = Number.isFinite(position.y) ? position.y : Number(match[3]);
  return `${text.slice(0, absoluteStart)}^${match[1]}${Math.round(x)},${Math.round(y)}${text.slice(absoluteEnd)}`;
}

function applyFieldPositionOverridesToTemplateSource(templateText, fieldPositionOverrides = {}) {
  const tokenByField = {
    color: "{{colorText}}",
    colorSmall: "{{colorSmallText}}",
    materialType: "{{materialTypeText}}",
    materialTypeSmall: "{{materialTypeSmallText}}",
    tolling: "{{tollingText}}",
    productDescription: "{{productDescriptionText}}"
  };
  let output = String(templateText || "");
  for (const [field, position] of Object.entries(fieldPositionOverrides || {})) {
    if (!tokenByField[field] || !isPlainObject(position)) continue;
    output = replaceLastCoordinateBeforeToken(output, tokenByField[field], position);
  }
  return output;
}

function sourceWindowAroundToken(source, tokenName, before = 420, after = 220) {
  const text = String(source || "");
  const pattern = new RegExp(`\\{\\{\\s*${String(tokenName || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*\\}\\}`, "g");
  const matches = Array.from(text.matchAll(pattern));
  if (!matches.length) return null;
  const match = matches.find((candidate) => {
    const preceding = text.slice(Math.max(0, candidate.index - 140), candidate.index);
    return !/\^BQ|\^RF/.test(preceding);
  }) || matches[0];
  return {
    tokenIndex: match.index,
    start: Math.max(0, match.index - before),
    end: Math.min(text.length, match.index + match[0].length + after)
  };
}

function replaceLastInSlice(text, sliceStart, sliceEnd, pattern, replacer) {
  const source = String(text || "");
  const slice = source.slice(sliceStart, sliceEnd);
  const matches = Array.from(slice.matchAll(pattern));
  if (!matches.length) return source;
  const match = matches[matches.length - 1];
  const absoluteStart = sliceStart + match.index;
  const absoluteEnd = absoluteStart + match[0].length;
  return `${source.slice(0, absoluteStart)}${replacer(match)}${source.slice(absoluteEnd)}`;
}

function replaceFirstInSlice(text, sliceStart, sliceEnd, pattern, replacer) {
  const source = String(text || "");
  const slice = source.slice(sliceStart, sliceEnd);
  const match = slice.match(pattern);
  if (!match || match.index === undefined) return source;
  const absoluteStart = sliceStart + match.index;
  const absoluteEnd = absoluteStart + match[0].length;
  return `${source.slice(0, absoluteStart)}${replacer(match)}${source.slice(absoluteEnd)}`;
}

function coerceOriginCommand(value, fallback = "FO") {
  const command = String(value || fallback || "FO").trim().toUpperCase();
  return command === "FT" ? "FT" : "FO";
}

function applySingleFieldGeometryOverride(source, tokenName, geometry = {}) {
  if (!tokenName || !isPlainObject(geometry)) return source;
  let output = String(source || "");
  const window = sourceWindowAroundToken(output, tokenName);
  if (!window) return output;

  if (Number.isFinite(geometry.x) || Number.isFinite(geometry.y) || geometry.originCommand || geometry.useOrigin) {
    output = replaceLastInSlice(output, window.start, window.tokenIndex, /\^(FO|FT)(-?\d+),(-?\d+)/g, (match) => {
      const command = coerceOriginCommand(geometry.originCommand || geometry.useOrigin || match[1], match[1]);
      const x = Number.isFinite(geometry.x) ? Math.round(geometry.x) : Number(match[2]);
      const y = Number.isFinite(geometry.y) ? Math.round(geometry.y) : Number(match[3]);
      return `^${command}${x},${y}`;
    });
  }

  const fontHeight = Number(geometry.fontHeight);
  const fontWidth = Number(geometry.fontWidth);
  if (Number.isFinite(fontHeight) || Number.isFinite(fontWidth)) {
    const refreshed = sourceWindowAroundToken(output, tokenName);
    if (refreshed) {
      output = replaceLastInSlice(output, refreshed.start, refreshed.tokenIndex, /\^A0([NIRB]),([^,\^\r\n]+),([^,\^\r\n]+)/g, (match) => {
        const height = Number.isFinite(fontHeight) && !/{{/.test(match[2]) ? Math.round(fontHeight) : match[2];
        const width = Number.isFinite(fontWidth) && !/{{/.test(match[3]) ? Math.round(fontWidth) : match[3];
        return `^A0${match[1]},${height},${width}`;
      });
    }
  }

  const fieldWidth = Number(geometry.fieldWidth ?? geometry.boxWidth);
  const maxLines = Number(geometry.maxLines);
  const lineSpacing = Number(geometry.lineSpacing);
  const alignment = trimString(geometry.alignment).toUpperCase();
  const hasFieldBlockEdit = Number.isFinite(fieldWidth) || Number.isFinite(maxLines) || Number.isFinite(lineSpacing) || ["L", "C", "R", "J"].includes(alignment);
  if (hasFieldBlockEdit) {
    const refreshed = sourceWindowAroundToken(output, tokenName);
    if (refreshed) {
      output = replaceLastInSlice(output, refreshed.start, refreshed.tokenIndex, /\^FB([^,\^\r\n]+),([^,\^\r\n]+),([^,\^\r\n]*),([^,\^\r\n]*),([^,\^\r\n]*)/g, (match) => {
        const width = Number.isFinite(fieldWidth) && !/{{/.test(match[1]) ? Math.round(fieldWidth) : match[1];
        const lines = Number.isFinite(maxLines) && !/{{/.test(match[2]) ? Math.round(maxLines) : match[2];
        const spacing = Number.isFinite(lineSpacing) ? Math.round(lineSpacing) : (match[3] || "0");
        const align = ["L", "C", "R", "J"].includes(alignment) && !/{{/.test(match[4]) ? alignment : (match[4] || "L");
        const hanging = match[5] || "0";
        return `^FB${width},${lines},${spacing},${align},${hanging}`;
      });
    }
  }

  const border = isPlainObject(geometry.border) ? geometry.border : {};
  const borderThickness = Number(border.thickness ?? geometry.borderThickness);
  const borderWidth = Number(border.width);
  const borderHeight = Number(border.height);
  const hasBorderEdit = Number.isFinite(borderThickness) || Number.isFinite(borderWidth) || Number.isFinite(borderHeight);
  if (hasBorderEdit) {
    const refreshed = sourceWindowAroundToken(output, tokenName, 300, 300);
    if (refreshed) {
      output = replaceFirstInSlice(output, refreshed.start, refreshed.end, /\^GB(-?\d+),(-?\d+),(-?\d+)((?:,[^^\r\n]*)?)/, (match) => {
        const width = Number.isFinite(borderWidth) ? Math.round(borderWidth) : match[1];
        const height = Number.isFinite(borderHeight) ? Math.round(borderHeight) : match[2];
        const thickness = Number.isFinite(borderThickness) ? clampStrokeThickness(borderThickness) : match[3];
        return `^GB${width},${height},${thickness}${match[4] || ""}`;
      });
    }
  }

  return output;
}

function fieldFitDefinitionFromGeometry(tokenName, geometry = {}) {
  const fieldKey = fittedFieldKeyFromToken(tokenName);
  if (!fieldKey || !isPlainObject(geometry)) return null;
  const definition = {};
  if (Number.isFinite(geometry.fieldWidth ?? geometry.boxWidth)) definition.boxWidth = Math.round(Number(geometry.fieldWidth ?? geometry.boxWidth));
  if (Number.isFinite(geometry.maxLines)) definition.maxLines = Math.max(1, Math.round(Number(geometry.maxLines)));
  if (Number.isFinite(geometry.fontHeight)) definition.fontHeight = Math.max(1, Math.round(Number(geometry.fontHeight)));
  if (Number.isFinite(geometry.fontWidth)) definition.fontWidth = Math.max(1, Math.round(Number(geometry.fontWidth)));
  if (Number.isFinite(geometry.borderThickness)) definition.borderThickness = clampStrokeThickness(geometry.borderThickness);
  const alignment = trimString(geometry.alignment).toUpperCase();
  if (["L", "C", "R", "J"].includes(alignment)) definition.alignment = alignment;
  return Object.keys(definition).length ? { fieldKey, definition } : null;
}

function buildFieldFitDefinitionsFromGeometry(fieldGeometryOverrides = {}) {
  const definitions = {};
  for (const [tokenName, geometry] of Object.entries(fieldGeometryOverrides || {})) {
    const entry = fieldFitDefinitionFromGeometry(tokenName, geometry);
    if (entry) definitions[entry.fieldKey] = deepMergePlainObjects(definitions[entry.fieldKey] || {}, entry.definition);
  }
  return definitions;
}

function bottomGridProfileHasControls(profile = {}) {
  return isPlainObject(profile?.bottomGrid) && Object.keys(normalizeBottomGridProfile(profile.bottomGrid)).length > 0;
}

function buildBottomGridZpl(bottomGrid = {}) {
  const grid = normalizeBottomGridProfile(bottomGrid);
  const x = Number.isFinite(grid.x) ? grid.x : 10;
  const y = Number.isFinite(grid.y) ? grid.y : 986;
  const width = Number.isFinite(grid.width) ? grid.width : 737;
  const height = Number.isFinite(grid.height) ? grid.height : 110;
  const columnCount = Number.isFinite(grid.columnCount) ? Math.max(1, grid.columnCount) : 5;
  const borderThickness = Number.isFinite(grid.borderThickness) ? clampStrokeThickness(grid.borderThickness) : 4;
  const columnLineThickness = Number.isFinite(grid.columnLineThickness) ? clampStrokeThickness(grid.columnLineThickness) : borderThickness;
  const commands = ["^FX Template Lab Bottom Grid/Footer Row"];
  if (borderThickness > 0) {
    commands.push(`^FO${Math.round(x)},${Math.round(y)}`);
    commands.push(`^GB${Math.round(width)},${Math.round(height)},${borderThickness}^FS`);
  }
  if (columnLineThickness > 0 && columnCount > 1) {
    for (let column = 1; column < columnCount; column += 1) {
      const lineX = Math.round(x + ((width * column) / columnCount));
      commands.push(`^FO${lineX},${Math.round(y)}`);
      commands.push(`^GB0,${Math.round(height)},${columnLineThickness}^FS`);
    }
  }
  return commands.join("\n");
}

function zplRegionEndAfterFieldSeparator(source, end) {
  const text = String(source || "");
  let outputEnd = Math.max(0, Math.min(text.length, Number(end) || 0));
  const after = text.slice(outputEnd);
  const match = after.match(/^\^FS(?:\r?\n)?/);
  if (match) outputEnd += match[0].length;
  return outputEnd;
}

function applyBottomGridOverrideToZpl(zpl, profile = {}) {
  if (!bottomGridProfileHasControls(profile)) return String(zpl || "");
  const source = String(zpl || "");
  const parsed = parseTemplateLabSourceGeometry(source);
  const detected = parsed.bottomGrid || {};
  const override = normalizeBottomGridProfile(profile.bottomGrid || {});
  const bottomGrid = {
    x: override.x ?? detected.x ?? 10,
    y: override.y ?? detected.y ?? 986,
    width: override.width ?? detected.width ?? 737,
    height: override.height ?? detected.height ?? 110,
    borderThickness: override.borderThickness ?? detected.borderThickness ?? 4,
    columnCount: override.columnCount ?? detected.columnCount ?? 5,
    columnLineThickness: override.columnLineThickness ?? detected.columnLineThickness ?? override.borderThickness ?? detected.borderThickness ?? 4
  };
  const replacement = `${buildBottomGridZpl(bottomGrid)}\n`;
  if (Number.isFinite(Number(detected.commandStart)) && Number.isFinite(Number(detected.commandEnd))) {
    const start = Math.max(0, Number(detected.commandStart));
    const end = zplRegionEndAfterFieldSeparator(source, Number(detected.commandEnd));
    return `${source.slice(0, start)}${replacement}${source.slice(end)}`;
  }
  const insertAt = source.search(/\^PQ|\^XZ/);
  if (insertAt >= 0) return `${source.slice(0, insertAt)}${replacement}${source.slice(insertAt)}`;
  return `${source}\n${replacement}`;
}

function applyFieldGeometryOverridesToTemplateSource(templateText, fieldGeometryOverrides = {}) {
  let output = String(templateText || "");
  for (const [tokenName, geometry] of Object.entries(fieldGeometryOverrides || {})) {
    output = applySingleFieldGeometryOverride(output, tokenName, geometry);
  }
  return output;
}

function roundedScaled(value, scale, offset = 0, options = {}) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return value;
  if (options.preserveZero && numeric === 0) return 0;
  let output = Math.round((numeric * scale) + offset);
  if (Number.isFinite(options.min)) output = Math.max(options.min, output);
  if (Number.isFinite(options.max)) output = Math.min(options.max, output);
  return output;
}

function clampStrokeThickness(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 1;
  return Math.max(0, Math.min(20, Math.round(number)));
}

function isFilledGraphicBox(width, height, thickness) {
  const minDimension = Math.min(Math.abs(Number(width) || 0), Math.abs(Number(height) || 0));
  return minDimension > 0 && Number(thickness) >= minDimension * 0.45;
}

function upsertZplScalarCommand(zpl, code, value) {
  if (!Number.isFinite(value)) return zpl;
  const normalizedValue = Math.round(value);
  const pattern = new RegExp(`\\^${code}-?\\d+`);
  if (pattern.test(zpl)) return zpl.replace(pattern, `^${code}${normalizedValue}`);
  return String(zpl || "").replace(/\^XA/, `^XA\n^${code}${normalizedValue}`);
}

function upsertZplPairCommand(zpl, code, x, y) {
  if (!Number.isFinite(x) && !Number.isFinite(y)) return zpl;
  const current = String(zpl || "").match(new RegExp(`\\^${code}(-?\\d+),(-?\\d+)`));
  const resolvedX = Number.isFinite(x) ? Math.round(x) : Number(current?.[1] || 0);
  const resolvedY = Number.isFinite(y) ? Math.round(y) : Number(current?.[2] || 0);
  const replacement = `^${code}${resolvedX},${resolvedY}`;
  if (current) return String(zpl || "").replace(new RegExp(`\\^${code}-?\\d+,-?\\d+`), replacement);
  return String(zpl || "").replace(/\^XA/, `^XA\n${replacement}`);
}

function applyTemplateLabLabelGeometryCommands(zpl, profile = {}) {
  const normalized = normalizeTemplateLabProfileGeometry(profile);
  let output = String(zpl || "");
  output = upsertZplScalarCommand(output, "PW", normalized.labelWidthDots);
  output = upsertZplScalarCommand(output, "LL", normalized.labelHeightDots);
  output = upsertZplPairCommand(output, "LH", normalized.labelHomeX, normalized.labelHomeY);
  output = upsertZplScalarCommand(output, "LS", normalized.labelShiftX);
  output = upsertZplScalarCommand(output, "LT", normalized.labelShiftY);
  return output;
}

function scaleGraphicBox(match, width, height, thickness, rest, profile) {
  const scaleX = profile.globalScaleX;
  const scaleY = profile.globalScaleY;
  const averageScale = (scaleX + scaleY) / 2;
  const scaledWidth = roundedScaled(width, scaleX, 0, { preserveZero: true });
  const scaledHeight = roundedScaled(height, scaleY, 0, { preserveZero: true });
  const originalThickness = Number(thickness);
  const filled = isFilledGraphicBox(width, height, originalThickness);
  let finalThickness;

  if (filled) {
    finalThickness = Math.max(1, Math.round(Math.min(Math.abs(scaledWidth), Math.abs(scaledHeight))));
  } else if (Number.isFinite(profile.borderThickness)) {
    finalThickness = clampStrokeThickness(profile.borderThickness);
  } else if (profile.scaleBorderThickness) {
    finalThickness = clampStrokeThickness(originalThickness * averageScale);
  } else {
    finalThickness = clampStrokeThickness(originalThickness);
  }

  return `^GB${scaledWidth},${scaledHeight},${finalThickness}${rest || ""}`;
}

function scaleB3Command(command, params, scaleY) {
  const parts = String(params || "").split(",");
  if (parts.length >= 3 && Number.isFinite(Number(parts[2]))) {
    parts[2] = String(roundedScaled(parts[2], scaleY, 0, { min: 1 }));
  }
  return `${command}${parts.join(",")}`;
}

function scaleByCommand(_match, width, ratio, height, profile) {
  const widthNumber = Number(width);
  const scaledWidth = Number.isFinite(widthNumber) ? roundedScaled(widthNumber, profile.globalScaleX, 0, { min: 1 }) : width;
  const output = [`^BY${scaledWidth}`];
  if (ratio !== undefined) output.push(`,${ratio}`);
  if (height !== undefined) {
    const scaledHeight = Number.isFinite(Number(height))
      ? roundedScaled(height, profile.globalScaleY, 0, { min: 1 })
      : height;
    output.push(`,${scaledHeight}`);
  }
  return output.join("");
}

function applyGlobalTemplateLabTransform(renderedZpl, profile = {}) {
  const normalized = normalizeTemplateLabProfileGeometry(profile);
  const scaleX = normalized.globalScaleX;
  const scaleY = normalized.globalScaleY;
  const offsetX = normalized.globalOffsetX;
  const offsetY = normalized.globalOffsetY;
  const averageScale = (scaleX + scaleY) / 2;

  return applyTemplateLabLabelGeometryCommands(String(renderedZpl || ""), normalized)
    .replace(/\^(FO|FT)(-?\d+),(-?\d+)/g, (_match, command, x, y) =>
      `^${command}${roundedScaled(x, scaleX, offsetX)},${roundedScaled(y, scaleY, offsetY)}`
    )
    .replace(/\^GB(-?\d+),(-?\d+),(-?\d+)((?:,[^^\r\n]*)?)/g, (match, width, height, thickness, rest) =>
      scaleGraphicBox(match, width, height, thickness, rest, normalized)
    )
    .replace(/\^A0([NIRB]),(\d+),(\d+)/g, (_match, orientation, height, width) =>
      `^A0${orientation},${roundedScaled(height, scaleY, 0, { min: 1 })},${roundedScaled(width, scaleX, 0, { min: 1 })}`
    )
    .replace(/\^FB(\d+),/g, (_match, width) =>
      `^FB${roundedScaled(width, scaleX, 0, { min: 1 })},`
    )
    .replace(/\^BQN,([^,\^]+),(\d+)/g, (_match, orientation, magnification) =>
      `^BQN,${orientation},${roundedScaled(magnification, averageScale, 0, { min: 1, max: 20 })}`
    )
    .replace(/\^B3([^,\^]*(?:,[^,\^]*){0,4})/g, (match, params) =>
      scaleB3Command("^B3", params, scaleY) || match
    )
    .replace(/\^BY(\d+)(?:,([^,\^]*))?(?:,(-?\d+))?/g, (match, width, ratio, height) =>
      scaleByCommand(match, width, ratio, height, normalized)
    )
    .replace(/\^GFA,\d+,\d+,\d+,[0-9A-Fa-f]+/g, (gfaCommand) => {
      const metrics = getGfaCommandMetrics(gfaCommand);
      if (!metrics) return gfaCommand;
      return scaleGfaCommand(gfaCommand, Math.max(1, Math.round(metrics.width * scaleX)), Math.max(1, Math.round(metrics.height * scaleY)));
    });
}

function applyQrOverrideToRenderedZpl(renderedZpl, profile = {}) {
  const qr = profile.qr || {};
  return String(renderedZpl || "").replace(
    /\^FO(-?\d+),(-?\d+)\r?\n\^BQN,2,(\d+)\^FDLA,([^^]+)\^FS/,
    (match, currentX, currentY, currentMagnification, payload) => {
      const x = Number.isFinite(qr.x) ? qr.x : Number(currentX);
      const y = Number.isFinite(qr.y) ? qr.y : Number(currentY);
      const magnification = Number.isFinite(qr.magnification) ? qr.magnification : Number(currentMagnification);
      return `^FO${Math.round(x)},${Math.round(y)}\n^BQN,2,${Math.round(magnification)}^FDLA,${payload}^FS`;
    }
  );
}

function readGfaBits(bytesPerRow, data) {
  const bytes = Buffer.from(String(data || ""), "hex");
  const height = Math.floor(bytes.length / bytesPerRow);
  const width = bytesPerRow * 8;
  const bits = [];
  for (let y = 0; y < height; y++) {
    const row = [];
    for (let x = 0; x < width; x++) {
      const byte = bytes[(y * bytesPerRow) + Math.floor(x / 8)];
      row.push((byte & (1 << (7 - (x % 8)))) ? 1 : 0);
    }
    bits.push(row);
  }
  return { bits, width, height };
}

function buildGfaFromBits(bits, width, height) {
  const bytesPerRow = Math.ceil(width / 8);
  const bytes = [];
  for (let y = 0; y < height; y++) {
    for (let byteX = 0; byteX < bytesPerRow; byteX++) {
      let byte = 0;
      for (let bit = 0; bit < 8; bit++) {
        const x = (byteX * 8) + bit;
        if (x < width && bits[y]?.[x]) {
          byte |= 1 << (7 - bit);
        }
      }
      bytes.push(byte);
    }
  }
  const total = bytesPerRow * height;
  const data = Buffer.from(bytes).toString("hex").toUpperCase();
  return `^GFA,${total},${total},${bytesPerRow},${data}`;
}

function normalizeGfaCommand(value) {
  const match = String(value || "").match(/\^GFA,\d+,\d+,\d+,[0-9A-Fa-f]+/);
  return match ? match[0] : "";
}

function getGfaCommandMetrics(gfaCommand) {
  const match = String(gfaCommand || "").match(/\^GFA,(\d+),(\d+),(\d+),([0-9A-Fa-f]+)/);
  if (!match) return null;
  const total = Number(match[1]);
  const bytesPerRow = Number(match[3]);
  if (!Number.isFinite(total) || !Number.isFinite(bytesPerRow) || bytesPerRow <= 0) return null;
  return {
    total,
    bytesPerRow,
    width: bytesPerRow * 8,
    height: Math.max(1, Math.floor(total / bytesPerRow)),
    payloadBytes: Math.floor(String(match[4] || "").length / 2)
  };
}

function scaleGfaCommand(gfaCommand, widthDots, heightDots) {
  const match = String(gfaCommand || "").match(/\^GFA,(\d+),(\d+),(\d+),([0-9A-Fa-f]+)/);
  if (!match) return gfaCommand;
  const bytesPerRow = Number(match[3]);
  const { bits, width, height } = readGfaBits(bytesPerRow, match[4]);
  const targetWidth = Math.max(1, Math.round(widthDots || width));
  const targetHeight = Math.max(1, Math.round(heightDots || height));
  if (targetWidth === width && targetHeight === height) return gfaCommand;

  const scaledBits = [];
  for (let y = 0; y < targetHeight; y++) {
    const sourceY = Math.min(height - 1, Math.floor((y * height) / targetHeight));
    const row = [];
    for (let x = 0; x < targetWidth; x++) {
      const sourceX = Math.min(width - 1, Math.floor((x * width) / targetWidth));
      row.push(bits[sourceY]?.[sourceX] ? 1 : 0);
    }
    scaledBits.push(row);
  }
  return buildGfaFromBits(scaledBits, targetWidth, targetHeight);
}

function resolveTemplateLabAssetPath(assetName) {
  const safeName = sanitizeFilename(assetName);
  if (!safeName) throw httpError(400, "VALIDATION_ERROR", "assetName is required.");
  const assetDir = path.resolve(ZPL_TEMPLATE_LAB_ASSET_DIR);
  const assetPath = path.resolve(assetDir, safeName);
  if (assetPath !== assetDir && !assetPath.startsWith(`${assetDir}${path.sep}`)) {
    throw httpError(400, "VALIDATION_ERROR", "Invalid logo asset path.");
  }
  return { safeName, assetDir, assetPath };
}

function paethPredictor(left, up, upLeft) {
  const p = left + up - upLeft;
  const pa = Math.abs(p - left);
  const pb = Math.abs(p - up);
  const pc = Math.abs(p - upLeft);
  if (pa <= pb && pa <= pc) return left;
  return pb <= pc ? up : upLeft;
}

function parsePngToRgba(buffer) {
  const bytes = Buffer.from(buffer || []);
  const signature = "89504e470d0a1a0a";
  if (bytes.length < 24 || bytes.subarray(0, 8).toString("hex") !== signature) {
    throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Logo upload must be a PNG image.");
  }

  let offset = 8;
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = 0;
  let interlace = 0;
  let palette = null;
  let transparency = null;
  const idatChunks = [];

  while (offset + 12 <= bytes.length) {
    const length = bytes.readUInt32BE(offset);
    const type = bytes.subarray(offset + 4, offset + 8).toString("ascii");
    const data = bytes.subarray(offset + 8, offset + 8 + length);
    offset += 12 + length;
    if (type === "IHDR") {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      bitDepth = data[8];
      colorType = data[9];
      interlace = data[12];
    } else if (type === "PLTE") {
      palette = data;
    } else if (type === "tRNS") {
      transparency = data;
    } else if (type === "IDAT") {
      idatChunks.push(data);
    } else if (type === "IEND") {
      break;
    }
  }

  if (!width || !height || bitDepth !== 8 || interlace !== 0) {
    throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Logo PNG must be non-interlaced 8-bit PNG.");
  }

  const channelsByColorType = { 0: 1, 2: 3, 3: 1, 4: 2, 6: 4 };
  const channels = channelsByColorType[colorType];
  if (!channels) throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Logo PNG color type is not supported.");
  const inflated = zlib.inflateSync(Buffer.concat(idatChunks));
  const stride = width * channels;
  const raw = Buffer.alloc(height * stride);
  let inputOffset = 0;

  for (let y = 0; y < height; y += 1) {
    const filter = inflated[inputOffset];
    inputOffset += 1;
    const previousRow = y > 0 ? raw.subarray((y - 1) * stride, y * stride) : null;
    const row = raw.subarray(y * stride, (y + 1) * stride);
    for (let x = 0; x < stride; x += 1) {
      const current = inflated[inputOffset + x];
      const left = x >= channels ? row[x - channels] : 0;
      const up = previousRow ? previousRow[x] : 0;
      const upLeft = previousRow && x >= channels ? previousRow[x - channels] : 0;
      if (filter === 0) row[x] = current;
      else if (filter === 1) row[x] = (current + left) & 0xff;
      else if (filter === 2) row[x] = (current + up) & 0xff;
      else if (filter === 3) row[x] = (current + Math.floor((left + up) / 2)) & 0xff;
      else if (filter === 4) row[x] = (current + paethPredictor(left, up, upLeft)) & 0xff;
      else throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Logo PNG uses an unsupported scanline filter.");
    }
    inputOffset += stride;
  }

  const rgba = Buffer.alloc(width * height * 4);
  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      const sourceIndex = (y * stride) + (x * channels);
      const targetIndex = ((y * width) + x) * 4;
      if (colorType === 0) {
        const value = raw[sourceIndex];
        rgba[targetIndex] = value;
        rgba[targetIndex + 1] = value;
        rgba[targetIndex + 2] = value;
        rgba[targetIndex + 3] = 255;
      } else if (colorType === 2) {
        rgba[targetIndex] = raw[sourceIndex];
        rgba[targetIndex + 1] = raw[sourceIndex + 1];
        rgba[targetIndex + 2] = raw[sourceIndex + 2];
        rgba[targetIndex + 3] = 255;
      } else if (colorType === 3) {
        const paletteIndex = raw[sourceIndex];
        rgba[targetIndex] = palette?.[paletteIndex * 3] ?? 255;
        rgba[targetIndex + 1] = palette?.[(paletteIndex * 3) + 1] ?? 255;
        rgba[targetIndex + 2] = palette?.[(paletteIndex * 3) + 2] ?? 255;
        rgba[targetIndex + 3] = transparency?.[paletteIndex] ?? 255;
      } else if (colorType === 4) {
        const value = raw[sourceIndex];
        rgba[targetIndex] = value;
        rgba[targetIndex + 1] = value;
        rgba[targetIndex + 2] = value;
        rgba[targetIndex + 3] = raw[sourceIndex + 1];
      } else if (colorType === 6) {
        rgba[targetIndex] = raw[sourceIndex];
        rgba[targetIndex + 1] = raw[sourceIndex + 1];
        rgba[targetIndex + 2] = raw[sourceIndex + 2];
        rgba[targetIndex + 3] = raw[sourceIndex + 3];
      }
    }
  }

  return { width, height, rgba };
}

function imageToGfaCommand(image, options = {}) {
  const targetWidth = numberFromInput(options.widthDots ?? options.targetWidthDots, { min: 1, max: 2000, integer: true }) || image.width;
  const targetHeight = numberFromInput(options.heightDots ?? options.targetHeightDots, { min: 1, max: 2000, integer: true }) || image.height;
  const threshold = numberFromInput(options.threshold, { min: 0, max: 255, integer: true }) ?? 128;
  const dithering = trimString(options.dithering).toLowerCase() === "ordered" ? "ordered" : "none";
  const bayer = [
    [0, 8, 2, 10],
    [12, 4, 14, 6],
    [3, 11, 1, 9],
    [15, 7, 13, 5]
  ];
  const bits = [];

  for (let y = 0; y < targetHeight; y += 1) {
    const sourceY = Math.min(image.height - 1, Math.floor((y * image.height) / targetHeight));
    const row = [];
    for (let x = 0; x < targetWidth; x += 1) {
      const sourceX = Math.min(image.width - 1, Math.floor((x * image.width) / targetWidth));
      const index = ((sourceY * image.width) + sourceX) * 4;
      const alpha = image.rgba[index + 3] / 255;
      const red = (image.rgba[index] * alpha) + (255 * (1 - alpha));
      const green = (image.rgba[index + 1] * alpha) + (255 * (1 - alpha));
      const blue = (image.rgba[index + 2] * alpha) + (255 * (1 - alpha));
      const luminance = (red * 0.299) + (green * 0.587) + (blue * 0.114);
      const thresholdShift = dithering === "ordered" ? (bayer[y % 4][x % 4] - 7.5) * 10 : 0;
      row.push(luminance < Math.max(0, Math.min(255, threshold + thresholdShift)) ? 1 : 0);
    }
    bits.push(row);
  }

  return {
    gfa: buildGfaFromBits(bits, targetWidth, targetHeight),
    widthDots: targetWidth,
    heightDots: targetHeight,
    threshold,
    dithering
  };
}

function convertPngBufferToGfa(buffer, options = {}) {
  return imageToGfaCommand(parsePngToRgba(buffer), options);
}

function storeTemplateLabLogoAsset(file, options = {}) {
  if (!file?.buffer) throw httpError(400, "VALIDATION_ERROR", "Logo upload requires multipart field 'file'.");
  const originalName = sanitizeFilename(file.originalname || `pri-logo-${Date.now()}.png`);
  const extension = path.extname(originalName).toLowerCase() || ".png";
  if (extension !== ".png") throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Logo upload currently supports PNG files.");
  const baseName = path.basename(originalName, extension).replace(/[^A-Za-z0-9_.-]/g, "_") || "pri-logo";
  const assetName = `${baseName}-${formatTemplateBackupTimestamp()}${extension}`;
  const { assetDir, assetPath } = resolveTemplateLabAssetPath(assetName);
  fs.mkdirSync(assetDir, { recursive: true });
  fs.writeFileSync(assetPath, file.buffer);

  const converted = convertPngBufferToGfa(file.buffer, options);
  const gfaName = `${baseName}-${formatTemplateBackupTimestamp()}.gfa.zpl`;
  const gfaPath = path.join(assetDir, gfaName);
  fs.writeFileSync(gfaPath, `${converted.gfa}\n`, "utf8");

  return {
    ok: true,
    assetName,
    assetPath,
    gfaName,
    gfaPath,
    ...converted,
    bytes: file.buffer.length
  };
}

function listTemplateLabLogoAssets() {
  fs.mkdirSync(ZPL_TEMPLATE_LAB_ASSET_DIR, { recursive: true });
  const assets = fs.readdirSync(ZPL_TEMPLATE_LAB_ASSET_DIR, { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .filter((entry) => /\.(png|gfa\.zpl)$/i.test(entry.name))
    .map((entry) => {
      const assetPath = path.join(ZPL_TEMPLATE_LAB_ASSET_DIR, entry.name);
      const stat = fs.statSync(assetPath);
      return { name: entry.name, path: assetPath, bytes: stat.size, updatedAt: stat.mtime.toISOString() };
    })
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  return { ok: true, assetDir: ZPL_TEMPLATE_LAB_ASSET_DIR, assets };
}

function loadTemplateLabLogoAsset(assetName, options = {}) {
  const { safeName, assetPath } = resolveTemplateLabAssetPath(assetName);
  if (!fs.existsSync(assetPath)) throw httpError(404, "LOGO_ASSET_NOT_FOUND", "Logo asset was not found.");
  if (/\.gfa\.zpl$/i.test(safeName)) {
    const gfa = normalizeGfaCommand(fs.readFileSync(assetPath, "utf8"));
    if (!gfa) throw httpError(400, "INVALID_LOGO_ASSET", "Selected GFA asset does not contain a ^GFA command.");
    const metrics = getGfaCommandMetrics(gfa);
    return {
      ok: true,
      assetName: safeName,
      assetPath,
      gfa,
      widthDots: metrics?.width || null,
      heightDots: metrics?.height || null
    };
  }
  if (!/\.png$/i.test(safeName)) throw httpError(400, "UNSUPPORTED_LOGO_IMAGE", "Selected logo asset must be PNG or .gfa.zpl.");
  const converted = convertPngBufferToGfa(fs.readFileSync(assetPath), options);
  return { ok: true, assetName: safeName, assetPath, ...converted };
}

function applyLogoOverrideToRenderedZpl(renderedZpl, profile = {}) {
  const logo = profile.logo || {};
  const uploadedGfa = normalizeGfaCommand(logo.gfa);
  if (logo.mode !== "static logo" && !uploadedGfa) return renderedZpl;

  const replaced = String(renderedZpl || "").replace(
    /(\^FX Static PRI logo[^\r\n]*\r?\n)?\^FO(-?\d+),(-?\d+)\r?\n(\^GFA,(\d+),(\d+),(\d+),([0-9A-Fa-f]+))\^FS/,
    (_match, comment, currentX, currentY, gfaCommand, _totalA, _totalB, currentBytesPerRow, gfaData) => {
      const baseGfa = uploadedGfa || gfaCommand;
      const metrics = getGfaCommandMetrics(baseGfa) || {
        width: Number(currentBytesPerRow) * 8,
        height: Math.floor((String(gfaData).length / 2) / Number(currentBytesPerRow))
      };
      const scale = Number.isFinite(logo.scale) ? Number(logo.scale) : 1;
      const targetWidth = Number.isFinite(logo.widthDots) ? logo.widthDots : Math.max(1, Math.round(metrics.width * scale));
      const targetHeight = Number.isFinite(logo.heightDots) ? logo.heightDots : Math.max(1, Math.round(metrics.height * scale));
      const x = Number.isFinite(logo.x) ? logo.x : Number(currentX);
      const y = Number.isFinite(logo.y) ? logo.y : Number(currentY);
      const scaled = scaleGfaCommand(baseGfa, targetWidth, targetHeight);
      const sourceComment = logo.assetName ? `^FX Static PRI logo template-lab asset ${sanitizeFilename(logo.assetName)}\n` : comment;
      return `${sourceComment || "^FX Static PRI logo template-lab override\n"}^FO${Math.round(x)},${Math.round(y)}\n${scaled}^FS`;
    }
  );
  if (replaced !== String(renderedZpl || "") || !uploadedGfa) return replaced;

  const x = Number.isFinite(logo.x) ? logo.x : 0;
  const y = Number.isFinite(logo.y) ? logo.y : 0;
  const metrics = getGfaCommandMetrics(uploadedGfa);
  const scale = Number.isFinite(logo.scale) ? Number(logo.scale) : 1;
  const targetWidth = Number.isFinite(logo.widthDots) ? logo.widthDots : Math.max(1, Math.round((metrics?.width || 96) * scale));
  const targetHeight = Number.isFinite(logo.heightDots) ? logo.heightDots : Math.max(1, Math.round((metrics?.height || 32) * scale));
  const command = `^FX Static PRI logo template-lab asset ${sanitizeFilename(logo.assetName || "uploaded")}\n^FO${Math.round(x)},${Math.round(y)}\n${scaleGfaCommand(uploadedGfa, targetWidth, targetHeight)}^FS\n`;
  return replaced.includes("^XZ") ? replaced.replace(/\^XZ/, `${command}^XZ`) : `${replaced}\n${command}`;
}

function applyTemplateLabRenderedOverrides(renderedZpl, profile = {}) {
  let output = applyQrOverrideToRenderedZpl(renderedZpl, profile);
  output = applyLogoOverrideToRenderedZpl(output, profile);
  output = applyGlobalTemplateLabTransform(output, profile);
  return output;
}

const DYNAMIC_ZPL_TOKEN_PATTERN = /{{\s*[A-Za-z][A-Za-z0-9_]*\s*}}/g;
const TEMPLATE_LAB_SAMPLE_VALUE_PATTERNS = Object.freeze([
  Object.freeze({ label: "PT000086", pattern: /PT000086/ }),
  Object.freeze({ label: "Template Lab Product", pattern: /Template Lab Product/ }),
  Object.freeze({ label: "PROOF", pattern: /\bPROOF\b/ }),
  Object.freeze({ label: "PT000086-Bxx", pattern: /PT000086-B\d{2}/ }),
  Object.freeze({ label: "PO12345", pattern: /\bPO12345\b/ }),
  Object.freeze({ label: "PROD001", pattern: /\bPROD001\b/ }),
  Object.freeze({ label: "LAB", pattern: /\^FD\s*LAB\s*\^FS|\bERP=LAB\b|\bLAB SAMPLE\b/i }),
  Object.freeze({ label: "ULTRAMAR", pattern: /ULTRAMAR/i }),
  Object.freeze({ label: "POLYPROP", pattern: /POLYPROP/i }),
  Object.freeze({ label: "P3 EXT", pattern: /\bP3 EXT\b/i }),
  Object.freeze({ label: "P8LABTEST", pattern: /\bP8LABTEST\b/i })
]);

function collectDynamicZplTokens(zpl) {
  return Array.from(new Set(Array.from(String(zpl || "").matchAll(DYNAMIC_ZPL_TOKEN_PATTERN), (match) => match[0]))).sort();
}

function hasDynamicZplTokens(zpl) {
  return collectDynamicZplTokens(zpl).length > 0;
}

function findTemplateLabSampleValues(zpl) {
  const text = String(zpl || "");
  return TEMPLATE_LAB_SAMPLE_VALUE_PATTERNS
    .filter((entry) => entry.pattern.test(text))
    .map((entry) => entry.label);
}

function collectRfidZplCommands(zpl) {
  return String(zpl || "").match(/\^RFW,[^^]+\^FD[^^]*\^FS/g) || [];
}

function collectChangedTemplateLabSections(overrides = {}) {
  const sections = [];
  const source = isPlainObject(overrides) ? overrides : {};
  const sectionKeys = [
    ["label", ["labelWidthDots", "labelHeightDots", "labelHomeX", "labelHomeY", "labelShiftX", "labelShiftY"]],
    ["wholeLabel", ["globalScaleX", "globalScaleY", "globalOffsetX", "globalOffsetY", "scaleX", "scaleY", "offsetX", "offsetY", "borderThickness", "scaleBorderThickness"]],
    ["qr", ["qr"]],
    ["logo", ["logo"]],
    ["fieldGeometry", ["fieldGeometryOverrides"]],
    ["fieldFit", ["fieldFitDefinitions"]],
    ["fieldPosition", ["fieldPositionOverrides"]],
    ["bottomGrid", ["bottomGrid"]]
  ];
  for (const [section, keys] of sectionKeys) {
    if (keys.some((key) => source[key] !== undefined)) sections.push(section);
  }
  return sections;
}

function applyFieldFitDefinitionsToTemplateSource(source, fieldFitDefinitions = {}) {
  let output = String(source || "");
  for (const field of ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"]) {
    if (!isPlainObject(fieldFitDefinitions[field])) continue;
    const fieldTokenPattern = new RegExp(`\\^FB[^\\^\\r\\n]*\\^FD\\{\\{\\s*${field}Text\\s*\\}\\}`, "g");
    output = output.replace(
      fieldTokenPattern,
      `^FB{{${field}BoxW}},{{${field}MaxLines}},0,{{${field}Alignment}},0^FD{{${field}Text}}`
    );
  }
  return output;
}

function scaleFieldFitNumber(value, scale, options = {}) {
  const number = Number(value);
  if (!Number.isFinite(number)) return value;
  return roundedScaled(number, scale, 0, options);
}

function scaleTemplateLabFieldFitDefinitionsForPromotion(fieldFitDefinitions = {}, profile = {}) {
  const normalized = normalizeTemplateLabProfileGeometry(profile);
  const scaleX = normalized.globalScaleX;
  const scaleY = normalized.globalScaleY;
  const scaleBorders = normalized.scaleBorderThickness;
  const scaled = deepCloneJson(fieldFitDefinitions || {});

  for (const definition of Object.values(scaled || {})) {
    if (!isPlainObject(definition)) continue;
    if (definition.boxWidth !== undefined) definition.boxWidth = scaleFieldFitNumber(definition.boxWidth, scaleX, { min: 1 });
    if (definition.boxHeight !== undefined) definition.boxHeight = scaleFieldFitNumber(definition.boxHeight, scaleY, { min: 1 });
    if (definition.fontHeight !== undefined) definition.fontHeight = scaleFieldFitNumber(definition.fontHeight, scaleY, { min: 1 });
    if (definition.fontWidth !== undefined) definition.fontWidth = scaleFieldFitNumber(definition.fontWidth, scaleX, { min: 1 });
    if (definition.borderThickness !== undefined && scaleBorders) {
      definition.borderThickness = clampStrokeThickness(definition.borderThickness * ((scaleX + scaleY) / 2));
    }
    for (const tier of ["large", "medium", "small", "min"]) {
      if (!isPlainObject(definition[tier])) continue;
      if (definition[tier].fontH !== undefined) definition[tier].fontH = scaleFieldFitNumber(definition[tier].fontH, scaleY, { min: 1 });
      if (definition[tier].fontW !== undefined) definition[tier].fontW = scaleFieldFitNumber(definition[tier].fontW, scaleX, { min: 1 });
    }
  }

  return scaled;
}

function templateLabFieldFitCommentPattern() {
  const escapedPrefix = FIELD_FIT_DEFINITIONS_COMMENT_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`\\^FX\\s*${escapedPrefix}[A-Za-z0-9+/=]+\\r?\\n?`, "g");
}

function upsertTemplateFieldFitDefinitionsComment(source, fieldFitDefinitions = {}) {
  const cleanSource = String(source || "").replace(templateLabFieldFitCommentPattern(), "");
  if (!isPlainObject(fieldFitDefinitions) || Object.keys(fieldFitDefinitions).length === 0) return cleanSource;

  const encoded = Buffer.from(JSON.stringify(fieldFitDefinitions), "utf8").toString("base64");
  const comment = `^FX ${FIELD_FIT_DEFINITIONS_COMMENT_PREFIX}${encoded}\n`;
  const lastLabelEnd = cleanSource.lastIndexOf("^XZ");
  if (lastLabelEnd < 0) return `${cleanSource.replace(/\s+$/, "")}\n${comment}`;
  return `${cleanSource.slice(0, lastLabelEnd)}${comment}${cleanSource.slice(lastLabelEnd)}`;
}

function applyTemplateLabDynamicSourceOverrides(sourceTemplate, profile = {}) {
  const geometryFieldFitDefinitions = buildFieldFitDefinitionsFromGeometry(profile?.fieldGeometryOverrides || {});
  const fieldFitDefinitions = deepMergePlainObjects(profile?.fieldFitDefinitions || {}, geometryFieldFitDefinitions);
  let output = applyFieldGeometryOverridesToTemplateSource(sourceTemplate, profile?.fieldGeometryOverrides || {});
  output = applyFieldPositionOverridesToTemplateSource(output, profile?.fieldPositionOverrides || {});
  output = applyQrOverrideToRenderedZpl(output, profile);
  output = applyLogoOverrideToRenderedZpl(output, profile);
  output = applyBottomGridOverrideToZpl(output, profile);
  output = applyFieldFitDefinitionsToTemplateSource(output, fieldFitDefinitions);
  const promotedFieldFitDefinitions = scaleTemplateLabFieldFitDefinitionsForPromotion(fieldFitDefinitions, profile);
  output = upsertTemplateFieldFitDefinitionsComment(output, promotedFieldFitDefinitions);
  output = applyGlobalTemplateLabTransform(output, profile);
  return output;
}

function formatTemplateBackupTimestamp(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
    "-",
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds())
  ].join("");
}

function promoteTemplateLabDynamicTemplate(body = {}) {
  const selected = normalizeTemplateLabTemplateName(body.template || body.templateName);
  const profileKey = trimString(body.profileKey || body.profile || selected.definition.defaultProfileKey).toUpperCase();
  const inlineOverrides = buildProfileOverridesFromInput(body);
  const profile = buildEffectiveTemplateLabProfile(profileKey, selected.definition.defaultProfileKey, inlineOverrides, {
    includeSaved: booleanFromInput(body.useSavedProfile) === true
  });
  if (!profile) {
    throw httpError(400, "UNSUPPORTED_TEMPLATE_LAB_PROFILE", "Template Lab can only promote approved station/template profiles.", {
      profileKey,
      supportedProfiles: listStationProfiles().map((item) => item.key)
    });
  }

  const sourceTemplate = loadZplTemplate(selected.templatePath);
  const tokensBefore = collectDynamicZplTokens(sourceTemplate);
  if (!tokensBefore.length) {
    throw httpError(400, "DYNAMIC_TEMPLATE_REQUIRED", "Production promotion requires a dynamic .template.zpl source with {{...}} tokens.", {
      template: selected.name,
      templatePath: selected.templatePath
    });
  }

  const updatedTemplate = applyTemplateLabDynamicSourceOverrides(sourceTemplate, profile);
  const remainingTokens = collectDynamicZplTokens(updatedTemplate);
  if (remainingTokens.length === 0) {
    throw httpError(400, "DYNAMIC_TEMPLATE_TOKENS_MISSING", "Promotion rejected because the updated template has no {{...}} tokens.", {
      template: selected.name,
      templatePath: selected.templatePath
    });
  }
  if (remainingTokens.length < tokensBefore.length) {
    throw httpError(400, "DYNAMIC_TEMPLATE_TOKEN_COUNT_DROPPED", "Promotion rejected because edited template lost dynamic {{...}} tokens.", {
      template: selected.name,
      templatePath: selected.templatePath,
      tokenCountBefore: tokensBefore.length,
      tokenCountAfter: remainingTokens.length,
      missingTokens: tokensBefore.filter((token) => !remainingTokens.includes(token))
    });
  }

  const sampleValues = findTemplateLabSampleValues(updatedTemplate);
  if (sampleValues.length) {
    throw httpError(400, "LAB_SAMPLE_VALUES_IN_TEMPLATE", "Promotion rejected because the updated template appears to contain Template Lab sample data.", {
      template: selected.name,
      templatePath: selected.templatePath,
      sampleValues
    });
  }

  const rfidCommandsBefore = collectRfidZplCommands(sourceTemplate);
  const rfidCommandsAfter = collectRfidZplCommands(updatedTemplate);
  if (selected.definition.requiresRfid && JSON.stringify(rfidCommandsBefore) !== JSON.stringify(rfidCommandsAfter)) {
    throw httpError(400, "RFID_COMMANDS_CHANGED", "Promotion rejected because RFID write commands changed.", {
      template: selected.name,
      templatePath: selected.templatePath,
      rfidCommandsBefore,
      rfidCommandsAfter
    });
  }

  const sourceQr = extractQrMetadata(updatedTemplate, updatedTemplate)?.payloadTemplate;
  if (selected.definition.requiresRfid && sourceQr !== "{{lotNumber}}") {
    throw httpError(400, "QR_PAYLOAD_CHANGED", "Promotion rejected because QR source payload is no longer {{lotNumber}} only.", {
      template: selected.name,
      templatePath: selected.templatePath,
      qrPayloadTemplate: sourceQr || null
    });
  }

  const verificationData = buildTemplateLabData(body, selected.definition);
  const renderOptions = { fieldFitDefinitions: getFittedFieldDefinitions(parseTemplateLabFieldFitDefinitionsFromSource(updatedTemplate)) };
  const verificationRendered = selected.definition.requiresRfid
    ? renderZplTemplateWithMetadata(updatedTemplate, verificationData, renderOptions).rendered
    : renderZplTemplateWithoutRfidWithMetadata(updatedTemplate, verificationData, renderOptions).rendered;
  const unreplacedTokens = collectDynamicZplTokens(verificationRendered);
  if (unreplacedTokens.length) {
    throw httpError(400, "PROMOTED_TEMPLATE_RENDER_HAS_TOKENS", "Promotion rejected because the promoted dynamic template would still render with unreplaced tokens.", {
      template: selected.name,
      templatePath: selected.templatePath,
      unreplacedTokens
    });
  }

  fs.mkdirSync(path.dirname(selected.templatePath), { recursive: true });
  const backupPath = `${selected.templatePath}.bak-${formatTemplateBackupTimestamp()}`;
  const tempWritePath = `${selected.templatePath}.tmp-${process.pid}-${Date.now()}`;
  fs.copyFileSync(selected.templatePath, backupPath);
  fs.writeFileSync(tempWritePath, updatedTemplate, "utf8");
  fs.renameSync(tempWritePath, selected.templatePath);
  const writtenTemplate = fs.readFileSync(selected.templatePath, "utf8");
  if (writtenTemplate !== updatedTemplate) {
    throw httpError(500, "PROMOTED_TEMPLATE_WRITE_VERIFY_FAILED", "Promotion failed because the written template did not match the verified output.", {
      template: selected.name,
      templatePath: selected.templatePath,
      backupPath
    });
  }
  const changedProfileSections = collectChangedTemplateLabSections(inlineOverrides);
  const payloadBytes = Number(body.renderedPayloadBytes) || Buffer.byteLength(updatedTemplate, "utf8");

  logInfo("template_lab_dynamic_template_promoted", {
    template: selected.name,
    profileKey: profile.key,
    templatePath: selected.templatePath,
    backupPath,
    tokenCountBefore: tokensBefore.length,
    tokenCountAfter: remainingTokens.length,
    payloadBytes,
    changedProfileSections,
    changedFields: Object.keys(profile.fieldGeometryOverrides || {})
  });

  return {
    ok: true,
    template: selected.name,
    profileKey: profile.key,
    templatePath: selected.templatePath,
    updatedTemplatePath: selected.templatePath,
    backupPath,
    tokenCount: remainingTokens.length,
    tokenCountBefore: tokensBefore.length,
    tokenCountAfter: remainingTokens.length,
    tokens: remainingTokens,
    changedFields: Object.keys(profile.fieldGeometryOverrides || {}),
    changedProfileSections,
    payloadBytes,
    bytes: Buffer.byteLength(updatedTemplate, "utf8"),
    verification: {
      unreplacedTokens: [],
      rfidCommandsUnchanged: true,
      qrPayloadLotNumberOnly: sourceQr === "{{lotNumber}}",
      productionTemplateFileModified: true
    },
    message: "Dynamic template promoted to production source. Rendered proof ZPL was not saved."
  };
}

function expectedTemplateNameForFamilyStation(family, station) {
  const fam = normalizeDirectZplScopeFamily(family);
  const st = String(station || "").trim().toUpperCase();
  if (fam === "RAW") return rawTemplateForStation(st);
  if (fam === "FG") return fgTemplateForStation(st);
  if (fam === "SAMPLE") return qcSampleTemplateForStation(st);
  if (fam === "RETAIN") return qcRetainTemplateForStation(st);
  if (fam === "SAMPLE_POUNDS") return qcSamplePoundsTemplateForStation(st);
  return "";
}

function addGroupedStationIssue(group, family, station, details) {
  const fam = normalizeDirectZplScopeFamily(family);
  const st = String(station || "").trim().toUpperCase();
  group[fam] = group[fam] || {};
  group[fam][st] = details;
}

function isSharedTemplateExplicitlyAllowed(directZpl, family, station, actualFileName) {
  const fam = normalizeDirectZplScopeFamily(family);
  const st = String(station || "").trim().toUpperCase();
  if (directZpl.allowTemplateFallbacks?.[fam]?.[st] === true) return true;

  const configured = directZpl.allowedSharedTemplates?.[fam]?.[st] ||
    directZpl.allowSharedTemplates?.[fam]?.[st] ||
    directZpl.explicitSharedTemplates?.[fam]?.[st];
  if (configured === true) return true;
  const values = Array.isArray(configured) ? configured : configured ? [configured] : [];
  return values.some((value) => path.basename(path.win32.basename(String(value || ""))) === actualFileName);
}

function listDirectZplTemplateValidationTargets(directZpl = getDirectZplConfig()) {
  const byKey = new Map();
  const add = (family, station) => {
    const fam = normalizeDirectZplScopeFamily(family);
    const st = String(station || "").trim().toUpperCase();
    if (!fam || !st) return;
    byKey.set(`${fam}:${st}`, { family: fam, station: st });
  };

  for (const station of RAW_STATIONS) add("RAW", station);
  for (const station of FG_STATIONS) add("FG", station);

  for (const [family, stationMap] of Object.entries(directZpl.templates || {})) {
    if (!isPlainObject(stationMap)) continue;
    for (const station of Object.keys(stationMap)) add(family, station);
  }

  return Array.from(byKey.values()).sort((a, b) => `${a.family}:${a.station}`.localeCompare(`${b.family}:${b.station}`));
}

function validateDirectZplTemplates() {
  const directZpl = getDirectZplConfig();
  const targets = listDirectZplTemplateValidationTargets(directZpl);
  const missingTemplates = {};
  const tokenlessTemplates = {};
  const wrongStationMappings = [];
  const checkedTemplates = [];

  for (const target of targets) {
    const printerConfig = getDirectZplPrinterConfig(directZpl, target.station, target.family) || {};
    const templateValue = getDirectZplTemplateValue(directZpl, target.family, target.station, printerConfig);
    const templatePath = templateValue ? resolveZplTemplatePath(templateValue) : "";
    const expectedFileName = expectedTemplateNameForFamilyStation(target.family, target.station);
    const actualFileName = templatePath ? path.basename(templatePath) : "";
    const check = {
      family: target.family,
      station: target.station,
      templateValue,
      templatePath,
      expectedFileName,
      actualFileName,
      exists: false,
      tokenCount: 0,
      tokens: []
    };

    if (!templatePath || !fs.existsSync(templatePath)) {
      addGroupedStationIssue(missingTemplates, target.family, target.station, {
        templatePath,
        expectedFileName,
        configuredValue: templateValue || null
      });
      checkedTemplates.push(check);
      continue;
    }

    check.exists = true;
    const sourceTemplate = fs.readFileSync(templatePath, "utf8");
    check.tokens = collectDynamicZplTokens(sourceTemplate);
    check.tokenCount = check.tokens.length;
    if (check.tokenCount === 0) {
      addGroupedStationIssue(tokenlessTemplates, target.family, target.station, {
        templatePath,
        expectedFileName,
        configuredValue: templateValue || null
      });
    }

    if (["RAW", "FG"].includes(target.family) && expectedFileName && actualFileName !== expectedFileName && !isSharedTemplateExplicitlyAllowed(directZpl, target.family, target.station, actualFileName)) {
      wrongStationMappings.push({
        family: target.family,
        station: target.station,
        templatePath,
        actualFileName,
        expectedFileName,
        message: `${target.family} ${target.station} maps to ${actualFileName}; expected ${expectedFileName} unless explicitly allowed.`
      });
    }

    checkedTemplates.push(check);
  }

  const missingCount = Object.values(missingTemplates).reduce((count, familyGroup) => count + Object.keys(familyGroup).length, 0);
  const tokenlessCount = Object.values(tokenlessTemplates).reduce((count, familyGroup) => count + Object.keys(familyGroup).length, 0);

  return {
    ok: missingCount === 0 && tokenlessCount === 0 && wrongStationMappings.length === 0,
    templateSourceDir: ZPL_TEMPLATE_SOURCE_DIR,
    checkedCount: checkedTemplates.length,
    missingTemplates,
    tokenlessTemplates,
    wrongStationMappings,
    checkedTemplates
  };
}

function escapeXml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function parseZplNumberList(value) {
  return String(value || "")
    .split(",")
    .map((part) => Number(part.trim()))
    .map((number) => (Number.isFinite(number) ? number : null));
}

function parseZplCommandStream(zpl) {
  const text = String(zpl || "");
  const commands = [];
  let index = 0;
  while (index < text.length) {
    const start = text.slice(index).search(/[\^~]/);
    if (start < 0) break;
    const absoluteStart = index + start;
    const prefix = text[absoluteStart];
    const code = text.slice(absoluteStart + 1, absoluteStart + 3);
    if (code.length < 2) break;
    let next = absoluteStart + 3;
    while (next < text.length && text[next] !== "^" && text[next] !== "~") next += 1;
    const params = text.slice(absoluteStart + 3, next).replace(/\r?\n/g, "");
    commands.push({ prefix, code, params, raw: `${prefix}${code}${params}` });
    index = next;
  }
  return commands;
}

function appendSvgText(elements, { x, y, text, fontH, fontW, fieldBlock, reverse }) {
  const width = Number(fieldBlock?.width);
  const alignment = String(fieldBlock?.alignment || "L").toUpperCase();
  let anchor = "start";
  let textX = x;
  if (Number.isFinite(width) && alignment === "C") {
    anchor = "middle";
    textX = x + (width / 2);
  } else if (Number.isFinite(width) && alignment === "R") {
    anchor = "end";
    textX = x + width;
  }
  const family = "Arial, Helvetica, sans-serif";
  elements.push(`<text x="${Math.round(textX)}" y="${Math.round(y)}" font-family="${family}" font-size="${Math.max(5, Math.round(fontH || 24))}" font-weight="700" text-anchor="${anchor}" fill="${reverse ? "#ffffff" : "#111827"}" data-font-width="${Math.round(fontW || 12)}">${escapeXml(text)}</text>`);
}

function buildApproximateZplPreview(renderedZpl, profile = {}) {
  const commands = parseZplCommandStream(renderedZpl);
  let labelWidth = Number(profile?.labelWidthDots) || 812;
  let labelHeight = Number(profile?.labelHeightDots) || 1218;
  const elements = [];
  const previewBorders = [];
  const unsupported = new Set();
  const supportedOrIgnored = new Set(["XA", "XZ", "FX", "RS", "RR", "SZ", "JM", "MC", "PM", "JS", "JZ", "LH", "LR", "CI", "PW", "FO", "FT", "GB", "A0", "FB", "FD", "FS", "FR", "BQ", "B3", "BY", "GF", "PQ", "RF"]);
  const state = {
    x: 0,
    y: 0,
    originMode: "FO",
    fontH: 24,
    fontW: 12,
    fieldBlock: null,
    reverse: false,
    barcode: null,
    qrDetected: false,
    logoDetected: false,
    fieldCount: 0
  };

  for (const command of commands) {
    const fullCode = `${command.prefix}${command.code}`;
    if (!supportedOrIgnored.has(command.code)) unsupported.add(fullCode);

    if (command.code === "PW") {
      const width = Number(command.params);
      if (Number.isFinite(width) && width > 0) labelWidth = width;
    } else if (command.code === "LL") {
      const height = Number(command.params);
      if (Number.isFinite(height) && height > 0) labelHeight = height;
    } else if (command.code === "FO" || command.code === "FT") {
      const [x, y] = parseZplNumberList(command.params);
      if (Number.isFinite(x)) state.x = x;
      if (Number.isFinite(y)) state.y = y;
      state.originMode = command.code;
    } else if (command.code === "GB") {
      const [widthRaw, heightRaw, thicknessRaw] = parseZplNumberList(command.params);
      const width = Number(widthRaw) || 0;
      const height = Number(heightRaw) || 0;
      const thickness = Math.max(1, Number(thicknessRaw) || 1);
      previewBorders.push({
        x: state.x,
        y: state.y,
        width,
        height,
        thickness,
        commandStart: command.start,
        commandEnd: command.end
      });
      if (width === 0 || height === 0) {
        const x2 = state.x + width;
        const y2 = state.y + height;
        elements.push(`<line x1="${Math.round(state.x)}" y1="${Math.round(state.y)}" x2="${Math.round(x2 || state.x)}" y2="${Math.round(y2 || state.y)}" stroke="#111827" stroke-width="${thickness}"/>`);
      } else {
        const filled = thickness >= Math.min(Math.abs(width), Math.abs(height)) * 0.45;
        elements.push(`<rect x="${Math.round(state.x)}" y="${Math.round(state.y)}" width="${Math.abs(Math.round(width))}" height="${Math.abs(Math.round(height))}" fill="${filled ? "#111827" : "none"}" stroke="#111827" stroke-width="${thickness}"/>`);
      }
    } else if (command.code === "A0") {
      const parts = String(command.params || "").split(",");
      const fontH = Number(parts[1]);
      const fontW = Number(parts[2]);
      if (Number.isFinite(fontH)) state.fontH = fontH;
      if (Number.isFinite(fontW)) state.fontW = fontW;
    } else if (command.code === "FB") {
      const parts = String(command.params || "").split(",");
      const width = Number(parts[0]);
      state.fieldBlock = {
        width: Number.isFinite(width) ? width : null,
        maxLines: Number(parts[1]) || 1,
        alignment: String(parts[3] || "L").toUpperCase()
      };
    } else if (command.code === "FR") {
      state.reverse = true;
    } else if (command.code === "BQ") {
      const parts = String(command.params || "").split(",");
      state.barcode = { type: "QR", magnification: Number(parts[2]) || 5 };
    } else if (command.code === "B3") {
      const parts = String(command.params || "").split(",");
      state.barcode = { type: "BARCODE", height: Number(parts[2]) || 45 };
    } else if (command.code === "GF") {
      const match = command.raw.match(/\^GFA,(\d+),(\d+),(\d+),([0-9A-Fa-f]+)/);
      const total = Number(match?.[1]);
      const bytesPerRow = Number(match?.[3]);
      const width = bytesPerRow ? bytesPerRow * 8 : Number(profile?.logo?.widthDots) || 96;
      const height = bytesPerRow && total ? Math.max(1, Math.round(total / bytesPerRow)) : Number(profile?.logo?.heightDots) || 32;
      state.logoDetected = true;
      elements.push(`<rect x="${Math.round(state.x)}" y="${Math.round(state.y)}" width="${Math.round(width)}" height="${Math.round(height)}" fill="#ffffff" stroke="#25408f" stroke-width="2"/>`);
      elements.push(`<text x="${Math.round(state.x + (width / 2))}" y="${Math.round(state.y + (height / 2) + 5)}" font-family="Arial, Helvetica, sans-serif" font-size="${Math.max(8, Math.round(height / 3))}" font-weight="900" text-anchor="middle" fill="#25408f">PRI Logo</text>`);
    } else if (command.code === "FD") {
      const data = command.params;
      if (state.barcode?.type === "QR") {
        const size = Math.max(42, state.barcode.magnification * 29);
        state.qrDetected = true;
        elements.push(`<rect x="${Math.round(state.x)}" y="${Math.round(state.y)}" width="${size}" height="${size}" fill="#ffffff" stroke="#111827" stroke-width="3"/>`);
        elements.push(`<path d="M${state.x + 8},${state.y + 8}h18v18h-18z M${state.x + size - 28},${state.y + 8}h18v18h-18z M${state.x + 8},${state.y + size - 28}h18v18h-18z" fill="#111827"/>`);
        elements.push(`<text x="${Math.round(state.x + (size / 2))}" y="${Math.round(state.y + size + 18)}" font-family="Arial, Helvetica, sans-serif" font-size="14" font-weight="800" text-anchor="middle" fill="#111827">QR ${escapeXml(data.replace(/^LA,/, ""))}</text>`);
      } else if (state.barcode?.type === "BARCODE") {
        const width = Math.max(120, Math.min(360, String(data).length * 11));
        const height = state.barcode.height;
        elements.push(`<rect x="${Math.round(state.x)}" y="${Math.round(state.y)}" width="${width}" height="${height}" fill="#f8fafc" stroke="#111827" stroke-width="1"/>`);
        for (let offset = 4; offset < width; offset += 8) {
          elements.push(`<line x1="${Math.round(state.x + offset)}" y1="${Math.round(state.y + 3)}" x2="${Math.round(state.x + offset)}" y2="${Math.round(state.y + height - 3)}" stroke="#111827" stroke-width="${offset % 16 === 4 ? 2 : 1}"/>`);
        }
      } else {
        const y = state.originMode === "FT" ? state.y : state.y + state.fontH;
        appendSvgText(elements, {
          x: state.x,
          y,
          text: data,
          fontH: state.fontH,
          fontW: state.fontW,
          fieldBlock: state.fieldBlock,
          reverse: state.reverse
        });
      }
      state.fieldCount += 1;
    } else if (command.code === "FS") {
      state.fieldBlock = null;
      state.reverse = false;
      state.barcode = null;
    }
  }

  const gridStep = Math.max(50, Math.round(labelWidth / 12));
  const svg = [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${Math.round(labelWidth)} ${Math.round(labelHeight)}" width="${Math.round(labelWidth)}" height="${Math.round(labelHeight)}" role="img" aria-label="Approximate ZPL label preview">`,
    "<defs>",
    `<pattern id="grid" width="${gridStep}" height="${gridStep}" patternUnits="userSpaceOnUse"><path d="M ${gridStep} 0 L 0 0 0 ${gridStep}" fill="none" stroke="#d8dee9" stroke-width="1"/></pattern>`,
    "</defs>",
    `<rect x="0" y="0" width="${Math.round(labelWidth)}" height="${Math.round(labelHeight)}" fill="#ffffff" stroke="#111827" stroke-width="3"/>`,
    `<rect x="0" y="0" width="${Math.round(labelWidth)}" height="${Math.round(labelHeight)}" fill="url(#grid)" opacity="0.55"/>`,
    ...elements,
    "</svg>"
  ].join("");

  return {
    mode: "approximate",
    ok: true,
    svg,
    dataUrl: `data:image/svg+xml;base64,${Buffer.from(svg, "utf8").toString("base64")}`,
    metadata: {
      previewMode: "approximate",
      labelWidthDots: Math.round(labelWidth),
      labelHeightDots: Math.round(labelHeight),
      unsupportedZplCommands: Array.from(unsupported).sort(),
      qrDetected: state.qrDetected,
      logoDetected: state.logoDetected,
      bottomGrid: detectBottomGridFromBorders(previewBorders, { labelHeightDots: labelHeight }),
      fieldCount: state.fieldCount
    }
  };
}

function readPngDimensions(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);
    if (buffer.length < 24 || buffer.toString("ascii", 1, 4) !== "PNG") return null;
    return {
      width: buffer.readUInt32BE(16),
      height: buffer.readUInt32BE(20)
    };
  } catch {
    return null;
  }
}

function extractLogoDiagnostics(renderedZpl) {
  const logoSourcePath = path.join(OFFLINE_ASSETS_DIR, "pri-logo.png");
  const sourceDimensions = readPngDimensions(logoSourcePath);
  const match = String(renderedZpl || "").match(/\^GFA,(\d+),(\d+),(\d+),([0-9A-Fa-f]+)/);
  const payloadBytes = Number(match?.[1]) || 0;
  const bytesPerRow = Number(match?.[3]) || 0;
  const renderedWidthDots = bytesPerRow ? bytesPerRow * 8 : 0;
  const renderedHeightDots = bytesPerRow && payloadBytes ? Math.round(payloadBytes / bytesPerRow) : 0;
  return {
    source: logoSourcePath,
    sourceExists: fs.existsSync(logoSourcePath),
    sourceWidth: sourceDimensions?.width || null,
    sourceHeight: sourceDimensions?.height || null,
    mode: match ? "static ^GFA" : "none",
    payloadBytes,
    renderedWidthDots,
    renderedHeightDots,
    qualityNote: match && renderedWidthDots < 128
      ? "Logo is a small 1-bit ZPL graphic; use a wider converted asset or keep it physically small to avoid pixelation."
      : "Logo source is high resolution; physical quality still depends on final dot size and thresholding."
  };
}

function extractQrMetadata(renderedZpl, sourceTemplate) {
  const renderedMatch = String(renderedZpl || "").match(/\^BQN,([^^]+)\^FDLA,([^^]+)\^FS/);
  const sourceMatch = String(sourceTemplate || "").match(/\^BQN,([^^]+)\^FDLA,([^^]+)\^FS/);
  if (!renderedMatch && !sourceMatch) return null;

  return {
    command: renderedMatch ? `^BQN,${renderedMatch[1]}` : `^BQN,${sourceMatch[1]}`,
    payload: renderedMatch ? renderedMatch[2] : null,
    payloadTemplate: sourceMatch ? sourceMatch[2] : null,
    lotNumberOnly: sourceMatch ? sourceMatch[2] === "{{lotNumber}}" : false
  };
}

function extractTemplateRenderMetadata({ renderedZpl, sourceTemplate, templateName, profile, fitDebug, previewInfo }) {
  const graphicCommands = String(renderedZpl || "").match(/\^GFA|~DG|\^XG/g) || [];
  const previewMetadata = previewInfo?.metadata || {};
  return {
    template: templateName,
    payloadBytes: Buffer.byteLength(String(renderedZpl || ""), "utf8"),
    qr: extractQrMetadata(renderedZpl, sourceTemplate),
    rfidCommandPresent: /\^RFW,/.test(String(renderedZpl || "")),
    rfidCommands: String(renderedZpl || "").match(/\^RFW,[^^]+\^FD[^^]*\^FS/g) || [],
    logoCommandPresent: /\^GFA/.test(String(renderedZpl || "")),
    bitmapGraphicCommandPresent: /~DG|\^XG/.test(String(renderedZpl || "")),
    graphicCommandCount: graphicCommands.length,
    fitDebug,
    logoMode: /\^GFA/.test(String(renderedZpl || "")) ? "static logo" : "none",
    logoDiagnostics: extractLogoDiagnostics(renderedZpl),
    previewMode: previewMetadata.previewMode || previewInfo?.mode || "unavailable",
    labelWidthDots: previewMetadata.labelWidthDots || profile?.labelWidthDots || null,
    labelHeightDots: previewMetadata.labelHeightDots || profile?.labelHeightDots || null,
    bottomGrid: previewMetadata.bottomGrid || profile?.bottomGrid || null,
    unsupportedZplCommands: previewMetadata.unsupportedZplCommands || [],
    qrDetected: previewMetadata.qrDetected ?? /\^BQN,/.test(String(renderedZpl || "")),
    logoDetected: previewMetadata.logoDetected ?? /\^GFA/.test(String(renderedZpl || "")),
    fieldCount: previewMetadata.fieldCount || 0,
    profile
  };
}

async function renderOptionalZplPreviewImage(renderedZpl, profile) {
  const approximatePreview = buildApproximateZplPreview(renderedZpl, profile);
  const rendererUrl = trimString(process.env.ZPL_PREVIEW_RENDERER_URL);
  if (!rendererUrl) {
    return {
      configured: false,
      ok: true,
      mode: "approximate",
      message: "Using built-in approximate SVG preview because ZPL_PREVIEW_RENDERER_URL is not configured.",
      data: { imageUrl: approximatePreview.dataUrl, svg: approximatePreview.svg },
      metadata: approximatePreview.metadata
    };
  }

  try {
    const response = await axios.post(
      rendererUrl,
      {
        zpl: renderedZpl,
        labelWidthDots: profile?.labelWidthDots || null,
        labelHeightDots: profile?.labelHeightDots || null,
        dpi: profile?.dpi || null
      },
      { timeout: 5000 }
    );
    return {
      configured: true,
      ok: true,
      mode: "external",
      data: response.data,
      metadata: {
        ...approximatePreview.metadata,
        previewMode: "external"
      }
    };
  } catch (error) {
    return {
      configured: true,
      ok: true,
      mode: "approximate",
      message: `External renderer failed; using built-in approximate preview. ${formatErrorDetail(error)}`,
      data: { imageUrl: approximatePreview.dataUrl, svg: approximatePreview.svg },
      metadata: approximatePreview.metadata,
      externalError: formatErrorDetail(error)
    };
  }
}

async function buildTemplatePreviewPayload(input = {}) {
  const selected = normalizeTemplateLabTemplateName(input.template || input.templateName);
  const profileKey = trimString(input.profileKey || input.profile || selected.definition.defaultProfileKey).toUpperCase();
  const inlineOverrides = buildProfileOverridesFromInput(input);
  const profile = buildEffectiveTemplateLabProfile(profileKey, selected.definition.defaultProfileKey, inlineOverrides, {
    includeSaved: booleanFromInput(input.useSavedProfile) === true
  });
  const data = buildTemplateLabData(input, selected.definition);
  const sourceTemplateText = loadZplTemplate(selected.templatePath);
  const geometryFieldFitDefinitions = buildFieldFitDefinitionsFromGeometry(profile?.fieldGeometryOverrides || {});
  const templateTextWithGeometry = applyFieldGeometryOverridesToTemplateSource(sourceTemplateText, profile?.fieldGeometryOverrides || {});
  const templateTextWithPositions = applyFieldPositionOverridesToTemplateSource(templateTextWithGeometry, profile?.fieldPositionOverrides || {});
  const templateText = applyBottomGridOverrideToZpl(templateTextWithPositions, profile);
  const renderOptions = { fieldFitDefinitions: deepMergePlainObjects(profile?.fieldFitDefinitions || {}, geometryFieldFitDefinitions) };
  const renderedResult = selected.definition.requiresRfid
    ? renderZplTemplateWithMetadata(templateText, data, renderOptions)
    : renderZplTemplateWithoutRfidWithMetadata(templateText, data, renderOptions);
  const renderedZpl = applyTemplateLabRenderedOverrides(renderedResult.rendered, profile);
  const imagePreview = await renderOptionalZplPreviewImage(renderedZpl, profile);
  const metadata = extractTemplateRenderMetadata({
    renderedZpl,
    sourceTemplate: templateText,
    templateName: selected.name,
    profile,
    fitDebug: renderedResult.fitDebug,
    previewInfo: imagePreview
  });

  return {
    ok: true,
    template: selected.name,
    templatePath: selected.templatePath,
    requiresRfid: selected.definition.requiresRfid,
    profileKey: profile?.key || null,
    sampleData: data,
    renderedZpl,
    metadata,
    profileOverrides: inlineOverrides,
    savedProfileOverrides: profile?.savedOverrides || {},
    profileConfigPath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
    imagePreview
  };
}

function setTemplateTestSendFunction(fn) {
  templateTestSendFunctionForTests = typeof fn === "function" ? fn : null;
}

function resetTemplateTestSendFunction() {
  templateTestSendFunctionForTests = null;
}

function saveTemplateLabProfileOverrides(body = {}) {
  const profileKey = trimString(body.profileKey || body.profile).toUpperCase();
  const baseProfile = getStationProfile(profileKey);
  if (!baseProfile) {
    throw httpError(400, "UNSUPPORTED_TEMPLATE_LAB_PROFILE", "Template Lab can only save overrides for approved station/template profiles.", {
      profileKey,
      supportedProfiles: listStationProfiles().map((profile) => profile.key)
    });
  }

  const explicitOverrides = parseJsonObjectField(body.overrides ?? body.profileOverrides, "overrides");
  const flatOverrides = buildProfileOverridesFromInput({ ...body, profileOverrides: undefined });
  const overrides = deepMergePlainObjects(explicitOverrides, flatOverrides);
  const templateName = trimString(body.template || body.templateName || overrides.templateName || baseProfile.template);
  let templatePath = "";
  try {
    templatePath = normalizeTemplateLabTemplateName(templateName).templatePath;
  } catch {
    templatePath = path.join(ZPL_TEMPLATE_SOURCE_DIR, path.basename(path.win32.basename(templateName || baseProfile.template || "")));
  }
  overrides.profileKey = profileKey;
  overrides.templateName = templateName;
  overrides.sourceTemplatePath = templatePath;
  overrides.updatedAt = isoNow();
  const updatedBy = trimString(body.updatedBy || body.operator || body.user || body.updatedByName);
  if (updatedBy) overrides.updatedBy = updatedBy;
  const config = readTemplateLabProfileConfig();
  config.profiles = isPlainObject(config.profiles) ? config.profiles : {};
  config.profiles[profileKey] = overrides;
  config.updatedAt = isoNow();
  writeTemplateLabProfileConfig(config);

  logInfo("template_lab_profile_saved", {
    profileKey,
    profilePath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
    overrideKeys: Object.keys(overrides)
  });

  return {
    ok: true,
    profileKey,
    profileConfigPath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
    overrides,
    profile: buildEffectiveTemplateLabProfile(profileKey, profileKey, {})
  };
}

function resetTemplateLabProfileOverrides(body = {}) {
  const profileKey = trimString(body.profileKey || body.profile).toUpperCase();
  const baseProfile = getStationProfile(profileKey);
  if (!baseProfile) {
    throw httpError(400, "UNSUPPORTED_TEMPLATE_LAB_PROFILE", "Template Lab can only reset approved station/template profiles.", {
      profileKey,
      supportedProfiles: listStationProfiles().map((profile) => profile.key)
    });
  }

  const config = readTemplateLabProfileConfig();
  config.profiles = isPlainObject(config.profiles) ? config.profiles : {};
  delete config.profiles[profileKey];
  config.updatedAt = isoNow();
  writeTemplateLabProfileConfig(config);

  return {
    ok: true,
    profileKey,
    profileConfigPath: ZPL_TEMPLATE_LAB_PROFILE_PATH,
    profile: buildEffectiveTemplateLabProfile(profileKey, profileKey, {})
  };
}

function buildCalibrationGridZpl(profile = {}) {
  const normalized = normalizeTemplateLabProfileGeometry(profile);
  const width = normalized.labelWidthDots || 812;
  const height = normalized.labelHeightDots || 1218;
  const centerX = Math.round(width / 2);
  const centerY = Math.round(height / 2);
  const rulerStep = 100;
  const commands = [
    "^XA",
    "^CI28",
    `^PW${width}`,
    `^LL${height}`,
    "^FO0,0^GB" + Math.max(1, width - 1) + "," + Math.max(1, height - 1) + ",3^FS",
    `^FO${centerX},0^GB0,${height},2^FS`,
    `^FO0,${centerY}^GB${width},0,2^FS`,
    "^A0N,24,24",
    "^FO12,28^FD0,0^FS",
    `^FO${Math.max(0, width - 125)},28^FDRIGHT^FS`,
    `^FO12,${Math.max(0, height - 24)}^FDBOTTOM^FS`,
    `^FO${Math.max(0, centerX - 65)},${Math.max(0, centerY - 12)}^FDCENTER^FS`
  ];

  for (let x = 0; x <= width; x += rulerStep) {
    commands.push(`^FO${x},0^GB0,35,2^FS`);
    commands.push(`^FO${Math.max(0, x + 4)},38^A0N,18,18^FD${x}^FS`);
  }
  for (let y = 0; y <= height; y += rulerStep) {
    commands.push(`^FO0,${y}^GB35,0,2^FS`);
    commands.push(`^FO40,${Math.max(0, y + 4)}^A0N,18,18^FD${y}^FS`);
  }

  commands.push("^XZ");
  return applyGlobalTemplateLabTransform(commands.join("\n"), normalized);
}

async function sendTemplateLabCalibrationGrid(body = {}) {
  if (body.confirmTestPrint !== true) {
    throw httpError(400, "TEMPLATE_TEST_CONFIRM_REQUIRED", "confirmTestPrint:true is required before sending rendered ZPL directly to a printer.");
  }
  const printerIp = trimString(body.printerIp || body.host);
  const printerPort = Number(body.port || body.printerPort || 9100);
  if (!printerIp) throw httpError(400, "VALIDATION_ERROR", "printerIp is required.");
  if (!Number.isInteger(printerPort) || printerPort <= 0 || printerPort > 65535) {
    throw httpError(400, "VALIDATION_ERROR", "port must be a valid TCP port.");
  }

  const selected = body.template || body.templateName ? normalizeTemplateLabTemplateName(body.template || body.templateName) : null;
  const profileKey = trimString(body.profileKey || body.profile || selected?.definition?.defaultProfileKey).toUpperCase();
  const inlineOverrides = buildProfileOverridesFromInput(body);
  const profile = buildEffectiveTemplateLabProfile(profileKey, selected?.definition?.defaultProfileKey || profileKey, inlineOverrides);
  if (!profile) throw httpError(400, "UNSUPPORTED_TEMPLATE_LAB_PROFILE", "Template Lab can only send calibration grids for approved profiles.");

  const zpl = buildCalibrationGridZpl(profile);
  const sendFn = templateTestSendFunctionForTests || sendZplOverTcp;
  const sendResult = await sendFn({ printerIp, port: printerPort, zpl, timeoutMs: getZplTcpTimeoutMs() });
  return {
    ok: true,
    calibrationPrint: true,
    profileKey: profile.key,
    printerIp,
    printerPort,
    bytesSent: sendResult?.bytesSent ?? Buffer.byteLength(zpl, "utf8"),
    message: "Template Lab calibration grid sent directly to printer. This bypassed the production queue."
  };
}

function resolvePrinterKeyForResume(body = {}) {
  const explicit = trimString(body.printerKey);
  if (explicit) return explicit;

  const station = normalizeStation(body.station);
  if (!station) {
    throw httpError(400, "VALIDATION_ERROR", "printerKey or station is required.");
  }

  const zpl = resolveZplPrinterAndTemplate({ station, family: "RAW" });
  return getZplQueueKey(zpl);
}

function resumeZplQueue(body = {}) {
  const printerKey = resolvePrinterKeyForResume(body);
  const state = getOrCreateZplWorkerState(printerKey);
  const now = isoNow();
  let reviewedCount = 0;

  for (const item of listZplQueueItems()) {
    if (item.printerKey === printerKey && item.status === "unknown_after_send" && !item.operatorReviewedAt) {
      writeZplQueueItem({
        ...item,
        operatorReviewedAt: now,
        operatorReviewedBy: trimString(body.operator || body.adminName || "local-operator"),
        operatorReviewNote: trimString(body.note || body.reason || "")
      });
      reviewedCount += 1;
    }
  }

  state.paused = false;
  state.lastError = null;
  logInfo("zpl_queue_worker_resumed", { printerKey, reviewedCount }, `[PrintSvc] Direct ZPL worker resumed printerKey=${printerKey}`);
  setImmediate(() => startZplQueueWorkerForPrinter(printerKey));
  return { ok: true, printerKey, reviewedCount, message: "Direct-ZPL printer queue resumed." };
}

function findZplQueueItemForRetry(body = {}) {
  const itemId = trimString(body.itemId);
  const items = listZplQueueItems();

  if (itemId) {
    const item = items.find((candidate) => candidate.itemId === itemId);
    if (!item) throw httpError(404, "ZPL_QUEUE_ITEM_NOT_FOUND", `No ZPL queue item found for itemId='${itemId}'.`);
    return item;
  }

  const station = normalizeStation(body.station);
  const lotNumber = trimString(body.lotNumber);
  const boxRaw = body.box ?? body.boxNumber;
  const box = Number(boxRaw);

  if (!station || !lotNumber || !Number.isInteger(box)) {
    throw httpError(400, "VALIDATION_ERROR", "itemId or station + lotNumber + box is required.");
  }

  const matches = items.filter((item) =>
    String(item.station || "").toUpperCase() === station &&
    String(item.lotNumber || "").trim().toUpperCase() === lotNumber.toUpperCase() &&
    Number(item.box) === box
  );

  if (matches.length === 0) {
    throw httpError(404, "ZPL_QUEUE_ITEM_NOT_FOUND", `No ZPL queue item found for station=${station} lot=${lotNumber} box=${box}.`);
  }

  if (matches.length > 1) {
    const failedMatches = matches.filter((item) => item.status === "failed_before_send");
    if (failedMatches.length === 1) return failedMatches[0];
    const error = httpError(409, "ZPL_QUEUE_RETRY_AMBIGUOUS", "Multiple ZPL queue items match. Retry by itemId.");
    error.details = { itemIds: matches.map((item) => item.itemId) };
    throw error;
  }

  return matches[0];
}

function retryFailedZplQueueItem(body = {}) {
  recoverStaleSendingItems({ reason: "retry failed stale sending recovery" });
  const item = findZplQueueItemForRetry(body);

  if (item.status === "unknown_after_send") {
    const error = httpError(409, "ZPL_RETRY_NOT_ALLOWED", "unknown_after_send items may have printed and cannot be retried automatically.");
    error.details = { item: summarizeZplQueueItem(item), safeToRetry: false };
    throw error;
  }

  if (!isQueueItemSafeToRetry(item)) {
    const error = httpError(409, "ZPL_RETRY_NOT_ALLOWED", "Only failed_before_send queue items with writeStarted=false and bytesSent=0 can be retried.");
    error.details = { item: summarizeZplQueueItem(item), safeToRetry: false };
    throw error;
  }

  const next = writeZplQueueItem({
    ...item,
    status: "queued",
    queueSequence: nextZplQueueSequence(),
    retryRequestedAt: isoNow(),
    retryRequestedBy: trimString(body.operator || body.adminName || "local-operator"),
    retryReason: trimString(body.note || body.reason || ""),
    requeuedFromStatus: item.status,
    lastError: null
  });

  logInfo(
    "zpl_queue_failed_item_requeued",
    { station: next.station, lotNumber: next.lotNumber, box: next.box, rfid: next.rfid, printerIp: next.printerIp, printerPort: next.printerPort, itemId: next.itemId, jobId: next.jobId },
    `[PrintSvc] Direct ZPL failed_before_send item requeued itemId=${next.itemId} station=${next.station} lot=${next.lotNumber} box=${next.box}`
  );

  setImmediate(() => startZplQueueWorkerForPrinter(next.printerKey));
  return {
    ok: true,
    queued: true,
    itemId: next.itemId,
    jobId: next.jobId,
    station: next.station,
    lotNumber: next.lotNumber,
    box: next.box,
    rfid: next.rfid,
    printerKey: next.printerKey,
    message: "Direct-ZPL failed_before_send item requeued."
  };
}

function setDirectZplQueueSendFunction(fn) {
  directZplQueueSendFunction = typeof fn === "function" ? fn : sendDirectZplQueueItem;
}

function resetDirectZplQueueSendFunction() {
  directZplQueueSendFunction = sendDirectZplQueueItem;
}

function setZplSocketFactoryForTests(fn) {
  zplSocketFactoryForTests = typeof fn === "function" ? fn : null;
}

function resetZplSocketFactoryForTests() {
  zplSocketFactoryForTests = null;
}

function clearZplWorkerStateForTests() {
  for (const printerKey of Array.from(zplPersistentSockets.keys())) {
    closeZplPersistentSocket(printerKey, "test_reset");
  }
  zplQueueWorkers.clear();
  for (const timer of zplStaleSendingRecoveryTimers.values()) clearTimeout(timer);
  zplStaleSendingRecoveryTimers.clear();
  zplPrinterLastSendStartedAt.clear();
  zplSocketFactoryForTests = null;
}

function buildZplRenderDataFromNamed({ lotNumber, box, rfid, namedDataSources }) {
  const named = namedDataSources || {};
  const resolvedRfid = String(rfid || named.RFID || named.rfid || `${lotNumber}-B${pad2(box)}`);
  const sampleLabel = named.sampleLabel || named.samplelabel || named.firstbox || named.box || named.Box || box;
  return {
    lotNumber,
    boxNumber: String(box),
    rfid: resolvedRfid,
    pounds: named.pounds,
    materialType: named.type,
    color: named.color,
    po: named.po,
    productCode: named.prodnum || named.productCode || named.productcode,
    productName: named.prodname || named.productName || named.product,
    productDescription: named.proddesc || named.prodname || named.product,
    tolling: named.tolling,
    erp: named.erp,
    qrData: named.qrData || resolvedRfid,
    machine: named.machine,
    company: named.company,
    labelType: named.labeltype || named.labelType,
    sampleType: named.sampletype || named.sampleType,
    sampleTime: named.sampleTime || named.sampletime || sampleLabel,
    sampleLabel,
    frequencyCheck: named.frequencyCheck || named.frequencycheck || named.pounds || sampleLabel,
    printedDate: named.printedDate || named.printeddate || named.date || new Date().toLocaleDateString("en-US")
  };
}

function renderDirectZplDryRunLabel({ zpl, station, lotNumber, box, rfid, namedDataSources, requiresRfidEncoding = true }) {
  try {
    const data = buildZplRenderDataFromNamed({ lotNumber, box, rfid, namedDataSources });
    const renderedZpl = requiresRfidEncoding === false
      ? renderZplTemplateFileWithoutRfid(zpl.templatePath, data)
      : renderZplTemplateFile(zpl.templatePath, data);

    const summary = {
      station,
      lotNumber,
      box,
      rfid,
      rfidHex: requiresRfidEncoding === false ? null : rfidTextToHex(rfid),
      printerIp: zpl.printerIp,
      printerPort: zpl.port,
      templatePath: zpl.templatePath,
      renderedBytes: Buffer.byteLength(renderedZpl, "utf8")
    };

    if (isDebugZplEnabled()) {
      logInfo("zpl_rendered_payload", { ...summary, zpl: renderedZpl });
    }

    return summary;
  } catch (error) {
    if (error.code === "INVALID_RFID") {
      logError(
        "print_validation_error",
        { station, lotNumber, box, invalidRfid: rfid, reason: error.message },
        `[PrintSvc] Direct ZPL validation failed station=${station} lot=${lotNumber} box=${box} invalid rfid="${rfid}": ${error.message}`
      );
    }

    throw error;
  }
}

async function sendRenderedZplPayload({
  zpl,
  renderedZpl,
  station,
  lotNumber,
  box,
  sendZplOverTcpFn = null,
  queueDepth = null,
  item = null
}) {
  const socketMode = sendZplOverTcpFn ? "per_label" : getZplSocketMode();
  const printerKey = item?.printerKey || getZplQueueKey(zpl);
  logZplSendTiming({ printerKey, station, lotNumber, box, socketMode, queueDepth });

  if (socketMode === "persistent") {
    return sendZplOverPersistentSocket({
      printerKey,
      printerIp: zpl.printerIp,
      port: zpl.port,
      zpl: renderedZpl,
      timeoutMs: getZplTcpTimeoutMs(),
      queueDepth
    });
  }

  const sendFn = sendZplOverTcpFn || sendZplOverTcp;
  return sendFn({
    printerIp: zpl.printerIp,
    port: zpl.port,
    zpl: renderedZpl,
    timeoutMs: getZplTcpTimeoutMs()
  });
}

async function sendDirectZplLabel({ zpl, station, lotNumber, box, rfid, namedDataSources, sendZplOverTcpFn = null, queueDepth = null, item = null }) {
  const startedAt = Date.now();
  const settings = getZplTransportSettings();
  const maxAttempts = settings.connectRetryCount + 1;
  let renderedZpl;

  try {
    renderedZpl = renderZplTemplateFile(
      zpl.templatePath,
      buildZplRenderDataFromNamed({ lotNumber, box, rfid, namedDataSources })
    );
  } catch (error) {
    if (error.code === "INVALID_RFID") {
      logError(
        "print_validation_error",
        { station, lotNumber, box, invalidRfid: rfid, reason: error.message },
        `[PrintSvc] Direct ZPL validation failed station=${station} lot=${lotNumber} box=${box} invalid rfid="${rfid}": ${error.message}`
      );
    } else {
      logError(
        "zpl_print_error",
        { station, lotNumber, box, rfid, printerIp: zpl.printerIp, port: zpl.port, attemptNumber: 0, maxAttempts, durationMs: Date.now() - startedAt, code: error.code || null, message: error.message },
        `[PrintSvc] Direct ZPL render failed box=${box} rfid=${rfid} printer=${zpl.printerIp}:${zpl.port}: ${error.message}`
      );
    }

    throw error;
  }

  if (isDebugZplEnabled()) {
    logInfo("zpl_rendered_payload", { station, lotNumber, box, rfid, printerIp: zpl.printerIp, templatePath: zpl.templatePath, zpl: renderedZpl });
  }

  let lastError = null;

  for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
    const attemptStartedAt = Date.now();
    logEvent(
      "zpl_print_attempt",
      { station, lotNumber, box, rfid, printerIp: zpl.printerIp, port: zpl.port, templatePath: zpl.templatePath, attemptNumber, maxAttempts },
      `[PrintSvc] -> Direct ZPL PRINT box=${box} rfid=${rfid} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port} template="${zpl.templatePath}"`
    );

    try {
      const sendResult = await sendRenderedZplPayload({
        zpl,
        renderedZpl,
        station,
        lotNumber,
        box,
        sendZplOverTcpFn,
        queueDepth,
        item
      });

      const durationMs = sendResult.durationMs ?? (Date.now() - attemptStartedAt);
      const totalDurationMs = Date.now() - startedAt;
      logInfo(
        "zpl_print_success",
        {
          station,
          lotNumber,
          box,
          rfid,
          printerIp: zpl.printerIp,
          port: zpl.port,
          durationMs,
          totalDurationMs,
          attemptNumber,
          bytesSent: sendResult.bytesSent,
          socketClosed: sendResult.socketClosed === true,
          socketMode: sendResult.socketMode || "per_label",
          sendAccepted: true,
          physicalPrintConfirmed: false,
          note: "TCP send accepted; physical RFID print must be verified by operator/scanner."
        },
        `[PrintSvc] <- Direct ZPL TCP send accepted box=${box} rfid=${rfid} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port} durationMs=${durationMs} bytes=${sendResult.bytesSent}; physical print not confirmed`
      );

      return {
        box,
        rfid,
        status: "tcp_send_accepted",
        printerIp: zpl.printerIp,
        printerPort: zpl.port,
        durationMs,
        totalDurationMs,
        attemptNumber,
        bytesSent: sendResult.bytesSent,
        socketClosed: sendResult.socketClosed === true,
        socketMode: sendResult.socketMode || "per_label",
        sendAccepted: true,
        physicalPrintConfirmed: false
      };
    } catch (error) {
      lastError = error;
      const durationMs = error.details?.durationMs ?? (Date.now() - attemptStartedAt);
      const totalDurationMs = Date.now() - startedAt;
      if (zplSendMayHaveReachedPrinter(error)) {
        const unknownError = toZplSendUnknownError(error, { box });
        logError(
          "zpl_send_unknown",
          {
            station,
            lotNumber,
            box,
            rfid,
            printerIp: zpl.printerIp,
            port: zpl.port,
            durationMs,
            totalDurationMs,
            attemptNumber,
            maxAttempts,
            code: unknownError.code,
            originalCode: error.code || null,
            message: unknownError.message,
            retryable: false,
            operatorAction: unknownError.operatorAction
          },
          `[PrintSvc] Direct ZPL send unknown box=${box} rfid=${rfid} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port}: ${unknownError.message}`
        );
        throw unknownError;
      }

      const retryable = isRetryableZplTcpError(error);
      const code = error.code || error.details?.code || "ZPL_TCP_ERROR";

      logError(
        "zpl_print_error",
        {
          station,
          lotNumber,
          box,
          rfid,
          printerIp: zpl.printerIp,
          port: zpl.port,
          durationMs,
          totalDurationMs,
          attemptNumber,
          maxAttempts,
          code,
          message: error.message,
          retryable
        },
        `[PrintSvc] Direct ZPL failed box=${box} rfid=${rfid} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port}: ${error.message}`
      );

      if (!retryable || attemptNumber >= maxAttempts) {
        error.retryable = retryable;
        throw error;
      }

      await sleep(getZplRetryDelayMs());
    }
  }

  throw lastError;
}

async function sendDirectZplNonRfidLabel({ zpl, station, lotNumber, box, rfid, namedDataSources, sendZplOverTcpFn = null, queueDepth = null, item = null }) {
  const startedAt = Date.now();
  const settings = getZplTransportSettings();
  const maxAttempts = settings.connectRetryCount + 1;
  let renderedZpl;

  try {
    renderedZpl = renderZplTemplateFileWithoutRfid(
      zpl.templatePath,
      buildZplRenderDataFromNamed({ lotNumber, box, rfid, namedDataSources })
    );
  } catch (error) {
    logError(
      "zpl_print_error",
      { station, lotNumber, box, rfid, printerIp: zpl.printerIp, port: zpl.port, attemptNumber: 0, maxAttempts, durationMs: Date.now() - startedAt, code: error.code || null, message: error.message },
      `[PrintSvc] Direct ZPL render failed box=${box} rfid=${rfid} printer=${zpl.printerIp}:${zpl.port}: ${error.message}`
    );
    throw error;
  }

  if (isDebugZplEnabled()) {
    logInfo("zpl_rendered_payload", { station, lotNumber, box, rfid, printerIp: zpl.printerIp, templatePath: zpl.templatePath, zpl: renderedZpl });
  }

  let lastError = null;

  for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
    const attemptStartedAt = Date.now();
    logEvent(
      "zpl_print_attempt",
      { station, lotNumber, box, rfid, printerIp: zpl.printerIp, port: zpl.port, templatePath: zpl.templatePath, attemptNumber, maxAttempts, requiresRfidEncoding: false },
      `[PrintSvc] -> Direct ZPL SAMPLE PRINT box=${box} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port} template="${zpl.templatePath}"`
    );

    try {
      const sendResult = await sendRenderedZplPayload({
        zpl,
        renderedZpl,
        station,
        lotNumber,
        box,
        sendZplOverTcpFn,
        queueDepth,
        item
      });

      const durationMs = sendResult.durationMs ?? (Date.now() - attemptStartedAt);
      const totalDurationMs = Date.now() - startedAt;
      logInfo(
        "zpl_print_success",
        {
          station,
          lotNumber,
          box,
          rfid,
          printerIp: zpl.printerIp,
          port: zpl.port,
          durationMs,
          totalDurationMs,
          attemptNumber,
          bytesSent: sendResult.bytesSent,
          socketClosed: sendResult.socketClosed === true,
          socketMode: sendResult.socketMode || "per_label",
          sendAccepted: true,
          physicalPrintConfirmed: false,
          requiresRfidEncoding: false,
          note: "TCP send accepted; physical sample label print must be verified by operator."
        },
        `[PrintSvc] <- Direct ZPL TCP send accepted sample box=${box} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port} durationMs=${durationMs} bytes=${sendResult.bytesSent}; physical print not confirmed`
      );

      return {
        box,
        rfid,
        status: "tcp_send_accepted",
        printerIp: zpl.printerIp,
        printerPort: zpl.port,
        durationMs,
        totalDurationMs,
        attemptNumber,
        bytesSent: sendResult.bytesSent,
        socketClosed: sendResult.socketClosed === true,
        socketMode: sendResult.socketMode || "per_label",
        sendAccepted: true,
        physicalPrintConfirmed: false,
        requiresRfidEncoding: false
      };
    } catch (error) {
      lastError = error;
      const durationMs = error.details?.durationMs ?? (Date.now() - attemptStartedAt);
      const totalDurationMs = Date.now() - startedAt;
      if (zplSendMayHaveReachedPrinter(error)) {
        const unknownError = toZplSendUnknownError(error, { box });
        logError(
          "zpl_send_unknown",
          {
            station,
            lotNumber,
            box,
            rfid,
            printerIp: zpl.printerIp,
            port: zpl.port,
            durationMs,
            totalDurationMs,
            attemptNumber,
            maxAttempts,
            code: unknownError.code,
            originalCode: error.code || null,
            message: unknownError.message,
            retryable: false,
            operatorAction: unknownError.operatorAction,
            requiresRfidEncoding: false
          },
          `[PrintSvc] Direct ZPL sample send unknown box=${box} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port}: ${unknownError.message}`
        );
        throw unknownError;
      }

      const retryable = isRetryableZplTcpError(error);
      const code = error.code || error.details?.code || "ZPL_TCP_ERROR";

      logError(
        "zpl_print_error",
        {
          station,
          lotNumber,
          box,
          rfid,
          printerIp: zpl.printerIp,
          port: zpl.port,
          durationMs,
          totalDurationMs,
          attemptNumber,
          maxAttempts,
          code,
          message: error.message,
          retryable,
          requiresRfidEncoding: false
        },
        `[PrintSvc] Direct ZPL sample failed box=${box} attempt=${attemptNumber}/${maxAttempts} printer=${zpl.printerIp}:${zpl.port}: ${error.message}`
      );

      if (!retryable || attemptNumber >= maxAttempts) {
        error.retryable = retryable;
        throw error;
      }

      await sleep(getZplRetryDelayMs());
    }
  }

  throw lastError;
}

async function sendDirectZplQueueItem(args) {
  if (args?.requiresRfidEncoding === false || args?.item?.requiresRfidEncoding === false) {
    return sendDirectZplNonRfidLabel(args);
  }
  return sendDirectZplLabel(args);
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

  const resolved = resolveRfidPrintTargetForFamily({
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
    printEngine: resolved.printEngine,
    zpl: resolved.zpl,
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

function getAuditPrintEngine(validated) {
  if (validated?.printEngine) return validated.printEngine;
  try {
    return getConfiguredPrintEngine();
  } catch {
    return String(process.env.PRINT_ENGINE || "bartender").trim().toLowerCase();
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
    printEngine: getAuditPrintEngine(validated),
    zplPrinterIp: validated?.zpl?.printerIp || "",
    zplTemplatePath: validated?.zpl?.templatePath || "",
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

app.get("/offline/print-health", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(path.join(OFFLINE_PUBLIC_DIR, "print-health.html"));
});

app.get("/offline/template-lab", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(path.join(OFFLINE_PUBLIC_DIR, "template-lab.html"));
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
    lockKey = validated.dryRun || validated.printEngine === "zpl" ? null : `offline|${validated.station}|${validated.lotNumber}`;

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
      const zplPreview = validated.printEngine === "zpl" ? [] : null;
      if (validated.printEngine === "zpl") {
        for (let box = validated.firstBox; box <= validated.lastBox; box++) {
          const namedDataSources = buildOfflineNamedDataSources(req.body || {}, validated.lotNumber, box);
          zplPreview.push(renderDirectZplDryRunLabel({
            zpl: validated.zpl,
            station: validated.station,
            lotNumber: validated.lotNumber,
            box,
            rfid: namedDataSources.RFID,
            namedDataSources
          }));
        }
      }

      writeOfflineAudit("offline_print_dry_run", req, buildOfflinePrintAuditDetails(validated, req.body, {
        printedCount: 0,
        preview,
        zplPreview,
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
        printEngine: validated.printEngine,
        zplPrinterIp: validated.zpl?.printerIp || null,
        zplPrinterPort: validated.zpl?.port || null,
        zplTemplatePath: validated.zpl?.templatePath || null,
        lotNumber: validated.lotNumber,
        firstBox: validated.firstBox,
        lastBox: validated.lastBox,
        requestedCount: validated.requestedCount,
        preview,
        zplPreview
      });
    }

    if (validated.printEngine === "zpl") {
      const jobId = makeZplJobId();
      const items = [];
      const requestedBoxes = [];

      for (let box = validated.firstBox; box <= validated.lastBox; box++) {
        const namedDataSources = buildOfflineNamedDataSources(req.body || {}, validated.lotNumber, box);
        const rfid = namedDataSources.RFID;
        requestedBoxes.push(box);
        items.push(buildZplQueueItem({
          jobId,
          station: validated.station,
          family: validated.family,
          lotNumber: validated.lotNumber,
          box,
          rfid,
          zpl: validated.zpl,
          namedDataSources
        }));
      }

      const { queuedItems, skippedDuplicates } = enqueueNormalDirectZplQueueItems(items);
      writeOfflineAudit("offline_print_queued", req, buildOfflinePrintAuditDetails(validated, req.body, {
        printedCount: 0,
        queuedCount: queuedItems.length,
        skippedDuplicateCount: skippedDuplicates.length,
        jobId,
        itemIds: queuedItems.map((item) => item.itemId),
        ok: true
      }));

      return res.json(buildDirectZplQueueResponse({
        jobId,
        station: validated.station,
        requestedFamily: validated.requestedFamily,
        family: validated.family,
        lotNumber: validated.lotNumber,
        requestedBoxes,
        firstBox: validated.firstBox,
        lastBox: validated.lastBox,
        requestedCount: requestedBoxes.length,
        printerIp: validated.zpl.printerIp,
        printerPort: validated.zpl.port,
        templatePath: validated.zpl.templatePath,
        queuedItems,
        skippedDuplicates
      }));
    }

    const results = [];
    let printedCount = 0;
    const printJobSpacingMs = getSafePrintJobSpacingMs();
    const zplLabelSpacingMs = getZplLabelSpacingMs();
    const queueKey = validated.printEngine === "zpl" ? getZplQueueKey(validated.zpl) : validated.printer;

    await enqueuePrinterWork(queueKey, async () => {
      if (validated.printEngine === "zpl") {
        const requestScope = getRequestScopeFromCount(validated.requestedCount);
        logInfo(
          "zpl_queue_start",
          { station: validated.station, lotNumber: validated.lotNumber, printerIp: validated.zpl.printerIp, printerPort: validated.zpl.port, firstBox: validated.firstBox, lastBox: validated.lastBox, requestedCount: validated.requestedCount, requestScope, labelSpacingMs: zplLabelSpacingMs },
          `[OfflinePrint] Direct ZPL queue start scope=${requestScope} station=${validated.station} lot=${validated.lotNumber} printer=${validated.zpl.printerIp}:${validated.zpl.port}`
        );
      }

      try {
        for (let box = validated.firstBox; box <= validated.lastBox; box++) {
          const namedDataSources = buildOfflineNamedDataSources(req.body || {}, validated.lotNumber, box);
          const rfid = namedDataSources.RFID;

          if (validated.printEngine === "zpl") {
            try {
              assertNoRecentZplDuplicate({
                station: validated.station,
                lotNumber: validated.lotNumber,
                box,
                rfid
              });

              const result = await sendDirectZplLabel({
                zpl: validated.zpl,
                station: validated.station,
                lotNumber: validated.lotNumber,
                box,
                rfid,
                namedDataSources
              });

              printedCount += 1;
              results.push(result);
              markRecentZplSendAccepted({
                station: validated.station,
                lotNumber: validated.lotNumber,
                box,
                rfid
              });

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
              decorateZplPartialFailure(error, { results, failedBox: box });
              throw error;
            }

            await sleep(zplLabelSpacingMs);
            continue;
          }

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
      } finally {
        if (validated.printEngine === "zpl") {
          const requestScope = getRequestScopeFromCount(validated.requestedCount);
          logInfo(
            "zpl_queue_complete",
            { station: validated.station, lotNumber: validated.lotNumber, printerIp: validated.zpl.printerIp, printerPort: validated.zpl.port, printedCount, requestScope },
            `[OfflinePrint] Direct ZPL queue complete scope=${requestScope} station=${validated.station} lot=${validated.lotNumber} printer=${validated.zpl.printerIp}:${validated.zpl.port} printed=${printedCount}`
          );
        }
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
      printEngine: validated.printEngine,
      zplPrinterIp: validated.zpl?.printerIp || null,
      zplPrinterPort: validated.zpl?.port || null,
      zplTemplatePath: validated.zpl?.templatePath || null,
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
      ...buildErrorResponsePayload(error, "OFFLINE_PRINT_FAILED"),
      bartender: error.response?.data || undefined
    });
  } finally {
    if (lockKey) activePrintJobs.delete(lockKey);
  }
});

app.get("/api/print/zpl-queue", requireOfflineLocalAccess, (req, res) => {
  try {
    return res.json(getZplQueueStatusPayload());
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "ZPL_QUEUE_STATUS_ERROR", message: error.message });
  }
});

app.get("/api/print/logs", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(getPrintLogsPayload(req.query || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "PRINT_LOG_READ_ERROR", message: error.message });
  }
});

app.get("/api/print/template-lab/catalog", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(getTemplateLabCatalogPayload());
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_CATALOG_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.get("/api/print/template-lab/template-geometry", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(getTemplateLabTemplateGeometryPayload(req.query || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_GEOMETRY_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/profile", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(saveTemplateLabProfileOverrides(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_PROFILE_SAVE_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/profile/reset", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(resetTemplateLabProfileOverrides(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_PROFILE_RESET_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/promote", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(promoteTemplateLabDynamicTemplate(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_PROMOTE_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.get("/api/print/template-lab/logo-assets", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(listTemplateLabLogoAssets());
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_LOGO_ASSETS_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/logo-assets", requireOfflineLocalAccess, upload.single("file"), (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(storeTemplateLabLogoAsset(req.file, req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_LOGO_UPLOAD_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/logo-assets/select", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(loadTemplateLabLogoAsset(req.body?.assetName || req.body?.logoAssetName, req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_LOGO_SELECT_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-lab/calibration-test-send", requireOfflineLocalAccess, async (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(await sendTemplateLabCalibrationGrid(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_LAB_CALIBRATION_SEND_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.get("/api/print/zpl-template-validation", requireOfflineLocalAccess, (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(validateDirectZplTemplates());
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "ZPL_TEMPLATE_VALIDATION_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.get("/api/print/template-preview", requireOfflineLocalAccess, async (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(await buildTemplatePreviewPayload(req.query || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_PREVIEW_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-preview", requireOfflineLocalAccess, async (req, res) => {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.json(await buildTemplatePreviewPayload(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_PREVIEW_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/template-test-send", requireOfflineLocalAccess, async (req, res) => {
  const body = req.body || {};
  if (body.confirmTestPrint !== true) {
    return res.status(400).json({
      ok: false,
      error: "TEMPLATE_TEST_CONFIRM_REQUIRED",
      message: "confirmTestPrint:true is required before sending rendered ZPL directly to a printer."
    });
  }

  const printerIp = trimString(body.printerIp || body.host);
  const printerPort = Number(body.port || body.printerPort || 9100);
  if (!printerIp) {
    return res.status(400).json({ ok: false, error: "VALIDATION_ERROR", message: "printerIp is required." });
  }
  if (!Number.isInteger(printerPort) || printerPort <= 0 || printerPort > 65535) {
    return res.status(400).json({ ok: false, error: "VALIDATION_ERROR", message: "port must be a valid TCP port." });
  }

  let preview = null;
  try {
    preview = await buildTemplatePreviewPayload(body);
    const bytes = Buffer.byteLength(preview.renderedZpl, "utf8");
    logInfo("template_test_send_attempt", {
      template: preview.template,
      profileKey: preview.profileKey,
      printerIp,
      printerPort,
      lotNumber: preview.sampleData.lotNumber,
      box: preview.sampleData.boxNumber,
      bytes
    });

    const sendFn = templateTestSendFunctionForTests || sendZplOverTcp;
    const startedAt = Date.now();
    const sendResult = await sendFn({
      printerIp,
      port: printerPort,
      zpl: preview.renderedZpl,
      timeoutMs: getZplTcpTimeoutMs()
    });

    logInfo("template_test_send_success", {
      template: preview.template,
      profileKey: preview.profileKey,
      printerIp,
      printerPort,
      lotNumber: preview.sampleData.lotNumber,
      box: preview.sampleData.boxNumber,
      bytes,
      durationMs: Date.now() - startedAt,
      bytesSent: sendResult?.bytesSent ?? bytes,
      sendAccepted: true,
      physicalPrintConfirmed: false
    });

    return res.json({
      ok: true,
      testPrint: true,
      queued: false,
      template: preview.template,
      profileKey: preview.profileKey,
      printerIp,
      printerPort,
      bytesSent: sendResult?.bytesSent ?? bytes,
      sendAccepted: true,
      physicalPrintConfirmed: false,
      message: "Template test ZPL sent directly to printer. This bypassed the production queue."
    });
  } catch (error) {
    logError("template_test_send_error", {
      template: preview?.template || trimString(body.template || body.templateName),
      profileKey: preview?.profileKey || trimString(body.profileKey),
      printerIp,
      printerPort,
      lotNumber: preview?.sampleData?.lotNumber || trimString(body.lotNumber),
      box: preview?.sampleData?.boxNumber || trimString(body.boxNumber || body.box),
      message: error.message,
      code: error.code || null
    });
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "TEMPLATE_TEST_SEND_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/zpl-queue/resume", requireOfflineLocalAccess, (req, res) => {
  try {
    return res.json(resumeZplQueue(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "ZPL_QUEUE_RESUME_ERROR", message: error.message, details: error.details || undefined });
  }
});

app.post("/api/print/zpl-queue/retry-failed", requireOfflineLocalAccess, (req, res) => {
  try {
    return res.json(retryFailedZplQueueItem(req.body || {}));
  } catch (error) {
    return res.status(error.statusCode || 500).json({ ok: false, error: error.code || "ZPL_QUEUE_RETRY_FAILED_ERROR", message: error.message, details: error.details || undefined });
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

startAllZplQueueWorkers();

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

function buildSampleNamedDataSources({ lotNumber, box, rfid = "", pounds = "", lotLabelData = {}, labelKind, byPounds = false }) {
  const kind = normalizeSampleLabelKind(labelKind);
  return {
    lot: lotNumber,
    firstbox: String(box),
    box: String(box),
    Box: String(box),
    RFID: String(rfid || ""),
    rfid: String(rfid || ""),
    pounds: pounds == null ? "" : String(pounds),
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
    labeltype: kind === "QCRetain" ? "Retain Sample" : "QC Sample",
    sampletype: kind === "QCRetain" ? "Retain" : "QC",
    sampleLabel: String(box),
    frequencyCheck: byPounds ? String(pounds || box) : "",
    erp: ""
  };
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
    const printTarget = resolveSamplePrintTarget({ station, labelKind, byPounds });
    const { printer, template } = printTarget;

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
        const zplPreview = printTarget.printEngine === "zpl" ? [] : null;
        if (printTarget.printEngine === "zpl") {
          const dryRunLotLabelData = await getLotLabelData(baseUrl, effectiveLotId, { includeMachine: true, includeCompany: true });
          for (const labelValue of requestedPoundLabels) {
            const namedDataSources = buildSampleNamedDataSources({
              lotNumber,
              box: labelValue,
              pounds: labelValue,
              lotLabelData: dryRunLotLabelData,
              labelKind,
              byPounds: true
            });
            zplPreview.push(renderDirectZplDryRunLabel({
              zpl: printTarget.zpl,
              station,
              lotNumber,
              box: labelValue,
              rfid: "",
              namedDataSources,
              requiresRfidEncoding: false
            }));
          }
        }

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
          ...(printTarget.printEngine === "zpl" ? {
            printEngine: printTarget.printEngine,
            zplPrinterIp: printTarget.zpl.printerIp,
            zplPrinterPort: printTarget.zpl.port,
            zplTemplatePath: printTarget.zpl.templatePath
          } : {}),
          requestedPoundLabels,
          requestedCount: requestedPoundLabels.length,
          missingBoxes: [],
          ...(printTarget.printEngine === "zpl" ? { zplPreview } : {})
        });
      }

      const lotLabelData = await getLotLabelData(baseUrl, effectiveLotId, { includeMachine: true, includeCompany: true });
      const printedBy = req.user?.preferred_username || req.user?.upn || "";
      const results = [];
      const printJobSpacingMs = getSafePrintJobSpacingMs();

      logInfo(
        "sample_print_pounds_sequence_resolved",
        { station, lotNumber, labelKind, printer, template, printEngine: printTarget.printEngine, zplPrinterIp: printTarget.zpl?.printerIp || null, requestedPoundLabels, printJobSpacingMs },
        `[PrintSvc] Sample-label by-pounds sequence resolved station=${station} lot=${lotNumber} kind=${labelKind}: ${requestedPoundLabels.join(",")}`
      );

      if (printTarget.printEngine === "zpl") {
        const jobId = makeZplJobId();
        const items = [];

        for (const poundLabel of requestedPoundLabels) {
          const labelValue = String(poundLabel);
          const logRfid = `${lotNumber}-${labelValue.replace(/\s+/g, "")}`;
          const namedDataSources = buildSampleNamedDataSources({
            lotNumber,
            box: labelValue,
            rfid: "",
            pounds: labelValue,
            lotLabelData,
            labelKind,
            byPounds: true
          });

          items.push(buildZplQueueItem({
            jobId,
            station,
            family: printTarget.directZplFamily,
            lotNumber,
            box: labelValue,
            rfid: logRfid,
            zpl: printTarget.zpl,
            namedDataSources,
            requiresRfidEncoding: false,
            labelKind,
            sampleByPounds: true,
            printLog: {
              baseUrl,
              lotId: effectiveLotId,
              inventoryId: null,
              printedBy,
              successResult: `Success-${labelKind}-Pounds`,
              successNotes: `By-pounds sample label: ${labelValue}`,
              failedResult: `Failed-${labelKind}-Pounds`
            }
          }));
        }

        const queuedItems = enqueueDirectZplQueueItems(items);
        return res.json(buildDirectZplQueueResponse({
          jobId,
          station,
          requestedFamily: labelKind,
          family: printTarget.directZplFamily,
          lotNumber,
          requestedBoxes: requestedPoundLabels,
          firstBox: requestedPoundLabels[0],
          lastBox: requestedPoundLabels[requestedPoundLabels.length - 1],
          requestedCount: requestedPoundLabels.length,
          missingBoxes: [],
          printerIp: printTarget.zpl.printerIp,
          printerPort: printTarget.zpl.port,
          templatePath: printTarget.zpl.templatePath,
          queuedItems,
          skippedDuplicates: [],
          extra: {
            baseUrl,
            lotId: normalizeGuid(effectiveLotId),
            labelKind,
            byPounds: true,
            requestedPoundLabels,
            printEngine: printTarget.printEngine,
            printer,
            template
          }
        }));
      }

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
      const zplPreview = printTarget.printEngine === "zpl" ? [] : null;
      if (printTarget.printEngine === "zpl") {
        const dryRunLotLabelData = await getLotLabelData(baseUrl, effectiveLotId, { includeMachine: true, includeCompany: true });
        for (const box of requestedBoxes) {
          const row = byBox.get(box);
          if (!row) continue;
          const rfid = row[DV_INV_RFID_COL] || `${lotNumber}-B${pad2(box)}`;
          const poundsVal = row[DV_INV_WEIGHT_COL];
          const isNoWeight = isTruthyDataverseBoolean(row[DV_INV_NOWEIGHT_COL]);
          const namedDataSources = buildSampleNamedDataSources({
            lotNumber,
            box,
            rfid,
            pounds: isNoWeight ? "_" : (poundsVal == null ? "" : String(poundsVal)),
            lotLabelData: dryRunLotLabelData,
            labelKind,
            byPounds: false
          });
          zplPreview.push(renderDirectZplDryRunLabel({
            zpl: printTarget.zpl,
            station,
            lotNumber,
            box,
            rfid,
            namedDataSources,
            requiresRfidEncoding: false
          }));
        }
      }

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
        ...(printTarget.printEngine === "zpl" ? {
          printEngine: printTarget.printEngine,
          zplPrinterIp: printTarget.zpl.printerIp,
          zplPrinterPort: printTarget.zpl.port,
          zplTemplatePath: printTarget.zpl.templatePath
        } : {}),
        requestedBoxes,
        requestedCount: requestedBoxes.length,
        foundCount: rows.length,
        missingBoxes,
        ...(printTarget.printEngine === "zpl" ? { zplPreview } : {})
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
      { station, lotNumber, labelKind, printer, template, printEngine: printTarget.printEngine, zplPrinterIp: printTarget.zpl?.printerIp || null, requestedBoxes, printJobSpacingMs },
      `[PrintSvc] Sample-label sequence resolved station=${station} lot=${lotNumber} kind=${labelKind}: ${requestedBoxes.join(",")}`
    );

    if (printTarget.printEngine === "zpl") {
      const jobId = makeZplJobId();
      const items = [];

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
        const namedDataSources = buildSampleNamedDataSources({
          lotNumber,
          box,
          rfid,
          pounds: isNoWeight ? "_" : (poundsVal == null ? "" : String(poundsVal)),
          lotLabelData,
          labelKind,
          byPounds: false
        });

        items.push(buildZplQueueItem({
          jobId,
          station,
          family: printTarget.directZplFamily,
          lotNumber,
          box,
          rfid,
          zpl: printTarget.zpl,
          namedDataSources,
          requiresRfidEncoding: false,
          labelKind,
          sampleByPounds: false,
          printLog: {
            baseUrl,
            lotId: effectiveLotId,
            inventoryId,
            printedBy,
            successResult: `Success-${labelKind}`,
            successNotes: "",
            failedResult: `Failed-${labelKind}`
          }
        }));
      }

      const queuedItems = enqueueDirectZplQueueItems(items);
      return res.json(buildDirectZplQueueResponse({
        jobId,
        station,
        requestedFamily: labelKind,
        family: printTarget.directZplFamily,
        lotNumber,
        requestedBoxes,
        firstBox,
        lastBox,
        requestedCount: requestedBoxes.length,
        missingBoxes,
        printerIp: printTarget.zpl.printerIp,
        printerPort: printTarget.zpl.port,
        templatePath: printTarget.zpl.templatePath,
        queuedItems,
        skippedDuplicates: [],
        extra: {
          baseUrl,
          lotId: normalizeGuid(effectiveLotId),
          labelKind,
          byPounds: false,
          printEngine: printTarget.printEngine,
          printer,
          template
        }
      }));
    }

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
    return res.status(e.statusCode || 500).json({ ok: false, error: e.code || "SAMPLE_PRINT_FAILED", message: e.message, details: e.details || undefined, bartender: e.response?.data || null });
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

    const lotNumber = await getLotNumberById(baseUrl, effectiveLotId);
    const printTarget = resolveRfidPrintTarget({ station, lotNumber });
    const { family, printer, template } = printTarget;

    // BarTender keeps the existing duplicate-print request lock. Direct-ZPL
    // uses the per-printer queue so separate one-box ERP requests wait their turn.
    if (!dryRun && printTarget.printEngine !== "zpl") {
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
    }

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
      const zplPreview = printTarget.printEngine === "zpl" ? [] : null;
      if (printTarget.printEngine === "zpl") {
        const dryRunLotLabelData = await getLotLabelData(baseUrl, effectiveLotId);

        for (const box of requestedBoxes) {
          const row = byBox.get(box);
          if (!row) continue;

          const rfid = row[DV_INV_RFID_COL] || `${lotNumber}-B${pad2(box)}`;
          const poundsVal = row[DV_INV_WEIGHT_COL];
          const isNoWeight = isTruthyDataverseBoolean(row[DV_INV_NOWEIGHT_COL]);
          const named = {
            lot: lotNumber,
            firstbox: String(box),
            RFID: String(rfid),
            pounds: isNoWeight ? "_" : (poundsVal == null ? "" : String(poundsVal)),
            po: dryRunLotLabelData.po,
            prodname: dryRunLotLabelData.prodname,
            proddesc: dryRunLotLabelData.proddesc,
            prodnum: dryRunLotLabelData.prodnum,
            product: dryRunLotLabelData.product,
            color: dryRunLotLabelData.color,
            type: dryRunLotLabelData.type,
            tolling: dryRunLotLabelData.tolling,
            erp: ""
          };

          zplPreview.push(renderDirectZplDryRunLabel({
            zpl: printTarget.zpl,
            station,
            lotNumber,
            box,
            rfid,
            namedDataSources: named
          }));
        }
      }

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
        printEngine: printTarget.printEngine,
        zplPrinterIp: printTarget.zpl?.printerIp || null,
        zplPrinterPort: printTarget.zpl?.port || null,
        zplTemplatePath: printTarget.zpl?.templatePath || null,
        firstBox: fb,
        lastBox: lb,
        requestedCount: requestedBoxes.length,
        foundCount: rows.length,
        missingBoxes,
        zplPreview
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

    if (printTarget.printEngine === "zpl") {
      const jobId = makeZplJobId();
      const items = [];
      const queuedBoxes = [];

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

        const namedDataSources = {
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

        queuedBoxes.push(box);
        items.push(buildZplQueueItem({
          jobId,
          station,
          family,
          lotNumber,
          box,
          rfid,
          zpl: printTarget.zpl,
          namedDataSources,
          printLog: {
            baseUrl,
            lotId: effectiveLotId,
            inventoryId,
            printedBy
          }
        }));
      }

      const { queuedItems, skippedDuplicates } = enqueueNormalDirectZplQueueItems(items);
      return res.json(buildDirectZplQueueResponse({
        jobId,
        station,
        family,
        lotNumber,
        requestedBoxes,
        firstBox: fb,
        lastBox: lb,
        requestedCount: requestedBoxes.length,
        missingBoxes,
        printerIp: printTarget.zpl.printerIp,
        printerPort: printTarget.zpl.port,
        templatePath: printTarget.zpl.templatePath,
        queuedItems,
        skippedDuplicates,
        extra: {
          baseUrl,
          lotId: normalizeGuid(effectiveLotId)
        }
      }));
    }

    const results = [];
    const printJobSpacingMs = getSafePrintJobSpacingMs();
    const zplLabelSpacingMs = getZplLabelSpacingMs();
    const queueKey = printTarget.printEngine === "zpl" ? getZplQueueKey(printTarget.zpl) : printer;

    logInfo(
      "print_sequence_resolved",
      { station, lotNumber, printer, printEngine: printTarget.printEngine, zplPrinterIp: printTarget.zpl?.printerIp || null, firstBox: fb, lastBox: lb, requestedBoxes, printJobSpacingMs, zplLabelSpacingMs },
      `[PrintSvc] Print sequence resolved station=${station} lot=${lotNumber}: ${requestedBoxes.join(",")}`
    );

    await enqueuePrinterWork(queueKey, async () => {
      if (printTarget.printEngine === "zpl") {
        const requestScope = getRequestScopeFromCount(requestedBoxes.length);
        logInfo(
          "zpl_queue_start",
          { station, lotNumber, printerIp: printTarget.zpl.printerIp, printerPort: printTarget.zpl.port, requestedBoxes, requestedCount: requestedBoxes.length, requestScope, labelSpacingMs: zplLabelSpacingMs },
          `[PrintSvc] Direct ZPL queue start scope=${requestScope} station=${station} lot=${lotNumber} printer=${printTarget.zpl.printerIp}:${printTarget.zpl.port}`
        );
      }

      try {
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
            if (printTarget.printEngine === "zpl") {
              assertNoRecentZplDuplicate({ station, lotNumber, box, rfid });

              const result = await sendDirectZplLabel({
                zpl: printTarget.zpl,
                station,
                lotNumber,
                box,
                rfid,
                namedDataSources: named
              });

              results.push({ ...result, pounds: named.pounds });
              markRecentZplSendAccepted({ station, lotNumber, box, rfid });

              await writePrintLog(baseUrl, {
                lotId: effectiveLotId,
                inventoryId,
                rfid,
                station,
                printedBy,
                result: "Success",
                notes: "Direct ZPL"
              });
            } else {
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
            }
          } catch (e) {
            const msg = e.response?.data ? JSON.stringify(e.response.data) : e.message;
            if (printTarget.printEngine !== "zpl") {
              logError("print_failure", { station, lotNumber, box, rfid, printer, template, message: msg }, `[PrintSvc] FAILED box=${box} lot=${lotNumber} station=${station}: ${msg}`);
            }

            await writePrintLog(baseUrl, {
              lotId: effectiveLotId,
              inventoryId,
              rfid,
              station,
              printedBy,
              result: "Failed",
              notes: msg
            });

            if (printTarget.printEngine === "zpl") {
              decorateZplPartialFailure(e, { results, failedBox: box });
            }

            throw e;
          }

          await sleep(printTarget.printEngine === "zpl" ? zplLabelSpacingMs : printJobSpacingMs);
        }
      } finally {
        if (printTarget.printEngine === "zpl") {
          const requestScope = getRequestScopeFromCount(requestedBoxes.length);
          logInfo(
            "zpl_queue_complete",
            { station, lotNumber, printerIp: printTarget.zpl.printerIp, printerPort: printTarget.zpl.port, printedCount: results.length, requestScope },
            `[PrintSvc] Direct ZPL queue complete scope=${requestScope} station=${station} lot=${lotNumber} printer=${printTarget.zpl.printerIp}:${printTarget.zpl.port} printed=${results.length}`
          );
        }
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
      printEngine: printTarget.printEngine,
      zplPrinterIp: printTarget.zpl?.printerIp || null,
      zplPrinterPort: printTarget.zpl?.port || null,
      zplTemplatePath: printTarget.zpl?.templatePath || null,
      firstBox: fb,
      lastBox: lb,
      requestedCount: requestedBoxes.length,
      printedCount: results.length,
      missingBoxes,
      results
    });
  } catch (e) {
    logError("print_request_failed", { message: e.message, code: e.code || null, bartender: e.response?.data || null, lockKey });
    return res.status(e.statusCode || 500).json(buildErrorResponsePayload(e, "PRINT_FAILED"));
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
  assertNoRecentZplDuplicate,
  buildErrorResponsePayload,
  buildDirectZplQueueResponse,
  buildZplQueueItem,
  buildOfflineNamedDataSources,
  clearRecentZplDuplicateGuard,
  clearZplWorkerStateForTests,
  decorateZplPartialFailure,
  enqueuePrinterWork,
  enqueueDirectZplQueueItems,
  enqueueNormalDirectZplQueueItems,
  generateOfflineRfid,
  getConfiguredPrintEngine,
  getDirectZplEnabledScopes,
  getZplBatchCollectMs,
  getZplBatchInterBatchDelayMs,
  getZplBatchMaxBytes,
  getZplBatchMaxLabels,
  getZplDuplicatePolicy,
  getZplMaxLabelsPerConnection,
  getZplSocketIdleCloseMs,
  getZplSocketMode,
  getZplStaleSendingThresholdMs,
  getZplTransportSettings,
  getZplQueueStatusPayload,
  getZplPersistentSocketStatusForAll,
  getTemplateLabCatalogPayload,
  applyGlobalTemplateLabTransform,
  applyTemplateLabDynamicSourceOverrides,
  applyFieldGeometryOverridesToTemplateSource,
  buildCalibrationGridZpl,
  getTemplateLabTemplateGeometryPayload,
  parseTemplateLabSourceGeometry,
  promoteTemplateLabDynamicTemplate,
  resetTemplateLabProfileOverrides,
  saveTemplateLabProfileOverrides,
  isQueueItemSafeToRetry,
  markRecentZplSendAccepted,
  normalizeOfflineFamily,
  recoverStaleSendingItems,
  resolvePrinterAndTemplate,
  resolvePrinterAndSampleTemplate,
  resolvePrinterAndTemplateForFamily,
  resolveRfidPrintTarget,
  resolveRfidPrintTargetForFamily,
  resolveSamplePrintTarget,
  resolveZplPrinterAndTemplate,
  resetDirectZplQueueSendFunction,
  resetTemplateTestSendFunction,
  retryFailedZplQueueItem,
  resumeZplQueue,
  sendDirectZplLabel,
  sendDirectZplNonRfidLabel,
  setDirectZplQueueSendFunction,
  setTemplateTestSendFunction,
  setZplSocketFactoryForTests,
  validateDirectZplTemplates,
  resetZplSocketFactoryForTests,
  startAllZplQueueWorkers,
  startZplQueueWorkerForPrinter
};
