const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const http = require("http");
const net = require("net");
const os = require("os");
const path = require("path");
const { EventEmitter } = require("events");
const { isOfflineLocalAccessAllowed } = require("../lib/offlineSecurity");

let apiServer;
let apiBaseUrl;
let bartenderServer;
let bartenderBaseUrl;
let tempDir;
let stateFile;
let auditFile;
let queueDir;
let logFile;
let bartenderRequests = [];
let serverModule;

function listen(server, host = "127.0.0.1") {
  return new Promise((resolve) => {
    server.listen(0, host, () => resolve(server.address().port));
  });
}

function close(server) {
  return new Promise((resolve, reject) => {
    if (!server?.listening) return resolve();
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function request(method, route, { body, cookie } = {}) {
  const headers = {};
  const options = { method, headers };

  if (cookie) headers.cookie = cookie;
  if (body !== undefined) {
    headers["content-type"] = "application/json";
    options.body = JSON.stringify(body);
  }

  const response = await fetch(`${apiBaseUrl}${route}`, options);
  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  return { response, status: response.status, headers: response.headers, text, json };
}

function offlinePayload(overrides = {}) {
  return {
    station: "P2",
    family: "AUTO",
    lotNumber: "PL123456",
    firstBox: 1,
    lastBox: 2,
    material: "ABS",
    color: "BLACK",
    format: "RG",
    productCode: "ABS001",
    productName: "ABS Regrind",
    productDescription: "ABS Regrind",
    purchaseOrder: "PO12345",
    customer: "Customer Name",
    tolling: false,
    pounds: "_",
    operator: "John Smith",
    reason: "Internet outage",
    confirmationAccepted: true,
    dryRun: false,
    ...overrides
  };
}

function resetZplQueueTestState() {
  if (queueDir) fs.rmSync(queueDir, { recursive: true, force: true });
  serverModule?.clearZplWorkerStateForTests?.();
  serverModule?.clearRecentZplDuplicateGuard?.();
  serverModule?.resetDirectZplQueueSendFunction?.();
  serverModule?.resetZplSocketFactoryForTests?.();
  serverModule?.resetTemplateTestSendFunction?.();
  if (tempDir) fs.rmSync(path.join(tempDir, "template-lab-profiles.json"), { force: true });
}

function waitForCondition(predicate, timeoutMs = 1000) {
  const startedAt = Date.now();
  return new Promise((resolve, reject) => {
    function tick() {
      try {
        if (predicate()) return resolve();
      } catch (error) {
        return reject(error);
      }
      if (Date.now() - startedAt > timeoutMs) {
        return reject(new Error("Timed out waiting for condition."));
      }
      setTimeout(tick, 10);
    }
    tick();
  });
}

function zplQueueItem(overrides = {}) {
  const jobId = overrides.jobId || `job-${Date.now()}`;
  const box = overrides.box || 1;
  const lotNumber = overrides.lotNumber || "PT000086";
  const rfid = overrides.rfid || `${lotNumber}-B${String(box).padStart(2, "0")}`;

  return serverModule.buildZplQueueItem({
    jobId,
    station: "P1",
    family: "RAW",
    lotNumber,
    box,
    rfid,
    zpl: {
      printerIp: "127.0.0.1",
      port: 9100,
      templatePath: path.join(tempDir, "queue-label.template.zpl")
    },
    namedDataSources: {
      pounds: "_",
      type: "RAW",
      color: "BLACK",
      po: "PO",
      prodname: "Queue Test",
      tolling: "",
      erp: ""
    },
    ...overrides
  });
}

function writeZplQueueTestItem(item) {
  fs.mkdirSync(queueDir, { recursive: true });
  fs.writeFileSync(path.join(queueDir, `${item.itemId}.json`), JSON.stringify(item, null, 2), "utf8");
  return item;
}

function writePersistentSocketTestTemplate() {
  const templatePath = path.join(tempDir, "queue-label.template.zpl");
  fs.writeFileSync(
    templatePath,
    "^XA^FDLOT={{lotNumber}} BOX={{boxNumber}} RFID={{rfid}}^FS^RFW,H,1,2,1^FD3400^FS^RFW,H,2,12,1^FD{{rfidHex}}^FS^XZ",
    "utf8"
  );
  return templatePath;
}

function saveEnv(keys) {
  return Object.fromEntries(keys.map((key) => [key, process.env[key]]));
}

function restoreEnv(previous) {
  for (const [key, value] of Object.entries(previous)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
}

test.before(async () => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rfid-offline-test-"));
  stateFile = path.join(tempDir, "offline-state.json");
  auditFile = path.join(tempDir, "offline-audit.ndjson");
  queueDir = path.join(tempDir, "zpl-queue");
  logFile = path.join(tempDir, "logs", "printsvc-out.log");

  bartenderServer = http.createServer((req, res) => {
    if (req.method !== "POST") {
      res.statusCode = 204;
      return res.end();
    }

    let raw = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      raw += chunk;
    });
    req.on("end", () => {
      bartenderRequests.push(JSON.parse(raw));
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ Id: `mock-${bartenderRequests.length}`, Status: "Completed" }));
    });
  });

  const bartenderPort = await listen(bartenderServer);
  bartenderBaseUrl = `http://127.0.0.1:${bartenderPort}/actions`;

  process.env.PORT = "0";
  process.env.TENANT_ID = "tenant-id";
  process.env.API_AUDIENCE = "api://printsvc-test";
  process.env.REQUIRED_SCOPE = "Print.Labels";
  process.env.BARTENDER_ACTIONS_URL = bartenderBaseUrl;
  process.env.PRINTSVC_CONFIG_DIR = process.cwd();
  process.env.BARTENDER_TEMPLATE_DIR = "C:\\RFID";
  process.env.OFFLINE_PRINT_ADMIN_PASSWORD = "correct-password";
  process.env.OFFLINE_PRINT_SESSION_SECRET = "test-session-secret";
  process.env.OFFLINE_PRINT_STATE_FILE = stateFile;
  process.env.OFFLINE_PRINT_AUDIT_FILE = auditFile;
  process.env.PRINTSVC_LOG_PATH = logFile;
  process.env.ZPL_TEMPLATE_LAB_PROFILE_PATH = path.join(tempDir, "template-lab-profiles.json");
  process.env.OFFLINE_PRINT_MAX_LABELS = "3";
  process.env.OFFLINE_PRINT_MAX_BOX_NUMBER = "5";
  process.env.OFFLINE_PRINT_ALLOWED_HOSTS = "localhost,127.0.0.1";
  process.env.PRINT_JOB_SPACING_MS = "0";
  process.env.ZPL_QUEUE_DIR = queueDir;
  delete process.env.PRINT_ENGINE;
  delete process.env.ZPL_DUPLICATE_POLICY;
  delete process.env.ZPL_SOCKET_MODE;
  delete process.env.ZPL_MAX_LABELS_PER_CONNECTION;
  delete process.env.ZPL_SOCKET_IDLE_CLOSE_MS;
  delete process.env.ZPL_BATCH_MAX_LABELS;
  delete process.env.ZPL_BATCH_COLLECT_MS;
  delete process.env.ZPL_BATCH_INTER_BATCH_DELAY_MS;
  delete process.env.ZPL_BATCH_MAX_BYTES;
  delete process.env.ZPL_PREVIEW_RENDERER_URL;

  serverModule = require("../server");
  const { app } = serverModule;
  apiServer = http.createServer(app);
  const apiPort = await listen(apiServer);
  apiBaseUrl = `http://127.0.0.1:${apiPort}`;
});

test.after(async () => {
  await close(apiServer);
  await close(bartenderServer);
  fs.rmSync(tempDir, { recursive: true, force: true });
});

test("offline local access resists spoofed local host headers", () => {
  const defaultEnv = { OFFLINE_PRINT_ALLOWED_HOSTS: "localhost,127.0.0.1" };
  const spoofed = isOfflineLocalAccessAllowed({
    headers: { host: "127.0.0.1:3000" },
    socket: { remoteAddress: "8.8.8.8" }
  }, defaultEnv);

  assert.equal(spoofed.ok, false);
  assert.equal(spoofed.code, "OFFLINE_LOCAL_ACCESS_REQUIRED");

  const lan = isOfflineLocalAccessAllowed({
    headers: { host: "printsvc01:3000" },
    socket: { remoteAddress: "192.168.1.44" }
  }, defaultEnv);

  assert.equal(lan.ok, true);
});

test("offline emergency printing flow", async () => {
  let result = await request("GET", "/health");
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);
  assert.equal(result.json.printEngine, "bartender");
  assert.equal(result.json.zplDuplicatePolicy, "skip_recent");
  assert.equal(result.json.zplSocketMode, "per_label");
  assert.equal(result.json.zplMaxLabelsPerConnection, 50);
  assert.equal(result.json.zplSocketIdleCloseMs, 30000);
  assert.equal(result.json.zplBatchMaxLabels, 60);
  assert.equal(result.json.zplBatchCollectMs, 1500);
  assert.equal(result.json.zplBatchInterBatchDelayMs, 0);
  assert.equal(result.json.zplBatchMaxBytes, 524288);

  result = await request("GET", "/api/offline/status");
  assert.equal(result.status, 200);
  assert.equal(result.json.enabled, false);
  assert.deepEqual(result.json.allowedStations, ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]);
  assert.deepEqual(result.json.stationOptions[1], {
    code: "P2",
    description: "Receiving",
    label: "P2 - Receiving"
  });

  result = await request("POST", "/api/offline/print-labels", { body: offlinePayload() });
  assert.equal(result.status, 403);
  assert.equal(result.json.error, "OFFLINE_PRINTING_DISABLED");

  result = await request("POST", "/api/offline/admin/login", {
    body: { password: "wrong-password", adminName: "Ryan" }
  });
  assert.equal(result.status, 401);

  result = await request("POST", "/api/offline/admin/login", {
    body: { password: "correct-password", adminName: "Ryan" }
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);

  const cookie = result.headers.get("set-cookie").split(";")[0];
  assert.match(cookie, /^offline_print_admin=/);

  result = await request("POST", "/api/offline/admin/toggle", {
    cookie,
    body: { enabled: true, reason: "Internet outage", adminName: "Ryan" }
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.state.enabled, true);
  assert.equal(JSON.parse(fs.readFileSync(stateFile, "utf8")).enabled, true);

  result = await request("POST", "/api/offline/print-labels", {
    body: offlinePayload({ dryRun: true })
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.dryRun, true);
  assert.equal(result.json.family, "FG");
  assert.equal(result.json.preview.firstRfid, "PL123456-B01");
  assert.equal(result.json.preview.lastRfid, "PL123456-B02");
  assert.equal(bartenderRequests.length, 0);

  result = await request("POST", "/api/offline/print-labels", {
    body: offlinePayload({ firstBox: 3, lastBox: 2 })
  });
  assert.equal(result.status, 400);

  result = await request("POST", "/api/offline/print-labels", {
    body: offlinePayload({ firstBox: 1, lastBox: 4 })
  });
  assert.equal(result.status, 400);

  result = await request("POST", "/api/offline/print-labels", {
    body: offlinePayload({ family: "RAW", firstBox: 1, lastBox: 2, formatCode: "RG" })
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.printedCount, 2);
  assert.equal(result.json.family, "RAW");
  assert.equal(result.json.printEngine, "bartender");
  assert.equal(bartenderRequests.length, 2);

  const firstNamed = bartenderRequests[0].PrintBTWAction.NamedDataSources;
  assert.deepEqual(Object.keys(firstNamed), ["lot", "firstbox", "RFID", "pounds", "po", "prodname", "color", "type", "tolling", "erp"]);
  assert.equal(firstNamed.RFID, "PL123456-B01");
  assert.equal(firstNamed.erp, "OFFLINE");

  const auditLines = fs.readFileSync(auditFile, "utf8").trim().split(/\r?\n/).map((line) => JSON.parse(line));
  const labelAudit = auditLines.filter((line) => line.eventType === "offline_print_label" && line.ok === true);
  assert.equal(labelAudit.length, 2);
  assert.equal(labelAudit[0].namedDataSources.RFID, "PL123456-B01");

  result = await request("POST", "/api/print", { body: {} });
  assert.equal(result.status, 401);
  assert.equal(result.json.error, "Missing Bearer token");
});

test("print health page and log endpoint expose filtered local logs", async () => {
  fs.mkdirSync(path.dirname(logFile), { recursive: true });
  const logLines = [
    JSON.stringify({
      timestamp: "2026-05-29T18:00:00.000Z",
      build: "test",
      level: "info",
      event: "zpl_batch_send_success",
      station: "P3",
      family: "FG",
      lotNumber: "PL004885",
      printerIp: "192.168.50.223",
      token: "secret-token"
    }),
    JSON.stringify({
      timestamp: "2026-05-29T18:01:00.000Z",
      build: "test",
      level: "error",
      event: "zpl_batch_send_error",
      station: "P3",
      family: "FG",
      lotNumber: "PL004885",
      printerIp: "192.168.50.223",
      message: "timeout waiting for TCP close"
    }),
    "raw diagnostic line password=super-secret"
  ];
  fs.writeFileSync(logFile, `${logLines.join("\n")}\n`, "utf8");

  let result = await request("GET", "/offline/print-health");
  assert.equal(result.status, 200);
  assert.equal(result.text.includes("Direct-ZPL Queue Health"), true);

  result = await request("GET", "/api/print/logs?tail=10&event=zpl_batch_send_error&level=error&station=P3&family=FG&lotNumber=PL004885&printerIp=192.168.50.223&search=timeout");
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);
  assert.equal(result.json.count, 1);
  assert.equal(result.json.lines[0].parsed, true);
  assert.equal(result.json.lines[0].record.event, "zpl_batch_send_error");

  result = await request("GET", "/api/print/logs?tail=10&search=password");
  assert.equal(result.status, 200);
  assert.equal(result.json.lines.some((line) => line.parsed === false && line.raw.includes("[redacted]")), true);

  result = await request("GET", "/api/print/logs?tail=10&search=zpl_batch_send_success");
  assert.equal(result.status, 200);
  assert.equal(result.json.lines[0].record.token, "[redacted]");
});

test("template lab page, preview, and test send stay outside production queue", async () => {
  resetZplQueueTestState();

  let result = await request("GET", "/offline/template-lab");
  assert.equal(result.status, 200);
  assert.equal(result.text.includes("Direct-ZPL Template Preview"), true);
  assert.equal(result.text.includes("Visual Tuning"), true);
  assert.equal(result.text.includes("Export Profile JSON"), true);
  assert.equal(result.text.includes("Copy JSON"), true);
  assert.equal(result.text.includes("H = font height dots"), true);
  assert.equal(result.text.includes("Product Description & Field Origins"), true);
  assert.equal(result.text.includes("zpl-preview-frame"), true);
  assert.equal(result.text.includes("scaleXRange"), true);

  result = await request("GET", "/api/print/template-lab/catalog");
  assert.equal(result.status, 200);
  assert.equal(result.json.templates.some((template) => template.name === "RFID-RAW-P1.template.zpl"), true);
  assert.equal(result.json.profiles.some((profile) => profile.key === "P1:RAW"), true);

  const previewBody = {
    template: "RFID-RAW-P1.template.zpl",
    profileKey: "P1:RAW",
    lotNumber: "PT000086",
    boxNumber: "52",
    rfid: "PT000086-B52",
    pounds: "_",
    materialType: "POLYPROPYLENE",
    color: "ULTRAMARINEBLUE",
    po: "PO12345",
    productDescription: "Template Lab Product",
    tolling: ""
  };

  result = await request("POST", "/api/print/template-preview", { body: previewBody });
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);
  assert.equal(result.json.renderedZpl.includes("^FDLA,PT000086^FS"), true);
  assert.equal(result.json.metadata.qr.payload, "PT000086");
  assert.equal(result.json.metadata.qr.payloadTemplate, "{{lotNumber}}");
  assert.equal(result.json.metadata.rfidCommandPresent, true);
  assert.equal(result.json.metadata.logoCommandPresent, true);
  assert.equal(result.json.metadata.fitDebug.color.truncated, true);
  assert.equal(result.json.metadata.fitDebug.color.fittedText.includes("-"), false);
  assert.equal(result.json.metadata.fitDebug.productDescription.alignment, "left");
  assert.equal(result.json.metadata.previewMode, "approximate");
  assert.equal(result.json.imagePreview.mode, "approximate");
  assert.equal(result.json.imagePreview.data.imageUrl.startsWith("data:image/svg+xml;base64,"), true);
  assert.equal(result.json.metadata.qrDetected, true);
  assert.equal(result.json.metadata.logoDetected, true);
  assert.equal(Array.isArray(result.json.metadata.unsupportedZplCommands), true);
  assert.ok(result.json.metadata.fieldCount > 0);
  assert.equal(result.json.metadata.logoDiagnostics.source.endsWith("pri-logo.png"), true);

  result = await request("POST", "/api/print/template-preview", {
    body: {
      ...previewBody,
      profileOverrides: {
        scaleX: 1.02,
        scaleY: 0.98,
        offsetX: 4,
        offsetY: 5,
        qr: { x: 111, y: 222, magnification: 7 },
        logo: { x: 650, y: 40, widthDots: 120, heightDots: 40 },
        fieldFitDefinitions: {
          color: { boxWidth: 99, maxChars: 6, min: { fontH: 18, fontW: 9 } },
          productDescription: { boxWidth: 222, maxChars: 20, alignment: "L" }
        },
        fieldPositionOverrides: { color: { x: 600, y: 700 }, productDescription: { x: 40, y: 60 } }
      }
    }
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.renderedZpl.includes("^FO111,222\n^BQN,2,7^FDLA,PT000086^FS"), true);
  assert.equal(result.json.renderedZpl.includes("^FO650,40\n^GFA,"), true);
  assert.equal(result.json.metadata.fitDebug.color.boxW, 99);
  assert.equal(result.json.metadata.fitDebug.productDescription.boxW, 222);
  assert.equal(result.json.renderedZpl.includes("^FO45,64"), true);
  assert.equal(result.json.renderedZpl.includes("^FB226,1,0,L,0"), true);
  assert.equal(result.json.metadata.profile.scaleX, 1.02);
  assert.equal(result.json.metadata.profile.fieldPositionOverrides.color.x, 600);
  assert.equal(result.json.metadata.profile.fieldPositionOverrides.productDescription.x, 40);

  result = await request("POST", "/api/print/template-lab/profile", {
    body: {
      profileKey: "P1:RAW",
      overrides: {
        qr: { x: 123, y: 234, magnification: 8 },
        fieldFitDefinitions: { materialType: { boxWidth: 333, maxChars: 7 } }
      }
    }
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);
  assert.equal(fs.existsSync(path.join(tempDir, "template-lab-profiles.json")), true);

  result = await request("GET", "/api/print/template-lab/catalog");
  const savedProfile = result.json.profiles.find((profile) => profile.key === "P1:RAW");
  assert.equal(savedProfile.qr.x, 123);
  assert.equal(savedProfile.qr.magnification, 8);
  assert.equal(savedProfile.effectiveFieldFitDefinitions.materialType.boxWidth, 333);

  result = await request("POST", "/api/print/template-test-send", {
    body: { ...previewBody, printerIp: "127.0.0.1", port: 9100 }
  });
  assert.equal(result.status, 400);
  assert.equal(result.json.error, "TEMPLATE_TEST_CONFIRM_REQUIRED");

  const beforeFiles = fs.existsSync(queueDir) ? fs.readdirSync(queueDir).length : 0;
  let sentCount = 0;
  serverModule.setTemplateTestSendFunction(async ({ zpl, printerIp, port }) => {
    sentCount += 1;
    assert.equal(printerIp, "127.0.0.1");
    assert.equal(port, 9100);
    assert.equal(zpl.includes("^FDLA,PT000086^FS"), true);
    return { bytesSent: Buffer.byteLength(zpl, "utf8") };
  });

  result = await request("POST", "/api/print/template-test-send", {
    body: { ...previewBody, printerIp: "127.0.0.1", port: 9100, confirmTestPrint: true }
  });
  assert.equal(result.status, 200);
  assert.equal(result.json.ok, true);
  assert.equal(result.json.queued, false);
  assert.equal(sentCount, 1);

  const afterFiles = fs.existsSync(queueDir) ? fs.readdirSync(queueDir).length : 0;
  assert.equal(afterFiles, beforeFiles);
  serverModule.resetTemplateTestSendFunction();
});

test("print engine routing keeps BarTender default and resolves direct ZPL when requested", () => {
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;

  try {
    delete process.env.PRINT_ENGINE;
    delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    assert.equal(serverModule.getConfiguredPrintEngine(), "bartender");

    let target = serverModule.resolveRfidPrintTarget({ station: "P1", lotNumber: "PT000086" });
    assert.equal(target.printEngine, "bartender");
    assert.equal(target.printer, "Zebra ZD621R P1 ENGR");
    assert.equal(target.zpl, null);

    process.env.PRINT_ENGINE = "zpl";
    assert.equal(serverModule.getConfiguredPrintEngine(), "zpl");

    target = serverModule.resolveRfidPrintTarget({ station: "P1", lotNumber: "PT000086" });
    assert.equal(target.printEngine, "zpl");
    assert.equal(target.family, "RAW");
    assert.equal(target.zpl.printerIp, "192.168.50.239");
    assert.equal(target.zpl.port, 9100);
    assert.equal(target.zpl.templatePath, "C:\\RFID\\zpl\\RFID-RAW-P1.template.zpl");

    assert.throws(
      () => serverModule.resolveRfidPrintTarget({ station: "P2", lotNumber: "PT000086" }),
      (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
    );

    assert.throws(
      () => serverModule.resolveRfidPrintTarget({ station: "P1", lotNumber: "PL123456" }),
      (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
    );
  } finally {
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
  }
});

test("DIRECT_ZPL_ENABLED_SCOPES controls all RAW stations", () => {
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;
  const rawStations = [
    ["P1", "192.168.50.239"],
    ["P2", "192.168.50.241"],
    ["P3", "192.168.50.223"],
    ["P4", "192.168.50.242"],
    ["P5", "192.168.50.244"],
    ["P6", "192.168.6.240"],
    ["P7", "192.168.8.200"],
    ["P8", "192.168.7.122"]
  ];

  try {
    process.env.PRINT_ENGINE = "zpl";
    process.env.DIRECT_ZPL_ENABLED_SCOPES = rawStations.map(([station]) => `${station}:RAW`).join(",");

    assert.deepEqual(
      serverModule.getDirectZplEnabledScopes(),
      rawStations.map(([station]) => ({ station, family: "RAW" }))
    );

    for (const [station, printerIp] of rawStations) {
      const target = serverModule.resolveRfidPrintTarget({ station, lotNumber: "PT000086" });
      assert.equal(target.family, "RAW");
      assert.equal(target.zpl.printerIp, printerIp);
      assert.equal(target.zpl.port, 9100);
      assert.equal(target.zpl.templatePath, "C:\\RFID\\zpl\\RFID-RAW-P1.template.zpl");
    }

    assert.throws(
      () => serverModule.resolveRfidPrintTarget({ station: "P2", lotNumber: "PL123456" }),
      (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
    );

    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P1:FG";
    for (const [station] of rawStations) {
      assert.throws(
        () => serverModule.resolveRfidPrintTarget({ station, lotNumber: "PT000086" }),
        (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
      );
    }
  } finally {
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
  }
});

test("DIRECT_ZPL_ENABLED_SCOPES controls all FG stations", () => {
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;
  const fgStations = [
    ["P1", "192.168.50.239", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P2", "192.168.50.241", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P3", "192.168.50.223", "C:\\RFID\\zpl\\RFID-FG-P3.template.zpl"],
    ["P4", "192.168.50.242", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P5", "192.168.50.244", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P6", "192.168.6.240", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P7", "192.168.8.200", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"],
    ["P8", "192.168.7.122", "C:\\RFID\\zpl\\RFID-FG-P1.template.zpl"]
  ];

  try {
    process.env.PRINT_ENGINE = "zpl";
    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P1:RAW";

    const rawTarget = serverModule.resolveRfidPrintTarget({ station: "P1", lotNumber: "PT000086" });
    assert.equal(rawTarget.family, "RAW");
    assert.equal(rawTarget.zpl.printerIp, "192.168.50.239");
    assert.equal(rawTarget.zpl.templatePath, "C:\\RFID\\zpl\\RFID-RAW-P1.template.zpl");

    for (const [station] of fgStations) {
      assert.throws(
        () => serverModule.resolveRfidPrintTarget({ station, lotNumber: "PL123456" }),
        (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
      );
    }

    process.env.DIRECT_ZPL_ENABLED_SCOPES = fgStations.map(([station]) => `${station}:FG`).join(",");

    for (const [station, printerIp, templatePath] of fgStations) {
      const target = serverModule.resolveRfidPrintTarget({ station, lotNumber: "PL123456" });
      assert.equal(target.family, "FG");
      assert.equal(target.zpl.printerIp, printerIp);
      assert.equal(target.zpl.port, 9100);
      assert.equal(target.zpl.templatePath, templatePath);
    }

    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P1:FG";
    assert.throws(
      () => serverModule.resolveRfidPrintTarget({ station: "P2", lotNumber: "PL123456" }),
      (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
    );

    process.env.PRINT_ENGINE = "bartender";
    const bartenderTarget = serverModule.resolveRfidPrintTarget({ station: "P1", lotNumber: "PL123456" });
    assert.equal(bartenderTarget.printEngine, "bartender");
    assert.equal(bartenderTarget.family, "FG");
    assert.equal(bartenderTarget.zpl, null);
    assert.equal(bartenderTarget.template, "C:\\RFID\\RFID-FG-P1.btw");
  } finally {
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
  }
});

test("P3 sample direct ZPL scopes are explicit and P3-only", () => {
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;

  try {
    process.env.PRINT_ENGINE = "zpl";
    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P3:SAMPLE,P3:RETAIN,P3:SAMPLE_POUNDS";

    const sampleTarget = serverModule.resolveSamplePrintTarget({ station: "P3", labelKind: "sample" });
    assert.equal(sampleTarget.printEngine, "zpl");
    assert.equal(sampleTarget.labelKind, "QCSample");
    assert.equal(sampleTarget.directZplFamily, "SAMPLE");
    assert.equal(sampleTarget.zpl.printerName, "Zebra ZT230 P3 EXT");
    assert.equal(sampleTarget.zpl.printerIp, "192.168.50.218");
    assert.equal(sampleTarget.zpl.port, 9100);
    assert.equal(sampleTarget.zpl.templatePath, "C:\\RFID\\zpl\\QCSample-P3.template.zpl");

    const retainTarget = serverModule.resolveSamplePrintTarget({ station: "P3", labelKind: "retain" });
    assert.equal(retainTarget.labelKind, "QCRetain");
    assert.equal(retainTarget.directZplFamily, "RETAIN");
    assert.equal(retainTarget.zpl.printerName, "Zebra ZT230 P3 EXT");
    assert.equal(retainTarget.zpl.printerIp, "192.168.50.218");
    assert.equal(retainTarget.zpl.port, 9100);
    assert.equal(retainTarget.zpl.templatePath, "C:\\RFID\\zpl\\QCRetain-P3.template.zpl");

    const poundsTarget = serverModule.resolveSamplePrintTarget({ station: "P3", labelKind: "qc", byPounds: true });
    assert.equal(poundsTarget.labelKind, "QCSample");
    assert.equal(poundsTarget.directZplFamily, "SAMPLE_POUNDS");
    assert.equal(poundsTarget.zpl.printerName, "Zebra ZT230 P3 EXT");
    assert.equal(poundsTarget.zpl.printerIp, "192.168.50.218");
    assert.equal(poundsTarget.zpl.port, 9100);
    assert.equal(poundsTarget.zpl.templatePath, "C:\\RFID\\zpl\\QCSamplePounds-P3.template.zpl");

    assert.deepEqual(serverModule.getDirectZplEnabledScopes(), [
      { station: "P3", family: "SAMPLE" },
      { station: "P3", family: "RETAIN" },
      { station: "P3", family: "SAMPLE_POUNDS" }
    ]);

    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P3:SAMPLE,P3:RETAIN,P3:SAMPLE_POUNDS,P3:RAW,P3:FG";
    const rawTarget = serverModule.resolveRfidPrintTarget({ station: "P3", lotNumber: "PT000086" });
    assert.equal(rawTarget.family, "RAW");
    assert.equal(rawTarget.zpl.printerIp, "192.168.50.223");
    assert.equal(rawTarget.zpl.port, 9100);
    assert.equal(rawTarget.zpl.templatePath, "C:\\RFID\\zpl\\RFID-RAW-P1.template.zpl");

    const fgTarget = serverModule.resolveRfidPrintTarget({ station: "P3", lotNumber: "PL123456" });
    assert.equal(fgTarget.family, "FG");
    assert.equal(fgTarget.zpl.printerIp, "192.168.50.223");
    assert.equal(fgTarget.zpl.port, 9100);
    assert.equal(fgTarget.zpl.templatePath, "C:\\RFID\\zpl\\RFID-FG-P3.template.zpl");

    assert.throws(
      () => serverModule.resolveSamplePrintTarget({ station: "P2", labelKind: "sample" }),
      /No QC\/Retain printer mapping/
    );

    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P3:RAW";
    assert.throws(
      () => serverModule.resolveSamplePrintTarget({ station: "P3", labelKind: "sample" }),
      (error) => error.code === "UNSUPPORTED_DIRECT_ZPL"
    );

    process.env.PRINT_ENGINE = "bartender";
    const bartenderTarget = serverModule.resolveSamplePrintTarget({ station: "P3", labelKind: "sample" });
    assert.equal(bartenderTarget.printEngine, "bartender");
    assert.equal(bartenderTarget.zpl, null);
    assert.equal(bartenderTarget.printer, "Zebra ZT230 P3 EXT");
    assert.equal(bartenderTarget.template, "C:\\RFID\\QCSample-P3.btw");
  } finally {
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
  }
});

test("per-printer queue serializes sends for the same printer key", async () => {
  const events = [];
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const queueKey = `test-zpl-queue-${Date.now()}`;

  const first = serverModule.enqueuePrinterWork(queueKey, async () => {
    events.push("first-start");
    await delay(20);
    events.push("first-end");
  });

  const second = serverModule.enqueuePrinterWork(queueKey, async () => {
    events.push("second-start");
    events.push("second-end");
  });

  await Promise.all([first, second]);

  assert.deepEqual(events, ["first-start", "first-end", "second-start", "second-end"]);
});

test("persistent direct ZPL queue serializes separate enqueue requests in FIFO order", async () => {
  resetZplQueueTestState();
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const events = [];
  let active = 0;
  let maxActive = 0;

  try {
    process.env.ZPL_LABEL_SPACING_MS = "0";
    serverModule.setDirectZplQueueSendFunction(async ({ box }) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      events.push(`start-${box}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
      events.push(`end-${box}`);
      active -= 1;
      return {
        box,
        rfid: `PT000086-B${String(box).padStart(2, "0")}`,
        bytesSent: 64,
        socketClosed: true,
        sendAccepted: true,
        physicalPrintConfirmed: false
      };
    });

    const firstStartedAt = Date.now();
    const first = serverModule.enqueueDirectZplQueueItems([zplQueueItem({ jobId: "job-a", box: 1 })]);
    const second = serverModule.enqueueDirectZplQueueItems([zplQueueItem({ jobId: "job-b", box: 2 })]);
    assert.equal(Date.now() - firstStartedAt < 50, true);
    assert.equal(first[0].status, "queued");
    assert.equal(second[0].status, "queued");

    await waitForCondition(() => {
      const status = serverModule.getZplQueueStatusPayload();
      return status.printers["zpl:127.0.0.1:9100"]?.counts.sent_to_printer === 2;
    }, 3000);

    assert.deepEqual(events, ["start-1", "end-1", "start-2", "end-2"]);
    assert.equal(maxActive, 1);
  } finally {
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    resetZplQueueTestState();
  }
});

test("per_label ZPL socket mode keeps one TCP connection per label", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_CONNECT_RETRY_DELAY_MS"
  ]);
  const tcpServer = net.createServer();
  let connections = 0;
  let received = "";

  try {
    process.env.ZPL_SOCKET_MODE = "per_label";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_CONNECT_RETRY_DELAY_MS = "0";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      connections += 1;
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        received += chunk;
      });
      socket.on("end", () => socket.end());
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-per-label", box: 1, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-per-label", box: 2, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 2 && received.includes("BOX=1") && received.includes("BOX=2");
    }, 3000);

    const status = serverModule.getZplQueueStatusPayload();
    assert.equal(connections, 2);
    assert.equal(received.includes("BOX=1"), true);
    assert.equal(received.includes("BOX=2"), true);
    assert.equal(status.socketMode, "per_label");
    assert.deepEqual(status.activeSockets, {});
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("persistent ZPL socket mode reuses one socket for queued labels", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_CONNECT_RETRY_DELAY_MS",
    "ZPL_MAX_LABELS_PER_CONNECTION",
    "ZPL_SOCKET_IDLE_CLOSE_MS"
  ]);
  const tcpServer = net.createServer();
  let connections = 0;
  let received = "";

  try {
    process.env.ZPL_SOCKET_MODE = "persistent";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_CONNECT_RETRY_DELAY_MS = "0";
    process.env.ZPL_MAX_LABELS_PER_CONNECTION = "50";
    process.env.ZPL_SOCKET_IDLE_CLOSE_MS = "1000";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      connections += 1;
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        received += chunk;
      });
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-persistent", box: 1, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-persistent", box: 2, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 2 && received.includes("BOX=1") && received.includes("BOX=2");
    }, 1000);

    const status = serverModule.getZplQueueStatusPayload();
    const socketState = status.activeSockets[`zpl:127.0.0.1:${port}`];
    assert.equal(connections, 1);
    assert.equal(received.includes("BOX=1"), true);
    assert.equal(received.includes("BOX=2"), true);
    assert.equal(status.socketMode, "persistent");
    assert.equal(socketState.connected, true);
    assert.equal(socketState.labelsSent, 2);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("persistent ZPL socket closes after max labels per connection", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_MAX_LABELS_PER_CONNECTION",
    "ZPL_SOCKET_IDLE_CLOSE_MS"
  ]);
  const tcpServer = net.createServer();
  let connections = 0;

  try {
    process.env.ZPL_SOCKET_MODE = "persistent";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_MAX_LABELS_PER_CONNECTION = "2";
    process.env.ZPL_SOCKET_IDLE_CLOSE_MS = "1000";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      connections += 1;
      socket.resume();
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-persistent-max", box: 1, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-persistent-max", box: 2, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-persistent-max", box: 3, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 3;
    }, 1000);

    assert.equal(connections, 2);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("persistent ZPL socket closes after idle timeout", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_MAX_LABELS_PER_CONNECTION",
    "ZPL_SOCKET_IDLE_CLOSE_MS"
  ]);
  const tcpServer = net.createServer();

  try {
    process.env.ZPL_SOCKET_MODE = "persistent";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_MAX_LABELS_PER_CONNECTION = "50";
    process.env.ZPL_SOCKET_IDLE_CLOSE_MS = "30";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => socket.resume());
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-persistent-idle", box: 1, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 1;
    }, 1000);

    assert.equal(serverModule.getZplPersistentSocketStatusForAll()[`zpl:127.0.0.1:${port}`]?.connected, true);

    await waitForCondition(() => {
      return !serverModule.getZplPersistentSocketStatusForAll()[`zpl:127.0.0.1:${port}`];
    }, 1000);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("persistent ZPL socket error after write becomes unknown_after_send and pauses queue", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_MAX_LABELS_PER_CONNECTION",
    "ZPL_SOCKET_IDLE_CLOSE_MS"
  ]);

  class FakeSocket extends EventEmitter {
    constructor() {
      super();
      this.destroyed = false;
    }
    connect() {
      setImmediate(() => this.emit("connect"));
    }
    write(_payload, _encoding, callback) {
      setImmediate(() => {
        const error = new Error("socket reset after write");
        error.code = "ECONNRESET";
        this.emit("error", error);
        if (callback) callback(error);
      });
      return true;
    }
    end() {
      setImmediate(() => this.emit("close"));
    }
    destroy() {
      this.destroyed = true;
    }
  }

  try {
    process.env.ZPL_SOCKET_MODE = "persistent";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "1";
    process.env.ZPL_MAX_LABELS_PER_CONNECTION = "50";
    process.env.ZPL_SOCKET_IDLE_CLOSE_MS = "1000";
    writePersistentSocketTestTemplate();
    serverModule.setZplSocketFactoryForTests(() => new FakeSocket());

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-persistent-unknown", box: 1 }),
      zplQueueItem({ jobId: "job-persistent-unknown", box: 2 })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.unknown_after_send === 1 && printer?.counts.queued === 1 && printer?.paused === true;
    }, 1000);

    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
    assert.equal(printer.reviewRequiredItems[0].box, 1);
    assert.equal(printer.reviewRequiredItems[0].safeToRetry, false);
    assert.equal(printer.counts.sent_to_printer, 0);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
  }
});

test("persistent ZPL socket error before write becomes failed_before_send", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_LABEL_SPACING_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_MAX_LABELS_PER_CONNECTION",
    "ZPL_SOCKET_IDLE_CLOSE_MS"
  ]);

  class FakeSocket extends EventEmitter {
    constructor() {
      super();
      this.destroyed = false;
    }
    connect() {
      setImmediate(() => {
        const error = new Error("connect ECONNREFUSED");
        error.code = "ECONNREFUSED";
        this.emit("error", error);
      });
    }
    write() {
      throw new Error("write should not be called");
    }
    end() {
      setImmediate(() => this.emit("close"));
    }
    destroy() {
      this.destroyed = true;
    }
  }

  try {
    process.env.ZPL_SOCKET_MODE = "persistent";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_MAX_LABELS_PER_CONNECTION = "50";
    process.env.ZPL_SOCKET_IDLE_CLOSE_MS = "1000";
    writePersistentSocketTestTemplate();
    serverModule.setZplSocketFactoryForTests(() => new FakeSocket());

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-persistent-prewrite", box: 1 })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.failed_before_send === 1;
    }, 1000);

    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
    assert.equal(printer.safeRetryItems[0].box, 1);
    assert.equal(printer.safeRetryItems[0].safeToRetry, true);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
  }
});

test("batch ZPL socket mode sends queued labels as one concatenated FIFO payload", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_BATCH_COLLECT_MS",
    "ZPL_BATCH_MAX_LABELS",
    "ZPL_BATCH_INTER_BATCH_DELAY_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT",
    "ZPL_LABEL_SPACING_MS"
  ]);
  const tcpServer = net.createServer();
  let connections = 0;
  let received = "";

  try {
    process.env.ZPL_SOCKET_MODE = "batch";
    process.env.ZPL_BATCH_COLLECT_MS = "0";
    process.env.ZPL_BATCH_MAX_LABELS = "60";
    process.env.ZPL_BATCH_INTER_BATCH_DELAY_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    process.env.ZPL_LABEL_SPACING_MS = "9999";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      connections += 1;
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        received += chunk;
      });
      socket.on("end", () => socket.end());
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-batch", box: 1, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-batch", box: 2, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-batch", box: 3, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 3 &&
        received.includes("BOX=1") &&
        received.includes("BOX=2") &&
        received.includes("BOX=3");
    }, 1000);

    const status = serverModule.getZplQueueStatusPayload();
    const printer = status.printers[`zpl:127.0.0.1:${port}`];
    assert.equal(connections, 1);
    assert.equal(received.indexOf("BOX=1") < received.indexOf("BOX=2"), true);
    assert.equal(received.indexOf("BOX=2") < received.indexOf("BOX=3"), true);
    assert.equal(status.socketMode, "batch");
    assert.equal(printer.counts.sent_to_printer, 3);
    assert.equal(printer.lastBatchDurationMs >= 0, true);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("batch ZPL socket mode splits batches at max label count", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_BATCH_COLLECT_MS",
    "ZPL_BATCH_MAX_LABELS",
    "ZPL_BATCH_INTER_BATCH_DELAY_MS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT"
  ]);
  const tcpServer = net.createServer();
  let connections = 0;

  try {
    process.env.ZPL_SOCKET_MODE = "batch";
    process.env.ZPL_BATCH_COLLECT_MS = "0";
    process.env.ZPL_BATCH_MAX_LABELS = "2";
    process.env.ZPL_BATCH_INTER_BATCH_DELAY_MS = "0";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      connections += 1;
      socket.resume();
      socket.on("end", () => socket.end());
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([1, 2, 3, 4, 5].map((box) =>
      zplQueueItem({ jobId: "job-batch-split", box, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ));

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.sent_to_printer === 5;
    }, 3000);

    assert.equal(connections, 3);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("batch ZPL pre-write failure marks included items failed_before_send", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_BATCH_COLLECT_MS",
    "ZPL_BATCH_MAX_LABELS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT"
  ]);

  class FakeSocket extends EventEmitter {
    setTimeout() {}
    connect() {
      setImmediate(() => {
        const error = new Error("connect ECONNREFUSED");
        error.code = "ECONNREFUSED";
        this.emit("error", error);
      });
    }
    write() {
      throw new Error("write should not be called");
    }
    end() {}
    destroy() {}
  }

  try {
    process.env.ZPL_SOCKET_MODE = "batch";
    process.env.ZPL_BATCH_COLLECT_MS = "0";
    process.env.ZPL_BATCH_MAX_LABELS = "60";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    writePersistentSocketTestTemplate();
    serverModule.setZplSocketFactoryForTests(() => new FakeSocket());

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-batch-prewrite", box: 1 }),
      zplQueueItem({ jobId: "job-batch-prewrite", box: 2 })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.failed_before_send === 2;
    }, 1000);

    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
    assert.equal(printer.safeRetryItems.length, 2);
    assert.equal(printer.safeRetryItems.every((item) => item.safeToRetry === true), true);
    assert.equal(printer.paused, false);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
  }
});

test("batch ZPL post-write failure marks included items unknown_after_send and pauses", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_BATCH_COLLECT_MS",
    "ZPL_BATCH_MAX_LABELS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT"
  ]);

  class FakeSocket extends EventEmitter {
    setTimeout() {}
    connect(_port, _host, callback) {
      setImmediate(callback);
    }
    write(_payload, _encoding, callback) {
      setImmediate(() => {
        const error = new Error("socket reset after batch write");
        error.code = "ECONNRESET";
        this.emit("error", error);
        if (callback) callback(error);
      });
      return true;
    }
    end() {}
    destroy() {}
  }

  try {
    process.env.ZPL_SOCKET_MODE = "batch";
    process.env.ZPL_BATCH_COLLECT_MS = "0";
    process.env.ZPL_BATCH_MAX_LABELS = "60";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    writePersistentSocketTestTemplate();
    serverModule.setZplSocketFactoryForTests(() => new FakeSocket());

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-batch-unknown", box: 1 }),
      zplQueueItem({ jobId: "job-batch-unknown", box: 2 })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.unknown_after_send === 2 && printer?.paused === true;
    }, 1000);

    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
    assert.equal(printer.reviewRequiredItems.length, 2);
    assert.equal(printer.reviewRequiredItems.every((item) => item.safeToRetry === false), true);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
  }
});

test("batch ZPL validation failure does not alter RFID rules or block valid labels", async () => {
  resetZplQueueTestState();
  const previous = saveEnv([
    "ZPL_SOCKET_MODE",
    "ZPL_BATCH_COLLECT_MS",
    "ZPL_BATCH_MAX_LABELS",
    "ZPL_TCP_TIMEOUT_MS",
    "ZPL_CONNECT_RETRY_COUNT"
  ]);
  const tcpServer = net.createServer();
  let received = "";

  try {
    process.env.ZPL_SOCKET_MODE = "batch";
    process.env.ZPL_BATCH_COLLECT_MS = "0";
    process.env.ZPL_BATCH_MAX_LABELS = "60";
    process.env.ZPL_TCP_TIMEOUT_MS = "1000";
    process.env.ZPL_CONNECT_RETRY_COUNT = "0";
    writePersistentSocketTestTemplate();

    tcpServer.on("connection", (socket) => {
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        received += chunk;
      });
      socket.on("end", () => socket.end());
    });
    const port = await listen(tcpServer);

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-batch-validation", box: 1, rfid: "BAD", zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } }),
      zplQueueItem({ jobId: "job-batch-validation", box: 2, zpl: { printerIp: "127.0.0.1", port, templatePath: path.join(tempDir, "queue-label.template.zpl") } })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
      return printer?.counts.failed_before_send === 1 && printer?.counts.sent_to_printer === 1 && received.includes("BOX=2");
    }, 1000);

    const printer = serverModule.getZplQueueStatusPayload().printers[`zpl:127.0.0.1:${port}`];
    assert.equal(printer.itemsByStatus.failed_before_send[0].lastError.code, "INVALID_RFID");
    assert.equal(received.includes("BOX=1"), false);
    assert.equal(received.includes("BOX=2"), true);
  } finally {
    resetZplQueueTestState();
    restoreEnv(previous);
    await close(tcpServer);
  }
});

test("unknown_after_send pauses queue and resume restarts it", async () => {
  resetZplQueueTestState();
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const sentBoxes = [];
  let calls = 0;

  try {
    process.env.ZPL_LABEL_SPACING_MS = "0";
    serverModule.setDirectZplQueueSendFunction(async ({ box }) => {
      calls += 1;
      if (calls === 1) {
        const error = new Error(`Box ${box} may or may not have printed. Verify before resuming.`);
        error.code = "ZPL_SEND_UNKNOWN";
        error.operatorAction = "Verify whether the label physically printed before retrying.";
        throw error;
      }

      sentBoxes.push(box);
      return {
        box,
        rfid: `PT000086-B${String(box).padStart(2, "0")}`,
        bytesSent: 64,
        socketClosed: true,
        sendAccepted: true,
        physicalPrintConfirmed: false
      };
    });

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({ jobId: "job-unknown", box: 1 }),
      zplQueueItem({ jobId: "job-unknown", box: 2 })
    ]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.unknown_after_send === 1 && printer?.counts.queued === 1 && printer?.paused === true;
    }, 1000);

    serverModule.resumeZplQueue({ printerKey: "zpl:127.0.0.1:9100", operator: "test" });

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.sent_to_printer === 1 && printer?.paused === false;
    }, 1000);

    assert.deepEqual(sentBoxes, [2]);
  } finally {
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    resetZplQueueTestState();
  }
});

test("direct ZPL queue rejects duplicate recent sent label", async () => {
  resetZplQueueTestState();
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const previousPolicy = process.env.ZPL_DUPLICATE_POLICY;

  try {
    delete process.env.ZPL_DUPLICATE_POLICY;
    process.env.ZPL_LABEL_SPACING_MS = "0";
    serverModule.setDirectZplQueueSendFunction(async ({ box, rfid }) => ({
      box,
      rfid,
      bytesSent: 64,
      socketClosed: true,
      sendAccepted: true,
      physicalPrintConfirmed: false
    }));

    const first = zplQueueItem({ jobId: "job-dupe-a", box: 3, rfid: "PT000086-B03" });
    serverModule.enqueueDirectZplQueueItems([first]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.sent_to_printer === 1;
    }, 1000);

    assert.throws(
      () => serverModule.enqueueDirectZplQueueItems([zplQueueItem({ jobId: "job-dupe-b", box: 3, rfid: "PT000086-B03" })]),
      (error) => error.code === "DUPLICATE_RECENT_ZPL"
    );
  } finally {
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    if (previousPolicy === undefined) delete process.env.ZPL_DUPLICATE_POLICY;
    else process.env.ZPL_DUPLICATE_POLICY = previousPolicy;
    resetZplQueueTestState();
  }
});

test("normal direct ZPL duplicate recent is skipped by default without enqueueing", async () => {
  resetZplQueueTestState();
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const previousPolicy = process.env.ZPL_DUPLICATE_POLICY;
  const originalWarn = console.warn;
  const warnings = [];

  try {
    process.env.PRINT_ENGINE = "zpl";
    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P1:RAW";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    delete process.env.ZPL_DUPLICATE_POLICY;
    fs.writeFileSync(stateFile, JSON.stringify({
      enabled: true,
      reason: "test",
      enabledBy: "test",
      enabledOn: new Date().toISOString(),
      updatedOn: new Date().toISOString()
    }), "utf8");

    serverModule.setDirectZplQueueSendFunction(async ({ box, rfid }) => ({
      box,
      rfid,
      bytesSent: 64,
      socketClosed: true,
      sendAccepted: true,
      physicalPrintConfirmed: false
    }));

    const body = offlinePayload({
      station: "P1",
      family: "RAW",
      lotNumber: "PT000086",
      firstBox: 1,
      lastBox: 1,
      formatCode: "RG"
    });

    let result = await request("POST", "/api/offline/print-labels", { body });
    assert.equal(result.status, 200);
    assert.equal(result.json.ok, true);
    assert.equal(result.json.queued, true);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:192.168.50.239:9100"];
      return printer?.counts.sent_to_printer === 1;
    }, 1000);

    console.warn = (...args) => warnings.push(args.join(" "));
    result = await request("POST", "/api/offline/print-labels", { body });
    console.warn = originalWarn;

    assert.equal(result.status, 200);
    assert.equal(result.json.ok, true);
    assert.equal(result.json.skippedDuplicate, true);
    assert.equal(result.json.queued, false);
    assert.equal(result.json.box, 1);
    assert.equal(result.json.rfid, "PT000086-B01");
    assert.match(result.json.acceptedAtUtc, /^\d{4}-\d{2}-\d{2}T/);
    assert.match(result.json.expiresAtUtc, /^\d{4}-\d{2}-\d{2}T/);
    assert.equal(result.json.message, "Label was already accepted recently and was skipped to prevent duplicate RFID.");
    assert.equal(warnings.some((line) => line.includes("duplicate_recent_zpl_skipped")), true);

    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:192.168.50.239:9100"];
    assert.equal(printer.counts.sent_to_printer, 1);
    assert.equal(printer.counts.queued, 0);
    assert.equal(printer.counts.rejected, 0);
  } finally {
    console.warn = originalWarn;
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    if (previousPolicy === undefined) delete process.env.ZPL_DUPLICATE_POLICY;
    else process.env.ZPL_DUPLICATE_POLICY = previousPolicy;
    resetZplQueueTestState();
  }
});

test("ZPL_DUPLICATE_POLICY allow permits immediate duplicate reprint", async () => {
  resetZplQueueTestState();
  const previousPrintEngine = process.env.PRINT_ENGINE;
  const previousScopes = process.env.DIRECT_ZPL_ENABLED_SCOPES;
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const previousPolicy = process.env.ZPL_DUPLICATE_POLICY;
  const originalLog = console.log;
  const logs = [];

  try {
    process.env.PRINT_ENGINE = "zpl";
    process.env.DIRECT_ZPL_ENABLED_SCOPES = "P1:RAW";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_DUPLICATE_POLICY = "allow";
    fs.writeFileSync(stateFile, JSON.stringify({
      enabled: true,
      reason: "test",
      enabledBy: "test",
      enabledOn: new Date().toISOString(),
      updatedOn: new Date().toISOString()
    }), "utf8");

    serverModule.setDirectZplQueueSendFunction(async ({ box, rfid }) => ({
      box,
      rfid,
      bytesSent: 64,
      socketClosed: true,
      sendAccepted: true,
      physicalPrintConfirmed: false
    }));

    const body = offlinePayload({
      station: "P1",
      family: "RAW",
      lotNumber: "PT000086",
      firstBox: 1,
      lastBox: 1,
      formatCode: "RG"
    });

    let result = await request("POST", "/api/offline/print-labels", { body });
    assert.equal(result.status, 200);
    assert.equal(result.json.ok, true);
    assert.equal(result.json.queued, true);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:192.168.50.239:9100"];
      return printer?.counts.sent_to_printer === 1;
    }, 1000);

    console.log = (...args) => logs.push(args.join(" "));
    result = await request("POST", "/api/offline/print-labels", { body });
    console.log = originalLog;

    assert.equal(result.status, 200);
    assert.equal(result.json.ok, true);
    assert.equal(result.json.queued, true);
    assert.equal(result.json.skippedDuplicate, false);
    assert.deepEqual(result.json.queuedBoxes, [1]);
    assert.equal(logs.some((line) => line.includes("zpl_duplicate_allowed")), true);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:192.168.50.239:9100"];
      return printer?.counts.sent_to_printer === 2;
    }, 1000);
  } finally {
    console.log = originalLog;
    if (previousPrintEngine === undefined) delete process.env.PRINT_ENGINE;
    else process.env.PRINT_ENGINE = previousPrintEngine;
    if (previousScopes === undefined) delete process.env.DIRECT_ZPL_ENABLED_SCOPES;
    else process.env.DIRECT_ZPL_ENABLED_SCOPES = previousScopes;
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    if (previousPolicy === undefined) delete process.env.ZPL_DUPLICATE_POLICY;
    else process.env.ZPL_DUPLICATE_POLICY = previousPolicy;
    resetZplQueueTestState();
  }
});

test("direct ZPL queue marks pre-write failure as failed_before_send", async () => {
  resetZplQueueTestState();

  try {
    serverModule.setDirectZplQueueSendFunction(async () => {
      const error = new Error("connect ECONNREFUSED");
      error.code = "ECONNREFUSED";
      error.details = { connected: false, writeStarted: false, bytesAttempted: 0 };
      throw error;
    });

    serverModule.enqueueDirectZplQueueItems([zplQueueItem({ jobId: "job-failed-before", box: 4 })]);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.failed_before_send === 1;
    }, 1000);
  } finally {
    resetZplQueueTestState();
  }
});

test("stale sending item recovers as unknown_after_send and pauses printer queue", () => {
  resetZplQueueTestState();
  const previousThreshold = process.env.ZPL_STALE_SENDING_THRESHOLD_MS;

  try {
    process.env.ZPL_STALE_SENDING_THRESHOLD_MS = "1";
    const oldTime = new Date(Date.now() - 5000).toISOString();
    const item = {
      ...zplQueueItem({ jobId: "job-stale", box: 8 }),
      status: "sending",
      attempts: 1,
      sendingStartedAt: oldTime,
      updatedAt: oldTime
    };
    writeZplQueueTestItem(item);

    serverModule.startAllZplQueueWorkers();
    const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];

    assert.equal(printer.counts.unknown_after_send, 1);
    assert.equal(printer.paused, true);
    assert.equal(printer.reviewRequiredItems[0].box, 8);
    assert.equal(printer.reviewRequiredItems[0].safeToRetry, false);
    assert.equal(printer.recoveredItems[0].recoveredFromStatus, "sending");
  } finally {
    if (previousThreshold === undefined) delete process.env.ZPL_STALE_SENDING_THRESHOLD_MS;
    else process.env.ZPL_STALE_SENDING_THRESHOLD_MS = previousThreshold;
    resetZplQueueTestState();
  }
});

test("queue status marks safe retry only for failed_before_send with no bytes written", () => {
  resetZplQueueTestState();
  const safeItem = {
    ...zplQueueItem({ jobId: "job-safe-retry", box: 5 }),
    status: "failed_before_send",
    failedAt: new Date().toISOString(),
    lastError: {
      code: "ECONNREFUSED",
      message: "connect ECONNREFUSED",
      details: {
        connected: false,
        writeStarted: false,
        writeCompleted: false,
        bytesAttempted: 0,
        bytesSent: 0
      },
      retryable: true
    }
  };
  const unknownItem = {
    ...zplQueueItem({ jobId: "job-unknown-safe", box: 6 }),
    status: "unknown_after_send",
    unknownAt: new Date().toISOString(),
    lastError: {
      code: "ZPL_SEND_UNKNOWN",
      message: "May have printed.",
      retryable: false
    },
    operatorAction: "Verify whether this label physically printed before resuming."
  };

  writeZplQueueTestItem(safeItem);
  writeZplQueueTestItem(unknownItem);

  const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
  assert.equal(printer.itemsByStatus.failed_before_send[0].safeToRetry, true);
  assert.equal(printer.itemsByStatus.unknown_after_send[0].safeToRetry, false);
  assert.equal(printer.safeRetryItems.length, 1);
  assert.equal(printer.safeRetryItems[0].box, 5);
  assert.equal(printer.reviewRequiredItems[0].box, 6);

  resetZplQueueTestState();
});

test("retry-failed endpoint requeues only safe failed_before_send items", async () => {
  resetZplQueueTestState();
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;

  try {
    process.env.ZPL_LABEL_SPACING_MS = "0";
    serverModule.setDirectZplQueueSendFunction(async ({ box }) => ({
      box,
      rfid: `PT000086-B${String(box).padStart(2, "0")}`,
      bytesSent: 64,
      socketClosed: true,
      sendAccepted: true,
      physicalPrintConfirmed: false
    }));

    const safeItem = {
      ...zplQueueItem({ jobId: "job-retry-safe", box: 2 }),
      status: "failed_before_send",
      failedAt: new Date().toISOString(),
      lastError: {
        code: "ECONNREFUSED",
        message: "connect ECONNREFUSED",
        details: { connected: false, writeStarted: false, writeCompleted: false, bytesAttempted: 0, bytesSent: 0 },
        retryable: true
      }
    };
    writeZplQueueTestItem(safeItem);

    const response = await request("POST", "/api/print/zpl-queue/retry-failed", {
      body: { itemId: safeItem.itemId, operator: "test" }
    });
    assert.equal(response.status, 200);
    assert.equal(response.json.ok, true);
    assert.equal(response.json.queued, true);

    await waitForCondition(() => {
      const printer = serverModule.getZplQueueStatusPayload().printers["zpl:127.0.0.1:9100"];
      return printer?.counts.sent_to_printer === 1;
    }, 1000);

    const unknownItem = {
      ...zplQueueItem({ jobId: "job-retry-unknown", box: 3 }),
      status: "unknown_after_send",
      unknownAt: new Date().toISOString(),
      lastError: { code: "ZPL_SEND_UNKNOWN", message: "May have printed.", retryable: false }
    };
    writeZplQueueTestItem(unknownItem);

    const rejected = await request("POST", "/api/print/zpl-queue/retry-failed", {
      body: { itemId: unknownItem.itemId, operator: "test" }
    });
    assert.equal(rejected.status, 409);
    assert.equal(rejected.json.error, "ZPL_RETRY_NOT_ALLOWED");
  } finally {
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    resetZplQueueTestState();
  }
});

test("separate station printer queues do not block each other", async () => {
  resetZplQueueTestState();
  const previousSpacing = process.env.ZPL_LABEL_SPACING_MS;
  const events = [];

  try {
    process.env.ZPL_LABEL_SPACING_MS = "0";
    serverModule.setDirectZplQueueSendFunction(async ({ station, box }) => {
      events.push(`${station}-start`);
      if (station === "P1") {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      events.push(`${station}-end`);
      return {
        box,
        rfid: `PT000086-B${String(box).padStart(2, "0")}`,
        bytesSent: 64,
        socketClosed: true,
        sendAccepted: true,
        physicalPrintConfirmed: false
      };
    });

    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({
        jobId: "job-p1",
        station: "P1",
        box: 1,
        zpl: { printerIp: "127.0.0.1", port: 9101, templatePath: path.join(tempDir, "queue-label.template.zpl") }
      })
    ]);
    serverModule.enqueueDirectZplQueueItems([
      zplQueueItem({
        jobId: "job-p2",
        station: "P2",
        box: 2,
        zpl: { printerIp: "127.0.0.1", port: 9102, templatePath: path.join(tempDir, "queue-label.template.zpl") }
      })
    ]);

    await waitForCondition(() => {
      const status = serverModule.getZplQueueStatusPayload();
      return status.printers["zpl:127.0.0.1:9101"]?.counts.sent_to_printer === 1 &&
        status.printers["zpl:127.0.0.1:9102"]?.counts.sent_to_printer === 1;
    }, 1000);

    assert.equal(events.indexOf("P2-end") < events.indexOf("P1-end"), true);
  } finally {
    if (previousSpacing === undefined) delete process.env.ZPL_LABEL_SPACING_MS;
    else process.env.ZPL_LABEL_SPACING_MS = previousSpacing;
    resetZplQueueTestState();
  }
});

test("direct ZPL env defaults are conservative", () => {
  const previous = {
    ZPL_TCP_TIMEOUT_MS: process.env.ZPL_TCP_TIMEOUT_MS,
    ZPL_LABEL_SPACING_MS: process.env.ZPL_LABEL_SPACING_MS,
    ZPL_CONNECT_RETRY_COUNT: process.env.ZPL_CONNECT_RETRY_COUNT,
    ZPL_CONNECT_RETRY_DELAY_MS: process.env.ZPL_CONNECT_RETRY_DELAY_MS,
    ZPL_STALE_SENDING_THRESHOLD_MS: process.env.ZPL_STALE_SENDING_THRESHOLD_MS,
    ZPL_SOCKET_MODE: process.env.ZPL_SOCKET_MODE,
    ZPL_MAX_LABELS_PER_CONNECTION: process.env.ZPL_MAX_LABELS_PER_CONNECTION,
    ZPL_SOCKET_IDLE_CLOSE_MS: process.env.ZPL_SOCKET_IDLE_CLOSE_MS,
    ZPL_BATCH_MAX_LABELS: process.env.ZPL_BATCH_MAX_LABELS,
    ZPL_BATCH_COLLECT_MS: process.env.ZPL_BATCH_COLLECT_MS,
    ZPL_BATCH_INTER_BATCH_DELAY_MS: process.env.ZPL_BATCH_INTER_BATCH_DELAY_MS,
    ZPL_BATCH_MAX_BYTES: process.env.ZPL_BATCH_MAX_BYTES
  };

  try {
    delete process.env.ZPL_TCP_TIMEOUT_MS;
    delete process.env.ZPL_LABEL_SPACING_MS;
    delete process.env.ZPL_CONNECT_RETRY_COUNT;
    delete process.env.ZPL_CONNECT_RETRY_DELAY_MS;
    delete process.env.ZPL_STALE_SENDING_THRESHOLD_MS;
    delete process.env.ZPL_SOCKET_MODE;
    delete process.env.ZPL_MAX_LABELS_PER_CONNECTION;
    delete process.env.ZPL_SOCKET_IDLE_CLOSE_MS;
    delete process.env.ZPL_BATCH_MAX_LABELS;
    delete process.env.ZPL_BATCH_COLLECT_MS;
    delete process.env.ZPL_BATCH_INTER_BATCH_DELAY_MS;
    delete process.env.ZPL_BATCH_MAX_BYTES;

    assert.deepEqual(serverModule.getZplTransportSettings(), {
      tcpTimeoutMs: 120000,
      labelSpacingMs: 8000,
      connectRetryCount: 0,
      connectRetryDelayMs: 3000,
      socketMode: "per_label",
      maxLabelsPerConnection: 50,
      socketIdleCloseMs: 30000,
      batchMaxLabels: 60,
      batchCollectMs: 1500,
      batchInterBatchDelayMs: 0,
      batchMaxBytes: 524288
    });
    assert.equal(serverModule.getZplStaleSendingThresholdMs(), 120000);
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("direct ZPL retries a failed TCP connection", async () => {
  const previous = {
    ZPL_TCP_TIMEOUT_MS: process.env.ZPL_TCP_TIMEOUT_MS,
    ZPL_LABEL_SPACING_MS: process.env.ZPL_LABEL_SPACING_MS,
    ZPL_CONNECT_RETRY_COUNT: process.env.ZPL_CONNECT_RETRY_COUNT,
    ZPL_CONNECT_RETRY_DELAY_MS: process.env.ZPL_CONNECT_RETRY_DELAY_MS
  };
  const templatePath = path.join(tempDir, "retry-label.template.zpl");
  const retryServer = http.createServer();
  let connections = 0;

  try {
    process.env.ZPL_TCP_TIMEOUT_MS = "200";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_CONNECT_RETRY_COUNT = "1";
    process.env.ZPL_CONNECT_RETRY_DELAY_MS = "80";
    fs.writeFileSync(templatePath, "^XA^FD{{rfid}}^FS^RFW,H,2,12,1^FD{{rfidHex}}^FS^XZ", "utf8");

    retryServer.on("connection", (socket) => {
      connections += 1;
      socket.resume();
      socket.on("end", () => socket.end());
    });

    const retryPort = await new Promise((resolve) => {
      const holder = http.createServer();
      holder.listen(0, "127.0.0.1", () => {
        const port = holder.address().port;
        holder.close(() => resolve(port));
      });
    });

    setTimeout(() => retryServer.listen(retryPort, "127.0.0.1"), 20);

    const result = await serverModule.sendDirectZplLabel({
      zpl: {
        printerIp: "127.0.0.1",
        port: retryPort,
        templatePath
      },
      station: "P1",
      lotNumber: "PT000086",
      box: 52,
      rfid: "PT000086-B52",
      namedDataSources: {
        pounds: "100",
        type: "RAW",
        color: "BLACK",
        po: "PO1",
        prodname: "Product",
        tolling: "",
        erp: ""
      }
    });

    assert.equal(result.attemptNumber, 2);
    assert.equal(result.bytesSent > 0, true);
    assert.equal(connections, 1);
  } finally {
    if (retryServer.listening) await close(retryServer);
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("direct ZPL does not retry after bytes may have been written", async () => {
  const previous = {
    ZPL_TCP_TIMEOUT_MS: process.env.ZPL_TCP_TIMEOUT_MS,
    ZPL_LABEL_SPACING_MS: process.env.ZPL_LABEL_SPACING_MS,
    ZPL_CONNECT_RETRY_COUNT: process.env.ZPL_CONNECT_RETRY_COUNT,
    ZPL_CONNECT_RETRY_DELAY_MS: process.env.ZPL_CONNECT_RETRY_DELAY_MS
  };
  const templatePath = path.join(tempDir, "timeout-retry-label.template.zpl");
  let attempts = 0;

  try {
    process.env.ZPL_TCP_TIMEOUT_MS = "50";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_CONNECT_RETRY_COUNT = "1";
    process.env.ZPL_CONNECT_RETRY_DELAY_MS = "0";
    fs.writeFileSync(templatePath, "^XA^FD{{rfid}}^FS^RFW,H,2,12,1^FD{{rfidHex}}^FS^XZ", "utf8");

    await assert.rejects(
      () => serverModule.sendDirectZplLabel({
        zpl: {
          printerIp: "127.0.0.1",
          port: 9100,
          templatePath
        },
        station: "P1",
        lotNumber: "PT000086",
        box: 52,
        rfid: "PT000086-B52",
        namedDataSources: {
          pounds: "100",
          type: "RAW",
          color: "BLACK",
          po: "PO1",
          prodname: "Product",
          tolling: "",
          erp: ""
        },
        sendZplOverTcpFn: async () => {
          attempts += 1;
          const error = new Error("Timed out sending ZPL");
          error.code = "ZPL_TCP_TIMEOUT";
          error.details = {
            connected: true,
            writeStarted: true,
            writeCompleted: false,
            bytesAttempted: 64,
            durationMs: 50
          };
          throw error;
        }
      }),
      (error) => {
        assert.equal(error.code, "ZPL_SEND_UNKNOWN");
        assert.equal(error.retryable, false);
        assert.equal(error.operatorAction, "Verify whether the label physically printed before retrying.");
        return true;
      }
    );

    assert.equal(attempts, 1);
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("retry is allowed before any bytes are written", async () => {
  const previous = {
    ZPL_TCP_TIMEOUT_MS: process.env.ZPL_TCP_TIMEOUT_MS,
    ZPL_LABEL_SPACING_MS: process.env.ZPL_LABEL_SPACING_MS,
    ZPL_CONNECT_RETRY_COUNT: process.env.ZPL_CONNECT_RETRY_COUNT,
    ZPL_CONNECT_RETRY_DELAY_MS: process.env.ZPL_CONNECT_RETRY_DELAY_MS
  };
  const templatePath = path.join(tempDir, "prewrite-retry-label.template.zpl");
  let attempts = 0;

  try {
    process.env.ZPL_TCP_TIMEOUT_MS = "50";
    process.env.ZPL_LABEL_SPACING_MS = "0";
    process.env.ZPL_CONNECT_RETRY_COUNT = "1";
    process.env.ZPL_CONNECT_RETRY_DELAY_MS = "0";
    fs.writeFileSync(templatePath, "^XA^FD{{rfid}}^FS^RFW,H,2,12,1^FD{{rfidHex}}^FS^XZ", "utf8");

    const result = await serverModule.sendDirectZplLabel({
      zpl: {
        printerIp: "127.0.0.1",
        port: 9100,
        templatePath
      },
      station: "P1",
      lotNumber: "PT000086",
      box: 52,
      rfid: "PT000086-B52",
      namedDataSources: {
        pounds: "100",
        type: "RAW",
        color: "BLACK",
        po: "PO1",
        prodname: "Product",
        tolling: "",
        erp: ""
      },
      sendZplOverTcpFn: async () => {
        attempts += 1;
        if (attempts === 1) {
          const error = new Error("connect ECONNREFUSED");
          error.code = "ECONNREFUSED";
          error.details = {
            connected: false,
            writeStarted: false,
            bytesAttempted: 0,
            durationMs: 1
          };
          throw error;
        }
        return {
          durationMs: 1,
          bytesSent: 64,
          socketClosed: true
        };
      }
    });

    assert.equal(attempts, 2);
    assert.equal(result.attemptNumber, 2);
    assert.equal(result.sendAccepted, true);
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("unknown direct ZPL response includes verification action", () => {
  const error = new Error("Box 11 may or may not have printed. Verify before resuming.");
  error.code = "ZPL_SEND_UNKNOWN";
  error.operatorAction = "Verify whether the label physically printed before retrying.";
  serverModule.decorateZplPartialFailure(error, {
    results: Array.from({ length: 10 }, (_, index) => ({ box: index + 1 })),
    failedBox: 11
  });

  const payload = serverModule.buildErrorResponsePayload(error, "PRINT_FAILED");

  assert.equal(payload.ok, false);
  assert.equal(payload.error, "ZPL_SEND_UNKNOWN");
  assert.deepEqual(payload.acceptedBoxes, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.equal(payload.unknownBox, 11);
  assert.equal(payload.failedBox, null);
  assert.equal(payload.retryable, false);
  assert.equal(payload.operatorAction, "Verify whether the label physically printed before retrying.");
});

test("partial direct ZPL failure response includes resume details", () => {
  const error = new Error("Timed out sending ZPL");
  error.code = "ZPL_TCP_TIMEOUT";
  serverModule.decorateZplPartialFailure(error, {
    results: [{ box: 1 }, { box: 2 }, { box: 3 }],
    failedBox: 4
  });

  const payload = serverModule.buildErrorResponsePayload(error, "PRINT_FAILED");

  assert.equal(payload.ok, false);
  assert.equal(payload.error, "ZPL_TCP_TIMEOUT");
  assert.deepEqual(payload.acceptedBoxes, [1, 2, 3]);
  assert.deepEqual(payload.printedBoxes, [1, 2, 3]);
  assert.equal(payload.failedBox, 4);
  assert.equal(payload.retryable, true);
});

test("duplicate recent direct ZPL label is rejected", () => {
  const now = Date.now();
  const previousPolicy = process.env.ZPL_DUPLICATE_POLICY;
  delete process.env.ZPL_DUPLICATE_POLICY;
  serverModule.clearRecentZplDuplicateGuard();

  try {
    serverModule.markRecentZplSendAccepted({
      station: "P1",
      lotNumber: "PT000086",
      box: 16,
      rfid: "PT000086-B16"
    }, now);

    assert.throws(
      () => serverModule.assertNoRecentZplDuplicate({
        station: "P1",
        lotNumber: "PT000086",
        box: 16,
        rfid: "PT000086-B16"
      }, now + 1000),
      (error) => error.code === "DUPLICATE_RECENT_ZPL"
    );

    serverModule.assertNoRecentZplDuplicate({
      station: "P1",
      lotNumber: "PT000086",
      box: 16,
      rfid: "PT000086-B16"
    }, now + (11 * 60 * 1000));
  } finally {
    serverModule.clearRecentZplDuplicateGuard();
    if (previousPolicy === undefined) delete process.env.ZPL_DUPLICATE_POLICY;
    else process.env.ZPL_DUPLICATE_POLICY = previousPolicy;
  }
});
