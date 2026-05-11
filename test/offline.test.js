const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");
const { isOfflineLocalAccessAllowed } = require("../lib/offlineSecurity");

let apiServer;
let apiBaseUrl;
let bartenderServer;
let bartenderBaseUrl;
let tempDir;
let stateFile;
let auditFile;
let bartenderRequests = [];

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

test.before(async () => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rfid-offline-test-"));
  stateFile = path.join(tempDir, "offline-state.json");
  auditFile = path.join(tempDir, "offline-audit.ndjson");

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
  process.env.OFFLINE_PRINT_MAX_LABELS = "3";
  process.env.OFFLINE_PRINT_MAX_BOX_NUMBER = "5";
  process.env.OFFLINE_PRINT_ALLOWED_HOSTS = "localhost,127.0.0.1";
  process.env.PRINT_JOB_SPACING_MS = "0";

  const { app } = require("../server");
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
