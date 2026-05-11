const fs = require("fs");
const path = require("path");

const DEFAULT_AUDIT_FILE = "C:\\PrintAgent\\offline-print-audit.ndjson";

function getAuditFilePath(env = process.env) {
  return env.OFFLINE_PRINT_AUDIT_FILE || DEFAULT_AUDIT_FILE;
}

function appendOfflineAuditEvent(event, options = {}) {
  const filePath = options.filePath || getAuditFilePath(options.env);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  const record = {
    timestamp: new Date().toISOString(),
    ...event
  };

  fs.appendFileSync(filePath, `${JSON.stringify(record)}\n`, "utf8");
  return record;
}

function readLatestOfflineAuditEvents(limit = 25, options = {}) {
  const filePath = options.filePath || getAuditFilePath(options.env);
  const safeLimit = Math.max(1, Math.min(Number(limit) || 25, 100));

  if (!fs.existsSync(filePath)) return [];

  const lines = fs
    .readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .filter(Boolean);

  return lines
    .slice(-safeLimit)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return { ok: false, error: "Invalid audit log line", raw: line };
      }
    })
    .reverse();
}

module.exports = {
  DEFAULT_AUDIT_FILE,
  appendOfflineAuditEvent,
  getAuditFilePath,
  readLatestOfflineAuditEvents
};
