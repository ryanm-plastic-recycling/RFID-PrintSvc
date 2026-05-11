const fs = require("fs");
const path = require("path");

const DEFAULT_STATE_FILE = "C:\\PrintAgent\\offline-print-state.json";

const DEFAULT_STATE = Object.freeze({
  enabled: false,
  reason: "",
  enabledBy: "",
  enabledOn: "",
  disabledBy: "",
  disabledOn: "",
  updatedOn: ""
});

function getStateFilePath(env = process.env) {
  return env.OFFLINE_PRINT_STATE_FILE || DEFAULT_STATE_FILE;
}

function normalizeState(raw) {
  const state = { ...DEFAULT_STATE, ...(raw && typeof raw === "object" ? raw : {}) };
  state.enabled = state.enabled === true;

  for (const key of ["reason", "enabledBy", "enabledOn", "disabledBy", "disabledOn", "updatedOn"]) {
    state[key] = state[key] == null ? "" : String(state[key]);
  }

  return state;
}

function readOfflineState(options = {}) {
  const filePath = options.filePath || getStateFilePath(options.env);
  if (!fs.existsSync(filePath)) return normalizeState();

  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return normalizeState(parsed);
}

function writeOfflineState(state, options = {}) {
  const filePath = options.filePath || getStateFilePath(options.env);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  const normalized = normalizeState(state);
  fs.writeFileSync(filePath, `${JSON.stringify(normalized, null, 2)}\n`, "utf8");
  return normalized;
}

module.exports = {
  DEFAULT_STATE,
  DEFAULT_STATE_FILE,
  getStateFilePath,
  normalizeState,
  readOfflineState,
  writeOfflineState
};
