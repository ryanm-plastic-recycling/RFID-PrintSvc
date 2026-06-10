const fs = require("fs");
const net = require("net");

const DEFAULT_ZPL_TIMEOUT_MS = 120000;
const TOKEN_PATTERN = /{{\s*([A-Za-z][A-Za-z0-9_]*)\s*}}/g;
const CONDITIONAL_PATTERN = /{{#if\s+([A-Za-z][A-Za-z0-9_]*)\s*}}([\s\S]*?){{\/if}}/g;
const FIELD_FIT_DEFINITIONS_COMMENT_PREFIX = "TEMPLATE_LAB_FIELD_FIT_DEFINITIONS_BASE64:";
const RFID_HEX_LENGTH = 24;
const DEFAULT_VISIBLE_FIELD_MAX_LENGTH = 48;
const VISIBLE_FIELD_MAX_LENGTHS = Object.freeze({
  lotNumber: 24,
  boxNumber: 8,
  rfid: 12,
  pounds: 12,
  materialType: 24,
  materialTypeText: 8,
  materialTypeSmallText: 8,
  materialTypeFontH: 4,
  materialTypeFontW: 4,
  materialTypeBoxW: 4,
  materialTypeSmallFontH: 4,
  materialTypeSmallFontW: 4,
  materialTypeSmallBoxW: 4,
  color: 24,
  colorText: 8,
  colorSmallText: 8,
  colorFontH: 4,
  colorFontW: 4,
  colorBoxW: 4,
  colorSmallFontH: 4,
  colorSmallFontW: 4,
  colorSmallBoxW: 4,
  po: 32,
  productCode: 24,
  productName: 48,
  productDescription: 48,
  productDescriptionText: 48,
  productDescriptionFontH: 4,
  productDescriptionFontW: 4,
  productDescriptionBoxW: 4,
  productDescriptionAlignment: 1,
  productDescriptionMaxLines: 2,
  tolling: 16,
  tollingText: 8,
  tollingFontH: 4,
  tollingFontW: 4,
  tollingBoxW: 4,
  erp: 16,
  qrData: 96,
  machine: 32,
  company: 48,
  labelType: 32,
  sampleType: 24,
  sampleTime: 24,
  sampleLabel: 32,
  frequencyCheck: 32,
  printedDate: 16
});

const FITTED_FIELD_DEFINITIONS = Object.freeze({
  color: {
    source: "color",
    boxWidth: 189,
    maxChars: 8,
    large: { fontH: 82, fontW: 60 },
    medium: { fontH: 54, fontW: 38 },
    small: { fontH: 38, fontW: 22 },
    min: { fontH: 28, fontW: 18 }
  },
  colorSmall: {
    source: "color",
    boxWidth: 94,
    maxChars: 8,
    large: { fontH: 44, fontW: 28 },
    medium: { fontH: 32, fontW: 18 },
    small: { fontH: 24, fontW: 12 },
    min: { fontH: 20, fontW: 10 }
  },
  materialType: {
    source: "materialType",
    boxWidth: 738,
    maxChars: 8,
    large: { fontH: 96, fontW: 130 },
    medium: { fontH: 82, fontW: 92 },
    small: { fontH: 68, fontW: 75 },
    min: { fontH: 54, fontW: 60 }
  },
  materialTypeSmall: {
    source: "materialType",
    boxWidth: 93,
    maxChars: 8,
    large: { fontH: 44, fontW: 28 },
    medium: { fontH: 32, fontW: 18 },
    small: { fontH: 24, fontW: 12 },
    min: { fontH: 20, fontW: 10 }
  },
  tolling: {
    source: "tolling",
    boxWidth: 195,
    maxChars: 8,
    large: { fontH: 46, fontW: 54 },
    medium: { fontH: 38, fontW: 40 },
    small: { fontH: 30, fontW: 28 },
    min: { fontH: 24, fontW: 18 }
  },
  productDescription: {
    source: "productDescription",
    boxWidth: 430,
    maxChars: 32,
    maxLines: 1,
    alignment: "L",
    large: { fontH: 92, fontW: 68 },
    medium: { fontH: 58, fontW: 34 },
    small: { fontH: 38, fontW: 22 },
    min: { fontH: 24, fontW: 14 }
  }
});

function zplError(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  if (code === "INVALID_RFID") error.statusCode = 400;
  return error;
}

function sanitizeVisibleFieldValue(value, maxLength = DEFAULT_VISIBLE_FIELD_MAX_LENGTH) {
  const sanitized = String(value ?? "")
    .replace(/[\x00-\x1F\x7F]/g, " ")
    .replace(/[\^~]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const limit = Number(maxLength);
  if (!Number.isInteger(limit) || limit <= 0) return sanitized;
  return sanitized.length > limit ? sanitized.slice(0, limit) : sanitized;
}

function fitZplBoxedText(value, options = {}) {
  const boxWidth = Number.isInteger(options.boxWidth) && options.boxWidth > 0 ? options.boxWidth : 120;
  const maxChars = Number.isInteger(options.maxChars) && options.maxChars > 0 ? options.maxChars : 8;
  const maxLines = Number.isInteger(options.maxLines) && options.maxLines > 0 ? options.maxLines : 1;
  const alignment = ["L", "C", "R", "J"].includes(String(options.alignment || "").toUpperCase())
    ? String(options.alignment).toUpperCase()
    : "C";
  const sanitized = sanitizeVisibleFieldValue(value, DEFAULT_VISIBLE_FIELD_MAX_LENGTH);
  const text = sanitized.length > maxChars ? sanitized.slice(0, maxChars) : sanitized;
  const truncated = sanitized.length > text.length;
  const visibleLength = Math.max(text.length, 1);
  const tier = visibleLength <= 3 ? options.large : visibleLength <= 6 ? options.medium : options.small;
  const selected = tier || options.small || options.medium || options.large || { fontH: 24, fontW: 12 };
  const minimum = options.min || { fontH: 20, fontW: 10 };
  const horizontalPadding = Number.isInteger(options.horizontalPadding) ? options.horizontalPadding : 8;
  const maxFontWForBox = Math.max(minimum.fontW, Math.floor((boxWidth - horizontalPadding) / visibleLength));
  const fontW = Math.max(minimum.fontW, Math.min(selected.fontW, maxFontWForBox));
  const fontH = Math.max(minimum.fontH, Math.min(selected.fontH, Math.round(fontW * 1.45)));

  return {
    original: String(value ?? ""),
    sanitized,
    text,
    fontH,
    fontW,
    boxW: boxWidth,
    maxChars,
    maxLines,
    alignment,
    truncated
  };
}

function mergeFittedTextDefinition(base = {}, override = {}) {
  return {
    ...base,
    ...override,
    large: { ...(base.large || {}), ...(override.large || {}) },
    medium: { ...(base.medium || {}), ...(override.medium || {}) },
    small: { ...(base.small || {}), ...(override.small || {}) },
    min: { ...(base.min || {}), ...(override.min || {}) }
  };
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
    } else if (value !== undefined) {
      output[key] = value;
    }
  }
  return output;
}

function extractTemplateFieldFitDefinitions(template) {
  const pattern = new RegExp(`\\^FX\\s*${FIELD_FIT_DEFINITIONS_COMMENT_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}([A-Za-z0-9+/=]+)`, "g");
  const matches = Array.from(String(template || "").matchAll(pattern));
  if (!matches.length) return {};

  try {
    const encoded = matches[matches.length - 1][1];
    const decoded = Buffer.from(encoded, "base64").toString("utf8");
    const parsed = JSON.parse(decoded);
    return isPlainObject(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function getFittedFieldDefinitions(overrides = {}) {
  const merged = {};

  for (const [prefix, definition] of Object.entries(FITTED_FIELD_DEFINITIONS)) {
    merged[prefix] = mergeFittedTextDefinition(definition, overrides[prefix]);
  }

  for (const [prefix, definition] of Object.entries(overrides || {})) {
    if (!merged[prefix]) {
      merged[prefix] = mergeFittedTextDefinition({}, definition);
    }
  }

  return merged;
}

function buildFittedZplFieldValues(inputValues = {}, options = {}) {
  const values = { ...inputValues };
  const debug = {};
  const definitions = getFittedFieldDefinitions(options.fieldFitDefinitions || {});

  for (const [prefix, definition] of Object.entries(definitions)) {
    const fit = fitZplBoxedText(values[definition.source], definition);
    values[`${prefix}Text`] = fit.text;
    values[`${prefix}FontH`] = String(fit.fontH);
    values[`${prefix}FontW`] = String(fit.fontW);
    values[`${prefix}BoxW`] = String(fit.boxW);
    values[`${prefix}MaxLines`] = String(fit.maxLines);
    values[`${prefix}Alignment`] = fit.alignment;
    debug[prefix] = {
      source: definition.source,
      original: fit.original,
      sanitized: fit.sanitized,
      fittedText: fit.text,
      fontH: fit.fontH,
      fontW: fit.fontW,
      boxW: fit.boxW,
      maxChars: fit.maxChars,
      maxLines: fit.maxLines,
      truncated: fit.truncated,
      oneLine: fit.maxLines === 1,
      alignment: fit.alignment === "C" ? "center" : fit.alignment === "R" ? "right" : fit.alignment === "J" ? "justified" : "left",
      hyphenation: false
    };
  }

  return { values, debug };
}

function addFittedZplFieldValues(values, options = {}) {
  const fitted = buildFittedZplFieldValues(values, options);
  Object.assign(values, fitted.values);
  return values;
}

function validateRfidText(rfid) {
  const text = String(rfid ?? "");

  if (text.length !== 12) {
    throw zplError("INVALID_RFID", "RFID text must be exactly 12 ASCII characters.", {
      rfid: text,
      length: text.length
    });
  }

  if (!/^[\x20-\x7E]{12}$/.test(text)) {
    throw zplError("INVALID_RFID", "RFID text must contain only printable ASCII characters.", {
      rfid: text
    });
  }

  return text;
}

function rfidTextToHex(rfid) {
  const text = validateRfidText(rfid);
  return Buffer.from(text, "ascii").toString("hex").toUpperCase();
}

function sanitizeRfidHex(value) {
  const text = String(value ?? "").trim().toUpperCase();
  if (!/^[0-9A-F]+$/.test(text) || text.length !== RFID_HEX_LENGTH) {
    throw zplError("INVALID_RFID_HEX", "RFID HEX must be 24 uppercase hexadecimal characters.", {
      rfidHex: text,
      length: text.length
    });
  }
  return text;
}

function loadZplTemplate(templatePath) {
  const pathText = String(templatePath || "").trim();
  if (!pathText) {
    throw zplError("ZPL_TEMPLATE_MISSING", "ZPL template path is required.");
  }
  try {
    return fs.readFileSync(pathText, "utf8");
  } catch (error) {
    throw zplError("ZPL_TEMPLATE_READ_FAILED", `Unable to load ZPL template '${pathText}': ${error.message}`, {
      templatePath: pathText
    });
  }
}

function collectTemplateTokens(template) {
  return new Set(Array.from(String(template ?? "").matchAll(TOKEN_PATTERN), (match) => match[1]));
}

function renderZplTemplateInternal(template, data = {}, options = {}) {
  const requireRfid = options.requireRfid !== false;
  const tokens = collectTemplateTokens(template);
  const templateFieldFitDefinitions = extractTemplateFieldFitDefinitions(template);
  const fieldFitDefinitions = deepMergePlainObjects(templateFieldFitDefinitions, options.fieldFitDefinitions || {});
  const fitted = buildFittedZplFieldValues(data, {
    fieldFitDefinitions
  });
  const values = fitted.values;
  const templateUsesRfid = tokens.has("rfid") || tokens.has("rfidHex");

  if (requireRfid || templateUsesRfid) {
    values.rfid = validateRfidText(values.rfid);
    values.rfidHex = sanitizeRfidHex(values.rfidHex || rfidTextToHex(values.rfid));
  } else if (values.rfidHex) {
    values.rfidHex = sanitizeRfidHex(values.rfidHex);
  }

  const conditionalTemplate = String(template ?? "").replace(CONDITIONAL_PATTERN, (_match, field, block) => {
    const visible = sanitizeVisibleFieldValue(values[field], VISIBLE_FIELD_MAX_LENGTHS[field] || DEFAULT_VISIBLE_FIELD_MAX_LENGTH);
    return visible ? block : "";
  });

  const rendered = conditionalTemplate.replace(TOKEN_PATTERN, (match, token) => {
    if (!Object.prototype.hasOwnProperty.call(values, token)) return match;
    if (token === "rfidHex") return values.rfidHex;
    return sanitizeVisibleFieldValue(values[token], VISIBLE_FIELD_MAX_LENGTHS[token]);
  });

  const remainingTokens = Array.from(rendered.matchAll(TOKEN_PATTERN), (match) => match[0]);
  if (remainingTokens.length) {
    throw zplError("UNREPLACED_ZPL_TOKENS", "ZPL template has unreplaced tokens after rendering.", {
      tokens: remainingTokens
    });
  }

  if (options.includeDebug) {
    return {
      rendered,
      fitDebug: fitted.debug
    };
  }

  return rendered;
}

function renderZplTemplate(template, data = {}, options = {}) {
  return renderZplTemplateInternal(template, data, { ...options, requireRfid: true });
}

function renderZplTemplateWithoutRfid(template, data = {}, options = {}) {
  return renderZplTemplateInternal(template, data, { ...options, requireRfid: false });
}

function renderZplTemplateWithMetadata(template, data = {}, options = {}) {
  return renderZplTemplateInternal(template, data, { ...options, requireRfid: true, includeDebug: true });
}

function renderZplTemplateWithoutRfidWithMetadata(template, data = {}, options = {}) {
  return renderZplTemplateInternal(template, data, { ...options, requireRfid: false, includeDebug: true });
}

function renderZplTemplateFile(templatePath, data = {}, options = {}) {
  return renderZplTemplate(loadZplTemplate(templatePath), data, options);
}

function renderZplTemplateFileWithoutRfid(templatePath, data = {}, options = {}) {
  return renderZplTemplateWithoutRfid(loadZplTemplate(templatePath), data, options);
}

function sendZplOverTcp({ printerIp, host, port = 9100, zpl, timeoutMs = DEFAULT_ZPL_TIMEOUT_MS, socketFactory }) {
  const targetHost = String(printerIp || host || "").trim();
  const targetPort = Number(port || 9100);
  const payload = String(zpl ?? "");
  const bytesSent = Buffer.byteLength(payload, "utf8");
  const effectiveTimeoutMs = Number(timeoutMs) || DEFAULT_ZPL_TIMEOUT_MS;

  if (!targetHost) {
    return Promise.reject(zplError("ZPL_PRINTER_IP_MISSING", "ZPL printer IP/host is required."));
  }

  if (!Number.isInteger(targetPort) || targetPort <= 0 || targetPort > 65535) {
    return Promise.reject(zplError("ZPL_PRINTER_PORT_INVALID", "ZPL printer port must be a valid TCP port.", {
      port
    }));
  }

  if (!payload) {
    return Promise.reject(zplError("ZPL_PAYLOAD_EMPTY", "Rendered ZPL payload is empty."));
  }

  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const socket = socketFactory ? socketFactory() : new net.Socket();
    let settled = false;
    let connected = false;
    let writeStarted = false;
    let writeCompleted = false;
    let endCompleted = false;
    let socketClosed = false;

    function finish(error) {
      if (settled) return;
      settled = true;
      socket.destroy();

      if (error) {
        error.details = {
          ...(error.details || {}),
          printerIp: targetHost,
          port: targetPort,
          durationMs: Date.now() - startedAt,
          connected,
          writeStarted,
          writeCompleted,
          endCompleted,
          socketClosed,
          bytesAttempted: writeStarted ? bytesSent : 0,
          bytesSent: writeCompleted ? bytesSent : 0
        };
        reject(error);
      } else {
        resolve({
          durationMs: Date.now() - startedAt,
          bytesSent,
          socketClosed,
          connected,
          writeStarted,
          writeCompleted,
          endCompleted
        });
      }
    }

    socket.setTimeout(effectiveTimeoutMs);

    socket.once("timeout", () => {
      finish(zplError("ZPL_TCP_TIMEOUT", `Timed out sending ZPL to ${targetHost}:${targetPort}.`, {
        printerIp: targetHost,
        port: targetPort,
        timeoutMs: effectiveTimeoutMs
      }));
    });

    socket.once("error", (error) => {
      finish(error);
    });

    socket.once("close", () => {
      socketClosed = true;
      if (connected && writeCompleted) finish(null);
    });

    socket.connect(targetPort, targetHost, () => {
      connected = true;
      writeStarted = true;
      socket.write(payload, "utf8", (error) => {
        if (error) return finish(error);
        writeCompleted = true;
        socket.end(() => {
          endCompleted = true;
          finish(null);
        });
      });
    });
  });
}

module.exports = {
  DEFAULT_ZPL_TIMEOUT_MS,
  DEFAULT_VISIBLE_FIELD_MAX_LENGTH,
  FIELD_FIT_DEFINITIONS_COMMENT_PREFIX,
  VISIBLE_FIELD_MAX_LENGTHS,
  buildFittedZplFieldValues,
  extractTemplateFieldFitDefinitions,
  getFittedFieldDefinitions,
  loadZplTemplate,
  renderZplTemplate,
  renderZplTemplateFile,
  renderZplTemplateWithoutRfid,
  renderZplTemplateFileWithoutRfid,
  renderZplTemplateWithMetadata,
  renderZplTemplateWithoutRfidWithMetadata,
  fitZplBoxedText,
  rfidTextToHex,
  sanitizeVisibleFieldValue,
  sendZplOverTcp,
  validateRfidText
};
