const crypto = require("crypto");

const ADMIN_COOKIE_NAME = "offline_print_admin";
const ADMIN_SESSION_TTL_MS = 30 * 60 * 1000;

function parseList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeHost(host) {
  let value = String(host || "").trim().toLowerCase();
  if (!value) return "";

  if (value.startsWith("[")) {
    const close = value.indexOf("]");
    return close >= 0 ? value.slice(1, close) : value.replace(/^\[/, "");
  }

  const colonCount = (value.match(/:/g) || []).length;
  if (colonCount === 1) value = value.split(":")[0];

  return value;
}

function normalizeIp(ip) {
  let value = String(ip || "").trim().toLowerCase();
  if (!value) return "";

  if (value.startsWith("::ffff:")) value = value.slice(7);
  if (value.startsWith("[")) value = normalizeHost(value);

  const zoneIndex = value.indexOf("%");
  if (zoneIndex >= 0) value = value.slice(0, zoneIndex);

  return value;
}

function parseAllowedHosts(env = process.env) {
  return new Set(parseList(env.OFFLINE_PRINT_ALLOWED_HOSTS).map(normalizeHost));
}

function isLocalhostName(host) {
  const normalized = normalizeHost(host);
  return normalized === "localhost" || normalized.endsWith(".localhost");
}

function parseIPv4(value) {
  const normalized = normalizeIp(value);
  const parts = normalized.split(".");
  if (parts.length !== 4) return null;

  const octets = parts.map((part) => {
    if (!/^\d+$/.test(part)) return NaN;
    const number = Number(part);
    return number >= 0 && number <= 255 ? number : NaN;
  });

  return octets.every((number) => Number.isInteger(number)) ? octets : null;
}

function isLoopbackIp(ip) {
  const normalized = normalizeIp(ip);
  if (normalized === "::1" || normalized === "0:0:0:0:0:0:0:1") return true;

  const octets = parseIPv4(normalized);
  return Boolean(octets && octets[0] === 127);
}

function isPrivateIPv4(ip) {
  const octets = parseIPv4(ip);
  if (!octets) return false;

  const [a, b] = octets;
  return a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
}

function hasCloudflareHeaders(req) {
  return Object.keys(req.headers || {}).some((header) => header.toLowerCase().startsWith("cf-"));
}

function getSourceIp(req) {
  return normalizeIp(req.socket?.remoteAddress || req.connection?.remoteAddress || "");
}

function isExplicitAllowedHost(host, env = process.env) {
  const normalized = normalizeHost(host);
  return Boolean(normalized && parseAllowedHosts(env).has(normalized));
}

function isOfflineLocalAccessAllowed(req, env = process.env) {
  const hostHeader = String(req.headers?.host || "");
  const hostName = normalizeHost(hostHeader);
  const sourceIp = getSourceIp(req);
  const explicitlyAllowedHost = isExplicitAllowedHost(hostName, env);
  const explicitlyAllowedSource = isExplicitAllowedHost(sourceIp, env);
  const explicitHostIsLocal =
    isLocalhostName(hostName) ||
    isLoopbackIp(hostName) ||
    isPrivateIPv4(hostName);
  const explicitHostRemoteOverride = explicitlyAllowedHost && !explicitHostIsLocal;

  if (hostName.includes("rfidprint.plastic-recycling.net") && !explicitlyAllowedHost) {
    return {
      ok: false,
      status: 403,
      code: "OFFLINE_PUBLIC_HOST_BLOCKED",
      message: "Emergency offline printing is only available from local/LAN hostnames."
    };
  }

  if (hasCloudflareHeaders(req) && !explicitlyAllowedHost) {
    return {
      ok: false,
      status: 403,
      code: "OFFLINE_CLOUDFLARE_BLOCKED",
      message: "Emergency offline printing is not available through Cloudflare/public proxy access."
    };
  }

  const localSource = isLoopbackIp(sourceIp) || isPrivateIPv4(sourceIp);

  if (!localSource && !explicitlyAllowedSource && !explicitHostRemoteOverride) {
    return {
      ok: false,
      status: 403,
      code: "OFFLINE_LOCAL_ACCESS_REQUIRED",
      message: "Emergency offline printing is only available from localhost or the plant LAN."
    };
  }

  return { ok: true, sourceIp, host: hostHeader, hostName };
}

function requireOfflineLocalAccess(req, res, next) {
  const result = isOfflineLocalAccessAllowed(req);
  if (!result.ok) {
    return res.status(result.status).json({
      ok: false,
      error: result.code,
      message: result.message
    });
  }

  req.offlineAccess = {
    sourceIp: result.sourceIp,
    host: result.host,
    hostName: result.hostName
  };

  return next();
}

function base64UrlEncode(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function base64UrlDecode(value) {
  const normalized = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
  return Buffer.from(padded, "base64").toString("utf8");
}

function signValue(value, secret) {
  return crypto
    .createHmac("sha256", String(secret || ""))
    .update(value)
    .digest("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a || ""));
  const right = Buffer.from(String(b || ""));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function parseCookies(cookieHeader) {
  const cookies = {};
  for (const part of String(cookieHeader || "").split(";")) {
    const index = part.indexOf("=");
    if (index <= 0) continue;
    const name = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (name) cookies[name] = value;
  }
  return cookies;
}

function createAdminCookieValue({ adminName = "", now = Date.now(), env = process.env } = {}) {
  const secret = env.OFFLINE_PRINT_SESSION_SECRET;
  if (!secret) throw new Error("OFFLINE_PRINT_SESSION_SECRET is not configured.");

  const payload = {
    sub: "offline-print-admin",
    adminName: String(adminName || "").trim(),
    iat: Math.floor(now / 1000),
    exp: Math.floor((now + ADMIN_SESSION_TTL_MS) / 1000)
  };

  const encoded = base64UrlEncode(JSON.stringify(payload));
  return `${encoded}.${signValue(encoded, secret)}`;
}

function verifyAdminCookieValue(cookieValue, { now = Date.now(), env = process.env } = {}) {
  const secret = env.OFFLINE_PRINT_SESSION_SECRET;
  if (!secret) throw new Error("OFFLINE_PRINT_SESSION_SECRET is not configured.");

  const [encoded, signature] = String(cookieValue || "").split(".");
  if (!encoded || !signature) return null;

  const expected = signValue(encoded, secret);
  if (!timingSafeEqualString(signature, expected)) return null;

  let payload;
  try {
    payload = JSON.parse(base64UrlDecode(encoded));
  } catch {
    return null;
  }

  if (payload?.sub !== "offline-print-admin") return null;
  if (!payload.exp || Number(payload.exp) * 1000 <= now) return null;

  return payload;
}

function setAdminCookie(res, { adminName = "" } = {}) {
  const value = createAdminCookieValue({ adminName });
  const maxAgeSeconds = Math.floor(ADMIN_SESSION_TTL_MS / 1000);
  res.setHeader(
    "Set-Cookie",
    `${ADMIN_COOKIE_NAME}=${value}; Max-Age=${maxAgeSeconds}; Path=/api/offline/admin; HttpOnly; SameSite=Strict`
  );
}

function requireOfflineAdminCookie(req, res, next) {
  try {
    const cookies = parseCookies(req.headers.cookie);
    const payload = verifyAdminCookieValue(cookies[ADMIN_COOKIE_NAME]);
    if (!payload) {
      return res.status(401).json({ ok: false, error: "ADMIN_LOGIN_REQUIRED", message: "Offline admin login is required." });
    }

    req.offlineAdmin = payload;
    return next();
  } catch (error) {
    return res.status(500).json({ ok: false, error: "OFFLINE_ADMIN_CONFIG_ERROR", message: error.message });
  }
}

module.exports = {
  ADMIN_COOKIE_NAME,
  ADMIN_SESSION_TTL_MS,
  createAdminCookieValue,
  getSourceIp,
  hasCloudflareHeaders,
  isOfflineLocalAccessAllowed,
  isPrivateIPv4,
  isLoopbackIp,
  normalizeHost,
  normalizeIp,
  parseAllowedHosts,
  parseCookies,
  requireOfflineAdminCookie,
  requireOfflineLocalAccess,
  setAdminCookie,
  verifyAdminCookieValue
};
