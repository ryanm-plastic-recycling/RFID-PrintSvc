(function () {
  "use strict";

  var LOCAL_OPERATOR_URL = "http://192.168.50.63:7079/offline";

  var banner = document.getElementById("statusBanner");
  var headerStatus = document.getElementById("headerStatus");
  var statusMeta = document.getElementById("statusMeta");
  var loginForm = document.getElementById("loginForm");
  var toggleForm = document.getElementById("toggleForm");
  var adminControls = document.getElementById("adminControls");
  var auditPanel = document.getElementById("auditPanel");
  var resultOutput = document.getElementById("resultOutput");
  var auditOutput = document.getElementById("auditOutput");

  function pretty(value) {
    return JSON.stringify(value, null, 2);
  }

  function setResult(value) {
    resultOutput.textContent = pretty(value);
  }

  function getText(id) {
    return document.getElementById(id).value.trim();
  }

  function displayValue(value) {
    return value ? String(value) : "-";
  }

  function displayDate(value) {
    if (!value) return "-";
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  }

  function setEnabledVisuals(enabled, reason) {
    banner.classList.toggle("status-enabled", enabled);
    banner.classList.toggle("status-disabled", !enabled);
    headerStatus.classList.toggle("status-pill-enabled", enabled);
    headerStatus.classList.toggle("status-pill-disabled", !enabled);

    banner.querySelector(".status-title").textContent = enabled
      ? "Emergency Offline Printing ENABLED"
      : "Emergency Offline Printing DISABLED";

    headerStatus.textContent = enabled ? "Status: Enabled" : "Status: Disabled";
    statusMeta.textContent = enabled
      ? "Emergency reason: " + (reason || "(no reason recorded)")
      : "Offline printing is disabled. Operators cannot print until an admin enables it.";
  }

  function applyStatus(payload) {
    var enabled = payload.enabled === true;
    var state = payload.state || {};

    setEnabledVisuals(enabled, payload.reason);

    document.getElementById("offlineUrl").textContent = LOCAL_OPERATOR_URL;
    document.getElementById("currentState").textContent = enabled ? "Enabled" : "Disabled";
    document.getElementById("currentReason").textContent = displayValue(payload.reason);
    document.getElementById("enabledBy").textContent = displayValue(state.enabledBy);
    document.getElementById("enabledOn").textContent = displayDate(state.enabledOn);
    document.getElementById("disabledBy").textContent = displayValue(state.disabledBy);
    document.getElementById("disabledOn").textContent = displayDate(state.disabledOn);
    document.getElementById("enabled").value = enabled ? "true" : "false";

    if (enabled && payload.reason) {
      document.getElementById("toggleReason").value = payload.reason;
    }
  }

  async function loadStatus() {
    var response = await fetch("/api/offline/status", { cache: "no-store" });
    var payload = await response.json();
    if (!response.ok) throw payload;
    applyStatus(payload);
    return payload;
  }

  async function loadAudit() {
    var response = await fetch("/api/offline/admin/audit", {
      cache: "no-store",
      credentials: "same-origin"
    });
    var payload = await response.json();
    if (!response.ok) throw payload;
    auditPanel.classList.remove("hidden");
    auditOutput.textContent = pretty(payload.records || []);
  }

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();

    var payload = {
      adminName: getText("adminName"),
      password: document.getElementById("password").value
    };

    try {
      var response = await fetch("/api/offline/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(payload)
      });

      var result = await response.json();
      setResult(result);
      if (!response.ok) return;

      adminControls.classList.remove("hidden");
      await loadStatus();
      await loadAudit().catch(function (error) {
        auditPanel.classList.remove("hidden");
        auditOutput.textContent = pretty(error);
      });
    } catch (error) {
      setResult(error);
    }
  });

  toggleForm.addEventListener("submit", async function (event) {
    event.preventDefault();

    var payload = {
      enabled: document.getElementById("enabled").value === "true",
      reason: getText("toggleReason"),
      adminName: getText("adminName")
    };

    try {
      var response = await fetch("/api/offline/admin/toggle", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(payload)
      });

      var result = await response.json();
      setResult(result);
      if (!response.ok) return;

      await loadStatus();
      await loadAudit().catch(function (error) {
        auditPanel.classList.remove("hidden");
        auditOutput.textContent = pretty(error);
      });
    } catch (error) {
      setResult(error);
    }
  });

  loadStatus().catch(function (error) {
    setEnabledVisuals(false, "");
    statusMeta.textContent = "Unable to read local offline printing status.";
    setResult(error);
  });
})();
