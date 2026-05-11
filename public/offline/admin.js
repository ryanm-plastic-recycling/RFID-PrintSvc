(function () {
  "use strict";

  var banner = document.getElementById("statusBanner");
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

  function applyStatus(payload) {
    var enabled = payload.enabled === true;
    banner.classList.toggle("status-enabled", enabled);
    banner.classList.toggle("status-disabled", !enabled);
    banner.textContent = enabled ? "Emergency Offline Printing ENABLED" : "Emergency Offline Printing DISABLED";
    statusMeta.textContent = enabled
      ? "Reason: " + (payload.reason || "(no reason recorded)")
      : "Offline printing is disabled.";

    document.getElementById("offlineUrl").textContent = window.location.origin + "/offline";
    document.getElementById("currentState").textContent = enabled ? "Enabled" : "Disabled";
    document.getElementById("currentReason").textContent = payload.reason || "-";
    document.getElementById("enabled").value = enabled ? "true" : "false";
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
        auditOutput.textContent = pretty(error);
      });
    } catch (error) {
      setResult(error);
    }
  });

  loadStatus().catch(function (error) {
    banner.classList.add("status-disabled");
    banner.textContent = "Emergency Offline Printing DISABLED";
    statusMeta.textContent = "Unable to read local offline printing status.";
    setResult(error);
  });
})();
