(function () {
  "use strict";

  var state = {
    enabled: false,
    maxLabels: 99,
    maxBoxNumber: 99
  };

  var form = document.getElementById("offlinePrintForm");
  var banner = document.getElementById("statusBanner");
  var statusMeta = document.getElementById("statusMeta");
  var stationSelect = document.getElementById("station");
  var familySelect = document.getElementById("family");
  var printButton = document.getElementById("printButton");
  var dryRunButton = document.getElementById("dryRunButton");
  var resultOutput = document.getElementById("resultOutput");

  function pretty(value) {
    return JSON.stringify(value, null, 2);
  }

  function setResult(value) {
    resultOutput.textContent = pretty(value);
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function getNumber(id) {
    return Number(document.getElementById(id).value);
  }

  function getText(id) {
    return document.getElementById(id).value.trim();
  }

  function updatePreview() {
    var lotNumber = getText("lotNumber");
    var firstBox = getNumber("firstBox");
    var lastBox = getNumber("lastBox");
    var validRange = Number.isInteger(firstBox) && Number.isInteger(lastBox) && firstBox >= 1 && firstBox <= lastBox;
    var count = validRange ? lastBox - firstBox + 1 : 0;

    document.getElementById("firstRfid").textContent = lotNumber && validRange ? lotNumber + "-B" + pad2(firstBox) : "-";
    document.getElementById("lastRfid").textContent = lotNumber && validRange ? lotNumber + "-B" + pad2(lastBox) : "-";
    document.getElementById("labelCount").textContent = String(count);
  }

  function populateSelect(select, values, labeler) {
    select.innerHTML = "";
    values.forEach(function (value) {
      var option = document.createElement("option");
      option.value = value;
      option.textContent = labeler ? labeler(value) : value;
      select.appendChild(option);
    });
  }

  function applyStatus(payload) {
    state.enabled = payload.enabled === true;
    state.maxLabels = Number(payload.maxLabels || 99);
    state.maxBoxNumber = Number(payload.maxBoxNumber || 99);

    banner.classList.toggle("status-enabled", state.enabled);
    banner.classList.toggle("status-disabled", !state.enabled);
    banner.textContent = state.enabled ? "Emergency Offline Printing ENABLED" : "Emergency Offline Printing DISABLED";

    statusMeta.textContent = state.enabled
      ? "Reason: " + (payload.reason || "(no reason recorded)")
      : "Offline printing is disabled by default. Contact an admin to enable it during an outage.";

    printButton.disabled = !state.enabled;
    dryRunButton.disabled = !state.enabled;

    populateSelect(stationSelect, payload.allowedStations || []);
    populateSelect(familySelect, payload.familyOptions || ["AUTO", "RAW", "FG"], function (value) {
      return value === "AUTO" ? "Auto" : value;
    });

    document.getElementById("lastBox").max = String(state.maxBoxNumber);
    document.getElementById("firstBox").max = String(state.maxBoxNumber);
    updatePreview();
  }

  async function loadStatus() {
    var response = await fetch("/api/offline/status", { cache: "no-store" });
    var payload = await response.json();
    if (!response.ok) throw payload;
    applyStatus(payload);
  }

  function buildPayload(dryRun) {
    return {
      station: getText("station"),
      family: getText("family"),
      lotNumber: getText("lotNumber"),
      firstBox: getNumber("firstBox"),
      lastBox: getNumber("lastBox"),
      material: getText("material"),
      color: getText("color"),
      format: getText("format"),
      productCode: getText("productCode"),
      productName: getText("productName"),
      productDescription: getText("productDescription"),
      purchaseOrder: getText("purchaseOrder"),
      customer: getText("customer"),
      tolling: document.getElementById("tolling").checked,
      pounds: getText("pounds") || "_",
      operator: getText("operator"),
      reason: getText("reason"),
      confirmationAccepted: document.getElementById("confirmationAccepted").checked,
      dryRun: dryRun === true
    };
  }

  async function sendPrintRequest(dryRun) {
    var payload = buildPayload(dryRun);
    setResult({ pending: true, dryRun: dryRun === true, payload: payload });

    var response = await fetch("/api/offline/print-labels", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    var result = await response.json();
    setResult(result);

    if (!response.ok) {
      throw result;
    }

    return result;
  }

  form.addEventListener("input", updatePreview);
  form.addEventListener("change", updatePreview);

  dryRunButton.addEventListener("click", function () {
    sendPrintRequest(true).catch(function () {});
  });

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    sendPrintRequest(false).catch(function () {});
  });

  loadStatus().catch(function (error) {
    banner.classList.add("status-disabled");
    banner.textContent = "Emergency Offline Printing DISABLED";
    statusMeta.textContent = "Unable to read local offline printing status.";
    printButton.disabled = true;
    dryRunButton.disabled = true;
    setResult(error);
  });
})();
