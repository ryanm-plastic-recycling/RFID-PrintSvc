(function () {
  "use strict";

  var LOCAL_OPERATOR_URL = "http://192.168.50.63:7079/offline";

  var MATERIAL_OPTIONS = [
    "ABS",
    "ADD",
    "BOPP",
    "COMP",
    "GPPS",
    "HDPE",
    "HIPS",
    "HMW",
    "KRES",
    "LLDPE",
    "LDPE",
    "MSTR",
    "MIX",
    "MOD",
    "OPS",
    "PC",
    "PCABS",
    "PE",
    "PET",
    "PP",
    "REINF",
    "TPO"
  ];

  var COLOR_OPTIONS = [
    "Black",
    "Blue",
    "Brown",
    "Clear",
    "Gray",
    "Green",
    "Mixed",
    "Natural",
    "Off Color",
    "Red",
    "Silver",
    "Tan",
    "White",
    "Yellow"
  ];

  var FORMAT_OPTIONS = ["Bales", "Mixed", "Parts", "Pellets", "Powder", "Regrind", "Rolls"];

  var FORMAT_CODES = {
    Bales: "BA",
    Mixed: "MX",
    Parts: "PT",
    Pellets: "FF",
    Powder: "PW",
    Regrind: "RG",
    Rolls: "RL"
  };

  var FAMILY_LABELS = {
    AUTO: "Auto - Use lot prefix",
    RAW: "RAW - Raw Goods Label",
    FG: "FG - Finished Goods Label"
  };

  var state = {
    enabled: false,
    maxLabels: 99,
    maxBoxNumber: 99
  };

  var form = document.getElementById("offlinePrintForm");
  var banner = document.getElementById("statusBanner");
  var headerStatus = document.getElementById("headerStatus");
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

  function populateDatalist(id, values) {
    var list = document.getElementById(id);
    list.innerHTML = "";
    values.forEach(function (value) {
      var option = document.createElement("option");
      option.value = value;
      list.appendChild(option);
    });
  }

  function populateSelect(select, options) {
    select.innerHTML = "";
    options.forEach(function (item) {
      var option = document.createElement("option");
      option.value = item.value;
      option.textContent = item.label;
      select.appendChild(option);
    });
  }

  function stationOptionsFromStatus(payload) {
    if (Array.isArray(payload.stationOptions) && payload.stationOptions.length) {
      return payload.stationOptions.map(function (item) {
        return {
          value: item.code,
          label: item.label || item.code
        };
      });
    }

    return (payload.allowedStations || []).map(function (station) {
      return { value: station, label: station };
    });
  }

  function familyOptionsFromStatus(payload) {
    return (payload.familyOptions || ["AUTO", "RAW", "FG"]).map(function (family) {
      return {
        value: family,
        label: FAMILY_LABELS[family] || family
      };
    });
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
      : "Offline printing is disabled. Contact an admin before printing during an outage.";
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

  function applyStatus(payload) {
    state.enabled = payload.enabled === true;
    state.maxLabels = Number(payload.maxLabels || 99);
    state.maxBoxNumber = Number(payload.maxBoxNumber || 99);

    setEnabledVisuals(state.enabled, payload.reason);

    printButton.disabled = !state.enabled;
    dryRunButton.disabled = !state.enabled;

    populateSelect(stationSelect, stationOptionsFromStatus(payload));
    populateSelect(familySelect, familyOptionsFromStatus(payload));

    document.getElementById("lastBox").max = String(state.maxBoxNumber);
    document.getElementById("firstBox").max = String(state.maxBoxNumber);
    document.getElementById("localUrl").textContent = LOCAL_OPERATOR_URL;
    updatePreview();
  }

  async function loadStatus() {
    var response = await fetch("/api/offline/status", { cache: "no-store" });
    var payload = await response.json();
    if (!response.ok) throw payload;
    applyStatus(payload);
  }

  function getFormatCode(formatName) {
    return FORMAT_CODES[formatName] || "";
  }

  function buildPayload(dryRun) {
    var payload = {
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

    var formatCode = getFormatCode(payload.format);
    if (formatCode) payload.formatCode = formatCode;

    return payload;
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

  populateDatalist("materialOptions", MATERIAL_OPTIONS);
  populateDatalist("colorOptions", COLOR_OPTIONS);
  populateDatalist("formatOptions", FORMAT_OPTIONS);

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
    setEnabledVisuals(false, "");
    statusMeta.textContent = "Unable to read local offline printing status.";
    printButton.disabled = true;
    dryRunButton.disabled = true;
    setResult(error);
  });
})();
