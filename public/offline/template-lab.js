(function () {
  var catalog = { templates: [], profiles: [] };
  var latestPreview = null;
  var latestRenderSignature = "";
  var fitFields = ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"];
  var renderButton = document.getElementById("renderButton");
  var downloadButton = document.getElementById("downloadButton");
  var sendButton = document.getElementById("sendButton");
  var loadProfileButton = document.getElementById("loadProfileButton");
  var reloadTemplateButton = document.getElementById("reloadTemplateButton");
  var compareTemplateButton = document.getElementById("compareTemplateButton");
  var exportProfileButton = document.getElementById("exportProfileButton");
  var saveProfileButton = document.getElementById("saveProfileButton");
  var resetProfileButton = document.getElementById("resetProfileButton");
  var promoteTemplateButton = document.getElementById("promoteTemplateButton");
  var printCalibrationButton = document.getElementById("printCalibrationButton");
  var copyProfileButton = document.getElementById("copyProfileButton");
  var downloadProfileButton = document.getElementById("downloadProfileButton");
  var templateSelect = document.getElementById("template");
  var profileSelect = document.getElementById("profileKey");
  var headerStatus = document.getElementById("headerStatus");
  var statusBanner = document.getElementById("statusBanner");
  var statusTitle = document.getElementById("statusTitle");
  var statusMeta = document.getElementById("statusMeta");
  var metadataGrid = document.getElementById("metadataGrid");
  var metadataBadges = document.getElementById("metadataBadges");
  var fitDebug = document.getElementById("fitDebug");
  var renderedZpl = document.getElementById("renderedZpl");
  var codeSizeBadge = document.getElementById("codeSizeBadge");
  var previewImage = document.getElementById("previewImage");
  var previewPlaceholder = document.getElementById("previewPlaceholder");
  var previewStatus = document.getElementById("previewStatus");
  var renderStateLine = document.getElementById("renderStateLine");
  var lastRenderedAt = document.getElementById("lastRenderedAt");
  var sendResult = document.getElementById("sendResult");
  var fieldFitControls = document.getElementById("fieldFitControls");
  var profileJson = document.getElementById("profileJson");
  var profileSaveResult = document.getElementById("profileSaveResult");
  var promoteTemplateResult = document.getElementById("promoteTemplateResult");
  var proofTargetLine = document.getElementById("proofTargetLine");
  var calibrationSummary = document.getElementById("calibrationSummary");
  var templateSourceLine = document.getElementById("templateSourceLine");
  var templateGeometryWarnings = document.getElementById("templateGeometryWarnings");
  var templateCompareResult = document.getElementById("templateCompareResult");
  var selectedFieldPanel = document.getElementById("selectedFieldPanel");
  var applyFieldBoostsButton = document.getElementById("applyFieldBoostsButton");
  var logoAssetFile = document.getElementById("logoAssetFile");
  var logoAssetSelect = document.getElementById("logoAssetSelect");
  var uploadLogoAssetButton = document.getElementById("uploadLogoAssetButton");
  var selectLogoAssetButton = document.getElementById("selectLogoAssetButton");
  var logoAssetResult = document.getElementById("logoAssetResult");
  var selectedLogoAsset = null;
  var currentTemplateGeometry = null;
  var geometryFieldTokens = [];
  var rfidProofPrintersByStation = Object.freeze({
    P1: Object.freeze({ ip: "192.168.50.239", port: 9100 }),
    P2: Object.freeze({ ip: "192.168.50.241", port: 9100 }),
    P3: Object.freeze({ ip: "192.168.50.223", port: 9100 }),
    P4: Object.freeze({ ip: "192.168.50.242", port: 9100 }),
    P5: Object.freeze({ ip: "192.168.50.244", port: 9100 }),
    P6: Object.freeze({ ip: "192.168.6.240", port: 9100 }),
    P7: Object.freeze({ ip: "192.168.8.200", port: 9100 }),
    P8: Object.freeze({ ip: "192.168.7.122", port: 9100 })
  });
  var qcProofPrintersByStation = Object.freeze({
    P3: Object.freeze({ ip: "192.168.50.218", port: 9100 }),
    P8: Object.freeze({ ip: "192.168.50.214", port: 9100 })
  });
  var proofFamilyLabels = Object.freeze({
    RAW: "RFID RAW",
    FG: "RFID FG",
    SAMPLE: "QC Sample",
    RETAIN: "QC Retain",
    SAMPLE_POUNDS: "QC Sample Pounds"
  });
  var proofPrinterTargetsByProfileKey = Object.freeze(buildProofPrinterTargetsByProfileKey());
  var proofPrinterTargetsByFamilyStation = Object.freeze(buildProofPrinterTargetsByFamilyStation());
  var rangePairs = [
    ["globalScaleX", "globalScaleXRange"],
    ["globalScaleY", "globalScaleYRange"],
    ["globalOffsetX", "globalOffsetXRange"],
    ["globalOffsetY", "globalOffsetYRange"],
    ["qrX", "qrXRange"],
    ["qrY", "qrYRange"],
    ["qrMagnification", "qrMagnificationRange"],
    ["logoX", "logoXRange"],
    ["logoY", "logoYRange"],
    ["logoScale", "logoScaleRange"]
  ];

  function proofTarget(station, family, config) {
    var normalizedStation = String(station || "").toUpperCase();
    var normalizedFamily = String(family || "").toUpperCase();
    return Object.freeze({
      station: normalizedStation,
      family: normalizedFamily,
      profileKey: normalizedStation + ":" + normalizedFamily,
      ip: config.ip,
      port: config.port,
      label: normalizedStation + " " + (proofFamilyLabels[normalizedFamily] || normalizedFamily)
    });
  }

  function buildProofPrinterTargetsByProfileKey() {
    var targets = {};
    Object.keys(rfidProofPrintersByStation).forEach(function (station) {
      ["RAW", "FG"].forEach(function (family) {
        targets[station + ":" + family] = proofTarget(station, family, rfidProofPrintersByStation[station]);
      });
    });
    Object.keys(qcProofPrintersByStation).forEach(function (station) {
      ["SAMPLE", "RETAIN", "SAMPLE_POUNDS"].forEach(function (family) {
        targets[station + ":" + family] = proofTarget(station, family, qcProofPrintersByStation[station]);
      });
    });
    return targets;
  }

  function buildProofPrinterTargetsByFamilyStation() {
    var targets = {};
    Object.keys(proofPrinterTargetsByProfileKey).forEach(function (profileKey) {
      var target = proofPrinterTargetsByProfileKey[profileKey];
      targets[target.family + ":" + target.station] = target;
    });
    return targets;
  }

  function setStatus(ok, title, message) {
    headerStatus.textContent = ok ? "Status: Ready" : "Status: Review";
    headerStatus.className = ok ? "status-pill status-pill-enabled" : "status-pill status-pill-disabled";
    statusBanner.className = ok ? "status-banner status-enabled" : "status-banner status-disabled";
    statusTitle.textContent = title;
    statusMeta.textContent = message;
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(async function (response) {
      var text = await response.text();
      var json = {};
      try {
        json = text ? JSON.parse(text) : {};
      } catch {
        json = { ok: false, raw: text };
      }
      if (!response.ok) throw new Error(json.message || "HTTP " + response.status);
      return json;
    });
  }

  function postJson(url, body) {
    return fetchJson(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });
  }

  function selectedTemplateDefinition() {
    return catalog.templates.find(function (template) {
      return template.name === templateSelect.value;
    });
  }

  function selectedProfile() {
    return catalog.profiles.find(function (profile) {
      return profile.key === profileSelect.value;
    });
  }

  function parseProfileKey(profileKey) {
    var parts = String(profileKey || "").toUpperCase().split(":");
    return {
      station: parts[0] || "",
      family: parts[1] || ""
    };
  }

  function resolveProofPrinterTarget(profile) {
    var selected = profile || selectedProfile() || {};
    var key = String(selected.key || profileSelect.value || "").toUpperCase();
    if (proofPrinterTargetsByProfileKey[key]) return proofPrinterTargetsByProfileKey[key];

    var parsed = parseProfileKey(key);
    var station = String(selected.station || parsed.station || "").toUpperCase();
    var family = String(selected.family || parsed.family || "").toUpperCase();
    return proofPrinterTargetsByFamilyStation[family + ":" + station] || null;
  }

  function updateProofTargetLine(target) {
    if (!proofTargetLine) return;
    proofTargetLine.textContent = target
      ? "Proof target: " + target.label + " -> " + target.ip + ":" + target.port
      : "Proof target: -";
    updateCalibrationSummary(target);
  }

  function updateCalibrationSummary(target) {
    if (!calibrationSummary) return;
    var profile = selectedProfile() || {};
    var resolvedTarget = target || resolveProofPrinterTarget(profile);
    calibrationSummary.textContent = [
      "Calibration: " + (templateSelect.value || "-"),
      "Profile " + (profileSelect.value || "-"),
      "Target " + (resolvedTarget ? resolvedTarget.ip + ":" + resolvedTarget.port : "-"),
      "Label " + (input("labelWidthDots")?.value || profile.labelWidthDots || "-") + " x " + (input("labelHeightDots")?.value || profile.labelHeightDots || "-"),
      "Offset " + (input("globalOffsetX")?.value || profile.globalOffsetX || profile.offsetX || 0) + "," + (input("globalOffsetY")?.value || profile.globalOffsetY || profile.offsetY || 0),
      "Scale " + (input("globalScaleX")?.value || profile.globalScaleX || profile.scaleX || 1) + " x " + (input("globalScaleY")?.value || profile.globalScaleY || profile.scaleY || 1)
    ].join(" | ");
  }

  function applyProofPrinterDefaults(profile) {
    var target = resolveProofPrinterTarget(profile);
    if (!target) {
      updateProofTargetLine(null);
      return;
    }
    setInputValue("printerIp", target.ip);
    setInputValue("printerPort", target.port);
    updateProofTargetLine(target);
  }

  function validateProofPrinterTarget(payload) {
    var target = resolveProofPrinterTarget();
    if (!target) return "";

    var printerIp = String(payload.printerIp || "").trim();
    var port = Number(payload.port || payload.printerPort || 9100);
    if (printerIp === target.ip && port === target.port) return "";

    return "Proof target mismatch: " + target.label + " must use " + target.ip + ":" + target.port +
      ". Current printer is " + (printerIp || "-") + ":" + (Number.isFinite(port) ? port : "-") + ".";
  }

  function input(id) {
    return document.getElementById(id);
  }

  function setInputValue(id, value) {
    var element = input(id);
    if (!element) return;
    var normalized = value === undefined || value === null ? "" : String(value);
    element.value = normalized;
    var pairedRange = input(id + "Range");
    if (pairedRange) pairedRange.value = normalized || pairedRange.defaultValue || pairedRange.min || "";
  }

  function readNumberInput(id) {
    var element = input(id);
    if (!element || element.value === "") return undefined;
    var number = Number(element.value);
    return Number.isFinite(number) ? number : undefined;
  }

  function putIfNumber(target, key, id) {
    var number = readNumberInput(id);
    if (number !== undefined) target[key] = number;
  }

  function putIfChecked(target, key, id) {
    var element = input(id);
    if (element) target[key] = Boolean(element.checked);
  }

  function setControlsDisabled(ids, disabled) {
    ids.forEach(function (id) {
      var element = input(id);
      if (element) element.disabled = disabled;
      var range = input(id + "Range");
      if (range) range.disabled = disabled;
    });
  }

  function fieldLabel(field) {
    return {
      color: "Color",
      colorSmall: "Color Small",
      materialType: "Material",
      materialTypeSmall: "Material Small",
      tolling: "Tolling",
      productDescription: "Product Description"
    }[field] || field;
  }

  function tokenLabel(tokenName) {
    return fieldLabel(String(tokenName || "").replace(/Text$/, "")) + " (" + tokenName + ")";
  }

  function safeTokenId(tokenName) {
    return String(tokenName || "").replace(/[^A-Za-z0-9_:-]/g, "_");
  }

  function fieldInputId(tokenName, suffix) {
    return "fieldGeo_" + safeTokenId(tokenName) + "_" + suffix;
  }

  function numberOrBlank(value) {
    return value === undefined || value === null || Number.isNaN(Number(value)) ? "" : String(value);
  }

  function buildFieldFitControls() {
    var parsedFields = currentTemplateGeometry && Array.isArray(currentTemplateGeometry.fields)
      ? currentTemplateGeometry.fields
      : [];
    geometryFieldTokens = parsedFields.map(function (field) { return field.tokenName; });
    if (!parsedFields.length) {
      fieldFitControls.innerHTML = "<p class=\"muted-line\">No dynamic fields parsed from the selected template.</p>";
      return;
    }

    fieldFitControls.innerHTML = parsedFields.map(function (field) {
      var tokenName = field.tokenName;
      return [
        "<fieldset class=\"field-fit-fieldset\" data-field=\"" + tokenName + "\">",
        "<legend>" + tokenLabel(tokenName) + "</legend>",
        "<p class=\"field-help\">Token: {{" + tokenName + "}} | Source: " + (field.originCommand || "-") + (field.conditional ? " | Conditional: " + field.conditional.field : "") + "</p>",
        "<label title=\"Field origin X from the current template.\">X<input id=\"" + fieldInputId(tokenName, "X") + "\" type=\"number\" step=\"1\" value=\"" + numberOrBlank(field.x) + "\"></label>",
        "<label title=\"Field origin Y from the current template.\">Y<input id=\"" + fieldInputId(tokenName, "Y") + "\" type=\"number\" step=\"1\" value=\"" + numberOrBlank(field.y) + "\"></label>",
        "<label title=\"Parsed ^A0 font height in printer dots.\">Font Height<input id=\"" + fieldInputId(tokenName, "FontHeight") + "\" type=\"number\" min=\"1\" step=\"1\" value=\"" + numberOrBlank(field.fontHeight) + "\"></label>",
        "<label title=\"Parsed ^A0 font width in printer dots.\">Font Width<input id=\"" + fieldInputId(tokenName, "FontWidth") + "\" type=\"number\" min=\"1\" step=\"1\" value=\"" + numberOrBlank(field.fontWidth) + "\"></label>",
        "<label title=\"Parsed ^FB field width in printer dots.\">Field Width<input id=\"" + fieldInputId(tokenName, "FieldWidth") + "\" type=\"number\" min=\"1\" step=\"1\" value=\"" + numberOrBlank(field.fieldWidth) + "\"></label>",
        "<label title=\"Parsed ^FB max lines.\">Max Lines<input id=\"" + fieldInputId(tokenName, "MaxLines") + "\" type=\"number\" min=\"1\" step=\"1\" value=\"" + numberOrBlank(field.maxLines) + "\"></label>",
        "<label title=\"Parsed ^FB alignment.\">Alignment<select id=\"" + fieldInputId(tokenName, "Alignment") + "\"><option value=\"L\">Left</option><option value=\"C\">Center</option><option value=\"R\">Right</option><option value=\"J\">Justify</option></select></label>",
        "<label title=\"Use ^FO or ^FT for this field origin.\">Use FO/FT<select id=\"" + fieldInputId(tokenName, "Origin") + "\"><option value=\"FO\">FO</option><option value=\"FT\">FT</option></select></label>",
        "<label title=\"Related border thickness where a nearby ^GB was parsed.\">Border<input id=\"" + fieldInputId(tokenName, "BorderThickness") + "\" type=\"number\" min=\"0\" max=\"20\" step=\"1\" value=\"" + numberOrBlank(field.border && field.border.thickness) + "\"></label>",
        "<label title=\"Related border width where a nearby ^GB was parsed.\">Border W<input id=\"" + fieldInputId(tokenName, "BorderWidth") + "\" type=\"number\" min=\"0\" step=\"1\" value=\"" + numberOrBlank(field.border && field.border.width) + "\"></label>",
        "<label title=\"Related border height where a nearby ^GB was parsed.\">Border H<input id=\"" + fieldInputId(tokenName, "BorderHeight") + "\" type=\"number\" min=\"0\" step=\"1\" value=\"" + numberOrBlank(field.border && field.border.height) + "\"></label>",
        "</fieldset>"
      ].join("");
    }).join("");

    parsedFields.forEach(function (field) {
      setInputValue(fieldInputId(field.tokenName, "Alignment"), field.alignment || "L");
      setInputValue(fieldInputId(field.tokenName, "Origin"), field.originCommand || "FO");
    });
  }

  function updateSelectedFieldPanel(field) {
    if (!selectedFieldPanel) return;
    if (!field) {
      selectedFieldPanel.textContent = "Selected field: -";
      return;
    }
    selectedFieldPanel.textContent = "Selected field: " + tokenLabel(field) +
      " | X " + (input(fieldInputId(field, "X"))?.value || "-") +
      " | Y " + (input(fieldInputId(field, "Y"))?.value || "-") +
      " | Width " + (input(fieldInputId(field, "FieldWidth"))?.value || "-") +
      " | Font " + (input(fieldInputId(field, "FontHeight"))?.value || "-") + " / " + (input(fieldInputId(field, "FontWidth"))?.value || "-");
  }

  function wireFieldSelection() {
    document.querySelectorAll(".field-fit-fieldset").forEach(function (fieldset) {
      fieldset.addEventListener("focusin", function () {
        updateSelectedFieldPanel(fieldset.getAttribute("data-field"));
      });
      fieldset.addEventListener("click", function () {
        updateSelectedFieldPanel(fieldset.getAttribute("data-field"));
      });
      fieldset.addEventListener("input", function () {
        updateSelectedFieldPanel(fieldset.getAttribute("data-field"));
      });
    });
    updateSelectedFieldPanel(fitFields[0]);
  }

  function setFieldDefinitionControls(field, definition) {
    var def = definition || {};
    setInputValue(field + "BoxWidth", def.boxWidth);
    setInputValue(field + "BoxHeight", def.boxHeight);
    setInputValue(field + "FontHeight", def.fontHeight);
    setInputValue(field + "FontWidth", def.fontWidth);
    setInputValue(field + "MaxChars", def.maxChars);
    setInputValue(field + "MaxLines", def.maxLines);
    setInputValue(field + "Alignment", def.alignment || (field === "productDescription" ? "L" : "C"));
    setInputValue(field + "BorderThickness", def.borderThickness);
    ["large", "medium", "small", "min"].forEach(function (tier) {
      var tierDef = def[tier] || {};
      var prefix = field + tier.charAt(0).toUpperCase() + tier.slice(1);
      setInputValue(prefix + "FontH", tierDef.fontH);
      setInputValue(prefix + "FontW", tierDef.fontW);
    });
  }

  async function loadTemplateGeometryFromServer() {
    var query = new URLSearchParams({
      template: templateSelect.value || "",
      profileKey: profileSelect.value || ""
    });
    currentTemplateGeometry = await fetchJson("/api/print/template-lab/template-geometry?" + query.toString(), { cache: "no-store" });
    if (templateSourceLine) {
      templateSourceLine.textContent = "Editing current production template: " + (currentTemplateGeometry.templatePath || "-");
    }
    if (templateGeometryWarnings) {
      var warnings = currentTemplateGeometry.warnings || [];
      templateGeometryWarnings.textContent = warnings.length
        ? "Parser warnings: " + warnings.map(function (warning) { return warning.tokenName + ": " + warning.message; }).join(" | ")
        : "";
    }
    return currentTemplateGeometry;
  }

  function applyGeometryToControls(geometry) {
    var parsed = geometry || currentTemplateGeometry || {};
    var profile = selectedProfile() || {};
    var label = parsed.label || {};
    var qr = parsed.qr || {};
    var logo = parsed.logo || {};
    setInputValue("labelWidthDots", label.labelWidthDots ?? profile.labelWidthDots ?? 812);
    setInputValue("labelHeightDots", label.labelHeightDots ?? profile.labelHeightDots ?? 1218);
    setInputValue("globalScaleX", 1);
    setInputValue("globalScaleY", 1);
    setInputValue("globalOffsetX", 0);
    setInputValue("globalOffsetY", 0);
    setInputValue("labelHomeX", label.labelHomeX);
    setInputValue("labelHomeY", label.labelHomeY);
    setInputValue("labelShiftX", label.labelShiftX);
    setInputValue("labelShiftY", label.labelShiftY);
    setInputValue("borderThickness", profile.borderThickness);
    if (input("scaleBorderThickness")) input("scaleBorderThickness").checked = false;
    setInputValue("qrX", qr.x ?? (profile.qr && profile.qr.x));
    setInputValue("qrY", qr.y ?? (profile.qr && profile.qr.y));
    setInputValue("qrMagnification", qr.magnification ?? (profile.qr && profile.qr.magnification));
    setInputValue("logoX", logo.x ?? (profile.logo && profile.logo.x));
    setInputValue("logoY", logo.y ?? (profile.logo && profile.logo.y));
    setInputValue("logoScale", profile.logo && profile.logo.scale);
    setInputValue("logoWidthDots", logo.widthDots ?? (profile.logo && profile.logo.widthDots));
    setInputValue("logoHeightDots", logo.heightDots ?? (profile.logo && profile.logo.heightDots));
    setInputValue("logoThreshold", (profile.logo && profile.logo.threshold) ?? 128);
    setInputValue("logoDithering", profile.logo && profile.logo.dithering || "none");
    selectedLogoAsset = profile.logo && profile.logo.gfa ? {
      assetName: profile.logo.assetName || "",
      gfa: profile.logo.gfa,
      widthDots: profile.logo.widthDots,
      heightDots: profile.logo.heightDots,
      threshold: profile.logo.threshold,
      dithering: profile.logo.dithering
    } : null;
    if (logoAssetSelect) logoAssetSelect.value = profile.logo && profile.logo.assetName || "";
    if (logoAssetResult) logoAssetResult.textContent = selectedLogoAsset ? "Selected logo asset: " + (selectedLogoAsset.assetName || "profile asset") : "";
    setControlsDisabled(["logoX", "logoY", "logoScale", "logoWidthDots", "logoHeightDots", "logoThreshold", "logoDithering"], false);

    buildFieldFitControls();
    wireFieldSelection();
    updateSelectedFieldPanel((parsed.fields && parsed.fields[0] && parsed.fields[0].tokenName) || "");
    applyProofPrinterDefaults(profile);
    updateCalibrationSummary(resolveProofPrinterTarget(profile));
    exportProfileJson();
  }

  async function loadSelectedProfileValues() {
    try {
      var geometry = await loadTemplateGeometryFromServer();
      applyGeometryToControls(geometry);
    } catch (error) {
      setStatus(false, "Template Geometry Failed", error.message);
    }
  }

  function loadSelectedProfileValuesOld() {
    var profile = selectedProfile() || {};
    var effectiveDefinitions = profile.effectiveFieldFitDefinitions || profile.fieldFitDefinitions || {};
    setInputValue("labelWidthDots", profile.labelWidthDots ?? 812);
    setInputValue("labelHeightDots", profile.labelHeightDots ?? 1218);
    setInputValue("globalScaleX", profile.globalScaleX ?? profile.scaleX ?? 1);
    setInputValue("globalScaleY", profile.globalScaleY ?? profile.scaleY ?? 1);
    setInputValue("globalOffsetX", profile.globalOffsetX ?? profile.offsetX ?? 0);
    setInputValue("globalOffsetY", profile.globalOffsetY ?? profile.offsetY ?? 0);
    setInputValue("labelHomeX", profile.labelHomeX);
    setInputValue("labelHomeY", profile.labelHomeY);
    setInputValue("labelShiftX", profile.labelShiftX);
    setInputValue("labelShiftY", profile.labelShiftY);
    setInputValue("borderThickness", profile.borderThickness);
    if (input("scaleBorderThickness")) input("scaleBorderThickness").checked = Boolean(profile.scaleBorderThickness);
    setInputValue("qrX", profile.qr && profile.qr.x);
    setInputValue("qrY", profile.qr && profile.qr.y);
    setInputValue("qrMagnification", profile.qr && profile.qr.magnification);
    setInputValue("logoX", profile.logo && profile.logo.x);
    setInputValue("logoY", profile.logo && profile.logo.y);
    setInputValue("logoScale", profile.logo && profile.logo.scale);
    setInputValue("logoWidthDots", profile.logo && profile.logo.widthDots);
    setInputValue("logoHeightDots", profile.logo && profile.logo.heightDots);
    setInputValue("logoThreshold", (profile.logo && profile.logo.threshold) ?? 128);
    setInputValue("logoDithering", profile.logo && profile.logo.dithering || "none");
    selectedLogoAsset = profile.logo && profile.logo.gfa ? {
      assetName: profile.logo.assetName || "",
      gfa: profile.logo.gfa,
      widthDots: profile.logo.widthDots,
      heightDots: profile.logo.heightDots,
      threshold: profile.logo.threshold,
      dithering: profile.logo.dithering
    } : null;
    if (logoAssetSelect) logoAssetSelect.value = profile.logo && profile.logo.assetName || "";
    if (logoAssetResult) logoAssetResult.textContent = selectedLogoAsset ? "Selected logo asset: " + (selectedLogoAsset.assetName || "profile asset") : "";
    setControlsDisabled(["logoX", "logoY", "logoScale", "logoWidthDots", "logoHeightDots", "logoThreshold", "logoDithering"], !profile.logo || profile.logo.mode !== "static logo");

    fitFields.forEach(function (field) {
      setFieldDefinitionControls(field, effectiveDefinitions[field]);
    });

    ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"].forEach(function (field) {
      var position = profile.fieldPositionOverrides && profile.fieldPositionOverrides[field] || {};
      setInputValue(field + "X", position.x);
      setInputValue(field + "Y", position.y);
    });

    applyProofPrinterDefaults(profile);
    updateCalibrationSummary(resolveProofPrinterTarget(profile));
    exportProfileJson();
  }

  function collectFieldFitDefinition(field) {
    var output = {};
    putIfNumber(output, "boxWidth", field + "BoxWidth");
    putIfNumber(output, "boxHeight", field + "BoxHeight");
    putIfNumber(output, "fontHeight", field + "FontHeight");
    putIfNumber(output, "fontWidth", field + "FontWidth");
    putIfNumber(output, "maxChars", field + "MaxChars");
    putIfNumber(output, "maxLines", field + "MaxLines");
    putIfNumber(output, "borderThickness", field + "BorderThickness");
    var alignment = input(field + "Alignment");
    if (alignment && alignment.value) output.alignment = alignment.value;
    ["large", "medium", "small", "min"].forEach(function (tier) {
      var tierOutput = {};
      var prefix = field + tier.charAt(0).toUpperCase() + tier.slice(1);
      putIfNumber(tierOutput, "fontH", prefix + "FontH");
      putIfNumber(tierOutput, "fontW", prefix + "FontW");
      if (Object.keys(tierOutput).length) output[tier] = tierOutput;
    });
    return output;
  }

  function collectFieldGeometryOverrides() {
    var output = {};
    geometryFieldTokens.forEach(function (tokenName) {
      var geometry = {};
      putIfNumber(geometry, "x", fieldInputId(tokenName, "X"));
      putIfNumber(geometry, "y", fieldInputId(tokenName, "Y"));
      putIfNumber(geometry, "fontHeight", fieldInputId(tokenName, "FontHeight"));
      putIfNumber(geometry, "fontWidth", fieldInputId(tokenName, "FontWidth"));
      putIfNumber(geometry, "fieldWidth", fieldInputId(tokenName, "FieldWidth"));
      putIfNumber(geometry, "maxLines", fieldInputId(tokenName, "MaxLines"));
      var alignment = input(fieldInputId(tokenName, "Alignment"));
      if (alignment && alignment.value) geometry.alignment = alignment.value;
      var origin = input(fieldInputId(tokenName, "Origin"));
      if (origin && origin.value) geometry.originCommand = origin.value;
      var border = {};
      putIfNumber(border, "thickness", fieldInputId(tokenName, "BorderThickness"));
      putIfNumber(border, "width", fieldInputId(tokenName, "BorderWidth"));
      putIfNumber(border, "height", fieldInputId(tokenName, "BorderHeight"));
      if (Object.keys(border).length) geometry.border = border;
      if (Object.keys(geometry).length) output[tokenName] = geometry;
    });
    return output;
  }

  function collectProfileOverrides() {
    var overrides = {};
    putIfNumber(overrides, "labelWidthDots", "labelWidthDots");
    putIfNumber(overrides, "labelHeightDots", "labelHeightDots");
    putIfNumber(overrides, "globalScaleX", "globalScaleX");
    putIfNumber(overrides, "globalScaleY", "globalScaleY");
    putIfNumber(overrides, "globalOffsetX", "globalOffsetX");
    putIfNumber(overrides, "globalOffsetY", "globalOffsetY");
    putIfNumber(overrides, "labelHomeX", "labelHomeX");
    putIfNumber(overrides, "labelHomeY", "labelHomeY");
    putIfNumber(overrides, "labelShiftX", "labelShiftX");
    putIfNumber(overrides, "labelShiftY", "labelShiftY");
    putIfNumber(overrides, "borderThickness", "borderThickness");
    putIfChecked(overrides, "scaleBorderThickness", "scaleBorderThickness");
    overrides.scaleX = overrides.globalScaleX;
    overrides.scaleY = overrides.globalScaleY;
    overrides.offsetX = overrides.globalOffsetX;
    overrides.offsetY = overrides.globalOffsetY;

    var qr = {};
    putIfNumber(qr, "x", "qrX");
    putIfNumber(qr, "y", "qrY");
    putIfNumber(qr, "magnification", "qrMagnification");
    if (Object.keys(qr).length) overrides.qr = qr;

    var logo = {};
    putIfNumber(logo, "x", "logoX");
    putIfNumber(logo, "y", "logoY");
    putIfNumber(logo, "scale", "logoScale");
    putIfNumber(logo, "widthDots", "logoWidthDots");
    putIfNumber(logo, "heightDots", "logoHeightDots");
    putIfNumber(logo, "threshold", "logoThreshold");
    var logoDithering = input("logoDithering");
    if (logoDithering && logoDithering.value) logo.dithering = logoDithering.value;
    if (selectedLogoAsset && selectedLogoAsset.gfa) {
      logo.assetName = selectedLogoAsset.assetName || "";
      logo.gfa = selectedLogoAsset.gfa;
      if (selectedLogoAsset.assetPath) logo.assetPath = selectedLogoAsset.assetPath;
      if (selectedLogoAsset.gfaPath) logo.gfaPath = selectedLogoAsset.gfaPath;
    }
    if (Object.keys(logo).length) overrides.logo = logo;

    var fieldGeometryOverrides = collectFieldGeometryOverrides();
    if (Object.keys(fieldGeometryOverrides).length) overrides.fieldGeometryOverrides = fieldGeometryOverrides;

    var fieldPositionOverrides = {};
    if (Object.keys(fieldPositionOverrides).length) overrides.fieldPositionOverrides = fieldPositionOverrides;

    return overrides;
  }

  function boostPercentForField(field) {
    var token = String(field.tokenName || "");
    if (token === "productDescriptionText" || token === "productDescription") return readNumberInput("productDescriptionFontBoostPercent") || 0;
    if (token === "tollingText") return readNumberInput("tollingFontBoostPercent") || 0;
    if (/SmallText$/.test(token)) return readNumberInput("smallFontBoostPercent") || 0;
    return readNumberInput("primaryFontBoostPercent") || 0;
  }

  function applyGroupedFieldAdjustments() {
    if (!currentTemplateGeometry || !Array.isArray(currentTemplateGeometry.fields)) return;
    var offsetX = readNumberInput("fieldGlobalOffsetX") || 0;
    var offsetY = readNumberInput("fieldGlobalOffsetY") || 0;
    currentTemplateGeometry.fields.forEach(function (field) {
      var boost = boostPercentForField(field);
      var multiplier = 1 + (boost / 100);
      if (Number.isFinite(Number(field.x))) setInputValue(fieldInputId(field.tokenName, "X"), Math.round(Number(field.x) + offsetX));
      if (Number.isFinite(Number(field.y))) setInputValue(fieldInputId(field.tokenName, "Y"), Math.round(Number(field.y) + offsetY));
      if (Number.isFinite(Number(field.fontHeight))) setInputValue(fieldInputId(field.tokenName, "FontHeight"), Math.max(1, Math.round(Number(field.fontHeight) * multiplier)));
      if (Number.isFinite(Number(field.fontWidth))) setInputValue(fieldInputId(field.tokenName, "FontWidth"), Math.max(1, Math.round(Number(field.fontWidth) * multiplier)));
    });
    exportProfileJson();
    updateRenderState(false);
  }

  function compareCurrentVsStaged() {
    if (!currentTemplateGeometry || !Array.isArray(currentTemplateGeometry.fields)) {
      if (templateCompareResult) templateCompareResult.textContent = "No parsed template geometry loaded.";
      return;
    }
    var staged = collectFieldGeometryOverrides();
    var rows = [];
    currentTemplateGeometry.fields.forEach(function (field) {
      var edited = staged[field.tokenName] || {};
      var changes = [];
      [
        ["x", "X"],
        ["y", "Y"],
        ["fontHeight", "FontHeight"],
        ["fontWidth", "FontWidth"],
        ["fieldWidth", "FieldWidth"],
        ["maxLines", "MaxLines"],
        ["alignment", "Alignment"],
        ["originCommand", "Origin"]
      ].forEach(function (pair) {
        var currentValue = field[pair[0]];
        var editedValue = edited[pair[0]];
        if (String(currentValue ?? "") !== String(editedValue ?? "")) {
          changes.push(pair[1] + ": " + (currentValue ?? "-") + " -> " + (editedValue ?? "-"));
        }
      });
      if (edited.border && field.border) {
        ["thickness", "width", "height"].forEach(function (key) {
          if (String(field.border[key] ?? "") !== String(edited.border[key] ?? "")) {
            changes.push("Border " + key + ": " + (field.border[key] ?? "-") + " -> " + (edited.border[key] ?? "-"));
          }
        });
      }
      if (changes.length) rows.push(field.tokenName + " | " + changes.join("; "));
    });
    if (templateCompareResult) {
      templateCompareResult.textContent = rows.length ? rows.join("\n") : "No changed fields.";
    }
  }

  function formPayload() {
    var form = document.getElementById("templateForm");
    var data = {};
    Array.from(new FormData(form).entries()).forEach(function (entry) {
      data[entry[0]] = String(entry[1] || "").trim();
    });
    data.port = Number(data.printerPort || 9100);
    data.profileOverrides = collectProfileOverrides();
    return data;
  }

  function exportProfileJson() {
    var payload = {
      profileKey: profileSelect.value,
      template: templateSelect.value,
      templateName: templateSelect.value,
      sourceTemplatePath: currentTemplateGeometry && currentTemplateGeometry.templatePath,
      overrides: collectProfileOverrides()
    };
    profileJson.value = JSON.stringify(payload, null, 2);
    profileSaveResult.textContent = "Profile JSON refreshed. Saved lab profiles affect preview/test only.";
    return payload;
  }

  function currentRenderSignature() {
    return JSON.stringify(formPayload());
  }

  function updateRenderState(rendered) {
    if (!renderStateLine) return;
    var current = currentRenderSignature();
    var isCurrent = rendered || (latestRenderSignature && current === latestRenderSignature);
    renderStateLine.textContent = isCurrent
      ? "Rendered with current controls"
      : "Unsaved changes: click Render / Re-render to update preview and metadata";
    renderStateLine.className = isCurrent ? "render-state render-state-current" : "render-state render-state-stale";
  }

  function badge(label, value, variant) {
    return [
      "<span class=\"template-badge template-badge-" + (variant || "neutral") + "\">",
      "<em>" + label + "</em>",
      "<strong>" + String(value ?? "-") + "</strong>",
      "</span>"
    ].join("");
  }

  function renderMetadataBadges(preview) {
    if (!metadataBadges) return;
    var metadata = preview.metadata || {};
    var qr = metadata.qr || {};
    var printer = input("printerIp").value + ":" + input("printerPort").value;
    var rendererState = preview.imagePreview && preview.imagePreview.configured
      ? (preview.imagePreview.ok ? "image ready" : "renderer error")
      : "approximate preview";

    metadataBadges.innerHTML = [
      badge("Safety", "Production-safe preview", "enabled"),
      badge("Test print", "bypasses queue", "warning"),
      badge("QR payload", qr.payload || "-", qr.payload ? "enabled" : "warning"),
      badge("RFID", metadata.rfidCommandPresent ? "encoding present" : "not encoded", metadata.rfidCommandPresent ? "enabled" : "neutral"),
      badge("Logo", metadata.logoCommandPresent ? "static logo" : "none", metadata.logoCommandPresent ? "enabled" : "neutral"),
      badge("Payload", (metadata.payloadBytes || 0) + " bytes", "neutral"),
      badge("Printer", printer, "neutral"),
      badge("Preview", rendererState, preview.imagePreview?.ok ? "enabled" : "neutral")
    ].join("");
  }

  function firstString() {
    for (var index = 0; index < arguments.length; index += 1) {
      if (typeof arguments[index] === "string" && arguments[index]) return arguments[index];
    }
    return "";
  }

  function previewImageSrc(imagePreview) {
    var data = imagePreview && imagePreview.data;
    if (!data) return "";
    if (typeof data === "string") {
      if (/^(data:image\/|https?:\/\/|\/)/i.test(data)) return data;
      return "data:image/png;base64," + data;
    }
    var direct = firstString(data.imageUrl, data.url, data.renderedUrl, data.previewUrl);
    if (direct) return direct;
    var base64 = firstString(data.pngBase64, data.imageBase64, data.base64, data.png, data.image);
    if (!base64) return "";
    return /^data:image\//i.test(base64) ? base64 : "data:image/png;base64," + base64;
  }

  function renderPreviewImage(preview) {
    if (!previewImage || !previewPlaceholder || !previewStatus) return;
    var imagePreview = preview.imagePreview || {};
    var src = previewImageSrc(imagePreview);
    previewImage.classList.add("hidden");
    previewImage.removeAttribute("src");
    previewPlaceholder.classList.remove("hidden");

    if (src) {
      previewImage.src = src;
      previewImage.classList.remove("hidden");
      previewPlaceholder.classList.add("hidden");
      previewStatus.textContent = "Preview renderer returned an image for this lab render.";
      return;
    }

    previewStatus.textContent = imagePreview.message || "Approximate preview shown. Use a physical proof print for final verification.";
    previewPlaceholder.textContent = imagePreview.configured ? "Renderer unavailable" : "Approximate preview unavailable";
  }

  function renderMetadata(preview) {
    var metadata = preview.metadata || {};
    var qr = metadata.qr || {};
    var profile = metadata.profile || {};
    var logo = profile.logo || {};
    renderMetadataBadges(preview);
    renderPreviewImage(preview);
    var rows = [
      ["Template", preview.template],
      ["Payload Bytes", metadata.payloadBytes],
      ["QR Command", qr.command || "-"],
      ["QR Payload", qr.payload || "-"],
      ["RFID Commands", metadata.rfidCommandPresent ? "present" : "none"],
      ["Logo Mode", metadata.logoMode || "none"],
      ["Logo X/Y", (logo.x ?? "-") + ", " + (logo.y ?? "-")],
      ["Logo Size", (logo.widthDots ?? "-") + " x " + (logo.heightDots ?? "-")],
      ["Logo Asset", logo.assetName || "-"],
      ["Profile", preview.profileKey || "-"],
      ["Scale", (profile.globalScaleX || profile.scaleX || 1) + " x " + (profile.globalScaleY || profile.scaleY || 1)],
      ["Offset", (profile.globalOffsetX || profile.offsetX || 0) + ", " + (profile.globalOffsetY || profile.offsetY || 0) + " dots"],
      ["Border Thickness", (profile.borderThickness ?? "-") + " dots"],
      ["Printer", input("printerIp").value + ":" + input("printerPort").value],
      ["Renderer", preview.imagePreview?.configured ? (preview.imagePreview.ok ? "ok" : "error") : "not configured"],
      ["Preview Mode", metadata.previewMode || preview.imagePreview?.mode || "-"],
      ["Label Dots", (metadata.labelWidthDots || "-") + " x " + (metadata.labelHeightDots || "-")],
      ["Unsupported ZPL", (metadata.unsupportedZplCommands || []).join(", ") || "none"],
      ["Fields Parsed", metadata.fieldCount || 0],
      ["Logo Source", metadata.logoDiagnostics?.source || "-"],
      ["Logo Payload", (metadata.logoDiagnostics?.payloadBytes || 0) + " bytes"],
      ["Logo Quality", metadata.logoDiagnostics?.qualityNote || "-"],
      ["Profile JSON", preview.profileConfigPath || "-"]
    ];

    metadataGrid.innerHTML = rows.map(function (row) {
      return "<div><span class=\"label-text\">" + row[0] + "</span><strong>" + String(row[1] ?? "-") + "</strong></div>";
    }).join("");
  }

  function renderFitDebug(preview) {
    var debug = (preview.metadata && preview.metadata.fitDebug) || {};
    var rows = Object.keys(debug).sort().map(function (key) {
      var item = debug[key] || {};
      return [
        "<tr>",
        "<td>" + key + "</td>",
        "<td>" + String(item.original || "") + "</td>",
        "<td>" + String(item.fittedText || "") + "</td>",
        "<td>" + String(item.fontH || "") + " / " + String(item.fontW || "") + "</td>",
        "<td>" + String(item.boxW || "") + "</td>",
        "<td>" + (item.truncated ? "yes" : "no") + "</td>",
        "</tr>"
      ].join("");
    });

    fitDebug.innerHTML = rows.length
      ? [
        "<h3>Field Fit Debug</h3>",
        "<table class=\"fit-table\"><thead><tr><th>Field</th><th>Original</th><th>Fitted</th><th>Font H/W</th><th>Box W</th><th>Truncated</th></tr></thead><tbody>",
        rows.join(""),
        "</tbody></table>"
      ].join("")
      : "<p class=\"muted-line\">No fitted field debug returned.</p>";
  }

  async function applyTemplateDefaults() {
    var definition = selectedTemplateDefinition();
    if (!definition) return;
    profileSelect.value = definition.defaultProfileKey || profileSelect.value;

    if (definition.requiresRfid) {
      input("rfid").disabled = false;
    } else {
      input("rfid").disabled = true;
      input("rfid").value = "";
    }

    await loadSelectedProfileValues();
  }

  async function loadCatalog() {
    catalog = await fetchJson("/api/print/template-lab/catalog", { cache: "no-store" });
    templateSelect.innerHTML = catalog.templates.map(function (template) {
      return "<option value=\"" + template.name + "\">" + template.label + "</option>";
    }).join("");
    profileSelect.innerHTML = catalog.profiles.map(function (profile) {
      return "<option value=\"" + profile.key + "\">" + profile.key + " - " + profile.template + "</option>";
    }).join("");
    await loadLogoAssets();
    await applyTemplateDefaults();
    setStatus(true, "Template Lab Ready", "Tune controls, render, then proof print only when explicitly confirmed.");
  }

  async function reloadCatalogAndProfile() {
    var selectedProfileKey = profileSelect.value;
    catalog = await fetchJson("/api/print/template-lab/catalog", { cache: "no-store" });
    profileSelect.innerHTML = catalog.profiles.map(function (profile) {
      return "<option value=\"" + profile.key + "\">" + profile.key + " - " + profile.template + "</option>";
    }).join("");
    profileSelect.value = selectedProfileKey;
    await loadLogoAssets();
    await loadSelectedProfileValues();
  }

  async function loadLogoAssets() {
    if (!logoAssetSelect) return;
    try {
      var payload = await fetchJson("/api/print/template-lab/logo-assets", { cache: "no-store" });
      var options = ["<option value=\"\">Current template logo</option>"].concat((payload.assets || []).map(function (asset) {
        return "<option value=\"" + asset.name + "\">" + asset.name + "</option>";
      }));
      logoAssetSelect.innerHTML = options.join("");
    } catch (error) {
      if (logoAssetResult) logoAssetResult.textContent = "Logo asset list unavailable: " + error.message;
    }
  }

  async function renderTemplate() {
    renderButton.disabled = true;
    downloadButton.disabled = true;
    sendResult.textContent = "";
    exportProfileJson();
    try {
      latestPreview = await postJson("/api/print/template-preview", formPayload());
      latestRenderSignature = currentRenderSignature();
      renderedZpl.textContent = latestPreview.renderedZpl || "";
      if (codeSizeBadge) codeSizeBadge.textContent = ((latestPreview.metadata && latestPreview.metadata.payloadBytes) || 0) + " bytes";
      renderMetadata(latestPreview);
      renderFitDebug(latestPreview);
      downloadButton.disabled = false;
      if (lastRenderedAt) lastRenderedAt.textContent = "Last rendered at " + new Date().toLocaleTimeString();
      updateRenderState(true);
      setStatus(true, "Template Rendered", "Tuning controls affected this preview/test render only.");
    } catch (error) {
      latestPreview = null;
      renderedZpl.textContent = error.message;
      if (codeSizeBadge) codeSizeBadge.textContent = "render failed";
      latestRenderSignature = "";
      updateRenderState(false);
      setStatus(false, "Template Render Failed", error.message);
    } finally {
      renderButton.disabled = false;
    }
  }

  function downloadRenderedZpl() {
    if (!latestPreview || !latestPreview.renderedZpl) return;
    var blob = new Blob([latestPreview.renderedZpl], { type: "text/plain" });
    var url = URL.createObjectURL(blob);
    var link = document.createElement("a");
    link.href = url;
    link.download = latestPreview.template.replace(/\.template\.zpl$/i, ".rendered.zpl");
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  function logoConversionOptions() {
    return {
      widthDots: readNumberInput("logoWidthDots"),
      heightDots: readNumberInput("logoHeightDots"),
      threshold: readNumberInput("logoThreshold") ?? 128,
      dithering: input("logoDithering")?.value || "none"
    };
  }

  async function uploadLogoAsset() {
    if (!logoAssetFile || !logoAssetFile.files || !logoAssetFile.files[0]) {
      if (logoAssetResult) logoAssetResult.textContent = "Choose a PNG logo file first.";
      return;
    }
    var options = logoConversionOptions();
    var data = new FormData();
    data.append("file", logoAssetFile.files[0]);
    Object.keys(options).forEach(function (key) {
      if (options[key] !== undefined) data.append(key, String(options[key]));
    });
    uploadLogoAssetButton.disabled = true;
    if (logoAssetResult) logoAssetResult.textContent = "Uploading and converting logo...";
    try {
      var result = await fetchJson("/api/print/template-lab/logo-assets", {
        method: "POST",
        body: data
      });
      selectedLogoAsset = result;
      setInputValue("logoWidthDots", result.widthDots);
      setInputValue("logoHeightDots", result.heightDots);
      if (logoAssetSelect) {
        await loadLogoAssets();
        logoAssetSelect.value = result.gfaName || result.assetName || "";
      }
      if (logoAssetResult) logoAssetResult.textContent = "Selected logo asset: " + (result.gfaName || result.assetName) + " -> " + result.widthDots + " x " + result.heightDots + " dots.";
      exportProfileJson();
      updateRenderState(false);
    } catch (error) {
      if (logoAssetResult) logoAssetResult.textContent = error.message;
      setStatus(false, "Logo Upload Failed", error.message);
    } finally {
      uploadLogoAssetButton.disabled = false;
    }
  }

  async function selectLogoAsset() {
    if (!logoAssetSelect || !logoAssetSelect.value) {
      selectedLogoAsset = null;
      if (logoAssetResult) logoAssetResult.textContent = "Using current template logo.";
      exportProfileJson();
      updateRenderState(false);
      return;
    }
    var options = logoConversionOptions();
    selectLogoAssetButton.disabled = true;
    if (logoAssetResult) logoAssetResult.textContent = "Loading logo asset...";
    try {
      var result = await postJson("/api/print/template-lab/logo-assets/select", {
        assetName: logoAssetSelect.value,
        widthDots: options.widthDots,
        heightDots: options.heightDots,
        threshold: options.threshold,
        dithering: options.dithering
      });
      selectedLogoAsset = result;
      if (result.widthDots) setInputValue("logoWidthDots", result.widthDots);
      if (result.heightDots) setInputValue("logoHeightDots", result.heightDots);
      if (logoAssetResult) logoAssetResult.textContent = "Selected logo asset: " + result.assetName + " -> " + (result.widthDots || "-") + " x " + (result.heightDots || "-") + " dots.";
      exportProfileJson();
      updateRenderState(false);
    } catch (error) {
      if (logoAssetResult) logoAssetResult.textContent = error.message;
      setStatus(false, "Logo Selection Failed", error.message);
    } finally {
      selectLogoAssetButton.disabled = false;
    }
  }

  async function sendProofPrint() {
    var payload = formPayload();
    payload.confirmTestPrint = input("confirmTestPrint").checked;
    var targetError = validateProofPrinterTarget(payload);
    if (targetError) {
      sendResult.textContent = targetError;
      setStatus(false, "Proof Target Blocked", targetError);
      return;
    }
    sendButton.disabled = true;
    sendResult.textContent = "Sending proof print...";
    try {
      var result = await postJson("/api/print/template-test-send", payload);
      sendResult.textContent = result.message || "Template test ZPL sent.";
      setStatus(true, "Proof Print Sent", "The send bypassed the production queue and does not confirm physical printing.");
    } catch (error) {
      sendResult.textContent = error.message;
      setStatus(false, "Proof Print Failed", error.message);
    } finally {
      sendButton.disabled = false;
    }
  }

  async function printCalibrationGrid() {
    var payload = formPayload();
    payload.confirmTestPrint = input("confirmTestPrint").checked;
    var targetError = validateProofPrinterTarget(payload);
    if (targetError) {
      sendResult.textContent = targetError;
      setStatus(false, "Proof Target Blocked", targetError);
      return;
    }
    printCalibrationButton.disabled = true;
    sendResult.textContent = "Sending calibration grid...";
    try {
      var result = await postJson("/api/print/template-lab/calibration-test-send", payload);
      sendResult.textContent = result.message || "Calibration grid sent.";
      setStatus(true, "Calibration Grid Sent", "The send bypassed the production queue and does not confirm physical printing.");
    } catch (error) {
      sendResult.textContent = error.message;
      setStatus(false, "Calibration Grid Failed", error.message);
    } finally {
      printCalibrationButton.disabled = false;
    }
  }

  async function saveProfile() {
    var exported = exportProfileJson();
    saveProfileButton.disabled = true;
    profileSaveResult.textContent = "Saving lab profile JSON...";
    try {
      var result = await postJson("/api/print/template-lab/profile", {
        profileKey: exported.profileKey,
        template: templateSelect.value,
        overrides: exported.overrides
      });
      profileSaveResult.textContent = "Saved to " + result.profileConfigPath;
      await reloadCatalogAndProfile();
      setStatus(true, "Lab Profile Saved", "Saved values affect Template Lab preview/test rendering only.");
    } catch (error) {
      profileSaveResult.textContent = error.message;
      setStatus(false, "Lab Profile Save Failed", error.message);
    } finally {
      saveProfileButton.disabled = false;
    }
  }

  async function resetProfileDefaults() {
    if (!window.confirm("Reset saved Template Lab overrides for " + profileSelect.value + "? Production templates are not changed.")) return;
    resetProfileButton.disabled = true;
    profileSaveResult.textContent = "Resetting saved lab profile overrides...";
    try {
      var result = await postJson("/api/print/template-lab/profile/reset", {
        profileKey: profileSelect.value
      });
      profileSaveResult.textContent = "Reset saved overrides for " + result.profileKey + ". Production templates were not changed.";
      selectedLogoAsset = null;
      await reloadCatalogAndProfile();
      updateRenderState(false);
      setStatus(true, "Lab Profile Reset", "Saved overrides were cleared for this profile only.");
    } catch (error) {
      profileSaveResult.textContent = error.message;
      setStatus(false, "Lab Profile Reset Failed", error.message);
    } finally {
      resetProfileButton.disabled = false;
    }
  }

  async function promoteDynamicTemplate() {
    var exported = exportProfileJson();
    if (!window.confirm("Promote the selected dynamic template to production? A backup will be created before overwrite.")) return;
    promoteTemplateButton.disabled = true;
    if (promoteTemplateResult) promoteTemplateResult.textContent = "Promoting dynamic template...";
    try {
      var result = await postJson("/api/print/template-lab/promote", {
        template: templateSelect.value,
        profileKey: exported.profileKey,
        profileOverrides: exported.overrides
      });
      if (promoteTemplateResult) {
        promoteTemplateResult.textContent = "Promoted " + result.templatePath + " (backup: " + result.backupPath + ")";
      }
      setStatus(true, "Dynamic Template Promoted", "Production source template was updated with dynamic tokens preserved.");
    } catch (error) {
      if (promoteTemplateResult) promoteTemplateResult.textContent = error.message;
      setStatus(false, "Promotion Failed", error.message);
    } finally {
      promoteTemplateButton.disabled = false;
    }
  }

  async function copyProfileJson() {
    exportProfileJson();
    try {
      await navigator.clipboard.writeText(profileJson.value);
      profileSaveResult.textContent = "Profile JSON copied to clipboard.";
    } catch {
      profileSaveResult.textContent = "Copy failed. Select the JSON text and copy it manually.";
    }
  }

  function downloadProfileJson() {
    var exported = exportProfileJson();
    var blob = new Blob([profileJson.value], { type: "application/json" });
    var url = URL.createObjectURL(blob);
    var link = document.createElement("a");
    link.href = url;
    link.download = exported.profileKey.replace(/[^A-Za-z0-9_.-]/g, "_") + ".template-lab-profile.json";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    profileSaveResult.textContent = "Profile JSON downloaded.";
  }

  function syncRangePair(numberId, rangeId) {
    var number = input(numberId);
    var range = input(rangeId);
    if (!number || !range) return;
    number.addEventListener("input", function () {
      if (number.value !== "") range.value = number.value;
      exportProfileJson();
      updateCalibrationSummary();
      updateRenderState(false);
    });
    range.addEventListener("input", function () {
      number.value = range.value;
      exportProfileJson();
      updateCalibrationSummary();
      updateRenderState(false);
    });
  }

  function wireProfileExportOnInput() {
    rangePairs.forEach(function (pair) {
      syncRangePair(pair[0], pair[1]);
    });
    document.querySelectorAll(".template-lab-shell input, .template-lab-shell select").forEach(function (element) {
      element.addEventListener("change", function () {
        exportProfileJson();
        updateCalibrationSummary();
        updateRenderState(false);
      });
    });
  }

  buildFieldFitControls();
  wireFieldSelection();
  wireProfileExportOnInput();
  templateSelect.addEventListener("change", function () {
    applyTemplateDefaults().then(function () {
      updateRenderState(false);
    });
  });
  profileSelect.addEventListener("change", function () {
    loadSelectedProfileValues().then(function () {
      updateRenderState(false);
    });
  });
  renderButton.addEventListener("click", renderTemplate);
  downloadButton.addEventListener("click", downloadRenderedZpl);
  sendButton.addEventListener("click", sendProofPrint);
  if (printCalibrationButton) printCalibrationButton.addEventListener("click", printCalibrationGrid);
  loadProfileButton.addEventListener("click", loadSelectedProfileValues);
  if (reloadTemplateButton) reloadTemplateButton.addEventListener("click", loadSelectedProfileValues);
  if (compareTemplateButton) compareTemplateButton.addEventListener("click", compareCurrentVsStaged);
  if (applyFieldBoostsButton) applyFieldBoostsButton.addEventListener("click", applyGroupedFieldAdjustments);
  exportProfileButton.addEventListener("click", exportProfileJson);
  saveProfileButton.addEventListener("click", saveProfile);
  if (resetProfileButton) resetProfileButton.addEventListener("click", resetProfileDefaults);
  if (promoteTemplateButton) promoteTemplateButton.addEventListener("click", promoteDynamicTemplate);
  if (copyProfileButton) copyProfileButton.addEventListener("click", copyProfileJson);
  if (downloadProfileButton) downloadProfileButton.addEventListener("click", downloadProfileJson);
  if (uploadLogoAssetButton) uploadLogoAssetButton.addEventListener("click", uploadLogoAsset);
  if (selectLogoAssetButton) selectLogoAssetButton.addEventListener("click", selectLogoAsset);

  loadCatalog().then(renderTemplate).catch(function (error) {
    setStatus(false, "Template Lab Unavailable", error.message);
  });
})();
