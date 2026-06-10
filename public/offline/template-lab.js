(function () {
  var catalog = { templates: [], profiles: [] };
  var latestPreview = null;
  var latestRenderSignature = "";
  var fitFields = ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"];
  var renderButton = document.getElementById("renderButton");
  var downloadButton = document.getElementById("downloadButton");
  var sendButton = document.getElementById("sendButton");
  var loadProfileButton = document.getElementById("loadProfileButton");
  var exportProfileButton = document.getElementById("exportProfileButton");
  var saveProfileButton = document.getElementById("saveProfileButton");
  var promoteTemplateButton = document.getElementById("promoteTemplateButton");
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
  var rangePairs = [
    ["scaleX", "scaleXRange"],
    ["scaleY", "scaleYRange"],
    ["offsetX", "offsetXRange"],
    ["offsetY", "offsetYRange"],
    ["qrX", "qrXRange"],
    ["qrY", "qrYRange"],
    ["qrMagnification", "qrMagnificationRange"],
    ["logoX", "logoXRange"],
    ["logoY", "logoYRange"],
    ["logoScale", "logoScaleRange"]
  ];

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

  function buildFieldFitControls() {
    fieldFitControls.innerHTML = fitFields.map(function (field) {
      return [
        "<fieldset class=\"field-fit-fieldset\">",
        "<legend>" + fieldLabel(field) + "</legend>",
        "<p class=\"field-help\">H = font height dots, W = font width dots. Large/medium/small/min are selected by text length. Box width feeds ^FB. Color/material/tolling stay one-line with no wrap or hyphenation; truncation is last resort.</p>",
        "<label title=\"^FB field block width in printer dots. Wider boxes allow larger text before shrinking.\">Box W<input id=\"" + field + "BoxWidth\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Maximum visible characters before truncation. Truncation adds no dash or ellipsis.\">Max Chars<input id=\"" + field + "MaxChars\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Maximum ^FB lines. Use 1 for no wrapping; product description may use 2 if physically verified.\">Max Lines<input id=\"" + field + "MaxLines\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"^FB alignment: L left, C center, R right, J justified.\">Align<select id=\"" + field + "Alignment\"><option value=\"L\">Left</option><option value=\"C\">Center</option><option value=\"R\">Right</option><option value=\"J\">Justify</option></select></label>",
        "<label title=\"Large tier font height in printer dots.\">Large H<input id=\"" + field + "LargeFontH\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Large tier font width in printer dots.\">Large W<input id=\"" + field + "LargeFontW\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Medium tier font height in printer dots.\">Medium H<input id=\"" + field + "MediumFontH\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Medium tier font width in printer dots.\">Medium W<input id=\"" + field + "MediumFontW\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Small tier font height in printer dots.\">Small H<input id=\"" + field + "SmallFontH\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Small tier font width in printer dots.\">Small W<input id=\"" + field + "SmallFontW\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Minimum readable font height in printer dots.\">Min H<input id=\"" + field + "MinFontH\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "<label title=\"Minimum readable font width in printer dots.\">Min W<input id=\"" + field + "MinFontW\" type=\"number\" min=\"1\" step=\"1\"></label>",
        "</fieldset>"
      ].join("");
    }).join("");
  }

  function setFieldDefinitionControls(field, definition) {
    var def = definition || {};
    setInputValue(field + "BoxWidth", def.boxWidth);
    setInputValue(field + "MaxChars", def.maxChars);
    setInputValue(field + "MaxLines", def.maxLines);
    setInputValue(field + "Alignment", def.alignment || (field === "productDescription" ? "L" : "C"));
    ["large", "medium", "small", "min"].forEach(function (tier) {
      var tierDef = def[tier] || {};
      var prefix = field + tier.charAt(0).toUpperCase() + tier.slice(1);
      setInputValue(prefix + "FontH", tierDef.fontH);
      setInputValue(prefix + "FontW", tierDef.fontW);
    });
  }

  function loadSelectedProfileValues() {
    var profile = selectedProfile() || {};
    var effectiveDefinitions = profile.effectiveFieldFitDefinitions || profile.fieldFitDefinitions || {};
    setInputValue("scaleX", profile.scaleX ?? 1);
    setInputValue("scaleY", profile.scaleY ?? 1);
    setInputValue("offsetX", profile.offsetX ?? 0);
    setInputValue("offsetY", profile.offsetY ?? 0);
    setInputValue("qrX", profile.qr && profile.qr.x);
    setInputValue("qrY", profile.qr && profile.qr.y);
    setInputValue("qrMagnification", profile.qr && profile.qr.magnification);
    setInputValue("logoX", profile.logo && profile.logo.x);
    setInputValue("logoY", profile.logo && profile.logo.y);
    setInputValue("logoScale", profile.logo && profile.logo.scale);
    setInputValue("logoWidthDots", profile.logo && profile.logo.widthDots);
    setInputValue("logoHeightDots", profile.logo && profile.logo.heightDots);
    setControlsDisabled(["logoX", "logoY", "logoScale", "logoWidthDots", "logoHeightDots"], !profile.logo || profile.logo.mode !== "static logo");

    fitFields.forEach(function (field) {
      setFieldDefinitionControls(field, effectiveDefinitions[field]);
    });

    ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"].forEach(function (field) {
      var position = profile.fieldPositionOverrides && profile.fieldPositionOverrides[field] || {};
      setInputValue(field + "X", position.x);
      setInputValue(field + "Y", position.y);
    });

    exportProfileJson();
  }

  function collectFieldFitDefinition(field) {
    var output = {};
    putIfNumber(output, "boxWidth", field + "BoxWidth");
    putIfNumber(output, "maxChars", field + "MaxChars");
    putIfNumber(output, "maxLines", field + "MaxLines");
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

  function collectProfileOverrides() {
    var overrides = {};
    putIfNumber(overrides, "scaleX", "scaleX");
    putIfNumber(overrides, "scaleY", "scaleY");
    putIfNumber(overrides, "offsetX", "offsetX");
    putIfNumber(overrides, "offsetY", "offsetY");

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
    if (Object.keys(logo).length) overrides.logo = logo;

    var fieldFitDefinitions = {};
    fitFields.forEach(function (field) {
      var definition = collectFieldFitDefinition(field);
      if (Object.keys(definition).length) fieldFitDefinitions[field] = definition;
    });
    if (Object.keys(fieldFitDefinitions).length) overrides.fieldFitDefinitions = fieldFitDefinitions;

    var fieldPositionOverrides = {};
    ["color", "colorSmall", "materialType", "materialTypeSmall", "tolling", "productDescription"].forEach(function (field) {
      var position = {};
      putIfNumber(position, "x", field + "X");
      putIfNumber(position, "y", field + "Y");
      if (Object.keys(position).length) fieldPositionOverrides[field] = position;
    });
    if (Object.keys(fieldPositionOverrides).length) overrides.fieldPositionOverrides = fieldPositionOverrides;

    return overrides;
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
      ["Profile", preview.profileKey || "-"],
      ["Scale", (profile.scaleX || 1) + " x " + (profile.scaleY || 1)],
      ["Offset", (profile.offsetX || 0) + ", " + (profile.offsetY || 0)],
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

  function applyTemplateDefaults() {
    var definition = selectedTemplateDefinition();
    if (!definition) return;
    profileSelect.value = definition.defaultProfileKey || profileSelect.value;

    if (definition.requiresRfid) {
      input("rfid").disabled = false;
    } else {
      input("rfid").disabled = true;
      input("rfid").value = "";
    }

    loadSelectedProfileValues();
  }

  async function loadCatalog() {
    catalog = await fetchJson("/api/print/template-lab/catalog", { cache: "no-store" });
    templateSelect.innerHTML = catalog.templates.map(function (template) {
      return "<option value=\"" + template.name + "\">" + template.label + "</option>";
    }).join("");
    profileSelect.innerHTML = catalog.profiles.map(function (profile) {
      return "<option value=\"" + profile.key + "\">" + profile.key + " - " + profile.template + "</option>";
    }).join("");
    applyTemplateDefaults();
    setStatus(true, "Template Lab Ready", "Tune controls, render, then proof print only when explicitly confirmed.");
  }

  async function reloadCatalogAndProfile() {
    var selectedProfileKey = profileSelect.value;
    catalog = await fetchJson("/api/print/template-lab/catalog", { cache: "no-store" });
    profileSelect.innerHTML = catalog.profiles.map(function (profile) {
      return "<option value=\"" + profile.key + "\">" + profile.key + " - " + profile.template + "</option>";
    }).join("");
    profileSelect.value = selectedProfileKey;
    loadSelectedProfileValues();
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

  async function sendProofPrint() {
    var payload = formPayload();
    payload.confirmTestPrint = input("confirmTestPrint").checked;
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

  async function saveProfile() {
    var exported = exportProfileJson();
    saveProfileButton.disabled = true;
    profileSaveResult.textContent = "Saving lab profile JSON...";
    try {
      var result = await postJson("/api/print/template-lab/profile", {
        profileKey: exported.profileKey,
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
      updateRenderState(false);
    });
    range.addEventListener("input", function () {
      number.value = range.value;
      exportProfileJson();
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
        updateRenderState(false);
      });
    });
  }

  buildFieldFitControls();
  wireProfileExportOnInput();
  templateSelect.addEventListener("change", applyTemplateDefaults);
  profileSelect.addEventListener("change", loadSelectedProfileValues);
  renderButton.addEventListener("click", renderTemplate);
  downloadButton.addEventListener("click", downloadRenderedZpl);
  sendButton.addEventListener("click", sendProofPrint);
  loadProfileButton.addEventListener("click", loadSelectedProfileValues);
  exportProfileButton.addEventListener("click", exportProfileJson);
  saveProfileButton.addEventListener("click", saveProfile);
  if (promoteTemplateButton) promoteTemplateButton.addEventListener("click", promoteDynamicTemplate);
  if (copyProfileButton) copyProfileButton.addEventListener("click", copyProfileJson);
  if (downloadProfileButton) downloadProfileButton.addEventListener("click", downloadProfileJson);

  loadCatalog().then(renderTemplate).catch(function (error) {
    setStatus(false, "Template Lab Unavailable", error.message);
  });
})();
