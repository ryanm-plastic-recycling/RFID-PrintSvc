(function () {
  var QUICK_EDIT_ZOOM_STORAGE_KEY = "templateQuickEditPreviewZoom";
  var BROWSER_DRAFT_KEY_PREFIX = "priTemplateLabDraft";
  var LAST_CONTEXT_KEY = "priTemplateLabLastContext";
  var DEFAULT_TEMPLATE = "RFID-RAW-P1.template.zpl";
  var DEFAULT_PROFILE = "P1:RAW";
  var DEFAULT_TEMPLATES = [
    "RFID-RAW-P1.template.zpl",
    "RFID-FG-P1.template.zpl",
    "RFID-FG-P3.template.zpl",
    "QCSample-P3.template.zpl",
    "QCRetain-P3.template.zpl",
    "QCSamplePounds-P3.template.zpl"
  ];
  var DEFAULT_PROFILES = [
    "P1:RAW", "P2:RAW", "P3:RAW", "P4:RAW", "P5:RAW", "P6:RAW", "P7:RAW", "P8:RAW",
    "P1:FG", "P2:FG", "P3:FG", "P4:FG", "P5:FG", "P6:FG", "P7:FG", "P8:FG",
    "P3:SAMPLE", "P3:RETAIN", "P3:SAMPLE_POUNDS"
  ];
  var SAMPLE_DEFAULTS = Object.freeze({
    lotNumber: "PT000086",
    boxNumber: "52",
    rfid: "",
    pounds: "1200",
    materialType: "PP",
    color: "Black",
    tolling: "Tolling",
    po: "PO12345",
    productDescription: "Template Lab Product"
  });
  var PROOF_TARGETS = Object.freeze({
    P1: Object.freeze({ ip: "192.168.50.239", port: 9100 }),
    P2: Object.freeze({ ip: "192.168.50.241", port: 9100 }),
    P3: Object.freeze({ ip: "192.168.50.223", port: 9100 }),
    P4: Object.freeze({ ip: "192.168.50.242", port: 9100 }),
    P5: Object.freeze({ ip: "192.168.50.244", port: 9100 }),
    P6: Object.freeze({ ip: "192.168.6.240", port: 9100 }),
    P7: Object.freeze({ ip: "192.168.8.200", port: 9100 }),
    P8: Object.freeze({ ip: "192.168.7.122", port: 9100 })
  });
  var QC_PROOF_TARGETS = Object.freeze({
    P3: Object.freeze({ ip: "192.168.50.218", port: 9100 }),
    P8: Object.freeze({ ip: "192.168.50.214", port: 9100 })
  });
  var AREA_TO_TOKEN = Object.freeze({
    lot: "lotNumber",
    box: "boxNumber",
    material: "materialTypeText",
    materialType: "materialTypeText",
    color: "colorText",
    productDescription: "productDescriptionText",
    po: "po",
    pounds: "pounds",
    tolling: "tollingText"
  });
  var AREA_TO_BORDER_KEY = Object.freeze({
    lot: "lot",
    box: "box",
    material: "materialType",
    materialType: "materialType",
    color: "color",
    productDescription: "productDescription",
    po: "po",
    pounds: "pounds",
    tolling: "tolling",
    bottomGrid: "bottomGrid",
    qr: "qrGuide",
    logo: "logoGuide"
  });
  var GROUP_LABELS = Object.freeze({
    lot: "Lot Number",
    box: "Box Number",
    material: "Material Type",
    materialType: "Material Type",
    color: "Color",
    productDescription: "Product Description",
    po: "PO",
    pounds: "Pounds",
    tolling: "Tolling",
    qr: "QR",
    logo: "Logo",
    bottomGrid: "Bottom Grid",
    label: "Whole Label"
  });
  var STALE_MESSAGE = "Render/Re-render before sending. Current controls have changed since the last render.";
  var catalog = { templates: [], profiles: [] };
  var latestPreview = null;
  var currentRenderSnapshot = null;
  var currentSourcePayload = null;
  var latestRenderSignature = "";
  var selectedObjectId = "";
  var dirty = false;
  var pendingExitHref = "/offline/template-lab";
  var profileOverrides = {
    borderVisibility: {}
  };

  var templateSelect = $("qeTemplate");
  var profileSelect = $("qeProfileKey");
  var printerIpInput = $("qePrinterIp");
  var printerPortInput = $("qePrinterPort");
  var renderButton = $("qeRenderButton");
  var sendProofButton = $("qeSendProofButton");
  var confirmProof = $("qeConfirmProof");
  var saveBrowserDraftButton = $("qeSaveBrowserDraftButton");
  var clearBrowserDraftButton = $("qeClearBrowserDraftButton");
  var reloadProductionButton = $("qeReloadProductionButton");
  var saveProfileButton = $("qeSaveProfileButton");
  var exportProfileButton = $("qeExportProfileButton");
  var printReportButton = $("qePrintReportButton");
  var backToLabButton = $("qeBackToLabButton");
  var exitButton = $("qeExitButton");
  var headerStatus = $("qeHeaderStatus");
  var statusBanner = $("qeStatusBanner");
  var statusTitle = $("qeStatusTitle");
  var statusMeta = $("qeStatusMeta");
  var templateBadge = $("qeTemplateBadge");
  var profileBadge = $("qeProfileBadge");
  var printerBadge = $("qePrinterBadge");
  var previewModeBadge = $("qePreviewModeBadge");
  var renderStateBadge = $("qeRenderStateBadge");
  var lastRenderBadge = $("qeLastRenderBadge");
  var sourceStatusBadge = $("qeSourceStatusBadge");
  var browserDraftBadge = $("qeBrowserDraftBadge");
  var previewFrame = $("qePreviewFrame");
  var previewSvgHost = $("qePreviewSvgHost");
  var previewImage = $("qePreviewImage");
  var previewPlaceholder = $("qePreviewPlaceholder");
  var previewStatus = $("qePreviewStatus");
  var zoomSelect = $("qeZoomSelect");
  var zoomStatus = $("qeZoomStatus");
  var gridToggle = $("qeGridToggle");
  var selectedObjectPanel = $("qeSelectedObjectPanel");
  var linkedControls = $("qeLinkedControls");
  var inspectorControls = $("qeInspectorControls");
  var nudgeControls = $("qeNudgeControls");
  var resetSelectedButton = $("qeResetSelectedButton");
  var showInFullLabButton = $("qeShowInFullLabButton");
  var sampleSummary = $("qeSampleSummary");
  var profileJson = $("qeProfileJson");
  var renderDetails = $("qeRenderDetails");
  var proofResult = $("qeProofResult");
  var saveResult = $("qeSaveResult");
  var printReportBody = $("quickEditPrintReportBody");
  var exitDialog = $("qeExitDialog");
  var exitSaveButton = $("qeExitSaveButton");
  var exitExportButton = $("qeExitExportButton");
  var exitDiscardButton = $("qeExitDiscardButton");
  var exitCancelButton = $("qeExitCancelButton");

  function $(id) {
    return document.getElementById(id);
  }

  function input(id) {
    return $(id);
  }

  function clonePlain(value) {
    return JSON.parse(JSON.stringify(value || {}));
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, function (char) {
      return {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "\"": "&quot;",
        "'": "&#39;"
      }[char];
    });
  }

  function safeText(value) {
    return escapeHtml(value || "-");
  }

  async function fetchJson(url, options) {
    var response = await fetch(url, options || {});
    var payload = await response.json().catch(function () { return {}; });
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message || response.statusText || "Request failed");
    }
    return payload;
  }

  async function postJson(url, body) {
    return fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    });
  }

  function setStatus(ok, title, meta) {
    if (headerStatus) {
      headerStatus.className = "status-pill " + (ok ? "status-pill-enabled" : "status-pill-disabled");
      headerStatus.textContent = "Status: " + title;
    }
    if (statusBanner) statusBanner.className = "status-banner " + (ok ? "status-enabled" : "status-disabled");
    if (statusTitle) statusTitle.textContent = title;
    if (statusMeta) statusMeta.textContent = meta || "";
  }

  function shortDigest(digest) {
    return digest ? String(digest).slice(0, 12) : "-";
  }

  function readValue(id) {
    var element = input(id);
    return element ? String(element.value || "").trim() : "";
  }

  function setValue(id, value) {
    var element = input(id);
    if (element) element.value = value ?? "";
  }

  function numberOrNull(value) {
    if (value === "" || value === null || value === undefined) return null;
    var number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function cleanProfileKey(value) {
    return String(value || DEFAULT_PROFILE).trim().toUpperCase();
  }

  function stationForProfile(profileKey) {
    return cleanProfileKey(profileKey).split(":")[0] || "P1";
  }

  function proofTargetForProfile(profileKey) {
    var key = cleanProfileKey(profileKey);
    var station = stationForProfile(key);
    if (/SAMPLE|RETAIN/.test(key)) return QC_PROOF_TARGETS[station] || PROOF_TARGETS[station] || PROOF_TARGETS.P1;
    return PROOF_TARGETS[station] || PROOF_TARGETS.P1;
  }

  function optionHtml(value, label) {
    return "<option value=\"" + escapeHtml(value) + "\">" + escapeHtml(label || value) + "</option>";
  }

  function uniqueOrdered(values) {
    var seen = new Set();
    return values.filter(function (value) {
      if (!value || seen.has(value)) return false;
      seen.add(value);
      return true;
    });
  }

  function fillTemplateSelector() {
    var catalogNames = (catalog.templates || []).map(function (template) { return template.name; });
    var names = uniqueOrdered(DEFAULT_TEMPLATES.concat(catalogNames));
    templateSelect.innerHTML = names.map(function (name) {
      var found = (catalog.templates || []).find(function (template) { return template.name === name; });
      return optionHtml(name, found && found.label ? found.label : name);
    }).join("");
  }

  function fillProfileSelector() {
    var catalogKeys = (catalog.profiles || []).map(function (profile) { return profile.key; });
    var keys = uniqueOrdered(DEFAULT_PROFILES.concat(catalogKeys));
    profileSelect.innerHTML = keys.map(function (key) {
      var found = (catalog.profiles || []).find(function (profile) { return profile.key === key; });
      return optionHtml(key, found && found.template ? key + " - " + found.template : key);
    }).join("");
  }

  function selectedTemplateDefinition() {
    return (catalog.templates || []).find(function (template) { return template.name === templateSelect.value; });
  }

  function applyTemplateDefaultProfile() {
    var definition = selectedTemplateDefinition();
    if (definition && definition.defaultProfileKey && hasOption(profileSelect, definition.defaultProfileKey)) {
      profileSelect.value = definition.defaultProfileKey;
    }
  }

  function hasOption(select, value) {
    return Array.from(select.options || []).some(function (option) { return option.value === value; });
  }

  function applyPrinterDefaults() {
    var target = proofTargetForProfile(profileSelect.value);
    if (target) {
      printerIpInput.value = target.ip;
      printerPortInput.value = String(target.port || 9100);
    }
    renderBadges();
  }

  function sampleData() {
    return {
      lotNumber: readValue("qeLotNumber") || SAMPLE_DEFAULTS.lotNumber,
      boxNumber: readValue("qeBoxNumber") || SAMPLE_DEFAULTS.boxNumber,
      rfid: readValue("qeRfid"),
      pounds: readValue("qePounds") || SAMPLE_DEFAULTS.pounds,
      materialType: readValue("qeMaterialType") || SAMPLE_DEFAULTS.materialType,
      color: readValue("qeColor") || SAMPLE_DEFAULTS.color,
      tolling: readValue("qeTolling") || SAMPLE_DEFAULTS.tolling,
      po: readValue("qePo") || SAMPLE_DEFAULTS.po,
      productDescription: readValue("qeProductDescription") || SAMPLE_DEFAULTS.productDescription
    };
  }

  function updateSampleSummary() {
    if (!sampleSummary) return;
    var sample = sampleData();
    sampleSummary.textContent = "Sample: lot " + sample.lotNumber +
      " | box " + sample.boxNumber +
      " | material " + sample.materialType +
      " | color " + sample.color +
      " | tolling " + sample.tolling +
      " | pounds " + sample.pounds;
  }

  function collectProfileOverrides() {
    return clonePlain(profileOverrides);
  }

  function formPayload() {
    var sample = sampleData();
    return {
      template: templateSelect.value || DEFAULT_TEMPLATE,
      templateName: templateSelect.value || DEFAULT_TEMPLATE,
      profileKey: profileSelect.value || DEFAULT_PROFILE,
      lotNumber: sample.lotNumber,
      boxNumber: sample.boxNumber,
      rfid: sample.rfid,
      pounds: sample.pounds,
      materialType: sample.materialType,
      color: sample.color,
      tolling: sample.tolling,
      po: sample.po,
      productDescription: sample.productDescription,
      printerIp: printerIpInput.value,
      printerPort: Number(printerPortInput.value || 9100),
      port: Number(printerPortInput.value || 9100),
      profileOverrides: collectProfileOverrides()
    };
  }

  function browserDraftKey(templateName, profileKey) {
    return BROWSER_DRAFT_KEY_PREFIX + ":" + encodeURIComponent(templateName || "") + ":" + encodeURIComponent(profileKey || "");
  }

  function currentDraftKey() {
    return browserDraftKey(templateSelect.value || DEFAULT_TEMPLATE, profileSelect.value || DEFAULT_PROFILE);
  }

  function browserDraftPayload() {
    return {
      templateName: templateSelect.value || DEFAULT_TEMPLATE,
      template: templateSelect.value || DEFAULT_TEMPLATE,
      profileKey: profileSelect.value || DEFAULT_PROFILE,
      sampleData: sampleData(),
      profileOverrides: collectProfileOverrides(),
      selectedObjectId: selectedObjectId || "",
      selectedArea: areaForItem(previewObjectById(selectedObjectId)) || "",
      zoomMode: zoomSelect ? zoomSelect.value : "fit",
      timestamp: new Date().toISOString(),
      dirty: dirty,
      stale: isStale()
    };
  }

  function setSourceStatusBadge(status) {
    if (!sourceStatusBadge) return;
    var labels = {
      production_with_sidecar: "Source: Production Template + Sidecar Profile",
      production_parsed: "Source: Parsed Production Template",
      missing_sidecar: "Source: Parsed Production Template",
      built_in_default: "Source: Built-in Defaults",
      browser_draft: "Source: Browser Draft"
    };
    sourceStatusBadge.textContent = labels[status] || "Source: " + (status || "-");
    sourceStatusBadge.className = "template-badge " + (status === "built_in_default" ? "template-badge-warning" : status === "browser_draft" ? "template-badge-enabled" : "template-badge-neutral");
  }

  function setBrowserDraftBadge(draft, loaded) {
    if (!browserDraftBadge) return;
    var text = "Browser draft: none";
    if (draft) {
      var promotedAt = currentSourcePayload?.sidecarProfileJson?.promotedAtUtc || "";
      var newerThanProduction = promotedAt && Date.parse(draft.timestamp || "") > Date.parse(promotedAt);
      text = (loaded ? "Browser draft: loaded" : "Browser draft: available") + (newerThanProduction ? " newer than production" : "");
    }
    browserDraftBadge.textContent = text;
    browserDraftBadge.className = "template-badge " + (draft ? "template-badge-enabled" : "template-badge-neutral");
  }

  function readBrowserDraft() {
    try {
      var parsed = JSON.parse(localStorage.getItem(currentDraftKey()) || "null");
      if (!parsed || parsed.templateName !== templateSelect.value || parsed.profileKey !== profileSelect.value) return null;
      return parsed;
    } catch {
      return null;
    }
  }

  function saveBrowserDraft(options) {
    var opts = options || {};
    try {
      var draft = browserDraftPayload();
      localStorage.setItem(currentDraftKey(), JSON.stringify(draft));
      localStorage.setItem(LAST_CONTEXT_KEY, JSON.stringify({
        templateName: draft.templateName,
        profileKey: draft.profileKey,
        timestamp: draft.timestamp
      }));
      setBrowserDraftBadge(draft, false);
      if (!opts.silent) {
        saveResult.textContent = "Browser draft saved to this browser only. Production unchanged.";
        setStatus(true, "Browser Draft Saved", "Browser draft saved to this browser only.");
      }
      return draft;
    } catch (error) {
      if (!opts.silent) setStatus(false, "Browser Draft Save Failed", error.message);
      return null;
    }
  }

  function clearBrowserDraft(options) {
    var opts = options || {};
    try {
      localStorage.removeItem(currentDraftKey());
      setBrowserDraftBadge(null, false);
      if (!opts.silent) {
        saveResult.textContent = "Browser draft cleared. Production template/profile source will be loaded.";
        setStatus(true, "Browser Draft Cleared", "The browser draft was removed; production files were not changed.");
      }
    } catch (error) {
      if (!opts.silent) setStatus(false, "Browser Draft Clear Failed", error.message);
    }
  }

  function applyBrowserDraft(draft) {
    if (!draft) return false;
    applyHandoff(draft);
    if (draft.zoomMode && zoomSelect && Array.from(zoomSelect.options || []).some(function (option) { return option.value === draft.zoomMode; })) {
      zoomSelect.value = draft.zoomMode;
      persistZoomPreference();
      applyZoom();
    }
    selectedObjectId = draft.selectedObjectId || "";
    setSourceStatusBadge("browser_draft");
    setBrowserDraftBadge(draft, true);
    renderProfileJson();
    dirty = Boolean(draft.dirty || draft.stale);
    renderBadges();
    return true;
  }

  async function loadProductionSource(options) {
    var opts = options || {};
    var query = new URLSearchParams({
      templateName: templateSelect.value || DEFAULT_TEMPLATE,
      profileKey: profileSelect.value || DEFAULT_PROFILE
    });
    currentSourcePayload = await fetchJson("/api/print/template-lab/source?" + query.toString(), { cache: "no-store" });
    var sourceOverrides = currentSourcePayload.hydrationProfileOverrides || currentSourcePayload.sidecarProfileJson?.hydrationProfileOverrides || currentSourcePayload.sidecarProfileJson?.profileOverrides || {};
    profileOverrides = clonePlain(sourceOverrides || {});
    profileOverrides.borderVisibility = profileOverrides.borderVisibility || {};
    setSourceStatusBadge(currentSourcePayload.sourceStatus);
    setBrowserDraftBadge(readBrowserDraft(), false);
    if (opts.allowDraft !== false) applyBrowserDraft(readBrowserDraft());
    renderProfileJson();
    renderBadges();
    return currentSourcePayload;
  }

  async function reloadProductionSource() {
    try {
      await loadProductionSource({ allowDraft: false });
      dirty = false;
      setStatus(true, "Production Source Reloaded", "Loaded current production template and sidecar profile; browser draft was not applied.");
    } catch (error) {
      setStatus(false, "Production Source Reload Failed", error.message);
    }
  }

  async function clearBrowserDraftAndReload() {
    clearBrowserDraft();
    await reloadProductionSource();
  }

  function currentRenderSignature() {
    return JSON.stringify({
      template: templateSelect.value || DEFAULT_TEMPLATE,
      profileKey: profileSelect.value || DEFAULT_PROFILE,
      sampleData: sampleData(),
      profileOverrides: collectProfileOverrides()
    });
  }

  function isStale() {
    return !currentRenderSnapshot || currentRenderSignature() !== latestRenderSignature;
  }

  function renderBadges() {
    var target = (printerIpInput.value || "-") + ":" + (printerPortInput.value || "9100");
    if (templateBadge) templateBadge.textContent = "Template: " + (templateSelect.value || "-");
    if (profileBadge) profileBadge.textContent = "Profile: " + (profileSelect.value || "-");
    if (printerBadge) printerBadge.textContent = "Printer: " + target;
    if (previewModeBadge) {
      var mode = latestPreview && (latestPreview.metadata?.previewMode || latestPreview.imagePreview?.mode);
      previewModeBadge.textContent = "Preview: " + (mode || "-");
    }
    if (lastRenderBadge) lastRenderBadge.textContent = "Last render: " + (currentRenderSnapshot?.renderedAt || "-");
    if (renderStateBadge) {
      var stale = isStale();
      renderStateBadge.className = "template-badge " + (stale ? "template-badge-warning" : "template-badge-enabled");
      renderStateBadge.textContent = stale ? "Stale: render needed" : "Fresh: proof uses last render";
    }
    renderStateDetails();
  }

  function renderStateDetails() {
    if (!renderDetails) return;
    var snapshot = currentRenderSnapshot || {};
    renderDetails.innerHTML = [
      detailBox("RenderId", snapshot.renderId || "-"),
      detailBox("Render digest", shortDigest(snapshot.renderedZplSha256)),
      detailBox("Template digest", shortDigest(snapshot.dynamicTemplateSha256)),
      detailBox("Stale", isStale() ? "yes" : "no"),
      detailBox("Payload bytes", snapshot.payloadBytes || "-"),
      detailBox("Proof source", snapshot.renderedZpl ? "last rendered ZPL snapshot" : "-")
    ].join("");
  }

  function detailBox(label, value) {
    return "<div><em>" + escapeHtml(label) + "</em><strong>" + escapeHtml(value) + "</strong></div>";
  }

  function setDirty(staleMessage) {
    dirty = true;
    renderBadges();
    if (previewStatus && staleMessage) previewStatus.textContent = staleMessage;
    renderProfileJson();
    saveBrowserDraft({ silent: true });
  }

  function renderProfileJson() {
    if (!profileJson) return null;
    var exported = {
      profileKey: profileSelect.value || DEFAULT_PROFILE,
      template: templateSelect.value || DEFAULT_TEMPLATE,
      templateName: templateSelect.value || DEFAULT_TEMPLATE,
      overrides: collectProfileOverrides()
    };
    profileJson.value = JSON.stringify(exported, null, 2);
    return exported;
  }

  function previewImageSrc(imagePreview) {
    if (!imagePreview) return "";
    var data = imagePreview.data || {};
    if (data.imageData) {
      var contentType = data.contentType || data.mimeType || "image/png";
      return "data:" + contentType + ";base64," + data.imageData;
    }
    if (data.base64) {
      var mime = data.contentType || data.mimeType || "image/png";
      return "data:" + mime + ";base64," + data.base64;
    }
    return data.imageUrl || data.url || data.renderedUrl || data.previewUrl || "";
  }

  function renderPreviewImage(preview) {
    var imagePreview = preview.imagePreview || {};
    var data = imagePreview.data || {};
    var svg = data.svg || preview.approximateSvg || "";
    var src = previewImageSrc(imagePreview);
    previewImage.classList.add("hidden");
    previewImage.removeAttribute("src");
    previewSvgHost.classList.add("hidden");
    previewSvgHost.innerHTML = "";
    previewPlaceholder.classList.remove("hidden");

    if (svg) {
      previewSvgHost.innerHTML = svg;
      previewSvgHost.classList.remove("hidden");
      previewPlaceholder.classList.add("hidden");
      previewStatus.textContent = imagePreview.message || "Interactive approximate preview shown. Hover or click objects to quick edit.";
      wirePreviewObjectHandlers();
      applyZoom();
      return;
    }

    if (src) {
      previewImage.onload = applyZoom;
      previewImage.src = src;
      previewImage.classList.remove("hidden");
      previewPlaceholder.classList.add("hidden");
      previewStatus.textContent = "Preview renderer returned an image for this Quick Edit render.";
      applyZoom();
      return;
    }

    previewStatus.textContent = imagePreview.message || "Approximate preview unavailable. Render again or use a proof print for final verification.";
    previewPlaceholder.textContent = imagePreview.configured ? "Renderer unavailable" : "Approximate preview unavailable";
  }

  function applyZoom() {
    var mode = zoomSelect ? zoomSelect.value : "fit";
    var element = activePreviewElement();
    if (!element) {
      updateZoomStatus(mode, null);
      return;
    }
    var fitScale = calculatedFitScale(element);
    var scale = scaleForZoomMode(mode, fitScale);
    var naturalSize = previewNaturalSize(element);
    element.setAttribute("data-zoom", mode || "fit");
    element.style.width = Math.max(1, Math.round(naturalSize.width * scale)) + "px";
    element.style.maxWidth = "none";
    if (previewFrame) previewFrame.classList.toggle("zoomed-out", scale < 1);
    updateZoomStatus(mode, scale);
  }

  function activePreviewElement() {
    if (previewSvgHost && !previewSvgHost.classList.contains("hidden") && previewSvgHost.innerHTML.trim()) return previewSvgHost;
    if (previewImage && !previewImage.classList.contains("hidden") && previewImage.getAttribute("src")) return previewImage;
    return null;
  }

  function previewNaturalSize(element) {
    if (element === previewImage && previewImage.naturalWidth && previewImage.naturalHeight) {
      return { width: previewImage.naturalWidth, height: previewImage.naturalHeight };
    }
    var svg = element && element.querySelector ? element.querySelector("svg") : null;
    if (svg) {
      var viewBox = String(svg.getAttribute("viewBox") || "").trim().split(/\s+/).map(Number);
      if (viewBox.length === 4 && viewBox.every(Number.isFinite) && viewBox[2] > 0 && viewBox[3] > 0) {
        return { width: viewBox[2], height: viewBox[3] };
      }
      var svgWidth = numberOrNull(svg.getAttribute("width"));
      var svgHeight = numberOrNull(svg.getAttribute("height"));
      if (svgWidth && svgHeight) return { width: svgWidth, height: svgHeight };
    }
    var rect = element ? element.getBoundingClientRect() : null;
    return {
      width: Math.max(1, rect?.width || 812),
      height: Math.max(1, rect?.height || 1218)
    };
  }

  function previewFrameUsableSize() {
    if (!previewFrame) return { width: 812, height: 1218 };
    var style = window.getComputedStyle(previewFrame);
    var horizontalPadding = Number.parseFloat(style.paddingLeft || "0") + Number.parseFloat(style.paddingRight || "0");
    var verticalPadding = Number.parseFloat(style.paddingTop || "0") + Number.parseFloat(style.paddingBottom || "0");
    return {
      width: Math.max(1, previewFrame.clientWidth - horizontalPadding),
      height: Math.max(1, previewFrame.clientHeight - verticalPadding)
    };
  }

  function calculatedFitScale(element) {
    var naturalSize = previewNaturalSize(element);
    var frameSize = previewFrameUsableSize();
    var widthScale = frameSize.width / naturalSize.width;
    var heightScale = frameSize.height / naturalSize.height;
    return Math.max(0.05, Math.min(widthScale, heightScale));
  }

  function fitScaleMultiplier(mode) {
    if (mode === "fit-25") return 0.75;
    if (mode === "fit-50") return 0.50;
    if (mode === "fit-75") return 0.25;
    return 1;
  }

  function scaleForZoomMode(mode, fitScale) {
    if (String(mode || "fit").startsWith("fit")) return fitScale * fitScaleMultiplier(mode);
    var absoluteScale = Number(mode);
    return Number.isFinite(absoluteScale) && absoluteScale > 0 ? absoluteScale : fitScale;
  }

  function zoomModeLabel(mode) {
    return {
      fit: "Fit",
      "fit-25": "Fit -25%",
      "fit-50": "Fit -50%",
      "fit-75": "Fit -75%",
      "0.25": "25%",
      "0.5": "50%",
      "0.75": "75%",
      "1": "100%",
      "1.25": "125%",
      "1.5": "150%",
      "2": "200%"
    }[String(mode || "fit")] || "Fit";
  }

  function updateZoomStatus(mode, scale) {
    if (!zoomStatus) return;
    var scaleText = Number.isFinite(scale) ? Math.round(scale * 100) + "%" : "-";
    zoomStatus.textContent = "Zoom: " + zoomModeLabel(mode) + " | Scale: " + scaleText;
  }

  function persistZoomPreference() {
    if (!zoomSelect) return;
    try {
      localStorage.setItem(QUICK_EDIT_ZOOM_STORAGE_KEY, zoomSelect.value || "fit");
    } catch {
      // Zoom persistence is visual-only convenience.
    }
  }

  function restoreZoomPreference() {
    if (!zoomSelect) return;
    try {
      var saved = localStorage.getItem(QUICK_EDIT_ZOOM_STORAGE_KEY);
      if (saved && Array.from(zoomSelect.options || []).some(function (option) { return option.value === saved; })) {
        zoomSelect.value = saved;
      }
    } catch {
      // Zoom persistence is visual-only convenience.
    }
    updateZoomStatus(zoomSelect.value || "fit", null);
  }

  function setGridVisible() {
    if (!previewFrame || !gridToggle) return;
    previewFrame.classList.toggle("grid-off", !gridToggle.checked);
  }

  function geometryItems() {
    if (currentRenderSnapshot && Array.isArray(currentRenderSnapshot.geometryMap)) return currentRenderSnapshot.geometryMap;
    if (latestPreview && Array.isArray(latestPreview.geometryMap)) return latestPreview.geometryMap;
    if (latestPreview && latestPreview.metadata && Array.isArray(latestPreview.metadata.geometryMap)) return latestPreview.metadata.geometryMap;
    return [];
  }

  function previewObjectById(objectId) {
    return geometryItems().find(function (item) { return item.id === objectId; });
  }

  function updatePreviewSelectionClasses() {
    if (!previewSvgHost) return;
    previewSvgHost.querySelectorAll(".preview-object").forEach(function (element) {
      element.classList.toggle("preview-object-selected", element.getAttribute("data-object-id") === selectedObjectId);
    });
  }

  function wirePreviewObjectHandlers() {
    if (!previewSvgHost) return;
    previewSvgHost.querySelectorAll(".preview-object").forEach(function (element) {
      var objectId = element.getAttribute("data-object-id") || "";
      element.setAttribute("tabindex", "0");
      element.addEventListener("mouseenter", function () {
        var item = previewObjectById(objectId);
        element.classList.add("preview-object-hover");
        previewStatus.textContent = objectSummary(item) || objectId || "Preview object";
      });
      element.addEventListener("mouseleave", function () {
        element.classList.remove("preview-object-hover");
        previewStatus.textContent = isStale() ? "Re-render after edit to update preview and proof payload." : "Preview ready. Click an object to quick edit.";
      });
      element.addEventListener("click", function () {
        selectPreviewObject(objectId);
      });
      element.addEventListener("keydown", function (event) {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        selectPreviewObject(objectId);
      });
    });
    updatePreviewSelectionClasses();
  }

  function objectSummary(item) {
    if (!item) return "";
    return (item.label || item.id || "Object") +
      " | " + (item.type || "unknown") +
      " | x " + (item.x ?? "-") +
      " y " + (item.y ?? "-") +
      " w " + (item.width ?? "-") +
      " h " + (item.height ?? "-");
  }

  function selectPreviewObject(objectId) {
    selectedObjectId = objectId || "";
    updatePreviewSelectionClasses();
    renderInspector(previewObjectById(selectedObjectId));
  }

  function areaForItem(item) {
    if (!item) return "";
    if (String(item.id || "").startsWith("bottomGrid")) return "bottomGrid";
    return item.area || "";
  }

  function tokenForItem(item) {
    var area = areaForItem(item);
    if (AREA_TO_TOKEN[area]) return AREA_TO_TOKEN[area];
    var linked = Array.isArray(item?.linkedControls) ? item.linkedControls.join(" ") : "";
    var match = linked.match(/fieldGeometryOverrides\.([A-Za-z0-9_:-]+)/);
    return match ? match[1] : "";
  }

  function borderKeyForItem(item) {
    return AREA_TO_BORDER_KEY[areaForItem(item)] || "";
  }

  function groupLabel(item) {
    return GROUP_LABELS[areaForItem(item)] || item?.area || "Label Object";
  }

  function linkedControlsForItem(item) {
    if (!item) return [];
    if (Array.isArray(item.linkedControls) && item.linkedControls.length) return item.linkedControls;
    if (areaForItem(item) === "qr") return ["qr.x", "qr.y", "qr.magnification"];
    if (areaForItem(item) === "logo") return ["logo.x", "logo.y", "logo.widthDots", "logo.heightDots", "logo.scale"];
    if (areaForItem(item) === "bottomGrid") return ["bottomGrid.x", "bottomGrid.y", "bottomGrid.width", "bottomGrid.height"];
    var token = tokenForItem(item);
    return token ? ["fieldGeometryOverrides." + token] : [];
  }

  function ensureFieldOverride(item) {
    var token = tokenForItem(item);
    if (!token) return null;
    profileOverrides.fieldGeometryOverrides = profileOverrides.fieldGeometryOverrides || {};
    profileOverrides.fieldGeometryOverrides[token] = profileOverrides.fieldGeometryOverrides[token] || {};
    return profileOverrides.fieldGeometryOverrides[token];
  }

  function fieldOverrideForItem(item) {
    var token = tokenForItem(item);
    return token && profileOverrides.fieldGeometryOverrides
      ? profileOverrides.fieldGeometryOverrides[token] || {}
      : {};
  }

  function ensureAreaOverride(item) {
    var area = areaForItem(item);
    if (area === "qr") {
      profileOverrides.qr = profileOverrides.qr || {};
      return profileOverrides.qr;
    }
    if (area === "logo") {
      profileOverrides.logo = profileOverrides.logo || {};
      return profileOverrides.logo;
    }
    if (area === "bottomGrid") {
      profileOverrides.bottomGrid = profileOverrides.bottomGrid || {};
      return profileOverrides.bottomGrid;
    }
    return ensureFieldOverride(item);
  }

  function areaOverrideForItem(item) {
    var area = areaForItem(item);
    if (area === "qr") return profileOverrides.qr || {};
    if (area === "logo") return profileOverrides.logo || {};
    if (area === "bottomGrid") return profileOverrides.bottomGrid || {};
    return fieldOverrideForItem(item);
  }

  function readControlValue(item, key) {
    var area = areaForItem(item);
    var areaOverride = areaOverrideForItem(item);
    if (key === "x" || key === "y") return areaOverride[key] ?? item[key] ?? "";
    if (key === "fontHeight" || key === "fontWidth" || key === "fieldWidth" || key === "alignment") return areaOverride[key] ?? item[key] ?? "";
    if (key === "width") {
      if (area === "logo") return areaOverride.widthDots ?? item.width ?? "";
      if (area === "bottomGrid" || area === "qr") return areaOverride.width ?? item.width ?? "";
      if (item.type === "text") return areaOverride.fieldWidth ?? item.width ?? "";
      return areaOverride.border?.width ?? areaOverride.width ?? item.width ?? "";
    }
    if (key === "height") {
      if (area === "logo") return areaOverride.heightDots ?? item.height ?? "";
      if (area === "bottomGrid" || area === "qr") return areaOverride.height ?? item.height ?? "";
      return areaOverride.border?.height ?? areaOverride.height ?? item.height ?? "";
    }
    if (key === "borderThickness") return areaOverride.border?.thickness ?? areaOverride.borderThickness ?? item.borderThickness ?? "";
    if (key === "qrMagnification") return profileOverrides.qr?.magnification ?? "";
    if (key === "logoScale") return profileOverrides.logo?.scale ?? "";
    if (key === "visible") {
      var borderKey = borderKeyForItem(item);
      return profileOverrides.borderVisibility?.[borderKey] !== false;
    }
    return "";
  }

  function setSelectedValue(key, value) {
    var item = previewObjectById(selectedObjectId);
    if (!item) return;
    var area = areaForItem(item);

    if (key === "visible") {
      var borderKey = borderKeyForItem(item);
      if (borderKey) {
        profileOverrides.borderVisibility = profileOverrides.borderVisibility || {};
        profileOverrides.borderVisibility[borderKey] = Boolean(value);
      }
      renderInspector(item);
      setDirty("Re-render after edit to update preview and proof payload.");
      return;
    }

    var areaOverride = ensureAreaOverride(item);
    if (!areaOverride) return;
    var numeric = numberOrNull(value);

    if (key === "x" || key === "y") {
      areaOverride[key] = numeric;
    } else if (key === "fontHeight" || key === "fontWidth" || key === "alignment") {
      areaOverride[key] = key === "alignment" ? String(value || "L") : numeric;
    } else if (key === "fieldWidth") {
      areaOverride.fieldWidth = numeric;
    } else if (key === "width") {
      if (area === "logo") areaOverride.widthDots = numeric;
      else if (area === "bottomGrid" || area === "qr") areaOverride.width = numeric;
      else if (item.type === "text") areaOverride.fieldWidth = numeric;
      else {
        areaOverride.border = areaOverride.border || {};
        areaOverride.border.width = numeric;
      }
    } else if (key === "height") {
      if (area === "logo") areaOverride.heightDots = numeric;
      else if (area === "bottomGrid" || area === "qr") areaOverride.height = numeric;
      else {
        areaOverride.border = areaOverride.border || {};
        areaOverride.border.height = numeric;
      }
    } else if (key === "borderThickness") {
      if (area === "bottomGrid") areaOverride.borderThickness = numeric;
      else {
        areaOverride.border = areaOverride.border || {};
        areaOverride.border.thickness = numeric;
      }
    } else if (key === "qrMagnification") {
      profileOverrides.qr = profileOverrides.qr || {};
      profileOverrides.qr.magnification = numeric;
    } else if (key === "logoScale") {
      profileOverrides.logo = profileOverrides.logo || {};
      profileOverrides.logo.scale = numeric;
    }

    renderInspector(item);
    setDirty("Re-render after edit to update preview and proof payload.");
  }

  function controlDefinitions(item) {
    if (!item) return [];
    var type = item.type || "unknown";
    var area = areaForItem(item);
    var controls = [
      { key: "x", label: "X", type: "number", step: "1" },
      { key: "y", label: "Y", type: "number", step: "1" }
    ];
    if (type === "text") {
      controls.push(
        { key: "fieldWidth", label: "Field Width", type: "number", step: "1" },
        { key: "fontHeight", label: "Font H", type: "number", step: "1" },
        { key: "fontWidth", label: "Font W", type: "number", step: "1" },
        { key: "alignment", label: "Alignment", type: "select" }
      );
    }
    if (type === "border" || type === "background" || type === "grid" || area === "bottomGrid") {
      controls.push(
        { key: "width", label: "Width", type: "number", step: "1" },
        { key: "height", label: "Height", type: "number", step: "1" },
        { key: "borderThickness", label: type === "background" ? "Fill/Border Thickness" : "Border Thickness", type: "number", step: "1" }
      );
    }
    if (area === "qr") {
      controls.push(
        { key: "width", label: "Guide Width", type: "number", step: "1" },
        { key: "height", label: "Guide Height", type: "number", step: "1" },
        { key: "qrMagnification", label: "QR Magnification", type: "number", step: "1" }
      );
    }
    if (area === "logo") {
      controls.push(
        { key: "width", label: "Logo Width Dots", type: "number", step: "1" },
        { key: "height", label: "Logo Height Dots", type: "number", step: "1" },
        { key: "logoScale", label: "Logo Scale", type: "number", step: "0.05" }
      );
    }
    if (borderKeyForItem(item) && (type === "border" || type === "background" || type === "grid" || area === "qr" || area === "logo")) {
      controls.push({ key: "visible", label: "Show Border / Guide", type: "checkbox" });
    }
    return controls;
  }

  function renderInspector(item) {
    if (!item) {
      selectedObjectPanel.textContent = "Selected object: click a preview object.";
      linkedControls.textContent = "Linked controls: -";
      inspectorControls.innerHTML = "<p class=\"muted-line\">Hover or click the rendered preview to identify label objects and quick-edit visual controls.</p>";
      return;
    }

    selectedObjectPanel.innerHTML = [
      "<strong>" + safeText(item.label || groupLabel(item)) + "</strong>",
      "<span>Semantic id: " + safeText(item.id) + "</span>",
      "<span>Type: " + safeText(item.type) + " | Area: " + safeText(areaForItem(item)) + "</span>",
      "<span>X " + safeText(item.x) + " | Y " + safeText(item.y) + " | W " + safeText(item.width) + " | H " + safeText(item.height) + "</span>"
    ].join("<br>");
    linkedControls.textContent = "Linked controls: " + (linkedControlsForItem(item).join(", ") || "-");

    var controls = controlDefinitions(item);
    inspectorControls.innerHTML = "<div class=\"quick-edit-control-grid\">" + controls.map(function (control) {
      var value = readControlValue(item, control.key);
      if (control.type === "select") {
        return "<label>" + escapeHtml(control.label) +
          "<select data-qe-control=\"" + escapeHtml(control.key) + "\">" +
          ["L", "C", "R", "J"].map(function (option) {
            return "<option value=\"" + option + "\"" + (String(value || "L") === option ? " selected" : "") + ">" + option + "</option>";
          }).join("") +
          "</select></label>";
      }
      if (control.type === "checkbox") {
        return "<label class=\"checkbox-label compact-checkbox\"><input type=\"checkbox\" data-qe-control=\"" + escapeHtml(control.key) + "\"" + (value ? " checked" : "") + "> " + escapeHtml(control.label) + "</label>";
      }
      return "<label>" + escapeHtml(control.label) + "<input type=\"number\" step=\"" + escapeHtml(control.step || "1") + "\" data-qe-control=\"" + escapeHtml(control.key) + "\" value=\"" + escapeHtml(value) + "\"></label>";
    }).join("") + "</div>";
  }

  function nudgeSelected(axis, delta) {
    var item = previewObjectById(selectedObjectId);
    if (!item) return;
    var controlKey = axis === "width" || axis === "height" ? axis : axis;
    var current = Number(readControlValue(item, controlKey));
    if (!Number.isFinite(current)) current = Number(item[controlKey]) || 0;
    setSelectedValue(controlKey, current + Number(delta || 0));
  }

  function resetSelectedObject() {
    var item = previewObjectById(selectedObjectId);
    if (!item) return;
    var area = areaForItem(item);
    if (area === "qr") delete profileOverrides.qr;
    else if (area === "logo") delete profileOverrides.logo;
    else if (area === "bottomGrid") delete profileOverrides.bottomGrid;
    else {
      var token = tokenForItem(item);
      if (token && profileOverrides.fieldGeometryOverrides) delete profileOverrides.fieldGeometryOverrides[token];
    }
    var borderKey = borderKeyForItem(item);
    if (borderKey && profileOverrides.borderVisibility) delete profileOverrides.borderVisibility[borderKey];
    renderInspector(item);
    setDirty("Selected object reset to profile/template default. Re-render to update preview.");
  }

  function snapshotOrBlock(targetElement) {
    if (!currentRenderSnapshot) {
      if (targetElement) targetElement.textContent = "Render/Re-render before sending. No render snapshot exists.";
      return null;
    }
    if (isStale()) {
      if (targetElement) targetElement.textContent = STALE_MESSAGE;
      setStatus(false, "Proof Blocked", STALE_MESSAGE);
      renderBadges();
      return null;
    }
    return currentRenderSnapshot;
  }

  async function renderTemplate() {
    renderButton.disabled = true;
    sendProofButton.disabled = true;
    proofResult.textContent = "";
    saveResult.textContent = "";
    renderProfileJson();
    var payload = formPayload();
    try {
      latestPreview = await postJson("/api/print/template-preview", payload);
      latestRenderSignature = currentRenderSignature();
      currentRenderSnapshot = {
        renderId: latestPreview.renderId,
        renderedAt: latestPreview.renderedAt,
        template: latestPreview.template,
        templatePath: latestPreview.templatePath,
        profileKey: latestPreview.profileKey,
        sampleData: clonePlain(latestPreview.sampleData || sampleData()),
        profileOverrides: clonePlain(latestPreview.profileOverrides || payload.profileOverrides || {}),
        fullProfileOverrides: clonePlain(latestPreview.fullProfileOverrides || latestPreview.profileOverrides || payload.profileOverrides || {}),
        renderedZpl: latestPreview.renderedZpl || "",
        dynamicTemplateZpl: latestPreview.dynamicTemplateZpl || "",
        renderedZplSha256: latestPreview.renderedZplSha256,
        dynamicTemplateSha256: latestPreview.dynamicTemplateSha256,
        payloadBytes: latestPreview.payloadBytes || latestPreview.metadata?.payloadBytes || (latestPreview.renderedZpl || "").length,
        metadata: clonePlain(latestPreview.metadata || {}),
        elementMap: clonePlain(latestPreview.elementMap || latestPreview.metadata?.elementMap || []),
        geometryMap: clonePlain(latestPreview.geometryMap || latestPreview.metadata?.geometryMap || [])
      };
      selectedObjectId = "";
      renderPreviewImage(latestPreview);
      renderInspector(null);
      renderBadges();
      setStatus(true, "Quick Edit Rendered", "Proof print will use the last rendered ZPL snapshot only.");
    } catch (error) {
      setStatus(false, "Render Failed", error.message);
      if (previewStatus) previewStatus.textContent = error.message;
    } finally {
      renderButton.disabled = false;
      sendProofButton.disabled = false;
    }
  }

  async function sendProofPrint() {
    var snapshot = snapshotOrBlock(proofResult);
    if (!snapshot) return;
    if (!confirmProof.checked) {
      proofResult.textContent = "Confirm proof print before sending rendered ZPL directly to the printer.";
      setStatus(false, "Proof Confirmation Required", "Quick Edit test sends require the confirmation checkbox.");
      return;
    }
    sendProofButton.disabled = true;
    try {
      var result = await postJson("/api/print/template-test-send", {
        confirmTestPrint: true,
        printerIp: printerIpInput.value,
        port: Number(printerPortInput.value || 9100),
        template: snapshot.template,
        profileKey: snapshot.profileKey,
        renderSnapshot: snapshot
      });
      proofResult.textContent = "Proof sent to " + result.printerIp + ":" + result.printerPort +
        " | renderId " + (result.renderId || snapshot.renderId || "-") +
        " | payload bytes " + (result.bytesSent || result.payloadBytes || snapshot.payloadBytes || "-");
      setStatus(true, "Proof Print Sent", "Sent exact last rendered ZPL snapshot directly to the selected printer.");
    } catch (error) {
      proofResult.textContent = error.message;
      setStatus(false, "Proof Print Failed", error.message);
    } finally {
      sendProofButton.disabled = false;
    }
  }

  async function saveProfile() {
    saveProfileButton.disabled = true;
    renderProfileJson();
    try {
      var result = await postJson("/api/print/template-lab/profile", {
        profileKey: profileSelect.value,
        template: templateSelect.value,
        templateName: templateSelect.value,
        overrides: collectProfileOverrides()
      });
      dirty = false;
      saveResult.textContent = "Saved " + result.profileKey + " to " + (result.savedPath || result.profileConfigPath || "-") +
        " at " + (result.savedAt || "-") + ". Preview/test only. Production unchanged.";
      setStatus(true, "Lab Profile Saved", "Profile settings only were saved. Production templates and queue were not changed.");
    } catch (error) {
      saveResult.textContent = error.message;
      setStatus(false, "Save Failed", error.message);
    } finally {
      saveProfileButton.disabled = false;
    }
  }

  function exportProfileJson(download) {
    var exported = renderProfileJson();
    if (!exported) return null;
    dirty = false;
    if (download !== false) {
      var blob = new Blob([JSON.stringify(exported, null, 2)], { type: "application/json" });
      var link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = exported.profileKey.replace(/[^A-Za-z0-9_.-]/g, "_") + ".quick-edit-profile.json";
      document.body.appendChild(link);
      link.click();
      link.remove();
      setTimeout(function () { URL.revokeObjectURL(link.href); }, 1000);
    }
    saveResult.textContent = "Profile JSON exported. Production unchanged.";
    return exported;
  }

  function applyHandoff(payload) {
    if (!payload || typeof payload !== "object") return;
    if (payload.template && hasOption(templateSelect, payload.template)) templateSelect.value = payload.template;
    if (payload.profileKey && hasOption(profileSelect, payload.profileKey)) profileSelect.value = cleanProfileKey(payload.profileKey);
    var sample = payload.sampleData || payload.renderedPayload || payload;
    Object.keys(SAMPLE_DEFAULTS).forEach(function (key) {
      var elementId = {
        lotNumber: "qeLotNumber",
        boxNumber: "qeBoxNumber",
        rfid: "qeRfid",
        pounds: "qePounds",
        materialType: "qeMaterialType",
        color: "qeColor",
        tolling: "qeTolling",
        po: "qePo",
        productDescription: "qeProductDescription"
      }[key];
      if (sample && sample[key] !== undefined) setValue(elementId, sample[key]);
    });
    if (payload.profileOverrides || payload.overrides) {
      profileOverrides = clonePlain(payload.profileOverrides || payload.overrides);
      profileOverrides.borderVisibility = profileOverrides.borderVisibility || {};
    }
    if (payload.printerIp) printerIpInput.value = payload.printerIp;
    if (payload.printerPort || payload.port) printerPortInput.value = String(payload.printerPort || payload.port);
  }

  function readStartupHandoff() {
    var query = new URLSearchParams(window.location.search);
    var handoff = null;
    try {
      handoff = JSON.parse(sessionStorage.getItem("templateQuickEditHandoff") || "null");
    } catch {
      handoff = null;
    }
    if (handoff) applyHandoff(handoff);
    if (!handoff) {
      try {
        var lastContext = JSON.parse(localStorage.getItem(LAST_CONTEXT_KEY) || "null");
        if (lastContext?.templateName && hasOption(templateSelect, lastContext.templateName)) templateSelect.value = lastContext.templateName;
        if (lastContext?.profileKey && hasOption(profileSelect, cleanProfileKey(lastContext.profileKey))) profileSelect.value = cleanProfileKey(lastContext.profileKey);
      } catch {
        // Last context is best-effort only.
      }
    }
    if (query.get("template") && hasOption(templateSelect, query.get("template"))) templateSelect.value = query.get("template");
    if (query.get("profileKey") && hasOption(profileSelect, cleanProfileKey(query.get("profileKey")))) profileSelect.value = cleanProfileKey(query.get("profileKey"));
    if (query.get("profile") && hasOption(profileSelect, cleanProfileKey(query.get("profile")))) profileSelect.value = cleanProfileKey(query.get("profile"));
  }

  function saveReturnHandoff() {
    var draft = saveBrowserDraft({ silent: true });
    try {
      sessionStorage.setItem("templateLabReturnHandoff", JSON.stringify({
        template: templateSelect.value,
        templateName: templateSelect.value,
        profileKey: profileSelect.value,
        sampleData: sampleData(),
        profileOverrides: collectProfileOverrides(),
        printerIp: printerIpInput.value,
        printerPort: printerPortInput.value,
        timestamp: draft?.timestamp || new Date().toISOString()
      }));
    } catch {
      // Best-effort handoff only.
    }
  }

  function showInFullLab() {
    saveReturnHandoff();
    window.location.href = "/offline/template-lab?handoff=quick-edit";
  }

  function requestExit(href) {
    pendingExitHref = href || "/offline/template-lab";
    if (!dirty) {
      saveReturnHandoff();
      window.location.href = pendingExitHref;
      return;
    }
    if (exitDialog && typeof exitDialog.showModal === "function") {
      exitDialog.showModal();
      return;
    }
    if (window.confirm("You have unsaved Quick Edit changes. Discard and exit?")) {
      saveReturnHandoff();
      window.location.href = pendingExitHref;
    }
  }

  function closeExitDialog() {
    if (exitDialog && typeof exitDialog.close === "function") exitDialog.close();
  }

  function reportSection(title, bodyHtml) {
    return "<section><h2>" + escapeHtml(title) + "</h2>" + bodyHtml + "</section>";
  }

  function reportRows(rows) {
    return "<dl>" + rows.map(function (row) {
      return "<dt>" + escapeHtml(row[0]) + "</dt><dd>" + escapeHtml(row[1] ?? "-") + "</dd>";
    }).join("") + "</dl>";
  }

  function jsonBlock(value) {
    return "<pre>" + escapeHtml(JSON.stringify(value || {}, null, 2)) + "</pre>";
  }

  function buildPrintReport() {
    if (!printReportBody) return;
    var snapshot = currentRenderSnapshot || {};
    var selected = previewObjectById(selectedObjectId) || {};
    var overrides = collectProfileOverrides();
    printReportBody.innerHTML = [
      reportSection("Summary", reportRows([
        ["Template", templateSelect.value || "-"],
        ["Profile", profileSelect.value || "-"],
        ["Printer target", (printerIpInput.value || "-") + ":" + (printerPortInput.value || "9100")],
        ["Timestamp", new Date().toISOString()],
        ["Selected object", selected.id || "-"],
        ["Render digest", snapshot.renderedZplSha256 || "-"],
        ["RenderId", snapshot.renderId || "-"],
        ["Stale", isStale() ? "yes" : "no"]
      ])),
      reportSection("Sample Data", jsonBlock(sampleData())),
      reportSection("Profile Override Values", jsonBlock(overrides)),
      reportSection("Border Visibility States", jsonBlock(overrides.borderVisibility || {})),
      reportSection("QR Settings", jsonBlock(overrides.qr || {})),
      reportSection("Logo Settings", jsonBlock(overrides.logo || {})),
      reportSection("Bottom Grid Settings", jsonBlock(overrides.bottomGrid || {})),
      reportSection("Render Metadata", jsonBlock({
        previewMode: snapshot.metadata?.previewMode,
        payloadBytes: snapshot.payloadBytes,
        dynamicTemplateSha256: snapshot.dynamicTemplateSha256,
        elementCount: (snapshot.geometryMap || []).length
      }))
    ].join("");
  }

  async function loadCatalog() {
    catalog = await fetchJson("/api/print/template-lab/catalog", { cache: "no-store" });
    fillTemplateSelector();
    fillProfileSelector();
    templateSelect.value = hasOption(templateSelect, DEFAULT_TEMPLATE) ? DEFAULT_TEMPLATE : templateSelect.options[0]?.value || DEFAULT_TEMPLATE;
    profileSelect.value = hasOption(profileSelect, DEFAULT_PROFILE) ? DEFAULT_PROFILE : profileSelect.options[0]?.value || DEFAULT_PROFILE;
    readStartupHandoff();
    if (!printerIpInput.value || !printerPortInput.value) applyPrinterDefaults();
    await loadProductionSource({ allowDraft: true });
    updateSampleSummary();
    renderProfileJson();
    renderBadges();
    setStatus(true, "Quick Edit Ready", "Render, click a preview object, then nudge or edit profile settings.");
  }

  function wireEvents() {
    renderButton.addEventListener("click", renderTemplate);
    sendProofButton.addEventListener("click", sendProofPrint);
    saveProfileButton.addEventListener("click", saveProfile);
    exportProfileButton.addEventListener("click", function () { exportProfileJson(true); });
    printReportButton.addEventListener("click", function () {
      buildPrintReport();
      window.print();
    });
    templateSelect.addEventListener("change", async function () {
      applyTemplateDefaultProfile();
      applyPrinterDefaults();
      updateSampleSummary();
      await loadProductionSource({ allowDraft: true });
      setDirty("Template changed. Render/Re-render before proof printing.");
    });
    profileSelect.addEventListener("change", async function () {
      applyPrinterDefaults();
      updateSampleSummary();
      await loadProductionSource({ allowDraft: true });
      setDirty("Profile changed. Render/Re-render before proof printing.");
    });
    [printerIpInput, printerPortInput].forEach(function (element) {
      element.addEventListener("input", renderBadges);
    });
    document.querySelectorAll("#qeSampleDrawer input").forEach(function (element) {
      element.addEventListener("input", function () {
        updateSampleSummary();
        setDirty("Sample data changed. Render/Re-render before proof printing.");
      });
    });
    if (zoomSelect) {
      zoomSelect.addEventListener("change", function () {
        persistZoomPreference();
        applyZoom();
        saveBrowserDraft({ silent: true });
      });
    }
    window.addEventListener("resize", applyZoom, { passive: true });
    if (gridToggle) gridToggle.addEventListener("change", setGridVisible);
    if (inspectorControls) {
      inspectorControls.addEventListener("input", function (event) {
        var control = event.target && event.target.getAttribute("data-qe-control");
        if (!control) return;
        setSelectedValue(control, event.target.type === "checkbox" ? event.target.checked : event.target.value);
      });
      inspectorControls.addEventListener("change", function (event) {
        var control = event.target && event.target.getAttribute("data-qe-control");
        if (!control) return;
        setSelectedValue(control, event.target.type === "checkbox" ? event.target.checked : event.target.value);
      });
    }
    if (nudgeControls) {
      nudgeControls.addEventListener("click", function (event) {
        var button = event.target.closest("[data-qe-nudge-axis]");
        if (!button) return;
        nudgeSelected(button.getAttribute("data-qe-nudge-axis"), Number(button.getAttribute("data-qe-nudge-delta")));
      });
    }
    document.addEventListener("keydown", function (event) {
      if (!selectedObjectId || !["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(event.key)) return;
      var active = document.activeElement;
      if (active && /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName)) return;
      var amount = event.shiftKey ? 5 : 1;
      if (event.key === "ArrowLeft") nudgeSelected("x", -amount);
      if (event.key === "ArrowRight") nudgeSelected("x", amount);
      if (event.key === "ArrowUp") nudgeSelected("y", -amount);
      if (event.key === "ArrowDown") nudgeSelected("y", amount);
      event.preventDefault();
    });
    resetSelectedButton.addEventListener("click", resetSelectedObject);
    showInFullLabButton.addEventListener("click", showInFullLab);
    if (saveBrowserDraftButton) saveBrowserDraftButton.addEventListener("click", function () { saveBrowserDraft(); });
    if (clearBrowserDraftButton) clearBrowserDraftButton.addEventListener("click", clearBrowserDraftAndReload);
    if (reloadProductionButton) reloadProductionButton.addEventListener("click", reloadProductionSource);
    backToLabButton.addEventListener("click", function (event) {
      event.preventDefault();
      requestExit(backToLabButton.href);
    });
    exitButton.addEventListener("click", function () { requestExit("/offline/template-lab"); });
    exitSaveButton.addEventListener("click", async function () {
      await saveProfile();
      closeExitDialog();
      saveReturnHandoff();
      window.location.href = pendingExitHref;
    });
    exitExportButton.addEventListener("click", function () {
      exportProfileJson(true);
      closeExitDialog();
    });
    exitDiscardButton.addEventListener("click", function () {
      dirty = false;
      closeExitDialog();
      saveReturnHandoff();
      window.location.href = pendingExitHref;
    });
    exitCancelButton.addEventListener("click", closeExitDialog);
    window.addEventListener("beforeprint", buildPrintReport);
  }

  wireEvents();
  restoreZoomPreference();
  setGridVisible();
  loadCatalog().then(renderTemplate).catch(function (error) {
    setStatus(false, "Quick Edit Unavailable", error.message);
  });
})();
