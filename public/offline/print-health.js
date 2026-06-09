(function () {
  var refreshButton = document.getElementById("refreshButton");
  var autoRefresh = document.getElementById("autoRefresh");
  var refreshInterval = document.getElementById("refreshInterval");
  var logFilterForm = document.getElementById("logFilterForm");
  var headerStatus = document.getElementById("headerStatus");
  var statusBanner = document.getElementById("statusBanner");
  var statusTitle = document.getElementById("statusTitle");
  var statusMeta = document.getElementById("statusMeta");
  var lastRefresh = document.getElementById("lastRefresh");
  var summaryGrid = document.getElementById("summaryGrid");
  var enabledScopes = document.getElementById("enabledScopes");
  var pausedPrinters = document.getElementById("pausedPrinters");
  var printerCards = document.getElementById("printerCards");
  var batchHealth = document.getElementById("batchHealth");
  var reviewItems = document.getElementById("reviewItems");
  var recentErrors = document.getElementById("recentErrors");
  var logMeta = document.getElementById("logMeta");
  var logOutput = document.getElementById("logOutput");
  var timer = null;

  function valueOrDash(value) {
    if (value === undefined || value === null || value === "") return "-";
    return String(value);
  }

  function formatDate(value) {
    if (!value) return "-";
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  }

  function setBanner(ok, title, message) {
    headerStatus.textContent = ok ? "Status: Healthy" : "Status: Review";
    headerStatus.className = ok ? "status-pill status-pill-enabled" : "status-pill status-pill-disabled";
    statusBanner.className = ok ? "status-banner status-enabled" : "status-banner status-disabled";
    statusTitle.textContent = title;
    statusMeta.textContent = message;
  }

  function fetchJson(url) {
    return fetch(url, { cache: "no-store" }).then(async function (response) {
      var text = await response.text();
      var json = {};
      try {
        json = text ? JSON.parse(text) : {};
      } catch {
        json = { ok: false, raw: text };
      }
      if (!response.ok) {
        throw new Error(json.message || "Request failed with HTTP " + response.status);
      }
      return json;
    });
  }

  function renderSummary(health, queue) {
    var rows = [
      ["Print Engine", health.printEngine],
      ["Socket Mode", health.zplSocketMode || queue.zplSocketMode],
      ["Duplicate Policy", health.zplDuplicatePolicy],
      ["Queue Path", health.zplQueuePath],
      ["Label Spacing", health.zplLabelSpacingMs + " ms"],
      ["TCP Timeout", health.zplTcpTimeoutMs + " ms"],
      ["Batch Max Labels", health.zplBatchMaxLabels],
      ["Batch Collect", health.zplBatchCollectMs + " ms"],
      ["Batch Delay", health.zplBatchInterBatchDelayMs + " ms"],
      ["Batch Max Bytes", health.zplBatchMaxBytes]
    ];

    summaryGrid.innerHTML = rows.map(function (row) {
      return "<div><span class=\"label-text\">" + row[0] + "</span><strong>" + valueOrDash(row[1]) + "</strong></div>";
    }).join("");

    var scopes = health.directZplEnabledScopes || queue.directZplEnabledScopes || [];
    enabledScopes.innerHTML = scopes.length
      ? scopes.map(function (scope) {
        return "<span class=\"scope-chip\">" + valueOrDash(scope.station) + ":" + valueOrDash(scope.family) + "</span>";
      }).join("")
      : "<span class=\"muted-line\">No direct-ZPL scopes reported.</span>";
  }

  function statusSummary(counts) {
    var names = ["queued", "sending", "sent_to_printer", "unknown_after_send", "failed_before_send", "rejected"];
    return names.map(function (name) {
      return "<span><b>" + name + "</b> " + Number(counts && counts[name] || 0) + "</span>";
    }).join("");
  }

  function renderPrinterQueues(queue) {
    var printers = Object.values(queue.printers || {}).sort(function (a, b) {
      return String(a.printerKey).localeCompare(String(b.printerKey));
    });

    pausedPrinters.innerHTML = (queue.pausedPrinterKeys || []).length
      ? "<div class=\"warning-item\"><strong>Paused printers:</strong> " + queue.pausedPrinterKeys.join(", ") + "</div>"
      : "";

    printerCards.innerHTML = printers.length ? printers.map(function (printer) {
      var active = printer.activeItem
        ? "Box " + valueOrDash(printer.activeItem.box) + " / " + valueOrDash(printer.activeItem.lotNumber)
        : "-";
      var activeBatch = printer.activeBatch
        ? "Batch " + valueOrDash(printer.activeBatch.batchId) + ", boxes " + (printer.activeBatch.boxes || []).join(",")
        : "-";
      var lastError = printer.lastError ? valueOrDash(printer.lastError.code || printer.lastError.message) : "-";
      return [
        "<article class=\"printer-card\">",
        "<h3>" + valueOrDash(printer.printerKey) + "</h3>",
        "<div class=\"printer-phase printer-phase-" + valueOrDash(printer.phase).replace(/[^a-z0-9_-]/gi, "") + "\">" + valueOrDash(printer.phase) + "</div>",
        "<div class=\"queue-counts\">" + statusSummary(printer.counts) + "</div>",
        "<dl class=\"health-dl\">",
        "<div><dt>Queue depth</dt><dd>" + valueOrDash(printer.queueDepth) + "</dd></div>",
        "<div><dt>Socket mode</dt><dd>" + valueOrDash(printer.socketMode) + "</dd></div>",
        "<div><dt>Active item</dt><dd>" + active + "</dd></div>",
        "<div><dt>Active batch</dt><dd>" + activeBatch + "</dd></div>",
        "<div><dt>Last batch ms</dt><dd>" + valueOrDash(printer.lastBatchDurationMs) + "</dd></div>",
        "<div><dt>Last error</dt><dd>" + lastError + "</dd></div>",
        "</dl>",
        "</article>"
      ].join("");
    }).join("") : "<p class=\"muted-line\">No direct-ZPL queue records found.</p>";
  }

  function flattenPrinterItems(queue, property) {
    return Object.values(queue.printers || {}).flatMap(function (printer) {
      return (printer[property] || []).map(function (item) {
        return {
          printerKey: printer.printerKey,
          item: item
        };
      });
    });
  }

  function renderBatchAndReview(queue, logs) {
    var activeBatches = Object.values(queue.printers || {}).filter(function (printer) {
      return printer.activeBatch;
    });
    var rows = activeBatches.map(function (printer) {
      var batch = printer.activeBatch;
      return "<div class=\"compact-item\"><strong>" + valueOrDash(printer.printerKey) + "</strong> " +
        valueOrDash(batch.batchLabelCount) + " labels, boxes " + (batch.boxes || []).join(",") +
        " started " + formatDate(batch.startedAt) + "</div>";
    });
    (logs.lines || []).filter(function (entry) {
      return entry.event && entry.event.indexOf("zpl_batch_") === 0;
    }).slice(0, 8).forEach(function (entry) {
      var record = entry.record || {};
      rows.push("<div class=\"compact-item\"><strong>" + valueOrDash(record.event) + "</strong> " +
        valueOrDash(record.printerKey) + " boxes " + ((record.boxes || []).join(",") || "-") +
        " at " + formatDate(record.timestamp) + "</div>");
    });
    batchHealth.innerHTML = rows.length ? rows.join("") : "<p class=\"muted-line\">No active or recent batch events reported.</p>";

    var review = flattenPrinterItems(queue, "reviewRequiredItems");
    var failed = flattenPrinterItems(queue, "safeRetryItems");
    var rows = [];
    review.forEach(function (entry) {
      rows.push("<div class=\"warning-item\"><strong>Unknown after send:</strong> " + valueOrDash(entry.printerKey) +
        " lot " + valueOrDash(entry.item.lotNumber) + " box " + valueOrDash(entry.item.box) +
        " RFID " + valueOrDash(entry.item.rfid) + "</div>");
    });
    failed.forEach(function (entry) {
      rows.push("<div class=\"compact-item\"><strong>Safe retry:</strong> " + valueOrDash(entry.printerKey) +
        " lot " + valueOrDash(entry.item.lotNumber) + " box " + valueOrDash(entry.item.box) + "</div>");
    });
    reviewItems.innerHTML = rows.length ? rows.join("") : "<p class=\"muted-line\">No unknown-after-send or safe-retry items reported.</p>";
  }

  function buildLogQuery() {
    var params = new URLSearchParams();
    Array.from(new FormData(logFilterForm).entries()).forEach(function (entry) {
      if (String(entry[1] || "").trim()) params.set(entry[0], String(entry[1]).trim());
    });
    return params.toString();
  }

  function renderLogs(logs) {
    var lines = logs.lines || [];
    logMeta.textContent = "Showing " + lines.length + " line(s) from tail " + logs.tail + ".";
    recentErrors.innerHTML = lines.filter(function (entry) {
      return entry.level === "error" || entry.level === "warn";
    }).slice(0, 8).map(function (entry) {
      return "<div class=\"warning-item\"><strong>" + valueOrDash(entry.level) + " / " + valueOrDash(entry.event) +
        "</strong> " + valueOrDash(entry.record && entry.record.message || entry.raw || entry.record && entry.record.code) + "</div>";
    }).join("");

    logOutput.textContent = JSON.stringify(lines.map(function (entry) {
      if (entry.parsed) return entry.record;
      return { raw: entry.raw };
    }), null, 2);
  }

  async function refreshDashboard() {
    refreshButton.disabled = true;
    try {
      var logQuery = buildLogQuery();
      var results = await Promise.all([
        fetchJson("/health"),
        fetchJson("/api/print/zpl-queue"),
        fetchJson("/api/print/logs" + (logQuery ? "?" + logQuery : ""))
      ]);
      var health = results[0];
      var queue = results[1];
      var logs = results[2];
      var paused = (queue.pausedPrinterKeys || []).length;
      var errors = (logs.lines || []).filter(function (entry) {
        return entry.level === "error";
      }).length;
      var ok = health.ok !== false && paused === 0 && errors === 0;

      renderSummary(health, queue);
      renderPrinterQueues(queue);
      renderBatchAndReview(queue, logs);
      renderLogs(logs);
      setBanner(ok, ok ? "Print Health OK" : "Print Health Needs Review", paused + " paused printer(s), " + errors + " recent error log(s).");
      lastRefresh.textContent = "Last refreshed " + new Date().toLocaleString();
    } catch (error) {
      setBanner(false, "Print Health Unavailable", error.message);
      logOutput.textContent = JSON.stringify({ ok: false, message: error.message }, null, 2);
    } finally {
      refreshButton.disabled = false;
    }
  }

  function resetAutoRefresh() {
    if (timer) clearInterval(timer);
    timer = null;
    if (autoRefresh.checked) {
      timer = setInterval(refreshDashboard, Number(refreshInterval.value || 30000));
    }
  }

  refreshButton.addEventListener("click", refreshDashboard);
  autoRefresh.addEventListener("change", resetAutoRefresh);
  refreshInterval.addEventListener("change", resetAutoRefresh);
  logFilterForm.addEventListener("submit", function (event) {
    event.preventDefault();
    refreshDashboard();
  });

  refreshDashboard();
})();
