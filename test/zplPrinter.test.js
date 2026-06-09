const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const { EventEmitter } = require("events");
const os = require("os");
const path = require("path");

const {
  renderZplTemplate,
  renderZplTemplateFile,
  renderZplTemplateFileWithoutRfid,
  renderZplTemplateWithoutRfid,
  buildFittedZplFieldValues,
  fitZplBoxedText,
  rfidTextToHex,
  sanitizeVisibleFieldValue,
  sendZplOverTcp,
  validateRfidText
} = require("../lib/zplPrinter");

const VALID_RFID = "PT000086-B52";
const VALID_HEX = "50543030303038362D423532";
const REPO_ROOT = path.join(__dirname, "..");

function templateData(overrides = {}) {
  return {
    lotNumber: "PT000086",
    boxNumber: "52",
    rfid: VALID_RFID,
    pounds: "_",
    materialType: "PET",
    color: "BLACK",
    po: "PO12345",
    productCode: "FG001",
    productName: "Finished Pellet",
    productDescription: "Finished Pellet",
    tolling: "",
    erp: "TEST",
    ...overrides
  };
}

function assertTemplateHasLotQr(templatePath, rendered, lotNumber, expectedMagnification) {
  const source = fs.readFileSync(templatePath, "utf8");
  assert.match(source, new RegExp(`\\^BQN,2,${expectedMagnification}\\^FDLA,\\{\\{lotNumber\\}\\}\\^FS`));
  assert.equal(source.includes("^FDLA,{{lotNumber}}^FS"), true);
  assert.equal(rendered.includes(`^FDLA,${lotNumber}^FS`), true);
}

function assertTemplateCentersMaterialAndColor(templatePath) {
  const source = fs.readFileSync(templatePath, "utf8");
  assert.match(source, /\^FB\d+,1,0,C,0\^FD\{\{materialType(?:Small)?Text\}\}\^FS/);
  assert.match(source, /\^FB\d+,1,0,C,0\^FD\{\{color(?:Small)?Text\}\}\^FS/);
}

test("converts ASCII RFID text to uppercase HEX", () => {
  assert.equal(rfidTextToHex(VALID_RFID), VALID_HEX);
});

test("rejects RFID shorter than 12 characters", () => {
  assert.throws(() => validateRfidText("PT000086-B5"), /exactly 12 ASCII/);
});

test("rejects RFID longer than 12 characters", () => {
  assert.throws(() => validateRfidText("PT000086-B520"), /exactly 12 ASCII/);
});

test("rejects non-ASCII RFID text", () => {
  assert.throws(() => validateRfidText("PT000086-B5\u00e9"), /printable ASCII/);
});

test("replaces placeholders and computes RFID HEX", () => {
  const rendered = renderZplTemplate(
    "LOT={{lotNumber}} BOX={{boxNumber}} RFID={{rfid}} HEX={{rfidHex}}",
    {
      lotNumber: "PT000086",
      boxNumber: 52,
      rfid: VALID_RFID
    }
  );

  assert.equal(rendered, `LOT=PT000086 BOX=52 RFID=${VALID_RFID} HEX=${VALID_HEX}`);
});

test("rejects unreplaced template tokens", () => {
  assert.throws(
    () => renderZplTemplate("LOT={{lotNumber}} MISSING={{missing}}", { lotNumber: "PT000086", rfid: VALID_RFID }),
    /unreplaced tokens/i
  );
});

test("sanitizes visible field values that would break ZPL fields", () => {
  assert.equal(sanitizeVisibleFieldValue("A^B~C\r\nD"), "A B C D");

  const rendered = renderZplTemplate("TYPE={{materialType}}", {
    materialType: "A^B~C\r\nD",
    rfid: VALID_RFID
  });

  assert.equal(rendered, "TYPE=A B C D");
});

test("truncates long visible fields without changing valid RFID HEX", () => {
  const longDescription = "X".repeat(80);
  const rendered = renderZplTemplate("DESC={{productDescription}} HEX={{rfidHex}}", {
    productDescription: longDescription,
    rfid: VALID_RFID,
    rfidHex: VALID_HEX
  });

  assert.equal(rendered, `DESC=${"X".repeat(48)} HEX=${VALID_HEX}`);
});

test("loads and renders a template file", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rfid-zpl-test-"));
  const templatePath = path.join(tempDir, "label.template.zpl");

  try {
    fs.writeFileSync(templatePath, "RFID={{rfid}} HEX={{rfidHex}} COLOR={{color}}", "utf8");
    const rendered = renderZplTemplateFile(templatePath, {
      rfid: VALID_RFID,
      color: "BLACK"
    });

    assert.equal(rendered, `RFID=${VALID_RFID} HEX=${VALID_HEX} COLOR=BLACK`);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test("renders non-RFID templates without requiring RFID", () => {
  const rendered = renderZplTemplateWithoutRfid("LOT={{lotNumber}} MACHINE={{machine}}", {
    lotNumber: "FF123456",
    machine: "Extruder 1"
  });

  assert.equal(rendered, "LOT=FF123456 MACHINE=Extruder 1");
});

test("fits boxed color and material fields by shrinking and truncating without hyphenation", () => {
  const shortColor = fitZplBoxedText("BLK", {
    boxWidth: 118,
    maxChars: 8,
    large: { fontH: 44, fontW: 28 },
    medium: { fontH: 32, fontW: 18 },
    small: { fontH: 24, fontW: 12 },
    min: { fontH: 20, fontW: 10 }
  });
  const longColor = fitZplBoxedText("ULTRAMARINEBLUE", {
    boxWidth: 118,
    maxChars: 8,
    large: { fontH: 44, fontW: 28 },
    medium: { fontH: 32, fontW: 18 },
    small: { fontH: 24, fontW: 12 },
    min: { fontH: 20, fontW: 10 }
  });
  const rendered = renderZplTemplateWithoutRfid(
    "C={{colorText}}/{{colorFontH}}/{{colorFontW}} M={{materialTypeText}}/{{materialTypeFontH}}/{{materialTypeFontW}}",
    {
      color: "ULTRAMARINEBLUE",
      materialType: "POLYPROPYLENE"
    }
  );

  assert.equal(longColor.text, "ULTRAMAR");
  assert.equal(longColor.text.includes("-"), false);
  assert.equal(/\s/.test(longColor.text), false);
  assert.ok(longColor.fontH < shortColor.fontH);
  assert.ok(longColor.fontW < shortColor.fontW);
  assert.equal(rendered.includes("ULTRAMAR"), true);
  assert.equal(rendered.includes("POLYPROP"), true);
  assert.equal(rendered.includes("-"), false);
});

test("fitted boxed fields are sanitized and rendered as one line", () => {
  const fit = fitZplBoxedText("RED^BLUE~GREEN\r\nNEXT", {
    boxWidth: 118,
    maxChars: 8,
    large: { fontH: 44, fontW: 28 },
    medium: { fontH: 32, fontW: 18 },
    small: { fontH: 24, fontW: 12 },
    min: { fontH: 20, fontW: 10 }
  });
  const rendered = renderZplTemplateWithoutRfid("{{colorText}}", {
    color: "RED^BLUE~GREEN\r\nNEXT"
  });

  assert.equal(fit.text, "RED BLUE");
  assert.equal(rendered, "RED BLUE");
  assert.equal(/[\r\n\^~]/.test(rendered), false);
  assert.equal(rendered.includes("-"), false);
});

test("fitted field metadata exposes profile-friendly debug values", () => {
  const fitted = buildFittedZplFieldValues({
    color: "ULTRAMARINEBLUE",
    materialType: "POLYPROPYLENE",
    tolling: "TOLLING",
    productDescription: "Finished Pellet Product"
  }, {
    fieldFitDefinitions: {
      color: { boxWidth: 118, maxChars: 8 },
      productDescription: { boxWidth: 300, maxChars: 24, alignment: "L", maxLines: 1 }
    }
  });

  assert.equal(fitted.values.colorText, "ULTRAMAR");
  assert.equal(fitted.debug.color.truncated, true);
  assert.equal(fitted.debug.color.hyphenation, false);
  assert.equal(fitted.debug.color.oneLine, true);
  assert.equal(fitted.debug.color.boxW, 118);
  assert.equal(fitted.debug.materialType.fittedText.includes("-"), false);
  assert.equal(fitted.values.productDescriptionAlignment, "L");
  assert.equal(fitted.debug.productDescription.alignment, "left");
  assert.equal(fitted.debug.productDescription.boxW, 300);
});

test("conditional tolling blocks render only when tolling has a value", () => {
  const template = "^XA{{#if tolling}}^FO10,10^GB100,30,30^FS^FO10,10^A0N,20,20^FR^FD{{tollingText}}^FS{{/if}}^XZ";
  const blank = renderZplTemplateWithoutRfid(template, { tolling: "" });
  const visible = renderZplTemplateWithoutRfid(template, { tolling: "TOLLING" });

  assert.equal(blank.includes("^GB100,30,30"), false);
  assert.equal(blank.includes("^FR"), false);
  assert.equal(visible.includes("^GB100,30,30"), true);
  assert.equal(visible.includes("^FR"), true);
  assert.equal(/{{/.test(visible), false);
});

test("template-only deploy script references required templates and copies only ZPL templates", () => {
  const scriptPath = path.join(REPO_ROOT, "Deploy-ZPL-Templates.bat");
  const script = fs.readFileSync(scriptPath, "utf8");
  const requiredTemplates = [
    "RFID-RAW-P1.template.zpl",
    "RFID-FG-P1.template.zpl",
    "RFID-FG-P3.template.zpl",
    "QCSample-P3.template.zpl",
    "QCRetain-P3.template.zpl",
    "QCSamplePounds-P3.template.zpl"
  ];

  for (const template of requiredTemplates) {
    assert.equal(script.includes(template), true);
  }
  assert.equal(script.includes("*.template.zpl"), true);
  assert.equal(script.includes("*.prn"), false);
  assert.equal(script.includes("*.btw"), false);
  assert.equal(script.includes("Restart-Service -Name '%SERVICE_NAME%'"), true);
  assert.equal(script.includes("curl.exe http://localhost:7079/health"), true);
  assert.equal(script.includes("curl.exe http://localhost:7079/api/print/zpl-queue"), true);
});

test("RAW and FG repo templates render without unreplaced tokens", () => {
  const rawTemplate = path.join(REPO_ROOT, "zpl", "RFID-RAW-P1.template.zpl");
  const fgTemplates = [
    path.join(REPO_ROOT, "zpl", "RFID-FG-P1.template.zpl"),
    path.join(REPO_ROOT, "zpl", "RFID-FG-P3.template.zpl")
  ];

  const raw = renderZplTemplateFile(rawTemplate, templateData());
  assert.equal(/{{\s*[A-Za-z][A-Za-z0-9_]*\s*}}/.test(raw), false);
  assertTemplateHasLotQr(rawTemplate, raw, "PT000086", 6);
  assertTemplateCentersMaterialAndColor(rawTemplate);
  assert.equal(raw.includes("^GFA,"), true);
  assert.match(raw, /\^FB430,1,0,L,0\^FD/);
  assert.equal(raw.includes("^GB340,73,73"), false);
  assert.equal(raw.includes("~DGR:"), false);
  assert.equal(raw.includes("^XGR:"), false);
  assert.match(raw, /\^RFW,H,1,2,1\^FD3400\^FS/);
  assert.match(raw, /\^RFW,H,2,12,1\^FD50543030303038362D423532\^FS/);

  for (const fgTemplate of fgTemplates) {
    const fg = renderZplTemplateFile(fgTemplate, templateData());
    assert.equal(/{{\s*[A-Za-z][A-Za-z0-9_]*\s*}}/.test(fg), false);
    assertTemplateHasLotQr(fgTemplate, fg, "PT000086", 6);
    assertTemplateCentersMaterialAndColor(fgTemplate);
    assert.equal(fg.includes("^GFA,"), true);
    assert.match(fg, /\^FB430,1,0,L,0\^FD/);
    assert.equal(fg.includes("^GB195,49,49"), false);
    assert.equal(fg.includes("~DGR:"), false);
    assert.equal(fg.includes("^XGR:"), false);
    assert.match(fg, /\^RFW,H,1,2,1\^FD3400\^FS/);
    assert.match(fg, /\^RFW,H,2,12,1\^FD50543030303038362D423532\^FS/);
  }

  const rawWithTolling = renderZplTemplateFile(rawTemplate, templateData({ tolling: "TOLLING" }));
  assert.equal(rawWithTolling.includes("^GB340,73,73"), true);
});

test("P3 sample repo templates render without unreplaced tokens and do not encode RFID", () => {
  const templates = [
    path.join(REPO_ROOT, "zpl", "QCSample-P3.template.zpl"),
    path.join(REPO_ROOT, "zpl", "QCRetain-P3.template.zpl"),
    path.join(REPO_ROOT, "zpl", "QCSamplePounds-P3.template.zpl")
  ];
  const data = {
    ...templateData({
      lotNumber: "FF123456",
      boxNumber: "12",
      materialType: "PP",
      color: "BLK",
      productDescription: "Extrusion Sample",
      tolling: "TOLLING"
    }),
    machine: "Extruder 1",
    printedDate: "5/29/2026",
    frequencyCheck: "5000"
  };

  for (const template of templates) {
    const rendered = renderZplTemplateFileWithoutRfid(template, data);
    assert.equal(/{{\s*[A-Za-z][A-Za-z0-9_]*\s*}}/.test(rendered), false);
    assertTemplateHasLotQr(template, rendered, "FF123456", 5);
    assertTemplateCentersMaterialAndColor(template);
    assert.equal(rendered.includes("^RFW,"), false);
    assert.equal(rendered.includes("^GFA,"), false);
    assert.equal(rendered.includes("~DGR:"), false);
    assert.equal(rendered.includes("^XGR:"), false);
  }
});

test("FG templates reject missing data and invalid RFID", () => {
  const fgTemplates = [
    path.join(REPO_ROOT, "zpl", "RFID-FG-P1.template.zpl"),
    path.join(REPO_ROOT, "zpl", "RFID-FG-P3.template.zpl")
  ];

  for (const fgTemplate of fgTemplates) {
    assert.throws(
      () => renderZplTemplateFile(fgTemplate, { rfid: VALID_RFID }),
      /unreplaced tokens/i
    );

    assert.throws(
      () => renderZplTemplateFile(fgTemplate, templateData({ rfid: "BAD" })),
      /exactly 12 ASCII/
    );
  }
});

test("TCP sender waits for socket end before reporting success", async () => {
  class FakeSocket extends EventEmitter {
    setTimeout() {}
    connect(_port, _host, callback) {
      setImmediate(callback);
    }
    write(_payload, _encoding, callback) {
      setImmediate(callback);
    }
    end(callback) {
      this.endCallback = callback;
    }
    destroy() {}
  }

  const fakeSocket = new FakeSocket();
  let settled = false;
  const sendPromise = sendZplOverTcp({
    printerIp: "127.0.0.1",
    port: 9100,
    zpl: "^XA^XZ",
    socketFactory: () => fakeSocket
  }).then((result) => {
    settled = true;
    return result;
  });

  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(settled, false);

  fakeSocket.endCallback();
  const result = await sendPromise;

  assert.equal(settled, true);
  assert.equal(result.bytesSent, Buffer.byteLength("^XA^XZ", "utf8"));
  assert.equal(result.endCompleted, true);
});
