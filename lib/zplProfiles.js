const RAW_TEMPLATE = "RFID-RAW-P1.template.zpl";
const FG_P1_TEMPLATE = "RFID-FG-P1.template.zpl";
const FG_P3_TEMPLATE = "RFID-FG-P3.template.zpl";
const QC_SAMPLE_TEMPLATE = "QCSample-P3.template.zpl";
const QC_RETAIN_TEMPLATE = "QCRetain-P3.template.zpl";
const QC_SAMPLE_POUNDS_TEMPLATE = "QCSamplePounds-P3.template.zpl";

const TEMPLATE_DEFINITIONS = Object.freeze([
  Object.freeze({ name: RAW_TEMPLATE, label: "RFID RAW P1", family: "RAW", requiresRfid: true, logoMode: "static logo", defaultProfileKey: "P1:RAW" }),
  Object.freeze({ name: FG_P1_TEMPLATE, label: "RFID FG P1", family: "FG", requiresRfid: true, logoMode: "static logo", defaultProfileKey: "P1:FG" }),
  Object.freeze({ name: FG_P3_TEMPLATE, label: "RFID FG P3", family: "FG", requiresRfid: true, logoMode: "static logo", defaultProfileKey: "P3:FG" }),
  Object.freeze({ name: QC_SAMPLE_TEMPLATE, label: "P3 QC Sample", family: "SAMPLE", requiresRfid: false, logoMode: "none", defaultProfileKey: "P3:SAMPLE" }),
  Object.freeze({ name: QC_RETAIN_TEMPLATE, label: "P3 QC Retain", family: "RETAIN", requiresRfid: false, logoMode: "none", defaultProfileKey: "P3:RETAIN" }),
  Object.freeze({ name: QC_SAMPLE_POUNDS_TEMPLATE, label: "P3 QC Sample Pounds", family: "SAMPLE_POUNDS", requiresRfid: false, logoMode: "none", defaultProfileKey: "P3:SAMPLE_POUNDS" })
]);

const FIELD_FIT_PROFILES = Object.freeze({
  rawP1: Object.freeze({
    color: Object.freeze({ boxWidth: 189, maxChars: 8 }),
    materialType: Object.freeze({ boxWidth: 738, maxChars: 8 }),
    tolling: Object.freeze({ boxWidth: 340, maxChars: 8 }),
    productDescription: Object.freeze({ boxWidth: 700, maxChars: 42, maxLines: 1, alignment: "L" })
  }),
  fgP1: Object.freeze({
    color: Object.freeze({ boxWidth: 449, maxChars: 8 }),
    colorSmall: Object.freeze({ boxWidth: 124, maxChars: 8 }),
    materialType: Object.freeze({ boxWidth: 764, maxChars: 8 }),
    materialTypeSmall: Object.freeze({ boxWidth: 126, maxChars: 8 }),
    tolling: Object.freeze({ boxWidth: 195, maxChars: 8 }),
    productDescription: Object.freeze({ boxWidth: 430, maxChars: 32, maxLines: 1, alignment: "L" })
  }),
  fgP3: Object.freeze({
    color: Object.freeze({ boxWidth: 449, maxChars: 8 }),
    colorSmall: Object.freeze({ boxWidth: 128, maxChars: 8 }),
    materialType: Object.freeze({ boxWidth: 766, maxChars: 8 }),
    materialTypeSmall: Object.freeze({ boxWidth: 128, maxChars: 8 }),
    tolling: Object.freeze({ boxWidth: 195, maxChars: 8 }),
    productDescription: Object.freeze({ boxWidth: 430, maxChars: 32, maxLines: 1, alignment: "L" })
  }),
  qcP3: Object.freeze({
    colorSmall: Object.freeze({ boxWidth: 118, maxChars: 8 }),
    materialTypeSmall: Object.freeze({ boxWidth: 118, maxChars: 8 }),
    tolling: Object.freeze({ boxWidth: 138, maxChars: 8 }),
    productDescription: Object.freeze({ boxWidth: 270, maxChars: 28, maxLines: 1, alignment: "L" })
  }),
  qcRetainP3: Object.freeze({
    colorSmall: Object.freeze({ boxWidth: 94, maxChars: 8 }),
    materialTypeSmall: Object.freeze({ boxWidth: 93, maxChars: 8 }),
    tolling: Object.freeze({ boxWidth: 212, maxChars: 8 }),
    productDescription: Object.freeze({ boxWidth: 340, maxChars: 32, maxLines: 1, alignment: "L" })
  })
});

const RAW_STATIONS = Object.freeze(["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]);
const FG_STATIONS = Object.freeze(["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]);

function baseProfile({ station, family, template, fieldFitProfile, labelWidthDots = 812, labelHeightDots = 1218, qrMagnification = 6, logo = null }) {
  return Object.freeze({
    key: `${station}:${family}`,
    station,
    family,
    template,
    labelWidthDots,
    labelHeightDots,
    dpi: 203,
    scaleX: 1,
    scaleY: 1,
    offsetX: 0,
    offsetY: 0,
    applyTransformsToProduction: false,
    fieldFitProfile,
    fieldFitDefinitions: FIELD_FIT_PROFILES[fieldFitProfile] || Object.freeze({}),
    qr: Object.freeze({
      payload: "lotNumber",
      magnification: qrMagnification
    }),
    logo: Object.freeze(logo || { mode: "none" })
  });
}

const STATION_PROFILES = Object.freeze({
  ...Object.fromEntries(RAW_STATIONS.map((station) => [
    `${station}:RAW`,
    baseProfile({
      station,
      family: "RAW",
      template: RAW_TEMPLATE,
      fieldFitProfile: "rawP1",
      qrMagnification: 6,
      logo: { mode: "static logo", x: 612, y: 32, widthDots: 96, heightDots: 32 }
    })
  ])),
  ...Object.fromEntries(FG_STATIONS.map((station) => [
    `${station}:FG`,
    baseProfile({
      station,
      family: "FG",
      template: station === "P3" ? FG_P3_TEMPLATE : FG_P1_TEMPLATE,
      fieldFitProfile: station === "P3" ? "fgP3" : "fgP1",
      qrMagnification: 6,
      logo: { mode: "static logo", x: 820, y: 28, widthDots: 96, heightDots: 32 }
    })
  ])),
  "P3:SAMPLE": baseProfile({
    station: "P3",
    family: "SAMPLE",
    template: QC_SAMPLE_TEMPLATE,
    fieldFitProfile: "qcP3",
    qrMagnification: 5,
    logo: { mode: "none" }
  }),
  "P3:RETAIN": baseProfile({
    station: "P3",
    family: "RETAIN",
    template: QC_RETAIN_TEMPLATE,
    fieldFitProfile: "qcRetainP3",
    qrMagnification: 5,
    logo: { mode: "none" }
  }),
  "P3:SAMPLE_POUNDS": baseProfile({
    station: "P3",
    family: "SAMPLE_POUNDS",
    template: QC_SAMPLE_POUNDS_TEMPLATE,
    fieldFitProfile: "qcP3",
    qrMagnification: 5,
    logo: { mode: "none" }
  })
});

function listTemplateLabTemplates() {
  return TEMPLATE_DEFINITIONS.map((definition) => ({ ...definition }));
}

function listStationProfiles() {
  return Object.values(STATION_PROFILES).map((profile) => ({
    ...profile,
    qr: { ...profile.qr },
    logo: { ...profile.logo },
    fieldFitDefinitions: { ...profile.fieldFitDefinitions }
  }));
}

function getTemplateDefinition(templateName) {
  const name = String(templateName || "").trim();
  return TEMPLATE_DEFINITIONS.find((definition) => definition.name === name) || null;
}

function getStationProfile(profileKey) {
  const key = String(profileKey || "").trim().toUpperCase();
  const profile = STATION_PROFILES[key];
  if (!profile) return null;
  return {
    ...profile,
    qr: { ...profile.qr },
    logo: { ...profile.logo },
    fieldFitDefinitions: { ...profile.fieldFitDefinitions }
  };
}

module.exports = {
  FIELD_FIT_PROFILES,
  TEMPLATE_DEFINITIONS,
  STATION_PROFILES,
  listTemplateLabTemplates,
  listStationProfiles,
  getTemplateDefinition,
  getStationProfile
};
