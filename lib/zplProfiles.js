const RAW_STATIONS = Object.freeze(["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]);
const FG_STATIONS = Object.freeze(["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]);
const QC_STATIONS = Object.freeze(["P3", "P8"]);

function rawTemplateForStation(station) {
  return `RFID-RAW-${station}.template.zpl`;
}

function fgTemplateForStation(station) {
  return `RFID-FG-${station}.template.zpl`;
}

function qcSampleTemplateForStation(station) {
  return `QCSample-${station}.template.zpl`;
}

function qcRetainTemplateForStation(station) {
  return `QCRetain-${station}.template.zpl`;
}

function qcSamplePoundsTemplateForStation(station) {
  return `QCSamplePounds-${station}.template.zpl`;
}

const TEMPLATE_DEFINITIONS = Object.freeze([
  ...RAW_STATIONS.map((station) => Object.freeze({
    name: rawTemplateForStation(station),
    label: `RFID RAW ${station}`,
    family: "RAW",
    station,
    requiresRfid: true,
    logoMode: "static logo",
    defaultProfileKey: `${station}:RAW`
  })),
  ...FG_STATIONS.map((station) => Object.freeze({
    name: fgTemplateForStation(station),
    label: `RFID FG ${station}`,
    family: "FG",
    station,
    requiresRfid: true,
    logoMode: "static logo",
    defaultProfileKey: `${station}:FG`
  })),
  ...QC_STATIONS.flatMap((station) => [
    Object.freeze({
      name: qcSampleTemplateForStation(station),
      label: `${station} QC Sample`,
      family: "SAMPLE",
      station,
      requiresRfid: false,
      logoMode: "none",
      defaultProfileKey: `${station}:SAMPLE`
    }),
    Object.freeze({
      name: qcRetainTemplateForStation(station),
      label: `${station} QC Retain`,
      family: "RETAIN",
      station,
      requiresRfid: false,
      logoMode: "none",
      defaultProfileKey: `${station}:RETAIN`
    }),
    Object.freeze({
      name: qcSamplePoundsTemplateForStation(station),
      label: `${station} QC Sample Pounds`,
      family: "SAMPLE_POUNDS",
      station,
      requiresRfid: false,
      logoMode: "none",
      defaultProfileKey: `${station}:SAMPLE_POUNDS`
    })
  ])
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
      template: rawTemplateForStation(station),
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
      template: fgTemplateForStation(station),
      fieldFitProfile: station === "P3" ? "fgP3" : "fgP1",
      qrMagnification: 6,
      logo: { mode: "static logo", x: 820, y: 28, widthDots: 96, heightDots: 32 }
    })
  ])),
  ...Object.fromEntries(QC_STATIONS.flatMap((station) => [
    [`${station}:SAMPLE`, baseProfile({
      station,
      family: "SAMPLE",
      template: qcSampleTemplateForStation(station),
      fieldFitProfile: "qcP3",
      qrMagnification: 5,
      logo: { mode: "none" }
    })],
    [`${station}:RETAIN`, baseProfile({
      station,
      family: "RETAIN",
      template: qcRetainTemplateForStation(station),
      fieldFitProfile: "qcRetainP3",
      qrMagnification: 5,
      logo: { mode: "none" }
    })],
    [`${station}:SAMPLE_POUNDS`, baseProfile({
      station,
      family: "SAMPLE_POUNDS",
      template: qcSamplePoundsTemplateForStation(station),
      fieldFitProfile: "qcP3",
      qrMagnification: 5,
      logo: { mode: "none" }
    })]
  ]))
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
  RAW_STATIONS,
  FG_STATIONS,
  QC_STATIONS,
  rawTemplateForStation,
  fgTemplateForStation,
  qcSampleTemplateForStation,
  qcRetainTemplateForStation,
  qcSamplePoundsTemplateForStation,
  listTemplateLabTemplates,
  listStationProfiles,
  getTemplateDefinition,
  getStationProfile
};
