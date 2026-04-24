const normalizeText = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const aliasMap = {
  tunisia: ["tunisia", "tunisian", "tunisie", "tn"],
  tunisian: ["tunisia", "tunisian", "tunisie", "tn"],
  tunisie: ["tunisia", "tunisian", "tunisie", "tn"],
  tn: ["tunisia", "tunisian", "tunisie", "tn"],
  morocco: ["morocco", "moroccan"],
  moroccan: ["morocco", "moroccan"],
  maroc: ["morocco", "moroccan", "ma"],
  ma: ["morocco", "moroccan", "maroc"],
  france: ["france", "french"],
  french: ["france", "french"],
  fr: ["france", "french"],
  brazil: ["brazil", "brazilian"],
  brazilian: ["brazil", "brazilian"],
  br: ["brazil", "brazilian"],
  argentina: ["argentina", "argentinian", "argentine"],
  argentinian: ["argentina", "argentinian", "argentine"],
  argentine: ["argentina", "argentinian", "argentine"],
  ar: ["argentina", "argentinian", "argentine"],
  england: ["england", "english"],
  english: ["england", "english"],
  gb: ["england", "english"],
  spain: ["spain", "spanish"],
  spanish: ["spain", "spanish"],
  es: ["spain", "spanish"],
  italy: ["italy", "italian"],
  italian: ["italy", "italian"],
  it: ["italy", "italian", "italia"],
  italia: ["italy", "italian", "italia"],
  germany: ["germany", "german"],
  german: ["germany", "german"],
  de: ["germany", "german"],
  portugal: ["portugal", "portuguese"],
  portuguese: ["portugal", "portuguese"],
  pt: ["portugal", "portuguese"],
  belgium: ["belgium", "belgian"],
  belgian: ["belgium", "belgian"],
  be: ["belgium", "belgian"],
  netherlands: ["netherlands", "dutch", "holland"],
  dutch: ["netherlands", "dutch", "holland"],
  holland: ["netherlands", "dutch", "holland"],
  nl: ["netherlands", "dutch", "holland"]
};

const countryCodeMap = {
  tunisia: "tn",
  tn: "tn",
  tunisie: "tn",
  morocco: "ma",
  maroc: "ma",
  ma: "ma",
  france: "fr",
  fr: "fr",
  england: "gb",
  gb: "gb",
  spain: "es",
  es: "es",
  italy: "it",
  it: "it",
  italia: "it",
  germany: "de",
  de: "de",
  netherlands: "nl",
  nl: "nl",
  portugal: "pt",
  pt: "pt",
  belgium: "be",
  be: "be",
  argentina: "ar",
  ar: "ar",
  brazil: "br",
  br: "br"
};

const countryDisplayMap = {
  tunisian: "Tunisia",
  tunisia: "Tunisia",
  tunisie: "Tunisia",
  tn: "Tunisia",
  moroccan: "Morocco",
  morocco: "Morocco",
  maroc: "Morocco",
  ma: "Morocco",
  french: "France",
  france: "France",
  fr: "France",
  english: "England",
  england: "England",
  gb: "England",
  spanish: "Spain",
  spain: "Spain",
  es: "Spain",
  italian: "Italy",
  italy: "Italy",
  italia: "Italy",
  it: "Italy",
  german: "Germany",
  germany: "Germany",
  de: "Germany",
  dutch: "Netherlands",
  holland: "Netherlands",
  netherlands: "Netherlands",
  nl: "Netherlands",
  portuguese: "Portugal",
  portugal: "Portugal",
  pt: "Portugal",
  belgian: "Belgium",
  belgium: "Belgium",
  be: "Belgium",
  argentine: "Argentina",
  argentinian: "Argentina",
  argentina: "Argentina",
  ar: "Argentina",
  brazilian: "Brazil",
  brazil: "Brazil",
  br: "Brazil"
};

const normalizeCountry = (country) => {
  const raw = String(country || "").trim();
  const key = normalizeText(raw);
  if (!key) {
    return "";
  }
  if (countryDisplayMap[key]) {
    return countryDisplayMap[key];
  }

  const tokens = raw
    .split(/[\/,;|&]+/)
    .map((part) => normalizeText(part))
    .filter(Boolean);

  for (const token of tokens) {
    if (countryDisplayMap[token]) {
      return countryDisplayMap[token];
    }
  }

  const words = key.split(/\s+/).filter(Boolean);
  for (const word of words) {
    if (countryDisplayMap[word]) {
      return countryDisplayMap[word];
    }
  }

  return raw;
};

const getCountryAliases = (country) => {
  const key = normalizeText(country);
  if (!key) {
    return [];
  }
  return [...new Set([key, ...(aliasMap[key] || [])])];
};

const getCountryCode = (country) => {
  const key = normalizeText(normalizeCountry(country));
  return countryCodeMap[key] || "";
};

const getCountryFlag = (country) => {
  const code = getCountryCode(country);
  return code ? `https://flagcdn.com/w320/${code}.png` : "";
};

module.exports = {
  normalizeText,
  normalizeCountry,
  getCountryAliases,
  getCountryCode,
  getCountryFlag
};
