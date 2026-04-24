const countryMap = {
  tunisia: ["tunisia", "tunisian"],
  morocco: ["morocco", "moroccan"],
  france: ["france", "french"],
  england: ["england", "english"],
  spain: ["spain", "spanish"],
  italy: ["italy", "italian"],
  germany: ["germany", "german"],
  netherlands: ["netherlands", "dutch", "holland"],
  portugal: ["portugal", "portuguese"],
  belgium: ["belgium", "belgian"],
  argentina: ["argentina", "argentine", "argentinian"],
  brazil: ["brazil", "brazilian"]
};

const normalize = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const getCountryTokens = (country) => {
  const base = normalize(country);
  const mapped = countryMap[base] || [];
  return [...new Set([base, ...mapped].filter(Boolean))];
};

const isNationalityMatch = (nationality, country) => {
  const nationalityNormalized = normalize(nationality);
  if (!nationalityNormalized) {
    return false;
  }

  const tokens = getCountryTokens(country);
  return tokens.some((token) => nationalityNormalized.includes(token));
};

module.exports = {
  normalize,
  getCountryTokens,
  isNationalityMatch
};
