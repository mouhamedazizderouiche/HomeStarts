const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");
const zlib = require("zlib");
const { connectDB } = require("../../backend/config/db");
const Player = require("../../backend/models/Player");
const {
  normalizeText,
  normalizeCountry,
  getCountryCode,
  getCountryFlag
} = require("../../backend/utils/countryUtils");

const backendRequire = createRequire(
  path.resolve(__dirname, "..", "..", "backend", "package.json")
);
const axios = backendRequire("axios");
const dotenv = backendRequire("dotenv");
const { parse } = backendRequire("csv-parse/sync");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SPORTSDB_BASE_URL =
  process.env.SPORTSDB_BASE_URL || "https://www.thesportsdb.com/api/v1/json/123/";
const WIKIPEDIA_API_BASE_URL =
  process.env.WIKIPEDIA_API_BASE_URL || "https://en.wikipedia.org/w/api.php";
const ETL_HTTP_TIMEOUT_MS = Number(process.env.ETL_HTTP_TIMEOUT_MS || 18000);
const BATCH_SIZE = Number(process.env.ETL_BATCH_SIZE || 500);
const MAX_ROWS = Number(process.env.ETL_MAX_ROWS || 0);
const ACTIVE_MIN_SEASON = Number(process.env.ACTIVE_MIN_SEASON || 2024);
const WIKIPEDIA_MAX_COUNTRIES = Number(process.env.WIKIPEDIA_MAX_COUNTRIES || 80);
const WIKIPEDIA_MIN_COUNTRY_TARGET = Number(process.env.WIKIPEDIA_MIN_COUNTRY_TARGET || 60);
const WIKIPEDIA_DELAY_MS = Number(process.env.WIKIPEDIA_DELAY_MS || 400);
const MAX_SPORTSDB_ENRICH = Number(process.env.ETL_MAX_SPORTSDB_ENRICH || 1000);
const SPORTSDB_DELAY_MS = Number(process.env.ETL_SPORTSDB_DELAY_MS || 220);

const SOURCE_RANK = {
  transfermarkt: 4,
  fbref: 3,
  wikipedia: 2,
  confederation: 2,
  manual: 5
};

const sportsDb = axios.create({
  baseURL: SPORTSDB_BASE_URL,
  timeout: ETL_HTTP_TIMEOUT_MS
});

const wikiApi = axios.create({
  baseURL: WIKIPEDIA_API_BASE_URL,
  timeout: ETL_HTTP_TIMEOUT_MS
});

const pick = (row, keys) => {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== "") {
      return row[key];
    }
  }
  return "";
};

const toNumberOrNull = (value) => {
  const raw = String(value || "")
    .replace(/[^\d.-]/g, "")
    .trim();
  if (!raw) {
    return null;
  }
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
};

const toIntegerOrNull = (value) => {
  const num = Number.parseInt(String(value || "").trim(), 10);
  return Number.isFinite(num) ? num : null;
};

const toDateOrNull = (value) => {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? null : date;
};

const avatar = (name) =>
  `https://ui-avatars.com/api/?name=${encodeURIComponent(name || "Unknown Player")}`;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getAgeFromDate = (dateValue) => {
  if (!dateValue) {
    return null;
  }
  const dob = new Date(dateValue);
  if (Number.isNaN(dob.getTime())) {
    return null;
  }
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const monthDiff = today.getMonth() - dob.getMonth();
  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < dob.getDate())) {
    age -= 1;
  }
  return age >= 0 ? age : null;
};

const hasClub = (club) => {
  const normalized = normalizeText(club);
  return Boolean(normalized && normalized !== "unknown club" && normalized !== "unknown");
};

const decodeHtml = (text) =>
  String(text || "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");

const stripHtml = (html) => decodeHtml(String(html || "").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();

const splitCsvLike = (value) =>
  String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

const loadTextFromCandidates = async (candidates, label) => {
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    try {
      const content = await fs.readFile(candidate, "utf8");
      if (content.trim()) {
        console.log(`[etl] Loaded ${label} from file: ${candidate}`);
        return content;
      }
    } catch (_error) {
      // try next
    }
  }
  return "";
};

const loadTextFromUrls = async (urls, label) => {
  for (const url of urls) {
    if (!url) {
      continue;
    }
    try {
      const response = await axios.get(url, {
        timeout: 30000,
        responseType: "arraybuffer"
      });
      let content = Buffer.from(response.data || []);
      const isGzip =
        url.endsWith(".gz") ||
        String(response.headers?.["content-type"] || "").includes("application/gzip");

      if (isGzip) {
        content = zlib.gunzipSync(content);
      }

      const text = content.toString("utf8");
      if (text.trim()) {
        console.log(`[etl] Loaded ${label} from URL: ${url}`);
        return text;
      }
    } catch (_error) {
      // try next
    }
  }
  return "";
};

const getTransfermarktCsv = async () => {
  const root = path.resolve(__dirname, "..", "..");
  const contentFromFile = await loadTextFromCandidates(
    [
      process.env.TRANSFERMARKT_PLAYERS_CSV_PATH,
      path.join(root, "data", "transfermarkt", "players.csv"),
      path.join(root, "data", "players.csv")
    ],
    "Transfermarkt CSV"
  );
  if (contentFromFile) {
    return contentFromFile;
  }

  const contentFromUrl = await loadTextFromUrls(
    [
      process.env.TRANSFERMARKT_PLAYERS_CSV_URL,
      "https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/players.csv.gz"
    ],
    "Transfermarkt CSV"
  );
  if (contentFromUrl) {
    return contentFromUrl;
  }

  throw new Error(
    "Could not load Transfermarkt players CSV. Set TRANSFERMARKT_PLAYERS_CSV_PATH or TRANSFERMARKT_PLAYERS_CSV_URL."
  );
};

const getFbrefRows = async () => {
  const root = path.resolve(__dirname, "..", "..");
  const contentFromFile = await loadTextFromCandidates(
    [
      process.env.FBREF_PLAYERS_CSV_PATH,
      path.join(root, "data", "fbref", "players.csv"),
      path.join(root, "data", "fbref_players.csv")
    ],
    "FBref CSV"
  );

  const content = contentFromFile
    ? contentFromFile
    : await loadTextFromUrls([process.env.FBREF_PLAYERS_CSV_URL], "FBref CSV");

  if (!content) {
    console.log("[etl] FBref source skipped (no local file or URL configured).");
    return [];
  }

  try {
    return parse(content, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_column_count: true
    });
  } catch (error) {
    console.warn("[etl] FBref CSV parse failed:", error.message);
    return [];
  }
};

const getConfederationRows = async () => {
  const root = path.resolve(__dirname, "..", "..");
  const jsonCandidates = [
    process.env.CONFED_PLAYERS_JSON_PATH,
    path.join(root, "data", "confederation", "players.json"),
    path.join(root, "data", "confederations", "players.json")
  ];
  const csvCandidates = [
    process.env.CONFED_PLAYERS_CSV_PATH,
    path.join(root, "data", "confederation", "players.csv"),
    path.join(root, "data", "confederations", "players.csv")
  ];

  for (const file of jsonCandidates) {
    if (!file) {
      continue;
    }
    try {
      const content = await fs.readFile(file, "utf8");
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed) && parsed.length) {
        console.log(`[etl] Loaded Confederation JSON: ${file}`);
        return parsed;
      }
    } catch (_error) {
      // try next
    }
  }

  const csvContent = await loadTextFromCandidates(csvCandidates, "Confederation CSV");
  if (csvContent) {
    try {
      return parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_column_count: true
      });
    } catch (error) {
      console.warn("[etl] Confederation CSV parse failed:", error.message);
    }
  }

  return [];
};

const loadManualPlayers = async () => {
  const file = path.resolve(__dirname, "..", "..", "data", "manualPlayers.json");
  try {
    const content = await fs.readFile(file, "utf8");
    const parsed = JSON.parse(content);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
};

const extractSeason = (row) =>
  toIntegerOrNull(pick(row, ["last_season", "season", "latest_season", "last_active_season"]));

const createBaseDoc = ({
  source,
  name,
  transfermarktId = "",
  apiFootballId = "",
  nationality = "",
  club = "",
  position = "Unknown",
  teamId = "",
  dateOfBirth = null,
  league = "",
  marketValue = null,
  imageUrl = "",
  minutesPlayed = 0,
  matches = 0,
  goals = 0,
  assists = 0,
  lastSeason = null,
  nationalSquad = false
}) => {
  const normalizedName = normalizeText(name);
  const normalizedNationality = normalizeCountry(nationality);
  const resolvedTransfermarktId = transfermarktId || `tm_${normalizedName}`;
  const resolvedId = apiFootballId || resolvedTransfermarktId;
  const resolvedImage = imageUrl || avatar(name);

  return {
    api_football_id: apiFootballId,
    transfermarkt_id: resolvedTransfermarktId,
    id: resolvedId,
    name: String(name || "").trim(),
    normalized_name: normalizedName,
    nationality: normalizedNationality,
    nationality_code: getCountryCode(normalizedNationality),
    club: String(club || "").trim(),
    league: String(league || "").trim(),
    position: String(position || "Unknown").trim() || "Unknown",
    date_of_birth: toDateOrNull(dateOfBirth),
    age: getAgeFromDate(toDateOrNull(dateOfBirth)),
    market_value: toNumberOrNull(marketValue),
    team_id: String(teamId || "").trim(),
    image_url: resolvedImage,
    image: resolvedImage,
    flag: getCountryFlag(normalizedNationality),
    source,
    sources: [source],
    is_active: true,
    last_season: lastSeason,
    minutes_played: Number(minutesPlayed || 0),
    matches_2025: Number(matches || 0),
    goals_2025: Number(goals || 0),
    assists_2025: Number(assists || 0),
    _national_squad: nationalSquad,
    updatedAt: new Date()
  };
};

const normalizeTransfermarktRow = (row) => {
  const name = String(
    pick(row, ["name", "player_name", "player"]) ||
      [pick(row, ["first_name"]), pick(row, ["last_name"])].filter(Boolean).join(" ")
  ).trim();
  if (!name) {
    return null;
  }

  const nationality = pick(row, [
    "nationality",
    "country_of_citizenship",
    "country",
    "citizenship"
  ]);
  if (!normalizeCountry(nationality)) {
    return null;
  }

  return createBaseDoc({
    source: "transfermarkt",
    name,
    transfermarktId: String(pick(row, ["transfermarkt_id", "player_id", "id"]) || "").trim(),
    apiFootballId: String(pick(row, ["api_football_id", "apiFootballId"]) || "").trim(),
    nationality,
    club: String(
      pick(row, ["club", "club_name", "current_club_name", "current_club"]) || "Unknown Club"
    ).trim(),
    position: pick(row, ["position", "main_position", "sub_position"]) || "Unknown",
    teamId: String(pick(row, ["team_id", "current_club_id", "club_id", "api_team_id"]) || "").trim(),
    dateOfBirth: pick(row, ["date_of_birth", "dob", "birth_date"]),
    league: pick(row, ["league", "competition_name", "current_club_domestic_competition_id"]),
    marketValue: pick(row, ["market_value", "market_value_in_eur", "market_value_eur"]),
    imageUrl: pick(row, ["image_fifa", "fifa_image", "image", "image_url", "photo"]),
    lastSeason: extractSeason(row)
  });
};

const normalizeFbrefRow = (row) => {
  const name = String(pick(row, ["name", "player", "player_name"])).trim();
  const nationality = String(pick(row, ["nationality", "nation", "country"])).trim();
  if (!name || !normalizeCountry(nationality)) {
    return null;
  }

  return createBaseDoc({
    source: "fbref",
    name,
    transfermarktId: String(pick(row, ["transfermarkt_id", "player_id"]) || "").trim(),
    nationality,
    club: String(pick(row, ["club", "squad", "team"]) || "Unknown Club").trim(),
    position: pick(row, ["position", "pos"]) || "Unknown",
    teamId: String(pick(row, ["team_id", "squad_id"]) || "").trim(),
    dateOfBirth: pick(row, ["date_of_birth", "dob"]),
    league: pick(row, ["league", "comp"]) || "",
    imageUrl: pick(row, ["image_fifa", "fifa_image", "image", "image_url", "photo"]) || "",
    minutesPlayed: toNumberOrNull(pick(row, ["minutes_played", "minutes", "min"])) || 0,
    matches: toNumberOrNull(pick(row, ["matches_2025", "matches", "apps", "starts"])) || 0,
    goals: toNumberOrNull(pick(row, ["goals_2025", "goals", "gls"])) || 0,
    assists: toNumberOrNull(pick(row, ["assists_2025", "assists", "ast"])) || 0,
    lastSeason: toIntegerOrNull(pick(row, ["last_season", "season"])) || null
  });
};

const normalizeConfederationRow = (row) => {
  const name = String(pick(row, ["name", "player", "player_name"])).trim();
  const nationality = String(pick(row, ["nationality", "country"])).trim();
  if (!name || !normalizeCountry(nationality)) {
    return null;
  }

  return createBaseDoc({
    source: "confederation",
    name,
    transfermarktId: String(pick(row, ["transfermarkt_id", "player_id"]) || "").trim(),
    nationality,
    club: String(pick(row, ["club", "team"]) || "Unknown Club").trim(),
    position: pick(row, ["position", "role"]) || "Unknown",
    teamId: String(pick(row, ["team_id"]) || "").trim(),
    dateOfBirth: pick(row, ["date_of_birth", "dob"]) || null,
    imageUrl: pick(row, ["image_fifa", "fifa_image", "image", "image_url", "photo"]) || "",
    matches: toNumberOrNull(pick(row, ["matches_2025", "matches"])) || 0,
    goals: toNumberOrNull(pick(row, ["goals_2025", "goals"])) || 0,
    assists: toNumberOrNull(pick(row, ["assists_2025", "assists"])) || 0,
    minutesPlayed: toNumberOrNull(pick(row, ["minutes_played", "minutes"])) || 0,
    lastSeason: toIntegerOrNull(pick(row, ["last_season", "season"])) || null
  });
};

const normalizeManualPlayer = (player) => {
  const name = String(player?.name || "").trim();
  const nationality = normalizeCountry(player?.nationality || "");
  if (!name || !nationality) {
    return null;
  }

  return createBaseDoc({
    source: "manual",
    name,
    transfermarktId: String(player?.transfermarkt_id || player?.id || "").trim(),
    nationality,
    club: String(player?.club || "Manual Selection").trim(),
    position: String(player?.position || "Unknown").trim(),
    teamId: String(player?.team_id || "manual").trim(),
    dateOfBirth: player?.date_of_birth || null,
    league: String(player?.league || "").trim(),
    imageUrl: String(player?.image || "").trim(),
    matches: toNumberOrNull(player?.matches_2025) || 0,
    goals: toNumberOrNull(player?.goals_2025) || 0,
    assists: toNumberOrNull(player?.assists_2025) || 0,
    minutesPlayed: toNumberOrNull(player?.minutes_played) || 1,
    nationalSquad: true
  });
};

const wikipediaSquadCache = new Map();

const extractWikipediaRows = (html, country) => {
  const rows = [];
  const trMatches = html.match(/<tr[\s\S]*?<\/tr>/gi) || [];

  for (const tr of trMatches) {
    const cells = [...tr.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((m) => m[1]);
    if (cells.length < 2) {
      continue;
    }

    const linkMatches = [...tr.matchAll(/<a[^>]*title="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)];
    const nameCandidate = linkMatches.find((m) => {
      const linkText = stripHtml(m[2]);
      return (
        linkText &&
        !/national team|fifa|association|league|cup|manager|coach|list of/i.test(linkText) &&
        !/\d{4}/.test(linkText)
      );
    });

    const name = stripHtml(nameCandidate ? nameCandidate[2] : cells[1]);
    if (!name || name.length < 3) {
      continue;
    }

    const rowText = stripHtml(cells.join(" "));
    const positionCell = cells.find((c) =>
      /\bGK\b|\bDF\b|\bMF\b|\bFW\b|Goalkeeper|Defender|Midfielder|Forward/i.test(stripHtml(c))
    );
    const position = stripHtml(positionCell || "Unknown");
    const possibleClub = stripHtml(cells[cells.length - 1]);

    if (
      !possibleClub ||
      /caps|goals|apps|age|number|no\.|manager|coach|notes?/i.test(possibleClub) ||
      possibleClub.length < 2
    ) {
      continue;
    }

    rows.push(
      createBaseDoc({
        source: "wikipedia",
        name,
        transfermarktId: "",
        nationality: country,
        club: possibleClub,
        position,
        teamId: `wiki_${normalizeText(possibleClub)}`,
        nationalSquad: true,
        minutesPlayed: 1
      })
    );
  }

  return rows;
};

const fetchWikipediaSquad = async (country) => {
  const key = normalizeText(country);
  if (wikipediaSquadCache.has(key)) {
    return wikipediaSquadCache.get(key);
  }

  const pageCandidates = [
    `${country} national football team`,
    `${country} national soccer team`,
    `${country} national football team squad`
  ];

  for (const page of pageCandidates) {
    try {
      await sleep(WIKIPEDIA_DELAY_MS);
      const response = await wikiApi.get("", {
        params: {
          action: "parse",
          page,
          prop: "text",
          format: "json",
          formatversion: 2
        }
      });
      const html = response.data?.parse?.text || "";
      if (!html) {
        continue;
      }
      const docs = extractWikipediaRows(html, country);
      if (docs.length) {
        wikipediaSquadCache.set(key, docs);
        return docs;
      }
    } catch (_error) {
      // try next title
    }
  }

  wikipediaSquadCache.set(key, []);
  return [];
};

const chooseWikipediaCountries = (transferDocs, fbrefDocs, manualDocs) => {
  const priorityDefaults = splitCsvLike(
    process.env.WIKIPEDIA_PRIORITY_COUNTRIES ||
      "Tunisia,Morocco,Algeria,Egypt,Senegal,Nigeria,Ghana,Cameroon,Mali,South Africa,Ivory Coast,Saudi Arabia,Qatar,UAE,Japan,South Korea,Australia,United States,Mexico"
  );

  const countByCountry = new Map();
  for (const doc of [...transferDocs, ...fbrefDocs]) {
    const country = normalizeCountry(doc.nationality);
    if (!country) {
      continue;
    }
    countByCountry.set(country, (countByCountry.get(country) || 0) + 1);
  }

  const lowCoverageCountries = [...countByCountry.entries()]
    .filter(([, count]) => count < WIKIPEDIA_MIN_COUNTRY_TARGET)
    .map(([country]) => country);

  const manualCountries = manualDocs.map((doc) => normalizeCountry(doc.nationality)).filter(Boolean);

  const combined = [...priorityDefaults, ...manualCountries, ...lowCoverageCountries];
  const unique = [...new Set(combined.map((item) => normalizeCountry(item)).filter(Boolean))];

  if (!unique.includes("Tunisia")) {
    unique.unshift("Tunisia");
  }

  return unique.slice(0, WIKIPEDIA_MAX_COUNTRIES);
};

const mergeSources = (existingSources, incomingSource) => {
  const all = new Set([...(existingSources || []), incomingSource].filter(Boolean));
  return [...all];
};

const preferValue = (currentValue, incomingValue, currentSourceRank, incomingSourceRank) => {
  const hasCurrent = String(currentValue || "").trim() !== "";
  const hasIncoming = String(incomingValue || "").trim() !== "";
  if (!hasIncoming) {
    return currentValue;
  }
  if (!hasCurrent) {
    return incomingValue;
  }
  return incomingSourceRank >= currentSourceRank ? incomingValue : currentValue;
};

const makeNameDobKey = (doc) => {
  const normalizedName = normalizeText(doc.normalized_name || doc.name);
  const dob = doc.date_of_birth ? new Date(doc.date_of_birth).toISOString().slice(0, 10) : "";
  if (!normalizedName || !dob) {
    return "";
  }
  return `${normalizedName}|${dob}`;
};

const buildMergedPlayers = (allDocs) => {
  const merged = [];
  const byTransfermarkt = new Map();
  const byNameDob = new Map();
  const byNormalized = new Map();

  const resolveIndex = (doc) => {
    const tm = normalizeText(doc.transfermarkt_id);
    const nameDob = makeNameDobKey(doc);
    const normalized = normalizeText(doc.normalized_name || doc.name);

    if (tm && byTransfermarkt.has(tm)) {
      return byTransfermarkt.get(tm);
    }
    if (nameDob && byNameDob.has(nameDob)) {
      return byNameDob.get(nameDob);
    }
    if (normalized && byNormalized.has(normalized)) {
      return byNormalized.get(normalized);
    }
    return -1;
  };

  const registerIndex = (idx, doc) => {
    const tm = normalizeText(doc.transfermarkt_id);
    const nameDob = makeNameDobKey(doc);
    const normalized = normalizeText(doc.normalized_name || doc.name);
    if (tm) {
      byTransfermarkt.set(tm, idx);
    }
    if (nameDob) {
      byNameDob.set(nameDob, idx);
    }
    if (normalized) {
      byNormalized.set(normalized, idx);
    }
  };

  for (const doc of allDocs) {
    const idx = resolveIndex(doc);
    if (idx === -1) {
      merged.push({
        ...doc,
        _clubRank: SOURCE_RANK[doc.source] || 0,
        _positionRank: SOURCE_RANK[doc.source] || 0,
        _imageRank: doc.image_url && !doc.image_url.includes("ui-avatars.com") ? 10 : SOURCE_RANK[doc.source] || 0
      });
      registerIndex(merged.length - 1, doc);
      continue;
    }

    const current = merged[idx];
    const incomingRank = SOURCE_RANK[doc.source] || 0;
    const incomingImageRank =
      doc.image_url && !doc.image_url.includes("ui-avatars.com") ? 10 : incomingRank;

    current.name = preferValue(current.name, doc.name, current._clubRank, incomingRank);
    current.nationality = preferValue(
      current.nationality,
      doc.nationality,
      current._clubRank,
      incomingRank
    );
    current.nationality_code =
      current.nationality_code || doc.nationality_code || getCountryCode(current.nationality || doc.nationality);
    current.club = preferValue(current.club, doc.club, current._clubRank, incomingRank);
    current.position = preferValue(
      current.position,
      doc.position,
      current._positionRank,
      incomingRank
    );

    if (!current.date_of_birth && doc.date_of_birth) {
      current.date_of_birth = doc.date_of_birth;
    }
    if (!current.transfermarkt_id && doc.transfermarkt_id) {
      current.transfermarkt_id = doc.transfermarkt_id;
    }
    if (!current.api_football_id && doc.api_football_id) {
      current.api_football_id = doc.api_football_id;
    }
    if (!current.team_id && doc.team_id) {
      current.team_id = doc.team_id;
    }
    if (!current.league && doc.league) {
      current.league = doc.league;
    }
    if (!current.market_value && doc.market_value) {
      current.market_value = doc.market_value;
    }

    current.minutes_played = Math.max(Number(current.minutes_played || 0), Number(doc.minutes_played || 0));
    current.matches_2025 = Math.max(Number(current.matches_2025 || 0), Number(doc.matches_2025 || 0));
    current.goals_2025 = Math.max(Number(current.goals_2025 || 0), Number(doc.goals_2025 || 0));
    current.assists_2025 = Math.max(Number(current.assists_2025 || 0), Number(doc.assists_2025 || 0));
    current.last_season = Math.max(
      Number(current.last_season || 0),
      Number(doc.last_season || 0)
    ) || null;
    current._national_squad = Boolean(current._national_squad || doc._national_squad);

    current.image_url = preferValue(
      current.image_url,
      doc.image_url,
      current._imageRank,
      incomingImageRank
    );
    current.image = current.image_url;
    current.sources = mergeSources(current.sources, doc.source);
    current.source = current.sources.includes("transfermarkt")
      ? "transfermarkt"
      : current.sources[0] || doc.source;
    current.normalized_name = normalizeText(current.name);
    current.flag = getCountryFlag(current.nationality);
    current.updatedAt = new Date();
    current._clubRank = Math.max(current._clubRank, incomingRank);
    current._positionRank = Math.max(current._positionRank, incomingRank);
    current._imageRank = Math.max(current._imageRank, incomingImageRank);
  }

  return merged;
};

const isStrictActive = (doc) => {
  const seasonActive = Number(doc.last_season || 0) >= ACTIVE_MIN_SEASON;
  const minutesActive = Number(doc.minutes_played || 0) > 0;
  const squadActive = Boolean(doc._national_squad);
  const teamFallback =
    !doc.last_season && Number(doc.minutes_played || 0) <= 0 && Boolean(String(doc.team_id || "").trim());
  return hasClub(doc.club) && (seasonActive || minutesActive || squadActive || teamFallback);
};

const sportsDbNameCache = new Map();
const findSportsDbImage = async (name) => {
  const key = normalizeText(name);
  if (!key) {
    return "";
  }
  if (sportsDbNameCache.has(key)) {
    return sportsDbNameCache.get(key);
  }
  await sleep(SPORTSDB_DELAY_MS);
  try {
    const response = await sportsDb.get("searchplayers.php", { params: { p: name } });
    const list = Array.isArray(response.data?.player) ? response.data.player : [];
    const exact = list.find((item) => normalizeText(item?.strPlayer) === key) || list[0] || null;
    const image =
      exact?.strCutout || exact?.strThumb || exact?.strRender || exact?.strFanart1 || "";
    sportsDbNameCache.set(key, image);
    return image;
  } catch (_error) {
    sportsDbNameCache.set(key, "");
    return "";
  }
};

const enrichMissingImages = async (docs) => {
  const output = [];
  let enrichCount = 0;
  for (const doc of docs) {
    const missing = !doc.image_url || doc.image_url.includes("ui-avatars.com");
    if (missing && enrichCount < MAX_SPORTSDB_ENRICH) {
      const sportsImage = await findSportsDbImage(doc.name);
      if (sportsImage) {
        doc.image_url = sportsImage;
        doc.image = sportsImage;
      }
      enrichCount += 1;
    }
    if (!doc.image_url) {
      doc.image_url = avatar(doc.name);
      doc.image = doc.image_url;
    }
    output.push(doc);
  }
  return output;
};

const cleanupDocForPersistence = (doc) => {
  const clean = { ...doc };
  delete clean._clubRank;
  delete clean._positionRank;
  delete clean._imageRank;
  delete clean._national_squad;

  clean.id = clean.api_football_id || clean.transfermarkt_id || `player_${clean.normalized_name}`;
  clean.date_of_birth = toDateOrNull(clean.date_of_birth);
  clean.age = getAgeFromDate(clean.date_of_birth);
  clean.nationality = normalizeCountry(clean.nationality);
  clean.nationality_code = clean.nationality_code || getCountryCode(clean.nationality);
  clean.flag = clean.flag || getCountryFlag(clean.nationality);
  clean.image_url = clean.image_url || avatar(clean.name);
  clean.image = clean.image_url;
  clean.updatedAt = new Date();
  clean.sources = Array.isArray(clean.sources) ? [...new Set(clean.sources)] : [clean.source].filter(Boolean);
  clean.source = clean.sources.includes("transfermarkt")
    ? "transfermarkt"
    : clean.sources[0] || clean.source || "unknown";
  clean.is_active = true;
  return clean;
};

const bulkUpsert = async (docs) => {
  if (!docs.length) {
    return { modified: 0, upserted: 0 };
  }

  const ops = docs.map((doc) => {
    const nameDobKey = makeNameDobKey(doc);
    const filterCandidates = [];
    if (doc.transfermarkt_id) {
      filterCandidates.push({ transfermarkt_id: doc.transfermarkt_id });
    }
    if (nameDobKey && doc.normalized_name) {
      filterCandidates.push({
        normalized_name: doc.normalized_name,
        date_of_birth: doc.date_of_birth || null
      });
    }
    filterCandidates.push({ normalized_name: doc.normalized_name });

    return {
      updateOne: {
        filter: filterCandidates.length > 1 ? { $or: filterCandidates } : filterCandidates[0],
        update: { $set: doc },
        upsert: true
      }
    };
  });

  const result = await Player.bulkWrite(ops, { ordered: false });
  return {
    modified: result.modifiedCount || 0,
    upserted: result.upsertedCount || 0
  };
};

const deactivateInactiveTransfermarktRows = async (allTransferIds, activeTransferIds) => {
  if (!allTransferIds.size) {
    return 0;
  }
  if (!activeTransferIds.size) {
    console.warn("[etl] Skipping deactivation because active transfermarkt set is empty.");
    return 0;
  }
  if (activeTransferIds.size < allTransferIds.size * 0.2) {
    console.warn(
      "[etl] Skipping deactivation safety gate: active transfermarkt set is unexpectedly low."
    );
    return 0;
  }
  const inactiveIds = [...allTransferIds].filter((id) => !activeTransferIds.has(id));
  if (!inactiveIds.length) {
    return 0;
  }
  const result = await Player.updateMany(
    { transfermarkt_id: { $in: inactiveIds }, source: { $ne: "manual" } },
    { $set: { is_active: false, updatedAt: new Date() } }
  );
  return result.modifiedCount || 0;
};

const countByCountry = (docs) => {
  const map = new Map();
  for (const doc of docs) {
    const country = normalizeCountry(doc.nationality);
    if (!country) {
      continue;
    }
    map.set(country, (map.get(country) || 0) + 1);
  }
  return map;
};

const runImport = async () => {
  await connectDB();

  const transferCsv = await getTransfermarktCsv();
  const transferRows = parse(transferCsv, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    relax_column_count: true
  });
  const limitedTransferRows = MAX_ROWS > 0 ? transferRows.slice(0, MAX_ROWS) : transferRows;
  const transferDocs = limitedTransferRows.map(normalizeTransfermarktRow).filter(Boolean);

  const fbrefRows = await getFbrefRows();
  const fbrefDocs = fbrefRows.map(normalizeFbrefRow).filter(Boolean);

  const confedRows = await getConfederationRows();
  const confedDocs = confedRows.map(normalizeConfederationRow).filter(Boolean);

  const manualPlayers = await loadManualPlayers();
  const manualDocs = manualPlayers.map(normalizeManualPlayer).filter(Boolean);

  const wikiCountries = chooseWikipediaCountries(transferDocs, fbrefDocs, manualDocs);
  const wikiDocs = [];
  for (const country of wikiCountries) {
    const squadPlayers = await fetchWikipediaSquad(country);
    wikiDocs.push(...squadPlayers);
  }

  const mergedDocs = buildMergedPlayers([
    ...transferDocs,
    ...fbrefDocs,
    ...confedDocs,
    ...wikiDocs,
    ...manualDocs
  ]);
  const strictlyActiveDocs = mergedDocs.filter(isStrictActive);
  if (!strictlyActiveDocs.length) {
    console.warn("[etl] Active filter produced zero players. Aborting write to protect dataset.");
    return;
  }
  const imageEnrichedDocs = await enrichMissingImages(strictlyActiveDocs);
  const finalDocs = imageEnrichedDocs.map(cleanupDocForPersistence);

  let batch = [];
  let modifiedTotal = 0;
  let upsertedTotal = 0;
  for (const doc of finalDocs) {
    batch.push(doc);
    if (batch.length >= BATCH_SIZE) {
      const result = await bulkUpsert(batch);
      modifiedTotal += result.modified;
      upsertedTotal += result.upserted;
      batch = [];
    }
  }
  if (batch.length) {
    const result = await bulkUpsert(batch);
    modifiedTotal += result.modified;
    upsertedTotal += result.upserted;
  }

  const allTransferIds = new Set(
    transferDocs.map((doc) => normalizeText(doc.transfermarkt_id)).filter(Boolean)
  );
  const activeTransferIds = new Set(
    finalDocs.map((doc) => normalizeText(doc.transfermarkt_id)).filter(Boolean)
  );
  const deactivatedCount = await deactivateInactiveTransfermarktRows(allTransferIds, activeTransferIds);

  const [totalPlayers, activePlayers, countriesCount, missingImages] = await Promise.all([
    Player.countDocuments(),
    Player.countDocuments({ is_active: { $ne: false } }),
    Player.distinct("nationality", { is_active: { $ne: false }, nationality: { $ne: "" } }).then((x) => x.length),
    Player.countDocuments({
      is_active: { $ne: false },
      $or: [{ image_url: { $exists: false } }, { image_url: "" }, { image_url: null }]
    })
  ]);

  const countryCoverage = countByCountry(finalDocs);
  const sortedCoverage = [...countryCoverage.entries()].sort((a, b) => b[1] - a[1]);
  const tunisiaCoverage = countryCoverage.get("Tunisia") || 0;
  const moroccoCoverage = countryCoverage.get("Morocco") || 0;

  console.log("[etl] ----------------------");
  console.log(`[etl] Transfermarkt rows: ${limitedTransferRows.length}`);
  console.log(`[etl] Transfermarkt docs: ${transferDocs.length}`);
  console.log(`[etl] FBref docs: ${fbrefDocs.length}`);
  console.log(`[etl] Wikipedia countries scanned: ${wikiCountries.length}`);
  console.log(`[etl] Wikipedia docs: ${wikiDocs.length}`);
  console.log(`[etl] Confederation docs: ${confedDocs.length}`);
  console.log(`[etl] Manual docs: ${manualDocs.length}`);
  console.log(`[etl] Merged docs: ${mergedDocs.length}`);
  console.log(`[etl] Strictly active docs: ${strictlyActiveDocs.length}`);
  console.log(`[etl] Upserted: ${upsertedTotal} | Modified: ${modifiedTotal}`);
  console.log(`[etl] Deactivated transfermarkt players: ${deactivatedCount}`);
  console.log(`[etl] Tunisia active players in run: ${tunisiaCoverage}`);
  console.log(`[etl] Morocco active players in run: ${moroccoCoverage}`);
  console.log(
    `[etl] Top countries: ${sortedCoverage
      .slice(0, 12)
      .map(([country, count]) => `${country} (${count})`)
      .join(", ")}`
  );
  console.log({
    totalPlayers,
    activePlayers,
    countriesCount,
    missingImages
  });
};

if (require.main === module) {
  runImport()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("[etl] Import failed:", error.message);
      process.exit(1);
    });
}

module.exports = {
  runImport
};
