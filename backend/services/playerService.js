const axios = require("axios");
const cheerio = require("cheerio");
const NodeCache = require("node-cache");
const pLimitImport = require("p-limit");
const Player = require("../models/Player");
const { getSupabaseClient, hasSupabaseConfig } = require("../config/supabase");
const {
  normalizeText,
  normalizeCountry,
  getCountryAliases,
  getCountryCode
} = require("../utils/countryUtils");

const PLAYERS_PER_COUNTRY_LIMIT = Number(process.env.PLAYERS_PER_COUNTRY_LIMIT || 40);
const TOP_PLAYERS_LIMIT = Number(process.env.TOP_PLAYERS_LIMIT || 10);
const CALENDAR_WINDOW_DAYS = Number(process.env.CALENDAR_WINDOW_DAYS || 7);
const NEXT_MATCH_WINDOW_DAYS = Number(process.env.NEXT_MATCH_WINDOW_DAYS || 14);
const MATCH_CACHE_TTL_SECONDS = Number(process.env.MATCH_CACHE_TTL_SECONDS || 600);
const CALENDAR_PAGE_WINDOW_DAYS = Number(process.env.CALENDAR_PAGE_WINDOW_DAYS || 30);
const SPORTSDB_BASE_URL =
  process.env.SPORTSDB_BASE_URL || "https://www.thesportsdb.com/api/v1/json/123/";
const SPORTSDB_TIMEOUT_MS = Number(process.env.SPORTSDB_TIMEOUT_MS || 12000);
const PLACEHOLDER_IMAGE =
  process.env.PLAYER_PLACEHOLDER_IMAGE ||
  "https://api.dicebear.com/7.x/initials/svg?seed=Unknown%20Player&backgroundColor=1A56A0&textColor=ffffff";
const PLACEHOLDER_FLAG = "https://flagcdn.com/64x48/un.png";

const MATCH_MIN_DATE = new Date("2024-01-01T00:00:00.000Z");
const MATCH_MAX_DATE = new Date("2026-12-31T23:59:59.999Z");

const sportsDb = axios.create({
  baseURL: SPORTSDB_BASE_URL,
  timeout: SPORTSDB_TIMEOUT_MS
});
const webClient = axios.create({
  timeout: 12000,
  headers: {
    "User-Agent":
      process.env.WIKIPEDIA_USER_AGENT ||
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  }
});
const imageCache = new NodeCache({ stdTTL: 60 * 60 * 24, checkperiod: 120 });
const matchCache = new NodeCache({ stdTTL: MATCH_CACHE_TTL_SECONDS, checkperiod: 120 });
const clubLogoCache = new NodeCache({ stdTTL: 60 * 60 * 24 * 7, checkperiod: 120 });
const API_CACHE_TTL_SECONDS = Number(process.env.API_CACHE_TTL_SECONDS || 300);
const apiResponseCache = new NodeCache({ stdTTL: API_CACHE_TTL_SECONDS, checkperiod: 120 });
const tableColumnsCache = new Map();
const tableAllColumnsCache = new Map();
const HYDRATION_CONCURRENCY = Number(process.env.HYDRATION_CONCURRENCY || 5);
const pLimit = typeof pLimitImport === "function" ? pLimitImport : pLimitImport.default;
const hydrateLimit = pLimit(HYDRATION_CONCURRENCY);
const ALLOW_REMOTE_IMAGES = String(process.env.ALLOW_REMOTE_IMAGES || "true").toLowerCase() === "true";

const avatar = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
    cleanPlayerName(name || "Unknown Player")
  )}&backgroundColor=1A56A0&textColor=ffffff`;
const getFlagUrl = (code) =>
  code ? `https://flagcdn.com/64x48/${String(code).toLowerCase()}.png` : PLACEHOLDER_FLAG;

const STATIC_CLUB_LOGOS = {
  "real madrid": "https://upload.wikimedia.org/wikipedia/en/5/56/Real_Madrid_CF.svg",
  "fc barcelona": "https://upload.wikimedia.org/wikipedia/en/4/47/FC_Barcelona_%28crest%29.svg",
  "manchester city": "https://upload.wikimedia.org/wikipedia/en/e/eb/Manchester_City_FC_badge.svg",
  "manchester united": "https://upload.wikimedia.org/wikipedia/en/7/7a/Manchester_United_FC_crest.svg",
  "liverpool fc": "https://upload.wikimedia.org/wikipedia/en/0/0c/Liverpool_FC.svg",
  "arsenal fc": "https://upload.wikimedia.org/wikipedia/en/5/53/Arsenal_FC.svg",
  "bayern munich": "https://upload.wikimedia.org/wikipedia/commons/1/1f/FC_Bayern_M%C3%BCnchen_logo_%282017%29.svg",
  "paris saint-germain": "https://upload.wikimedia.org/wikipedia/en/a/a7/Paris_Saint-Germain_F.C..svg",
  juventus: "https://upload.wikimedia.org/wikipedia/commons/1/15/Juventus_FC_2017_logo.svg",
  "ac milan": "https://upload.wikimedia.org/wikipedia/commons/d/d0/Logo_of_AC_Milan.svg"
};

const safeImageUrl = (url) => (isImageUrl(url) ? String(url).trim() : "");
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const cleanPlayerName = (value) =>
  cleanText(
    String(value || "")
      .replace(/\s*\((?:#)?\d{4,}\)\s*/g, " ")
      .replace(/^#\s*\d+\s*/i, "")
  );
const extractTransfermarktId = (row) => {
  const direct = Number(row?.transfermarkt_id || row?.tm_id || 0);
  if (Number.isFinite(direct) && direct > 0) {
    return direct;
  }
  const match = String(row?.name || "").match(/\((\d{4,})\)/);
  const parsed = Number(match?.[1] || 0);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }

  const imageHint = decodeURIComponent(String(row?.image_url || row?.image || ""));
  const imageMatch = imageHint.match(/\((\d{4,})\)/);
  const imageParsed = Number(imageMatch?.[1] || 0);
  return Number.isFinite(imageParsed) && imageParsed > 0 ? imageParsed : 0;
};
const normalizeCountryInput = (value) => normalizeCountry(value || "");

const hasValue = (value) => String(value || "").trim() !== "";
const isImageUrl = (value) => /^https?:\/\/.+/i.test(String(value || "").trim());
const isAvatarPlaceholder = (value) => /(ui-avatars\.com|avatar)/i.test(String(value || ""));
const escapeRegex = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const parseDateMs = (value) => {
  if (!value) {
    return 0;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 0 : date.getTime();
};
const normalizeClubKey = (value) =>
  normalizeText(value)
    .replace(/\b(fc|cf|sc|ac|afc|calcio|club|deportivo|sporting|athletic|olympique)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
const isClubLikelySame = (left, right) => {
  const a = normalizeClubKey(left);
  const b = normalizeClubKey(right);
  if (!a || !b) {
    return false;
  }
  if (a === b) {
    return true;
  }
  if (a.length >= 5 && b.includes(a)) {
    return true;
  }
  if (b.length >= 5 && a.includes(b)) {
    return true;
  }
  return false;
};
const normalizeStatusRank = (status) => (String(status || "").toLowerCase() === "verified" ? 2 : 1);

const clampDateRangeEnd = (days) => {
  const now = new Date();
  const candidate = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);
  return candidate > MATCH_MAX_DATE ? MATCH_MAX_DATE : candidate;
};

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

const normalizeDataSources = (row) => {
  if (Array.isArray(row?.data_sources)) {
    return row.data_sources;
  }
  if (Array.isArray(row?.sources)) {
    return row.sources;
  }
  return [];
};

const wikipediaSearchImage = async (name) => {
  const key = `wikipedia:${normalizeText(name)}`;
  const cached = imageCache.get(key);
  if (cached !== undefined) {
    return cached;
  }
  try {
    const searchUrl = `https://en.wikipedia.org/wiki/${encodeURIComponent(String(name || "").replace(/\s+/g, "_"))}`;
    const response = await webClient.get(searchUrl);
    const $ = cheerio.load(response.data || "");
    const imageSrc = $("table.infobox img").first().attr("src") || "";
    const image = imageSrc
      ? safeImageUrl(imageSrc.startsWith("//") ? `https:${imageSrc}` : imageSrc)
      : "";
    imageCache.set(key, image);
    return image;
  } catch (_error) {
    imageCache.set(key, "");
    return "";
  }
};

const transfermarktSearchImage = async (name) => {
  const key = `transfermarkt:${normalizeText(name)}`;
  const cached = imageCache.get(key);
  if (cached !== undefined) {
    return cached;
  }
  try {
    const query = encodeURIComponent(name || "");
    const response = await webClient.get(
      `https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query=${query}`
    );
    const $ = cheerio.load(response.data || "");
    const candidate =
      $("table.items td.hauptlink a img").first().attr("data-src") ||
      $("table.items td.hauptlink a img").first().attr("src") ||
      "";
    const image = safeImageUrl(candidate);
    imageCache.set(key, image);
    return image;
  } catch (_error) {
    imageCache.set(key, "");
    return "";
  }
};

const getSportsDbImage = async (name, allowRemote = true) => {
  const cacheKey = `sportsdb:${normalizeText(name)}`;
  const cached = imageCache.get(cacheKey);
  if (cached !== undefined) {
    return cached;
  }
  if (!allowRemote) {
    return "";
  }

  try {
    const response = await sportsDb.get("searchplayers.php", { params: { p: name } });
    const players = Array.isArray(response.data?.player) ? response.data.player : [];
    const hit = players.find((item) => normalizeText(item?.strPlayer) === normalizeText(name)) || players[0];
    const candidate = hit?.strCutout || hit?.strThumb || hit?.strRender || hit?.strFanart1 || "";
    const image = isImageUrl(candidate) ? candidate : "";
    imageCache.set(cacheKey, image);
    return image;
  } catch (_error) {
    imageCache.set(cacheKey, "");
    return "";
  }
};

const getSportsDbClubLogo = async (club, allowRemote = true) => {
  const key = `club:sportsdb:${normalizeText(club)}`;
  const cached = clubLogoCache.get(key);
  if (cached !== undefined) {
    return cached;
  }
  if (!allowRemote) {
    return "";
  }
  try {
    const response = await sportsDb.get("searchteams.php", { params: { t: club } });
    const teams = Array.isArray(response.data?.teams) ? response.data.teams : [];
    const hit = teams.find((item) => normalizeText(item?.strTeam) === normalizeText(club)) || teams[0];
    const logo = safeImageUrl(hit?.strBadge || hit?.strLogo || hit?.strTeamBadge);
    clubLogoCache.set(key, logo);
    return logo;
  } catch (_error) {
    clubLogoCache.set(key, "");
    return "";
  }
};

const getFootballDataClubLogo = async (club, allowRemote = true) => {
  const key = `club:football-data:${normalizeText(club)}`;
  const cached = clubLogoCache.get(key);
  if (cached !== undefined) {
    return cached;
  }
  if (!allowRemote || !process.env.FOOTBALL_DATA_API_KEY) {
    return "";
  }
  try {
    const response = await webClient.get("https://api.football-data.org/v4/teams", {
      headers: { "X-Auth-Token": process.env.FOOTBALL_DATA_API_KEY },
      params: { limit: 500 }
    });
    const teams = Array.isArray(response.data?.teams) ? response.data.teams : [];
    const hit = teams.find((item) => normalizeText(item?.name) === normalizeText(club));
    const crest = safeImageUrl(hit?.crest);
    clubLogoCache.set(key, crest);
    return crest;
  } catch (_error) {
    clubLogoCache.set(key, "");
    return "";
  }
};

const resolveClubLogo = async (club, options = {}) => {
  const allowRemote = options.allowRemote === true;
  const cleanClub = cleanText(club);
  if (!cleanClub) {
    return avatar("Club");
  }

  const staticLogo = STATIC_CLUB_LOGOS[normalizeText(cleanClub)];
  if (safeImageUrl(staticLogo)) {
    return staticLogo;
  }

  const sportsDbLogo = await getSportsDbClubLogo(cleanClub, allowRemote);
  if (sportsDbLogo) {
    return sportsDbLogo;
  }

  const footballDataLogo = await getFootballDataClubLogo(cleanClub, allowRemote);
  if (footballDataLogo) {
    return footballDataLogo;
  }

  return avatar(cleanClub);
};

const resolveImageUrl = async (row, options = {}) => {
  try {
    const allowRemote = options.allowRemote === true;
    const pickImage = (values) =>
      values.find((value) => isImageUrl(value) && !isAvatarPlaceholder(value)) || "";

    // 1) Database image
    const dbImage = pickImage([
      row.image_url,
      row.image,
      row.photo,
      row.wikipedia_image_url,
      row.image_wikipedia,
      row.wiki_image_url,
      row.fifa_image_url,
      row.fifa_image,
      row.image_fifa
    ]);
    if (hasValue(dbImage)) {
      return dbImage;
    }

    // 1.5) Transfermarkt deterministic image URL if transfermarkt_id exists
    const tmId = extractTransfermarktId(row);
    if (Number.isFinite(tmId) && tmId > 0) {
      return `https://img.transfermarkt.com/portrait/header/default/${tmId}.jpg`;
    }

    // 2) TheSportsDB
    const sportsDbImage = await getSportsDbImage(row.name, allowRemote);
    if (hasValue(sportsDbImage)) {
      return sportsDbImage;
    }

    if (allowRemote) {
      // 3) Wikipedia
      const wikiImage = await wikipediaSearchImage(row.name);
      if (hasValue(wikiImage)) {
        return wikiImage;
      }

      // 4) Transfermarkt (lightweight)
      const tmImage = await transfermarktSearchImage(row.name);
      if (hasValue(tmImage)) {
        return tmImage;
      }
    }

    // 5) Avatar fallback
    return avatar(cleanPlayerName(row?.name)) || PLACEHOLDER_IMAGE;
  } catch (error) {
    console.error("SERVICE ERROR:resolveImageUrl", error);
    return avatar(cleanPlayerName(row?.name)) || PLACEHOLDER_IMAGE;
  }
};

const isEligiblePlayer = (row) => {
  const club = String(row.current_club || row.club || "").trim();
  const active = row.is_active === true || row.is_active === 1;
  const status = String(row.player_status || "active").trim().toLowerCase();
  const lastSeason = Number(row.last_season || 0);
  const nationalTeam = Boolean(row.is_national_team_player);
  const verified = status === "verified";

  if (verified) {
    return true;
  }

  const seasonEligible = lastSeason >= 2023 || nationalTeam;
  return (
    active &&
    status !== "uncertain" &&
    seasonEligible &&
    hasValue(club) &&
    !/unknown|without club|free agent/i.test(club)
  );
};

const sortByStatusThenActivity = (a, b) => {
  const statusDelta = normalizeStatusRank(b.player_status) - normalizeStatusRank(a.player_status);
  if (statusDelta !== 0) {
    return statusDelta;
  }
  const matchDelta = parseDateMs(b.last_match_date) - parseDateMs(a.last_match_date);
  if (matchDelta !== 0) {
    return matchDelta;
  }
  const seenDelta = parseDateMs(b.last_seen_at) - parseDateMs(a.last_seen_at);
  if (seenDelta !== 0) {
    return seenDelta;
  }
  return String(a.name || "").localeCompare(String(b.name || ""));
};

const normalizeMatchRow = (row) => ({
  id: row.id,
  home_club_id: row.home_club_id || null,
  away_club_id: row.away_club_id || null,
  club: row.club || row.home_team || "",
  opponent: row.opponent || row.away_team || "",
  home_team: row.home_team || row.club || "",
  away_team: row.away_team || row.opponent || "",
  match_date: row.match_date,
  competition: row.competition,
  home_away: row.home_away,
  home_score: row.home_score ?? null,
  away_score: row.away_score ?? null,
  stadium: row.stadium || "",
  status: row.status || "scheduled"
});

const detectTableColumns = async (tableName, candidates) => {
  if (!hasSupabaseConfig()) {
    return new Set(candidates);
  }

  if (tableAllColumnsCache.has(tableName)) {
    const knownColumns = tableAllColumnsCache.get(tableName);
    return new Set(candidates.filter((column) => knownColumns.has(column)));
  }

  const cacheKey = `${tableName}:${candidates.join(",")}`;
  if (tableColumnsCache.has(cacheKey)) {
    return tableColumnsCache.get(cacheKey);
  }

  const supabase = getSupabaseClient();
  try {
    const { data, error } = await supabase.rpc("get_columns", { table_name: tableName });
    if (!error && Array.isArray(data) && data.length) {
      const knownColumns = new Set(
        data.map((row) => String(row?.column_name || "").trim()).filter(Boolean)
      );
      tableAllColumnsCache.set(tableName, knownColumns);
      const columns = new Set(candidates.filter((column) => knownColumns.has(column)));
      tableColumnsCache.set(cacheKey, columns);
      return columns;
    }
  } catch (_error) {
    // fallback to per-column probing
  }

  const columns = new Set();
  for (const column of candidates) {
    try {
      const { error } = await supabase.from(tableName).select(column).limit(1);
      if (!error) {
        columns.add(column);
      }
    } catch (_error) {
      // ignore
    }
  }
  tableColumnsCache.set(cacheKey, columns);
  return columns;
};

const buildSelectFromAvailable = async (tableName, candidates) => {
  const available = await detectTableColumns(tableName, candidates);
  const columns = candidates.filter((column) => available.has(column));
  return {
    available,
    columns,
    select: columns.join(",")
  };
};

const mapNextMatch = (match) => {
  if (!match) {
    return null;
  }
  return {
    id: match.id,
    opponent: match.opponent,
    home_team: match.home_team || "",
    away_team: match.away_team || "",
    date: match.match_date,
    match_date: match.match_date,
    competition: match.competition,
    home_away: match.home_away,
    home_score: match.home_score ?? null,
    away_score: match.away_score ?? null,
    stadium: match.stadium || "",
    status: match.status || "scheduled"
  };
};

const getMatchesByClubsWithinWindow = async (clubs, daysWindow) => {
  if (!clubs.length || !hasSupabaseConfig()) {
    return [];
  }

  const uniqueClubs = [...new Set(clubs.map((club) => String(club || "").trim()).filter(Boolean))];
  if (!uniqueClubs.length) {
    return [];
  }

  const cacheKey = `matches:${daysWindow}:${uniqueClubs.sort((a, b) => a.localeCompare(b)).join("|")}`;
  const cached = matchCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const supabase = getSupabaseClient();
  const now = new Date();
  const fromDate = now < MATCH_MIN_DATE ? MATCH_MIN_DATE : now;
  const toDate = clampDateRangeEnd(daysWindow);

  const wantedColumns = [
    "id",
    "home_club_id",
    "away_club_id",
    "club",
    "opponent",
    "home_team",
    "away_team",
    "match_date",
    "competition",
    "home_away",
    "home_score",
    "away_score",
    "stadium",
    "status"
  ];
  const availableColumns = await detectTableColumns("matches", wantedColumns);
  const selectColumns = wantedColumns.filter((col) => availableColumns.has(col)).join(",");

  const query = supabase
    .from("matches")
    .select(selectColumns || "id,club,opponent,match_date,competition")
    .gte("match_date", fromDate.toISOString())
    .lte("match_date", toDate.toISOString())
    .order("match_date", { ascending: true })
    .limit(2000);

  const { data, error } = await query;

  if (error) {
    throw new Error(error.message);
  }

  const rows = Array.isArray(data) ? data.map(normalizeMatchRow) : [];
  const filteredRows = rows.filter((row) =>
    uniqueClubs.some((clubName) => {
      return (
        isClubLikelySame(clubName, row.club) ||
        isClubLikelySame(clubName, row.home_team) ||
        isClubLikelySame(clubName, row.away_team)
      );
    })
  );
  matchCache.set(cacheKey, filteredRows, MATCH_CACHE_TTL_SECONDS);
  return filteredRows;
};

const buildNextMatchMapForClubs = async (clubs) => {
  const matches = await getMatchesByClubsWithinWindow(clubs, NEXT_MATCH_WINDOW_DAYS);
  const map = new Map();
  const cleanClubs = [...new Set(clubs.map((club) => cleanText(club)).filter(Boolean))];
  for (const clubName of cleanClubs) {
    const hit = matches.find(
      (match) =>
        isClubLikelySame(clubName, match.club) ||
        isClubLikelySame(clubName, match.home_team) ||
        isClubLikelySame(clubName, match.away_team)
    );
    if (hit) {
      map.set(clubName, hit);
    }
  }
  return map;
};

const buildCountryRegex = (country) => {
  const aliases = getCountryAliases(country);
  if (!aliases.length) {
    return /$^/i;
  }
  return new RegExp(aliases.map((alias) => escapeRegex(alias)).join("|"), "i");
};

const buildSupabaseCountryOrClause = (country) => {
  const aliases = getCountryAliases(country);
  const countryCode = getCountryCode(country);
  if (countryCode) {
    return [`nationality_code.eq.${countryCode}`];
  }
  return aliases.map((alias) => `nationality.ilike.%${alias}%`);
};

const getPlayerKey = (row) => String(row?.id || row?.player_id || row?.transfermarkt_id || "").trim();

const getPlayerNationalTeamCountryMap = async (players = []) => {
  const playerIds = [...new Set(players.map(getPlayerKey).filter(Boolean))];
  if (!playerIds.length || !hasSupabaseConfig()) {
    return new Map();
  }

  try {
    const supabase = getSupabaseClient();
    const linkCols = await buildSelectFromAvailable("national_team_players", ["player_id", "national_team_id"]);
    const teamCols = await buildSelectFromAvailable("national_teams", ["id", "country_code"]);
    if (!linkCols.columns.length || !teamCols.columns.length) {
      return new Map();
    }

    const linkRows = [];
    const chunkSize = 150;
    for (let index = 0; index < playerIds.length; index += chunkSize) {
      const chunk = playerIds.slice(index, index + chunkSize);
      const { data, error } = await supabase
        .from("national_team_players")
        .select(linkCols.select)
        .in("player_id", chunk);
      if (error) {
        throw new Error(error.message);
      }
      if (Array.isArray(data)) {
        linkRows.push(...data);
      }
    }

    const teamIds = [...new Set(linkRows.map((row) => row.national_team_id).filter(Boolean))];
    if (!teamIds.length) {
      return new Map();
    }

    const { data: teamsData, error: teamsError } = await supabase
      .from("national_teams")
      .select(teamCols.select)
      .in("id", teamIds);
    if (teamsError) {
      throw new Error(teamsError.message);
    }

    const teamCodeMap = new Map();
    for (const team of Array.isArray(teamsData) ? teamsData : []) {
      const code = String(team.country_code || "").toLowerCase().trim();
      if (team?.id && code) {
        teamCodeMap.set(String(team.id), code);
      }
    }

    const playerCountryMap = new Map();
    for (const row of linkRows) {
      const playerId = String(row.player_id || "").trim();
      const countryCode = teamCodeMap.get(String(row.national_team_id || ""));
      if (!playerId || !countryCode) {
        continue;
      }
      if (!playerCountryMap.has(playerId)) {
        playerCountryMap.set(playerId, new Set());
      }
      playerCountryMap.get(playerId).add(countryCode);
    }

    return playerCountryMap;
  } catch (_error) {
    return new Map();
  }
};

const applyDiasporaFilter = (rows, country, diaspora) => {
  if (!diaspora) {
    return rows;
  }

  const aliases = new Set(getCountryAliases(country));
  const matchesCountryToken = (value) => aliases.has(normalizeText(value));

  return rows.filter((row) => {
    const dual = Array.isArray(row.dual_nationality) ? row.dual_nationality : [];
    const origin = Array.isArray(row.origin_countries) ? row.origin_countries : [];
    return [...dual, ...origin].some(matchesCountryToken);
  });
};

const dedupePlayers = (rows) => {
  const seen = new Set();
  const output = [];
  for (const row of rows) {
    const key = `${normalizeText(row.name)}|${String(row.date_of_birth || row.dob || "").slice(0, 10)}|${normalizeText(
      row.current_club || row.club || ""
    )}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(row);
  }
  return output;
};

const queryPlayersByCountryUnified = async ({
  supabase,
  countryInput,
  select,
  availableColumns,
  limit,
  orderByTopScore = false
}) => {
  const normalizedCountry = normalizeCountryInput(countryInput);
  const inputText = String(countryInput || "").trim();
  const inputCode = inputText.length === 2 ? inputText.toLowerCase() : "";
  const mappedCode = String(getCountryCode(normalizedCountry) || "").toLowerCase();
  const countryCode = inputCode || mappedCode;

  const applyBaseFilters = (query) => {
    let q = query;
    if (availableColumns.has("is_active")) {
      q = q.eq("is_active", true);
    }
    if (orderByTopScore && availableColumns.has("top_score")) {
      q = q.order("top_score", { ascending: false });
    } else if (availableColumns.has("market_value")) {
      q = q.order("market_value", { ascending: false, nullsFirst: false });
    }
    return q.limit(limit);
  };

  if (countryCode && availableColumns.has("nationality_code")) {
    console.log(`[players-query] strategy=nationality_code country=${countryInput} code=${countryCode}`);
    const byCode = applyBaseFilters(
      supabase.from("players").select(select || "id,name").eq("nationality_code", countryCode)
    );
    const { data, error } = await byCode;
    if (error) {
      throw new Error(error.message);
    }
    if (Array.isArray(data) && data.length > 0) {
      return data;
    }
  }

  console.log(`[players-query] strategy=nationality_ilike country=${countryInput}`);
  const fallback = applyBaseFilters(
    supabase
      .from("players")
      .select(select || "id,name")
      .ilike("nationality", `%${normalizedCountry || inputText}%`)
  );
  const { data, error } = await fallback;
  if (error) {
    throw new Error(error.message);
  }
  return Array.isArray(data) ? data : [];
};

const getPrimaryNationality = (nationalityString) => {
  if (!nationalityString) return "";
  // Extract the first nationality if multiple are present (e.g., "Tunisia, France" -> "Tunisia")
  const primary = String(nationalityString).split(/[,;|]/)[0].trim();
  return normalizeCountryInput(primary);
};

const matchesNormalizedCountry = (row, normalizedCountry, options = {}) => {
  if (!normalizedCountry) {
    return true;
  }
  const { primaryOnly = false, nationalTeamCountryMap = null, preferNationalTeam = false } = options;
  const targetCountry = normalizeText(normalizedCountry);
  const targetCode = String(getCountryCode(normalizedCountry) || "").toLowerCase();

  const nationalTeamCodes = nationalTeamCountryMap?.get?.(getPlayerKey(row)) || null;
  if (preferNationalTeam && nationalTeamCodes?.size) {
    return targetCode ? nationalTeamCodes.has(targetCode) : false;
  }

  const rowNationality = primaryOnly
    ? getPrimaryNationality(row?.nationality || "")
    : normalizeCountryInput(row?.nationality || "");
  const rowCountry = normalizeText(rowNationality);
  const rowCode = String(row?.nationality_code || "").toLowerCase();
  return rowCountry === targetCountry || (targetCode && rowCode === targetCode);
};

const getClubAndCountryMaps = async (players) => {
  if (!hasSupabaseConfig()) {
    return { clubMap: new Map(), countryMap: new Map() };
  }
  const supabase = getSupabaseClient();
  const clubMap = new Map();
  const countryMap = new Map();

  const clubIds = [...new Set(players.map((p) => p.club_id).filter(Boolean))];
  if (clubIds.length) {
    try {
      const clubCols = await buildSelectFromAvailable("clubs", [
        "id",
        "name",
        "logo_url",
        "transfermarkt_id",
        "country_code"
      ]);
      if (clubCols.columns.length) {
        const { data } = await supabase.from("clubs").select(clubCols.select).in("id", clubIds);
        for (const club of Array.isArray(data) ? data : []) {
          clubMap.set(String(club.id), club);
        }
      }
    } catch (_error) {
      // fallback silently
    }
  }

  const countryCodes = [...new Set(players.map((p) => String(p.nationality_code || "").toLowerCase()).filter(Boolean))];
  if (countryCodes.length) {
    try {
      const countryCols = await buildSelectFromAvailable("countries", ["code", "name", "flag_url"]);
      if (countryCols.columns.length) {
        const { data } = await supabase.from("countries").select(countryCols.select).in("code", countryCodes);
        for (const country of Array.isArray(data) ? data : []) {
          countryMap.set(String(country.code || "").toLowerCase(), country);
        }
      }
    } catch (_error) {
      // fallback silently
    }
  }

  return { clubMap, countryMap };
};

const mapPlayerListItem = async (player, nextMatchMap, options = {}, context = {}) => {
  const clubFromTable = context.clubMap?.get?.(String(player.club_id || "")) || null;
  const countryFromTable =
    context.countryMap?.get?.(String(player.nationality_code || "").toLowerCase()) || null;
  const allowRemote = options.allowRemote === true || ALLOW_REMOTE_IMAGES;
  const imageUrl = await resolveImageUrl(player, { allowRemote });
  const clubName = cleanText(clubFromTable?.name || player.current_club || player.club || "Unknown Club");
  const tmClubLogo =
    Number(clubFromTable?.transfermarkt_id || 0) > 0
      ? `https://tmssl.akamaized.net/images/wappen/normal/${clubFromTable.transfermarkt_id}.png`
      : "";
  const clubLogoUrl = clubFromTable?.logo_url || tmClubLogo || (await resolveClubLogo(clubName, { allowRemote }));
  const nextMatch = nextMatchMap.get(clubName) || null;
  const countryName = normalizeCountryInput(countryFromTable?.name || player.nationality || "");
  const countryCode = String(player.nationality_code || countryFromTable?.code || getCountryCode(countryName) || "un")
    .toLowerCase()
    .trim();
  const flagUrl = countryFromTable?.flag_url || getFlagUrl(countryCode);
  const image = imageUrl || avatar(player.name);
  const position = cleanText(player.position || "Unknown");

  return {
    id: player.id,
    transfermarkt_id: player.transfermarkt_id || null,
    name: cleanPlayerName(player.name || "Unknown Player"),
    club: clubName,
    current_club: clubName,
    position,
    image,
    image_url: image,
    flag: flagUrl,
    flag_url: flagUrl,
    club_logo_url: clubLogoUrl,
    club_data: {
      id: clubFromTable?.id || player.club_id || null,
      name: clubName,
      logo_url: clubLogoUrl
    },
    country: {
      code: countryCode || "un",
      name: countryName,
      flag_url: flagUrl
    },
    player_status: String(player.player_status || "active"),
    matches_2025: Number(player.matches_2025 || 0),
    goals_2025: Number(player.goals_2025 || 0),
    assists_2025: Number(player.assists_2025 || 0),
    rating_2025: Number(player.rating_2025 || 0),
    form_score: Number(player.form_score || 0),
    top_score: Number(player.top_score || 0),
    market_value: Number(player.market_value || 0),
    last_seen_at: player.last_seen_at || null,
    last_match_date: player.last_match_date || null,
    next_match: mapNextMatch(nextMatch)
  };
};

const listCountriesFromSupabase = async () => {
  const supabase = getSupabaseClient();
  try {
    const countryTableCols = await buildSelectFromAvailable("countries", [
      "name",
      "code",
      "iso_code",
      "player_count",
      "flag_url"
    ]);
    if (countryTableCols.columns.length && countryTableCols.available.has("name")) {
      const codeColumn = countryTableCols.available.has("code") ? "code" : "iso_code";
      if (codeColumn) {
        let countriesQuery = supabase.from("countries").select(countryTableCols.select);
        if (countryTableCols.available.has("player_count")) {
          countriesQuery = countriesQuery.order("player_count", {
            ascending: false,
            nullsFirst: false
          });
        }
        const { data, error } = await countriesQuery;
        if (!error && Array.isArray(data) && data.length) {
          const hasPlayerCountColumn = countryTableCols.available.has("player_count");
          const mapped = data
            .map((row) => {
              const countryName = normalizeCountryInput(row?.name || "");
              const rawCode = String(row?.[codeColumn] || "").trim().toLowerCase();
              const code = rawCode || String(getCountryCode(countryName) || "un").toLowerCase();
              if (!countryName || code === "un" || !/^[a-z]{2}$/.test(code)) {
                return null;
              }
              return {
                name: countryName,
                code,
                player_count: hasPlayerCountColumn ? Number(row?.player_count || 0) : null,
                flag_url: safeImageUrl(row?.flag_url) || getFlagUrl(code)
              };
            })
            .filter(Boolean)
            .sort((a, b) => Number(b.player_count || 0) - Number(a.player_count || 0));

          if (!hasPlayerCountColumn && mapped.length) {
            const playersCols = await detectTableColumns("players", ["id", "nationality_code", "is_active"]);
            const countByCode = new Map();
            await Promise.all(
              mapped.map((country) =>
                hydrateLimit(async () => {
                  let q = supabase
                    .from("players")
                    .select(playersCols.has("id") ? "id" : "nationality_code", {
                      count: "exact",
                      head: true
                    })
                    .eq("nationality_code", country.code);
                  if (playersCols.has("is_active")) {
                    q = q.eq("is_active", true);
                  }
                  const { count, error: countError } = await q;
                  if (!countError) {
                    countByCode.set(country.code, Number(count || 0));
                  } else {
                    countByCode.set(country.code, 0);
                  }
                })
              )
            );
            for (const country of mapped) {
              country.player_count = Number(countByCode.get(country.code) || 0);
            }
            mapped.sort((a, b) => b.player_count - a.player_count);
          }

          if (mapped.length >= 10) {
            return mapped;
          }
        }
      }
    }
  } catch (_error) {
    // fallback to players scan below
  }

  const countryCols = await buildSelectFromAvailable("players", [
    "nationality",
    "nationality_code",
    "is_active"
  ]);
  const pageSize = 1000;
  let from = 0;
  let done = false;
  const allRows = [];
  while (!done) {
    let pageQuery = supabase
      .from("players")
      .select(countryCols.select || "nationality,nationality_code")
      .range(from, from + pageSize - 1);
    if (countryCols.available.has("is_active")) {
      pageQuery = pageQuery.eq("is_active", true);
    }
    const { data, error } = await pageQuery;
    if (error) {
      throw new Error(error.message);
    }
    const rows = Array.isArray(data) ? data : [];
    if (!rows.length) {
      done = true;
      continue;
    }
    allRows.push(...rows);
    from += pageSize;
    if (rows.length < pageSize) {
      done = true;
    }
  }

  const filtered = allRows.filter((row) => String(row.nationality || "").trim() !== "");
  const grouped = new Map();

  for (const row of filtered) {
    const countryName = normalizeCountryInput(row.nationality);
    if (!countryName) {
      continue;
    }
    const key = normalizeText(countryName);
    const code = String(row.nationality_code || getCountryCode(countryName) || "")
      .trim()
      .toLowerCase() || "un";
    if (code === "un") {
      continue;
    }
    const item = grouped.get(key) || {
      name: countryName,
      code,
      player_count: 0,
      flag_url: getFlagUrl(code)
    };
    item.player_count += 1;
    grouped.set(key, item);
  }

  return [...grouped.values()].sort((a, b) => b.player_count - a.player_count);
};

const getPlayersByCountryFromSupabase = async (country, statusFilter, diaspora = false) => {
  const normalizedCountry = normalizeCountryInput(country);
  const supabase = getSupabaseClient();
  const listCandidates = [
    "id",
    "transfermarkt_id",
    "name",
    "club",
    "club_id",
    "current_club",
    "position",
    "image",
    "image_url",
    "photo",
    "nationality",
    "nationality_code",
    "player_status",
    "data_sources",
    "sources",
    "is_active",
    "last_season",
    "is_national_team_player",
    "form_score",
    "top_score",
    "market_value",
    "matches_2025",
    "goals_2025",
    "assists_2025",
    "rating_2025",
    "last_seen_at",
    "last_match_date",
    "dual_nationality",
    "origin_countries"
  ];
  const listSelect = await buildSelectFromAvailable("players", listCandidates);

  let players = await queryPlayersByCountryUnified({
    supabase,
    countryInput: country,
    select: listSelect.select,
    availableColumns: listSelect.available,
    limit: PLAYERS_PER_COUNTRY_LIMIT * 3,
    orderByTopScore: false
  });
  const nationalTeamCountryMap = await getPlayerNationalTeamCountryMap(players);
  players = applyDiasporaFilter(players, normalizedCountry, diaspora).filter((row) => {
    if (
      !matchesNormalizedCountry(row, normalizedCountry, {
        primaryOnly: !diaspora,
        nationalTeamCountryMap,
        preferNationalTeam: !diaspora
      })
    ) {
      return false;
    }
    if (!isEligiblePlayer(row)) {
      return false;
    }
    if (statusFilter && String(row.player_status || "active") !== statusFilter) {
      return false;
    }
    return true;
  });
  players = dedupePlayers(players);

  players.sort(sortByStatusThenActivity);
  players = players.slice(0, PLAYERS_PER_COUNTRY_LIMIT);
  const clubMap = new Map();
  const countryMap = new Map();
  const nextMatchMap = new Map();
  const hydrated = await Promise.all(
    players.map((player) =>
      hydrateLimit(() =>
        mapPlayerListItem(player, nextMatchMap, { allowRemote: ALLOW_REMOTE_IMAGES }, { clubMap, countryMap })
      )
    )
  );

  return {
    players: hydrated,
    total: hydrated.length
  };
};

const getTopPlayersByCountryFromSupabase = async (country, diaspora = false) => {
  const normalizedCountry = normalizeCountryInput(country);
  const supabase = getSupabaseClient();
  const topCandidates = [
    "id",
    "transfermarkt_id",
    "name",
    "club",
    "club_id",
    "current_club",
    "position",
    "image",
    "image_url",
    "photo",
    "nationality",
    "nationality_code",
    "player_status",
    "data_sources",
    "sources",
    "is_active",
    "last_season",
    "is_national_team_player",
    "form_score",
    "top_score",
    "market_value",
    "matches_2025",
    "goals_2025",
    "assists_2025",
    "rating_2025",
    "last_seen_at",
    "last_match_date",
    "dual_nationality",
    "origin_countries"
  ];
  const topSelect = await buildSelectFromAvailable("players", topCandidates);

  let players = await queryPlayersByCountryUnified({
    supabase,
    countryInput: country,
    select: topSelect.select,
    availableColumns: topSelect.available,
    limit: TOP_PLAYERS_LIMIT * 5,
    orderByTopScore: true
  });
  const nationalTeamCountryMap = await getPlayerNationalTeamCountryMap(players);
  players = applyDiasporaFilter(players, normalizedCountry, diaspora)
    .filter((row) =>
      matchesNormalizedCountry(row, normalizedCountry, {
        primaryOnly: !diaspora,
        nationalTeamCountryMap,
        preferNationalTeam: !diaspora
      })
    )
    .filter(isEligiblePlayer);
  players = dedupePlayers(players);
  players.sort((a, b) => {
    const scoreDelta = Number(b.top_score || 0) - Number(a.top_score || 0);
    if (scoreDelta !== 0) {
      return scoreDelta;
    }
    return sortByStatusThenActivity(a, b);
  });
  players = players.slice(0, TOP_PLAYERS_LIMIT);
  const clubMap = new Map();
  const countryMap = new Map();

  const nextMatchMap = new Map();
  const hydrated = await Promise.all(
    players.map((player) =>
      hydrateLimit(() =>
        mapPlayerListItem(player, nextMatchMap, { allowRemote: ALLOW_REMOTE_IMAGES }, { clubMap, countryMap })
      )
    )
  );

  return {
    players: hydrated,
    total: hydrated.length
  };
};

const getCalendarByCountryFromSupabase = async (country, diaspora = false) => {
  const normalizedCountry = normalizeCountryInput(country);
  const supabase = getSupabaseClient();
  const countryOrClause = buildSupabaseCountryOrClause(normalizedCountry);
  const calendarCandidates = [
    "id",
    "club",
    "club_id",
    "current_club",
    "nationality",
    "nationality_code",
    "player_status",
    "is_active",
    "last_season",
    "is_national_team_player",
    "dual_nationality",
    "origin_countries"
  ];
  const calendarSelect = await buildSelectFromAvailable("players", calendarCandidates);
  const cacheKey = `calendar:${normalizeText(normalizedCountry)}:${diaspora ? "diaspora" : "base"}`;
  const cached = matchCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  let playersQuery = supabase
    .from("players")
    .select(calendarSelect.select || "club,current_club,nationality")
    .limit(300);
  if (calendarSelect.available.has("is_active")) {
    playersQuery = playersQuery.eq("is_active", true);
  }

  const playersResponse = countryOrClause.length
    ? await playersQuery.or(countryOrClause.join(","))
    : await playersQuery;

  if (playersResponse.error) {
    throw new Error(playersResponse.error.message);
  }

  let players = Array.isArray(playersResponse.data) ? playersResponse.data : [];
  const nationalTeamCountryMap = await getPlayerNationalTeamCountryMap(players);
  players = applyDiasporaFilter(players, normalizedCountry, diaspora)
    .filter((row) =>
      matchesNormalizedCountry(row, normalizedCountry, {
        primaryOnly: !diaspora,
        nationalTeamCountryMap,
        preferNationalTeam: !diaspora
      })
    )
    .filter(isEligiblePlayer);
  const uniqueClubs = [...new Set(players.map((row) => row.current_club || row.club).filter(Boolean))];
  const matches = await getMatchesByClubsWithinWindow(uniqueClubs, CALENDAR_WINDOW_DAYS);

  const output = matches
    .map((match) => ({
      id: match.id,
      club: match.club,
      opponent: match.opponent,
      match_date: match.match_date,
      competition: match.competition,
      home_away: match.home_away,
      status: match.status,
      stadium: match.stadium
    }))
    .sort((a, b) => parseDateMs(a.match_date) - parseDateMs(b.match_date));

  matchCache.set(cacheKey, output, MATCH_CACHE_TTL_SECONDS);
  return output;
};

const getCalendarMatchesGroupedFromSupabase = async ({ country = "", club = "", diaspora = false } = {}) => {
  const normalizedCountry = normalizeCountryInput(country);
  const supabase = getSupabaseClient();
  const cacheKey = `calendar-grouped:${normalizeText(normalizedCountry)}:${normalizeText(club)}:${diaspora ? "1" : "0"}`;
  const cached = matchCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  let clubFilter = [];
  if (club) {
    clubFilter = [String(club).trim()];
  } else if (normalizedCountry) {
    const countryOrClause = buildSupabaseCountryOrClause(normalizedCountry);
    const playerColumns = await buildSelectFromAvailable("players", [
      "id",
      "club",
      "current_club",
      "nationality",
      "nationality_code",
      "is_active",
      "player_status",
      "last_season",
      "is_national_team_player",
      "dual_nationality",
      "origin_countries"
    ]);
    let playersQuery = supabase.from("players").select(playerColumns.select || "club,current_club,nationality");
    if (playerColumns.available.has("is_active")) {
      playersQuery = playersQuery.eq("is_active", true);
    }
    const playersResponse = countryOrClause.length
      ? await playersQuery.or(countryOrClause.join(",")).limit(300)
      : await playersQuery.limit(300);

    if (playersResponse.error) {
      throw new Error(playersResponse.error.message);
    }
    const players = applyDiasporaFilter(
      Array.isArray(playersResponse.data) ? playersResponse.data : [],
      normalizedCountry,
      diaspora
    );
    const nationalTeamCountryMap = await getPlayerNationalTeamCountryMap(players);
    clubFilter = [
      ...new Set(
        players
          .filter((row) =>
            matchesNormalizedCountry(row, normalizedCountry, {
              primaryOnly: !diaspora,
              nationalTeamCountryMap,
              preferNationalTeam: !diaspora
            })
          )
          .filter(isEligiblePlayer)
          .map((row) => row.current_club || row.club)
          .filter(Boolean)
      )
    ];
  }

  const now = new Date();
  const fromDate = now < MATCH_MIN_DATE ? MATCH_MIN_DATE : now;
  const toDate = clampDateRangeEnd(CALENDAR_PAGE_WINDOW_DAYS);
  const matchColumns = await buildSelectFromAvailable("matches", [
    "id",
    "home_club_id",
    "away_club_id",
    "club",
    "opponent",
    "home_team",
    "away_team",
    "match_date",
    "competition",
    "home_away",
    "home_score",
    "away_score",
    "stadium",
    "status"
  ]);
  let query = supabase
    .from("matches")
    .select(matchColumns.select || "id,club,opponent,match_date,competition")
    .gte("match_date", fromDate.toISOString())
    .lte("match_date", toDate.toISOString())
    .order("match_date", { ascending: true })
    .limit(2000);

  if (clubFilter.length) {
    if (matchColumns.available.has("club")) {
      query = query.in("club", clubFilter);
    } else if (matchColumns.available.has("home_team") || matchColumns.available.has("away_team")) {
      const clauses = [];
      if (matchColumns.available.has("home_team")) {
        clauses.push(...clubFilter.map((name) => `home_team.eq.${name}`));
      }
      if (matchColumns.available.has("away_team")) {
        clauses.push(...clubFilter.map((name) => `away_team.eq.${name}`));
      }
      if (clauses.length) {
        query = query.or(clauses.join(","));
      }
    }
  }

  const { data, error } = await query;
  if (error) {
    throw new Error(error.message);
  }

  const grouped = {};
  const rows = (Array.isArray(data) ? data : []).map(normalizeMatchRow);
  const clubMapsById = new Map();
  if (hasSupabaseConfig()) {
    const homeIds = rows.map((r) => r.home_club_id).filter(Boolean);
    const awayIds = rows.map((r) => r.away_club_id).filter(Boolean);
    const clubIds = [...new Set([...homeIds, ...awayIds].map(String))];
    if (clubIds.length) {
      try {
        const clubCols = await buildSelectFromAvailable("clubs", ["id", "name", "logo_url"]);
        if (clubCols.columns.length) {
          const { data: clubsData } = await supabase.from("clubs").select(clubCols.select).in("id", clubIds);
          for (const club of Array.isArray(clubsData) ? clubsData : []) {
            clubMapsById.set(String(club.id), club);
          }
        }
      } catch (_error) {
        // silent fallback
      }
    }
  }

  const logoNameSet = new Set();
  for (const row of rows) {
    if (!clubMapsById.get(String(row.home_club_id || ""))) {
      logoNameSet.add(cleanText(row.home_team || row.club || ""));
    }
    if (!clubMapsById.get(String(row.away_club_id || ""))) {
      logoNameSet.add(cleanText(row.away_team || row.opponent || ""));
    }
  }

  const logoMap = new Map();
  await Promise.all(
    [...logoNameSet]
      .filter(Boolean)
      .map((teamName) =>
        hydrateLimit(async () => {
          const logo = await resolveClubLogo(teamName, { allowRemote: ALLOW_REMOTE_IMAGES });
          logoMap.set(teamName, logo);
        })
      )
  );

  for (const row of rows) {
    const homeClub = clubMapsById.get(String(row.home_club_id || "")) || null;
    const awayClub = clubMapsById.get(String(row.away_club_id || "")) || null;
    const homeName = cleanText(row.home_team || row.club || "");
    const awayName = cleanText(row.away_team || row.opponent || "");
    row.home_club_logo = homeClub?.logo_url || logoMap.get(homeName) || avatar(homeName || "Club");
    row.away_club_logo = awayClub?.logo_url || logoMap.get(awayName) || avatar(awayName || "Club");
    const key = String(row.match_date || "").slice(0, 10);
    if (!key) {
      continue;
    }
    if (!grouped[key]) {
      grouped[key] = [];
    }
    grouped[key].push(row);
  }

  matchCache.set(cacheKey, grouped, MATCH_CACHE_TTL_SECONDS);
  return grouped;
};

const getPlayerByIdFromSupabase = async (id) => {
  const supabase = getSupabaseClient();
  const detailCandidates = [
    "id",
    "transfermarkt_id",
    "api_football_id",
    "name",
    "nationality",
    "nationality_code",
    "club",
    "current_club",
    "position",
    "club_id",
    "date_of_birth",
    "dob",
    "image",
    "image_url",
    "photo",
    "player_status",
    "is_active",
    "data_sources",
    "sources",
    "last_season",
    "is_national_team_player",
    "form_score",
    "top_score",
    "market_value",
    "goals_2025",
    "assists_2025",
    "matches_2025",
    "rating_2025",
    "dual_nationality",
    "origin_countries",
    "last_seen_at",
    "last_match_date"
  ];
  const detailSelect = await buildSelectFromAvailable("players", detailCandidates);
  const idClauses = ["id.eq." + id];
  if (detailSelect.available.has("transfermarkt_id")) {
    idClauses.push("transfermarkt_id.eq." + id);
  }
  if (detailSelect.available.has("api_football_id")) {
    idClauses.push("api_football_id.eq." + id);
  }

  const { data, error } = await supabase
    .from("players")
    .select(detailSelect.select || "id,name")
    .or(idClauses.join(","))
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(error.message);
  }
  if (!data || !isEligiblePlayer(data)) {
    return null;
  }

  const clubName = data.current_club || data.club || "";
  const nextMatchMap = await buildNextMatchMapForClubs([clubName]);
  const imageUrl = await resolveImageUrl(data, { allowRemote: ALLOW_REMOTE_IMAGES });
  const { clubMap, countryMap } = await getClubAndCountryMaps([data]);
  const clubFromTable = clubMap.get(String(data.club_id || "")) || null;
  const countryFromTable = countryMap.get(String(data.nationality_code || "").toLowerCase()) || null;
  const resolvedClubName = cleanText(clubFromTable?.name || clubName || "Unknown Club");
  const tmClubLogo =
    Number(clubFromTable?.transfermarkt_id || 0) > 0
      ? `https://tmssl.akamaized.net/images/wappen/normal/${clubFromTable.transfermarkt_id}.png`
      : "";
  const clubLogoUrl =
    clubFromTable?.logo_url || tmClubLogo || (await resolveClubLogo(resolvedClubName, { allowRemote: ALLOW_REMOTE_IMAGES }));
  const countryName = normalizeCountryInput(countryFromTable?.name || data.nationality || "");
  const countryCode = String(data.nationality_code || countryFromTable?.code || getCountryCode(countryName) || "un")
    .toLowerCase()
    .trim();
  const flagUrl = countryFromTable?.flag_url || getFlagUrl(countryCode);
  const image = imageUrl || avatar(data.name);

  return {
    id: data.id,
    transfermarkt_id: data.transfermarkt_id || null,
    name: cleanPlayerName(data.name || "Unknown Player"),
    club: resolvedClubName,
    current_club: resolvedClubName,
    position: cleanText(data.position || "Unknown"),
    age: getAgeFromDate(data.date_of_birth || data.dob),
    image,
    image_url: image,
    photo: image,
    flag_url: flagUrl,
    club_logo_url: clubLogoUrl,
    country: {
      code: countryCode || "un",
      name: countryName,
      flag_url: flagUrl
    },
    nationality: countryName,
    nationality_code: countryCode || "un",
    primary_nationality: getPrimaryNationality(data.nationality || countryName || ""),
    dual_nationality: Array.isArray(data.dual_nationality) ? data.dual_nationality : [],
    origin_countries: Array.isArray(data.origin_countries) ? data.origin_countries : [],
    club_data: {
      id: clubFromTable?.id || data.club_id || null,
      name: resolvedClubName || "Unknown Club",
      logo_url: clubLogoUrl
    },
    player_status: data.player_status || "active",
    market_value: Number(data.market_value || 0),
    form_score: Number(data.form_score || 0),
    top_score: Number(data.top_score || 0),
    goals_2025: Number(data.goals_2025 || 0),
    assists_2025: Number(data.assists_2025 || 0),
    matches_2025: Number(data.matches_2025 || 0),
    rating_2025: Number(data.rating_2025 || 0),
    data_sources: normalizeDataSources(data),
    transfermarkt_url:
      Number(data.transfermarkt_id || 0) > 0
        ? `https://www.transfermarkt.com/-/profil/spieler/${data.transfermarkt_id}`
        : "",
    last_seen_at: data.last_seen_at || null,
    last_match_date: data.last_match_date || null,
    next_match: mapNextMatch(nextMatchMap.get(resolvedClubName) || nextMatchMap.get(clubName)),
    matches: []
  };
};

const getMatchesByClubFromSupabase = async (clubName) => {
  const cleanClub = String(clubName || "").trim();
  if (!cleanClub) {
    return [];
  }

  const matches = await getMatchesByClubsWithinWindow([cleanClub], NEXT_MATCH_WINDOW_DAYS);
  return matches
    .filter((match) => {
      return (
        isClubLikelySame(cleanClub, match.club) ||
        isClubLikelySame(cleanClub, match.home_team) ||
        isClubLikelySame(cleanClub, match.away_team)
      );
    })
    .sort((a, b) => parseDateMs(a.match_date) - parseDateMs(b.match_date));
};

const getPlayerNextMatchFromSupabase = async (id) => {
  const player = await getPlayerByIdFromSupabase(id);
  if (!player) {
    return null;
  }
  return mapNextMatch(player.next_match);
};

const getNationalTeamByCountryFromSupabase = async (country) => {
  const supabase = getSupabaseClient();
  const normalizedCountry = normalizeCountryInput(country);
  const code = String(getCountryCode(normalizedCountry) || normalizedCountry || "")
    .toLowerCase()
    .trim();
  if (!code) {
    return { country_code: "", team: null, players: [] };
  }

  const teamCols = await buildSelectFromAvailable("national_teams", [
    "id",
    "country_code",
    "name",
    "transfermarkt_id"
  ]);
  if (!teamCols.columns.length) {
    return { country_code: code, team: null, players: [] };
  }

  const { data: team, error: teamError } = await supabase
    .from("national_teams")
    .select(teamCols.select)
    .eq("country_code", code)
    .limit(1)
    .maybeSingle();
  if (teamError || !team) {
    return { country_code: code, team: null, players: [] };
  }

  const linkCols = await buildSelectFromAvailable("national_team_players", ["player_id", "national_team_id"]);
  if (!linkCols.columns.length) {
    return { country_code: code, team, players: [] };
  }
  const { data: links } = await supabase
    .from("national_team_players")
    .select(linkCols.select)
    .eq("national_team_id", team.id)
    .limit(300);
  const playerIds = [...new Set((Array.isArray(links) ? links : []).map((r) => r.player_id).filter(Boolean))];
  if (!playerIds.length) {
    return { country_code: code, team, players: [] };
  }

  const playerCols = await buildSelectFromAvailable("players", [
    "id",
    "transfermarkt_id",
    "name",
    "current_club",
    "club",
    "position",
    "image_url",
    "nationality_code",
    "club_id"
  ]);
  const { data: playersData } = await supabase
    .from("players")
    .select(playerCols.select || "id,name,current_club,club,position,image_url,nationality_code,club_id")
    .in("id", playerIds);
  let players = Array.isArray(playersData) ? playersData : [];
  const { clubMap, countryMap } = await getClubAndCountryMaps(players);
  const nextMatchMap = await buildNextMatchMapForClubs(players.map((p) => p.current_club || p.club).filter(Boolean));
  const hydrated = await Promise.all(
    players.map((player) =>
      hydrateLimit(() =>
        mapPlayerListItem(player, nextMatchMap, { allowRemote: ALLOW_REMOTE_IMAGES }, { clubMap, countryMap })
      )
    )
  );
  return { country_code: code, team, players: hydrated };
};

const getPlayersByCountryMongo = async (country, statusFilter) => {
  const regex = buildCountryRegex(country);
  const query = {
    is_active: { $ne: false },
    $or: [{ nationality: regex }, { nationality_code: regex }]
  };
  if (statusFilter) {
    query.player_status = statusFilter;
  }

  let players = await Player.find(query).sort({ top_score: -1, name: 1 }).limit(PLAYERS_PER_COUNTRY_LIMIT).lean();
  players = players.filter(isEligiblePlayer).sort(sortByStatusThenActivity);

  const nextMatchMap = new Map();
  const hydrated = await Promise.all(
    players.map((player) =>
      hydrateLimit(() => mapPlayerListItem(player, nextMatchMap, { allowRemote: ALLOW_REMOTE_IMAGES }))
    )
  );
  return { players: hydrated, total: hydrated.length };
};

const getTopPlayersByCountryMongo = async (country) => {
  const regex = buildCountryRegex(country);
  const query = {
    is_active: { $ne: false },
    $or: [{ nationality: regex }, { nationality_code: regex }]
  };

  const players = await Player.find(query).sort({ top_score: -1 }).limit(TOP_PLAYERS_LIMIT).lean();
  const eligible = players.filter(isEligiblePlayer);
  const nextMatchMap = new Map();
  const hydrated = await Promise.all(
    eligible.map((player) =>
      hydrateLimit(() => mapPlayerListItem(player, nextMatchMap, { allowRemote: ALLOW_REMOTE_IMAGES }))
    )
  );
  return { players: hydrated, total: hydrated.length };
};

const getCalendarByCountryMongo = async () => [];
const getMatchesCalendarMongo = async () => ({});
const getNationalTeamByCountryMongo = async (country) => ({
  country_code: String(getCountryCode(country) || country || "")
    .toLowerCase()
    .trim(),
  team: null,
  players: []
});

const getPlayersByCountry = async (country, statusFilter = "", diaspora = false) => {
  try {
    const normalizedCountry = normalizeCountryInput(country);
    const cacheKey = `players:${normalizeText(normalizedCountry)}:${String(statusFilter || "all").toLowerCase()}:${diaspora ? "1" : "0"}`;
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    let result;
    if (hasSupabaseConfig()) {
      result = await getPlayersByCountryFromSupabase(normalizedCountry, statusFilter, diaspora);
    } else {
      result = await getPlayersByCountryMongo(normalizedCountry, statusFilter);
    }
    apiResponseCache.set(cacheKey, result, 3600);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getPlayersByCountry", error);
    return { players: [], total: 0 };
  }
};

const getTopPlayersByCountry = async (country, diaspora = false) => {
  try {
    const normalizedCountry = normalizeCountryInput(country);
    const cacheKey = `top:${normalizeText(normalizedCountry)}:${diaspora ? "1" : "0"}`;
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    let result;
    if (hasSupabaseConfig()) {
      result = await getTopPlayersByCountryFromSupabase(normalizedCountry, diaspora);
    } else {
      result = await getTopPlayersByCountryMongo(normalizedCountry);
    }
    apiResponseCache.set(cacheKey, result);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getTopPlayersByCountry", error);
    return { players: [], total: 0 };
  }
};

const getCalendarByCountry = async (country, diaspora = false) => {
  try {
    const normalizedCountry = normalizeCountryInput(country);
    const cacheKey = `calendar-country:${normalizeText(normalizedCountry)}:${diaspora ? "1" : "0"}`;
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    let result;
    if (hasSupabaseConfig()) {
      result = await getCalendarByCountryFromSupabase(normalizedCountry, diaspora);
    } else {
      result = await getCalendarByCountryMongo(normalizedCountry);
    }
    apiResponseCache.set(cacheKey, result);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getCalendarByCountry", error);
    return [];
  }
};

const getMatchesCalendar = async ({ country = "", club = "", diaspora = false } = {}) => {
  try {
    const normalizedCountry = normalizeCountryInput(country);
    const cacheKey = `matches-calendar:${normalizeText(normalizedCountry)}:${normalizeText(club)}:${diaspora ? "1" : "0"}`;
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    let result;
    if (hasSupabaseConfig()) {
      result = await getCalendarMatchesGroupedFromSupabase({ country: normalizedCountry, club, diaspora });
    } else {
      result = await getMatchesCalendarMongo();
    }
    apiResponseCache.set(cacheKey, result);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getMatchesCalendar", error);
    return {};
  }
};

const countRecentMatchesForClub = async (clubName, days = 365) => {
  if (!hasSupabaseConfig() || !clubName) return 0;
  try {
    const supabase = getSupabaseClient();
    const now = new Date();
    const fromDate = new Date(now.getTime() - days * 24 * 60 * 60 * 1000).toISOString();
    const wanted = ["id", "club", "home_team", "away_team", "match_date", "opponent"];
    const available = await detectTableColumns("matches", wanted);
    const selectCols = wanted.filter((c) => available.has(c)).join(",") || "id,club,match_date";
    const { data, error } = await supabase
      .from("matches")
      .select(selectCols)
      .gte("match_date", fromDate)
      .limit(10000);
    if (error || !Array.isArray(data)) return 0;
    const needle = normalizeText(cleanText(clubName));
    const matches = data.filter((r) => {
      const club = normalizeText(cleanText(r.club || r.home_team || ""));
      const opp = normalizeText(cleanText(r.opponent || r.away_team || ""));
      return (club && club.includes(needle)) || (opp && opp.includes(needle)) || (needle && needle.includes(club)) || (needle && needle.includes(opp));
    });
    return matches.length;
  } catch (error) {
    return 0;
  }
};

const getPlayerById = async (id) => {
  try {
    const cacheKey = `player:${String(id)}`;
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }
    if (hasSupabaseConfig()) {
      const result = await getPlayerByIdFromSupabase(id);
      apiResponseCache.set(cacheKey, result, API_CACHE_TTL_SECONDS);
      return result;
    }

    const query = {
      is_active: { $ne: false },
      $or: [{ id: String(id) }, { transfermarkt_id: String(id) }, { api_football_id: String(id) }]
    };
    const player = await Player.findOne(query).lean();
    if (!player || !isEligiblePlayer(player)) {
      return null;
    }

    const imageUrl = await resolveImageUrl(player, { allowRemote: ALLOW_REMOTE_IMAGES });
    const clubLogoUrl = await resolveClubLogo(player.current_club || player.club, { allowRemote: ALLOW_REMOTE_IMAGES });
    const countryName = normalizeCountryInput(player.nationality || "");
    const countryCode = String(player.nationality_code || getCountryCode(countryName) || "un").toLowerCase();
    const flagUrl = getFlagUrl(countryCode);
    const clubName = cleanText(player.current_club || player.club || "Unknown Club");
    const image = imageUrl || avatar(player.name);
    const result = {
      id: player.id,
      name: cleanPlayerName(player.name || "Unknown Player"),
      club: clubName,
      current_club: clubName,
      position: cleanText(player.position || "Unknown"),
      age: getAgeFromDate(player.date_of_birth),
      image,
      image_url: image,
      photo: image,
      flag_url: flagUrl,
      club_logo_url: clubLogoUrl,
      country: {
        name: countryName,
        code: countryCode,
        flag_url: flagUrl
      },
      player_status: player.player_status || "active",
      form_score: Number(player.form_score || 0),
      top_score: Number(player.top_score || 0),
      goals_2025: Number(player.goals_2025 || 0),
      assists_2025: Number(player.assists_2025 || 0),
      matches_2025: Number(player.matches_2025 || 0),
      rating_2025: Number(player.rating_2025 || 0),
      last_seen_at: player.last_seen_at || null,
      last_match_date: player.last_match_date || null,
      next_match: null,
      matches: []
    };
    apiResponseCache.set(cacheKey, result, API_CACHE_TTL_SECONDS);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getPlayerById", error);
    return null;
  }
};

const getNationalTeamByCountry = async (country) => {
  try {
    if (hasSupabaseConfig()) {
      return await getNationalTeamByCountryFromSupabase(country);
    }
    return await getNationalTeamByCountryMongo(country);
  } catch (error) {
    console.error("SERVICE ERROR:getNationalTeamByCountry", error);
    return {
      country_code: String(getCountryCode(country) || country || "")
        .toLowerCase()
        .trim(),
      team: null,
      players: []
    };
  }
};

const getCountries = async () => {
  try {
    const cacheKey = "countries:active";
    const cached = apiResponseCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    let result;
    if (hasSupabaseConfig()) {
      result = await listCountriesFromSupabase();
    } else {
      const results = await Player.aggregate([
        {
          $match: {
            nationality: { $exists: true, $ne: "" },
            is_active: { $ne: false }
          }
        },
        {
          $group: {
            _id: "$nationality",
            count: { $sum: 1 }
          }
        },
        { $sort: { _id: 1 } }
      ]);

      result = results.map((item) => {
        const normalizedName = normalizeCountryInput(item._id);
        const countryCode = String(getCountryCode(normalizedName) || "un").toLowerCase();
        return {
          name: normalizedName || item._id,
          code: countryCode,
          player_count: item.count,
          flag_url: getFlagUrl(countryCode)
        };
      });
    }

    apiResponseCache.set(cacheKey, result);
    return result;
  } catch (error) {
    console.error("SERVICE ERROR:getCountries", error);
    return [];
  }
};

const getMatchesByClub = async (clubName) => {
  try {
    if (!hasSupabaseConfig()) {
      return [];
    }
    return await getMatchesByClubFromSupabase(clubName);
  } catch (error) {
    console.error("SERVICE ERROR:getMatchesByClub", error);
    return [];
  }
};

const getPlayerNextMatch = async (id) => {
  try {
    if (!hasSupabaseConfig()) {
      return null;
    }
    return await getPlayerNextMatchFromSupabase(id);
  } catch (error) {
    console.error("SERVICE ERROR:getPlayerNextMatch", error);
    return null;
  }
};

module.exports = {
  getPlayersByCountry,
  getTopPlayersByCountry,
  getCalendarByCountry,
  getMatchesCalendar,
  getMatchesByClub,
  getPlayerNextMatch,
  getPlayerById,
  getNationalTeamByCountry,
  getCountries,
  getFlagUrl,
  normalizeText,
  resolveImageUrl
};
