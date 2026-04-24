const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(
  path.resolve(__dirname, "..", "..", "backend", "package.json")
);
const dotenv = backendRequire("dotenv");
const axios = backendRequire("axios");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const MATCHES_TABLE = process.env.SUPABASE_MATCHES_TABLE || "matches";

const FOOTBALL_DATA_API_KEY = process.env.FOOTBALL_DATA_API_KEY || "";
const FOOTBALL_DATA_BASE_URL =
  process.env.FOOTBALL_DATA_BASE_URL || "https://api.football-data.org/v4";
const FOOTBALL_DATA_COMPETITIONS = String(
  process.env.FOOTBALL_DATA_COMPETITIONS || "PL,PD,SA,BL1,FL1,DED,PPL,ELC,CLI"
)
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

const SPORTSDB_BASE_URL =
  process.env.SPORTSDB_BASE_URL ||
  `https://www.thesportsdb.com/api/v1/json/${process.env.SPORTSDB_API_KEY || "123"}`;

const LOOKAHEAD_DAYS = Number(process.env.MATCHES_LOOKAHEAD_DAYS || 14);
const MIN_LOOKAHEAD_DAYS = Number(process.env.MATCHES_MIN_LOOKAHEAD_DAYS || 7);
const RATE_DELAY_MS = Number(process.env.MATCHES_RATE_DELAY_MS || 6000);
const MAX_FALLBACK_CLUBS = Number(process.env.MATCHES_FALLBACK_MAX_CLUBS || 20);
const WRITE_BATCH_SIZE = Number(process.env.MATCHES_WRITE_BATCH_SIZE || 300);

const MIN_SUPPORTED_DATE = new Date("2024-01-01T00:00:00.000Z");
const MAX_SUPPORTED_DATE = new Date("2026-12-31T23:59:59.999Z");

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const footballDataClient = axios.create({
  baseURL: FOOTBALL_DATA_BASE_URL,
  timeout: 25000,
  headers: FOOTBALL_DATA_API_KEY ? { "X-Auth-Token": FOOTBALL_DATA_API_KEY } : {},
});

const sportsDbClient = axios.create({
  baseURL: SPORTSDB_BASE_URL,
  timeout: 25000,
});

let lastExternalCallAt = 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeText = (value) =>
  cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const toIsoDate = (date) => new Date(date).toISOString().slice(0, 10);

const withinSupportedYears = (isoDate) => {
  const date = new Date(isoDate);
  return !Number.isNaN(date.getTime()) && date >= MIN_SUPPORTED_DATE && date <= MAX_SUPPORTED_DATE;
};

const clampDateRange = () => {
  const now = new Date();
  const dateFrom = now < MIN_SUPPORTED_DATE ? MIN_SUPPORTED_DATE : now;
  const rawDateTo = new Date(
    dateFrom.getTime() + Math.max(LOOKAHEAD_DAYS, MIN_LOOKAHEAD_DAYS) * 86400000
  );
  const dateTo = rawDateTo > MAX_SUPPORTED_DATE ? MAX_SUPPORTED_DATE : rawDateTo;
  return { dateFrom, dateTo };
};

const throttledGet = async (client, url, config = {}) => {
  const elapsed = Date.now() - lastExternalCallAt;
  if (elapsed < RATE_DELAY_MS) {
    await sleep(RATE_DELAY_MS - elapsed);
  }
  const response = await client.get(url, config);
  lastExternalCallAt = Date.now();
  return response;
};

const normalizeStatus = (status) => {
  const s = String(status || "").toLowerCase();
  if (s.includes("finish")) {
    return "finished";
  }
  return "scheduled";
};

const dedupeRows = (rows) => {
  const seen = new Set();
  const output = [];
  for (const row of rows) {
    const key = `${normalizeText(row.club)}|${new Date(row.match_date).toISOString()}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(row);
  }
  return output;
};

const getTableColumns = async (tableName) => {
  const candidatesByTable = {
    [SUPABASE_PLAYERS_TABLE]: ["current_club", "club", "is_active"],
    [MATCHES_TABLE]: [
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
      "status",
      "score"
    ]
  };
  const candidates = candidatesByTable[tableName] || [];

  try {
    const rpcResponse = await supabase.rpc("get_columns", {
      table_name: tableName,
    });
    if (!rpcResponse.error && Array.isArray(rpcResponse.data) && rpcResponse.data.length) {
      return new Set(
        rpcResponse.data
          .map((item) => String(item.column_name || item.name || "").trim())
          .filter(Boolean)
      );
    }
  } catch (_error) {
    // fallback below
  }

  const columns = new Set();
  for (const column of candidates) {
    try {
      const { error } = await supabase.from(tableName).select(column).limit(1);
      if (!error) {
        columns.add(column);
      }
    } catch (_error) {
      // ignore per-column probe failures
    }
  }

  if (!columns.size) {
    console.log(`[matches] Skipping step: could not detect columns for ${tableName}.`);
  }
  return columns;
};

const toMatchRowsFromFootballData = (match, fallbackCompetition) => {
  const homeTeam = cleanText(match?.homeTeam?.name);
  const awayTeam = cleanText(match?.awayTeam?.name);
  const matchDate = match?.utcDate;
  if (!homeTeam || !awayTeam || !matchDate || !withinSupportedYears(matchDate)) {
    return [];
  }

  const competition = cleanText(match?.competition?.name || fallbackCompetition || "Unknown Competition");
  const stadium = cleanText(match?.venue || "");
  const status = normalizeStatus(match?.status);
  const homeGoals = match?.score?.fullTime?.home;
  const awayGoals = match?.score?.fullTime?.away;
  const score =
    Number.isFinite(homeGoals) && Number.isFinite(awayGoals) ? `${homeGoals}-${awayGoals}` : "";

  return [
    {
      club: homeTeam,
      opponent: awayTeam,
      home_team: homeTeam,
      away_team: awayTeam,
      match_date: matchDate,
      competition,
      home_away: "home",
      home_score: Number.isFinite(homeGoals) ? homeGoals : null,
      away_score: Number.isFinite(awayGoals) ? awayGoals : null,
      stadium,
      status,
      score,
    },
    {
      club: awayTeam,
      opponent: homeTeam,
      home_team: homeTeam,
      away_team: awayTeam,
      match_date: matchDate,
      competition,
      home_away: "away",
      home_score: Number.isFinite(homeGoals) ? homeGoals : null,
      away_score: Number.isFinite(awayGoals) ? awayGoals : null,
      stadium,
      status,
      score,
    },
  ];
};

const fetchPrimaryFootballData = async () => {
  if (!FOOTBALL_DATA_API_KEY) {
    console.warn("[matches] FOOTBALL_DATA_API_KEY missing. Skipping primary source.");
    return [];
  }

  const { dateFrom, dateTo } = clampDateRange();
  const rows = [];

  for (const competition of FOOTBALL_DATA_COMPETITIONS) {
    try {
      console.log(`[matches] football-data.org -> ${competition}`);
      const response = await throttledGet(
        footballDataClient,
        `/competitions/${competition}/matches`,
        {
          params: {
            dateFrom: toIsoDate(dateFrom),
            dateTo: toIsoDate(dateTo),
          },
        }
      );
      const matches = Array.isArray(response.data?.matches) ? response.data.matches : [];
      for (const match of matches) {
        rows.push(...toMatchRowsFromFootballData(match, competition));
      }
    } catch (error) {
      console.log(`[matches] Skipping step: football-data failed for ${competition}:`, error.message);
      continue;
    }
  }

  return dedupeRows(rows);
};

const parseSportsDbDateTime = (dateStr, timeStr) => {
  const date = cleanText(dateStr);
  const time = cleanText(timeStr);
  if (!date) {
    return "";
  }
  const iso = time ? `${date}T${time}` : `${date}T12:00:00`;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toISOString();
};

const fetchTrackedClubs = async (playersColumns) => {
  const selectable = ["current_club", "club", "is_active"].filter((column) =>
    playersColumns.has(column)
  );
  if (!selectable.length) {
    console.log("[matches] Skipping step: no usable players club columns.");
    return [];
  }

  const activeFilterEnabled = playersColumns.has("is_active");
  let query = supabase.from(SUPABASE_PLAYERS_TABLE).select(selectable.join(",")).limit(5000);
  if (activeFilterEnabled) {
    query = query.eq("is_active", true);
  }

  const { data, error } = await query;
  if (error) {
    throw new Error(`Could not load clubs for fallback: ${error.message}`);
  }

  const clubs = [];
  for (const row of Array.isArray(data) ? data : []) {
    const club = cleanText(
      (playersColumns.has("current_club") ? row.current_club : "") ||
        (playersColumns.has("club") ? row.club : "")
    );
    if (!club) {
      continue;
    }
    clubs.push(club);
  }

  return [...new Set(clubs)].slice(0, MAX_FALLBACK_CLUBS);
};

const toMatchRowsFromSportsDbEvent = (clubName, event) => {
  const home = cleanText(event?.strHomeTeam);
  const away = cleanText(event?.strAwayTeam);
  const isoDateTime = parseSportsDbDateTime(event?.dateEvent, event?.strTime);
  if (!home || !away || !isoDateTime || !withinSupportedYears(isoDateTime)) {
    return [];
  }

  const clubNormalized = normalizeText(clubName);
  const homeNormalized = normalizeText(home);
  const awayNormalized = normalizeText(away);
  let homeAway = "";
  let opponent = "";

  if (clubNormalized === homeNormalized) {
    homeAway = "home";
    opponent = away;
  } else if (clubNormalized === awayNormalized) {
    homeAway = "away";
    opponent = home;
  } else {
    return [];
  }

  return [
    {
      club: clubName,
      opponent,
      home_team: home,
      away_team: away,
      match_date: isoDateTime,
      competition: cleanText(event?.strLeague || "Unknown Competition"),
      home_away: homeAway,
      home_score: Number.parseInt(cleanText(event?.intHomeScore || ""), 10) || null,
      away_score: Number.parseInt(cleanText(event?.intAwayScore || ""), 10) || null,
      stadium: cleanText(event?.strVenue || ""),
      status: "scheduled",
      score: cleanText(event?.intHomeScore) && cleanText(event?.intAwayScore)
        ? `${cleanText(event?.intHomeScore)}-${cleanText(event?.intAwayScore)}`
        : "",
    },
  ];
};

const fetchFallbackSportsDb = async (playersColumns) => {
  let clubs = [];
  try {
    clubs = await fetchTrackedClubs(playersColumns);
  } catch (error) {
    console.log("[matches] Skipping step:", error.message);
    return [];
  }

  const rows = [];
  const { dateFrom, dateTo } = clampDateRange();

  for (const club of clubs) {
    if (!club) {
      continue;
    }

    try {
      console.log(`[matches] fallback TheSportsDB -> ${club}`);
      const teamSearch = await throttledGet(sportsDbClient, "/searchteams.php", {
        params: { t: club },
      });
      const team = Array.isArray(teamSearch.data?.teams) ? teamSearch.data.teams[0] : null;
      const teamId = team?.idTeam;
      if (!teamId) {
        continue;
      }

      const eventsResponse = await throttledGet(sportsDbClient, "/eventsnext.php", {
        params: { id: teamId },
      });
      const events = Array.isArray(eventsResponse.data?.events) ? eventsResponse.data.events : [];
      for (const event of events) {
        const generated = toMatchRowsFromSportsDbEvent(club, event);
        for (const row of generated) {
          const rowDate = new Date(row.match_date);
          if (rowDate >= dateFrom && rowDate <= dateTo) {
            rows.push(row);
          }
        }
      }
    } catch (error) {
      console.log(`[matches] Skipping step: TheSportsDB fallback failed for ${club}:`, error.message);
      continue;
    }
  }

  return dedupeRows(rows);
};

const projectMatchPayload = (row, matchesColumns) => {
  const payload = {};
  const allowed = [
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
    "status",
    "score",
  ];
  for (const field of allowed) {
    if (matchesColumns.has(field)) {
      payload[field] = row[field];
    }
  }
  return payload;
};

const upsertMatches = async (rows, matchesColumns) => {
  if (!rows.length) {
    return 0;
  }
  if (!matchesColumns.has("club") || !matchesColumns.has("match_date")) {
    console.log("[matches] Skipping step: matches table missing club/match_date columns.");
    return 0;
  }

  let written = 0;
  for (let i = 0; i < rows.length; i += WRITE_BATCH_SIZE) {
    const batch = rows
      .slice(i, i + WRITE_BATCH_SIZE)
      .map((row) => projectMatchPayload(row, matchesColumns));
    try {
      const { error } = await supabase.from(MATCHES_TABLE).upsert(batch, {
        onConflict: "club,match_date",
      });
      if (error) {
        if (
          String(error.message || "").includes(
            "no unique or exclusion constraint matching the ON CONFLICT specification"
          )
        ) {
          const insertResult = await supabase.from(MATCHES_TABLE).insert(batch);
          if (!insertResult.error) {
            written += batch.length;
            continue;
          }
          console.log("[matches] Skipping step:", insertResult.error.message);
          continue;
        }
        console.log("[matches] Skipping step:", error.message);
        continue;
      }
      written += batch.length;
    } catch (error) {
      console.log("[matches] Skipping step:", error.message);
      continue;
    }
  }
  return written;
};

const runSyncMatches = async () => {
  if (!hasSupabaseEnv) {
    console.log("[matches] Skipping step: missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
    return;
  }

  console.log("[matches] Starting sync...");

  const [playersColumns, matchesColumns] = await Promise.all([
    getTableColumns(SUPABASE_PLAYERS_TABLE),
    getTableColumns(MATCHES_TABLE),
  ]);

  if (!matchesColumns.size) {
    console.log("[matches] Skipping step: matches schema metadata unavailable.");
    return;
  }

  let rows = [];
  try {
    rows = await fetchPrimaryFootballData();
  } catch (error) {
    console.log("[matches] Skipping step:", error.message);
    rows = [];
  }

  if (!rows.length) {
    console.warn("[matches] Primary source returned 0 rows. Trying TheSportsDB fallback...");
    try {
      rows = await fetchFallbackSportsDb(playersColumns);
    } catch (error) {
      console.log("[matches] Skipping step:", error.message);
      rows = [];
    }
  }

  rows = rows.filter((row) => withinSupportedYears(row.match_date));
  const written = await upsertMatches(rows, matchesColumns);
  console.log(`[matches] Upserted rows: ${written}`);
  console.log(`[matches] Source rows: ${rows.length}`);
};

if (require.main === module) {
  runSyncMatches()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("[matches] Sync failed:", error.message);
      process.exit(1);
    });
}

module.exports = {
  runSyncMatches,
};
