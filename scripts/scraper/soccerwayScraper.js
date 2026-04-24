const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const axios = backendRequire("axios");
const cheerio = backendRequire("cheerio");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MATCHES_TABLE = process.env.SUPABASE_MATCHES_TABLE || "matches";
const START_URL = process.env.SOCCERWAY_START_URL || "https://www.soccerway.com/matches/";
const MAX_PAGES = Number(process.env.SOCCERWAY_MAX_PAGES || 3);
const REQUEST_DELAY_MS = Number(process.env.SOCCERWAY_DELAY_MS || 2000);
const OUTPUT_JSON =
  process.env.SOCCERWAY_OUTPUT_JSON ||
  path.resolve(__dirname, "..", "..", "data", "soccerway", `matches-${new Date().toISOString().slice(0, 10)}.json`);
const DEBUG_HTML_ON_EMPTY = String(process.env.SOCCERWAY_DEBUG_HTML || "true").toLowerCase() === "true";
const UPSERT_TO_SUPABASE = String(process.env.SOCCERWAY_UPSERT || "false").toLowerCase() === "true";

const hasSupabase = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabase
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

const client = axios.create({
  timeout: 25000,
  headers: {
    "User-Agent":
      process.env.SOCCERWAY_USER_AGENT ||
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
  }
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clean = (v) => String(v || "").replace(/\s+/g, " ").trim();
const norm = (v) => clean(v).toLowerCase();

const parseScore = (value) => {
  const m = String(value || "").match(/(\d+)\s*[-:]\s*(\d+)/);
  if (!m) return { home: null, away: null, score: "" };
  return { home: Number(m[1]), away: Number(m[2]), score: `${m[1]}-${m[2]}` };
};

const toIsoDateTime = (dateText, timeText) => {
  const raw = clean(`${dateText} ${timeText}`);
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  const dateOnly = new Date(clean(dateText));
  if (!Number.isNaN(dateOnly.getTime())) return dateOnly.toISOString();
  return "";
};

const extractMatches = ($, pageUrl) => {
  const matches = [];
  const rowSelectors = [
    "table.matches tbody tr",
    "table#page_matches tbody tr",
    "tr.match",
    ".matches .match-row"
  ];

  let rows = [];
  for (const selector of rowSelectors) {
    rows = $(selector).toArray();
    if (rows.length) break;
  }

  for (const tr of rows) {
    const row = $(tr);
    const dateText =
      clean(row.find("td.date").text()) || clean(row.find("td.full-date").text()) || clean(row.find("td").eq(0).text());
    const timeText = clean(row.find("td.time").text()) || "";
    const competition =
      clean(row.find("td.competition").text()) || clean(row.find("td.league").text()) || clean($("h1").first().text());

    const home =
      clean(row.find("td.team-a a").text()) || clean(row.find("td.home a").text()) || clean(row.find("td").eq(1).text());
    const away =
      clean(row.find("td.team-b a").text()) || clean(row.find("td.away a").text()) || clean(row.find("td").eq(3).text());

    const scoreText =
      clean(row.find("td.score").text()) || clean(row.find("td.result").text()) || clean(row.find("td").eq(2).text());

    if (!home || !away) continue;

    const { home: homeScore, away: awayScore, score } = parseScore(scoreText);
    const matchDate = toIsoDateTime(dateText, timeText);

    matches.push({
      source: "soccerway",
      source_url: pageUrl,
      home_team: home,
      away_team: away,
      club: home,
      opponent: away,
      competition,
      match_date: matchDate,
      home_score: homeScore,
      away_score: awayScore,
      score,
      status: score ? "finished" : "scheduled"
    });
  }

  return matches;
};

const findNextPage = ($, currentUrl) => {
  const links = [
    $("a.next").attr("href"),
    $("li.next a").attr("href"),
    $("a[rel='next']").attr("href")
  ].filter(Boolean);
  if (!links.length) return "";
  return new URL(links[0], currentUrl).toString();
};

const dedupeMatches = (rows) => {
  const seen = new Set();
  return rows.filter((row) => {
    const key = `${norm(row.home_team)}|${norm(row.away_team)}|${String(row.match_date || "").slice(0, 16)}|${norm(row.competition)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const detectColumns = async () => {
  if (!supabase) return new Set();
  const candidates = [
    "home_team",
    "away_team",
    "club",
    "opponent",
    "match_date",
    "competition",
    "score",
    "home_score",
    "away_score",
    "status"
  ];
  const out = new Set();
  for (const col of candidates) {
    try {
      const { error } = await supabase.from(MATCHES_TABLE).select(col).limit(1);
      if (!error) out.add(col);
    } catch (_error) {
      // ignore
    }
  }
  return out;
};

const upsertSupabase = async (rows) => {
  if (!UPSERT_TO_SUPABASE || !supabase) {
    return { written: 0, skipped: rows.length };
  }

  const cols = await detectColumns();
  if (!cols.has("match_date") || (!cols.has("club") && !cols.has("home_team"))) {
    console.log("[soccerway] Skipping upsert: required columns missing.");
    return { written: 0, skipped: rows.length };
  }

  const payload = rows.map((row) => {
    const out = {};
    for (const [key, value] of Object.entries(row)) {
      if (cols.has(key)) out[key] = value;
    }
    return out;
  });

  let written = 0;
  const batch = 200;
  for (let i = 0; i < payload.length; i += batch) {
    const chunk = payload.slice(i, i + batch);
    const conflict = cols.has("home_team") && cols.has("away_team") ? "home_team,away_team,match_date" : "club,match_date";
    const { error } = await supabase.from(MATCHES_TABLE).upsert(chunk, { onConflict: conflict });
    if (!error) {
      written += chunk.length;
      continue;
    }

    const fallback = await supabase.from(MATCHES_TABLE).insert(chunk);
    if (!fallback.error) {
      written += chunk.length;
      continue;
    }
    console.log(`[soccerway] batch skipped: ${fallback.error.message}`);
  }

  return { written, skipped: rows.length - written };
};

const run = async () => {
  let url = START_URL;
  const collected = [];

  for (let i = 0; i < MAX_PAGES && url; i += 1) {
    console.log(`[soccerway] Fetching page ${i + 1}/${MAX_PAGES}: ${url}`);
    try {
      const response = await client.get(url);
      const $ = cheerio.load(response.data || "");
      const rows = extractMatches($, url);
      if (!rows.length && DEBUG_HTML_ON_EMPTY) {
        await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
        const debugPath = path.resolve(
          path.dirname(OUTPUT_JSON),
          `soccerway-debug-page-${i + 1}-${Date.now()}.html`
        );
        await fs.writeFile(debugPath, String(response.data || ""), "utf8");
        console.log(`[soccerway] No rows parsed on page ${i + 1}; saved debug HTML -> ${debugPath}`);
      }
      collected.push(...rows);
      url = findNextPage($, url);
    } catch (error) {
      console.log(`[soccerway] page failed: ${error.message}`);
      break;
    }
    if (i < MAX_PAGES - 1 && url) await sleep(REQUEST_DELAY_MS);
  }

  const matches = dedupeMatches(collected);
  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.writeFile(
    OUTPUT_JSON,
    JSON.stringify({
      generated_at: new Date().toISOString(),
      start_url: START_URL,
      pages: MAX_PAGES,
      total: matches.length,
      matches
    }, null, 2),
    "utf8"
  );

  const db = await upsertSupabase(matches);
  console.log(`[soccerway] Saved ${matches.length} matches -> ${OUTPUT_JSON}`);
  if (UPSERT_TO_SUPABASE) {
    console.log(`[soccerway] DB write: written=${db.written}, skipped=${db.skipped}`);
  }
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[soccerway] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
