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
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const BATCH_SIZE = Math.min(20, Number(process.env.FULL_ENRICH_BATCH_SIZE || 20));
const REQUEST_DELAY_MS = Number(process.env.FULL_ENRICH_REQUEST_DELAY_MS || 2500);
const BATCH_DELAY_MS = Number(process.env.FULL_ENRICH_BATCH_DELAY_MS || 2500);
const MAX_PLAYERS = Number(process.env.FULL_ENRICH_MAX_PLAYERS || 400);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const sportsDb = axios.create({
  baseURL: "https://www.thesportsdb.com/api/v1/json/3",
  timeout: 15000
});

const wikiClient = axios.create({
  timeout: 15000,
  headers: {
    "User-Agent":
      process.env.WIKIPEDIA_USER_AGENT ||
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  }
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeText = (value) =>
  cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
const toNumber = (value) => {
  const parsed = Number(String(value || "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
};
const isHttp = (value) => /^https?:\/\//i.test(String(value || "").trim());

const probeColumns = async () => {
  const candidates = [
    "id",
    "name",
    "current_club",
    "club",
    "position",
    "age",
    "height",
    "height_cm",
    "preferred_foot",
    "market_value",
    "image_url",
    "club_logo_url",
    "data_sources",
    "updated_at",
    "is_active"
  ];
  const available = new Set();
  for (const column of candidates) {
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).select(column).limit(1);
      if (!error) {
        available.add(column);
      }
    } catch (_error) {
      // ignore
    }
  }
  return available;
};

const loadPlayers = async (columns) => {
  const selectCols = ["id", "name", "current_club", "club", "image_url"].filter((col) => columns.has(col));
  let query = supabase.from(PLAYERS_TABLE).select(selectCols.join(",")).limit(MAX_PLAYERS);
  if (columns.has("is_active")) {
    query = query.eq("is_active", true);
  }
  const { data, error } = await query;
  if (error) {
    throw new Error(error.message);
  }
  return Array.isArray(data) ? data : [];
};

const sportsCache = new Map();
const teamCache = new Map();
const wikiCache = new Map();

const fetchSportsPlayer = async (name) => {
  const key = normalizeText(name);
  if (sportsCache.has(key)) {
    return sportsCache.get(key);
  }
  await sleep(REQUEST_DELAY_MS);
  try {
    const response = await sportsDb.get("/searchplayers.php", { params: { p: name } });
    const list = Array.isArray(response.data?.player) ? response.data.player : [];
    const hit = list.find((item) => normalizeText(item?.strPlayer) === key) || list[0] || null;
    sportsCache.set(key, hit);
    return hit;
  } catch (error) {
    console.log(`[etl:full] Skipping sportsdb player ${name}: ${error.message}`);
    sportsCache.set(key, null);
    return null;
  }
};

const fetchSportsTeamLogo = async (clubName) => {
  const key = normalizeText(clubName);
  if (!key) {
    return "";
  }
  if (teamCache.has(key)) {
    return teamCache.get(key);
  }
  await sleep(REQUEST_DELAY_MS);
  try {
    const response = await sportsDb.get("/searchteams.php", { params: { t: clubName } });
    const teams = Array.isArray(response.data?.teams) ? response.data.teams : [];
    const hit = teams.find((item) => normalizeText(item?.strTeam) === key) || teams[0] || null;
    const logo = cleanText(hit?.strBadge || hit?.strLogo || "");
    teamCache.set(key, isHttp(logo) ? logo : "");
    return teamCache.get(key);
  } catch (error) {
    console.log(`[etl:full] Skipping sportsdb logo ${clubName}: ${error.message}`);
    teamCache.set(key, "");
    return "";
  }
};

const fetchWikipedia = async (name) => {
  const key = normalizeText(name);
  if (wikiCache.has(key)) {
    return wikiCache.get(key);
  }
  await sleep(REQUEST_DELAY_MS);
  try {
    const url = `https://en.wikipedia.org/wiki/${encodeURIComponent(cleanText(name).replace(/\s+/g, "_"))}`;
    const response = await wikiClient.get(url);
    const $ = cheerio.load(response.data || "");

    const imageSrc = $("table.infobox img").first().attr("src") || "";
    const image = imageSrc
      ? imageSrc.startsWith("//")
        ? `https:${imageSrc}`
        : imageSrc
      : "";

    const infoboxText = $("table.infobox").text();
    const heightMatch = infoboxText.match(/(\d{2,3})\s*cm/i);
    const footMatch = infoboxText.match(/(left|right|both)-?footed/i);

    const payload = {
      image_url: isHttp(image) ? image : "",
      height_cm: heightMatch ? Number(heightMatch[1]) : null,
      preferred_foot: footMatch ? footMatch[1].toLowerCase() : ""
    };

    wikiCache.set(key, payload);
    return payload;
  } catch (error) {
    console.log(`[etl:full] Skipping wikipedia ${name}: ${error.message}`);
    const fallback = { image_url: "", height_cm: null, preferred_foot: "" };
    wikiCache.set(key, fallback);
    return fallback;
  }
};

const buildPayload = ({ player, sportsPlayer, wikiInfo, clubLogo, columns }) => {
  const payload = {};

  const position = cleanText(sportsPlayer?.strPosition || "");
  const age = toNumber(sportsPlayer?.strAge || sportsPlayer?.intAge || "");
  const heightCm =
    toNumber(sportsPlayer?.strHeight || "") ||
    toNumber(sportsPlayer?.strHeightCM || "") ||
    wikiInfo.height_cm;
  const preferredFoot = cleanText(sportsPlayer?.strSide || wikiInfo.preferred_foot || "").toLowerCase();
  const marketValue = toNumber(sportsPlayer?.strWage || "") || null;
  const sportsImage = cleanText(sportsPlayer?.strCutout || sportsPlayer?.strThumb || sportsPlayer?.strRender || "");
  const imageUrl = isHttp(player.image_url) ? player.image_url : isHttp(sportsImage) ? sportsImage : wikiInfo.image_url;

  if (columns.has("position") && position) payload.position = position;
  if (columns.has("age") && age) payload.age = age;
  if (columns.has("height_cm") && heightCm) payload.height_cm = heightCm;
  if (columns.has("height") && heightCm) payload.height = `${heightCm} cm`;
  if (columns.has("preferred_foot") && preferredFoot) payload.preferred_foot = preferredFoot;
  if (columns.has("market_value") && marketValue) payload.market_value = marketValue;
  if (columns.has("image_url") && imageUrl) payload.image_url = imageUrl;
  if (columns.has("club_logo_url") && clubLogo) payload.club_logo_url = clubLogo;

  if (columns.has("data_sources")) {
    payload.data_sources = ["transfermarkt", "sportsdb", "wikipedia"];
  }
  if (columns.has("updated_at")) {
    payload.updated_at = new Date().toISOString();
  }

  return payload;
};

const run = async () => {
  const columns = await probeColumns();
  const players = await loadPlayers(columns);
  console.log(`[etl:full] Loaded players: ${players.length}`);

  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < players.length; i += BATCH_SIZE) {
    const batch = players.slice(i, i + BATCH_SIZE);
    for (const player of batch) {
      const name = cleanText(player.name);
      if (!name) {
        skipped += 1;
        continue;
      }

      const clubName = cleanText(player.current_club || player.club || "");
      const [sportsPlayer, wikiInfo, clubLogo] = await Promise.all([
        fetchSportsPlayer(name),
        fetchWikipedia(name),
        fetchSportsTeamLogo(clubName)
      ]);

      const payload = buildPayload({ player, sportsPlayer, wikiInfo, clubLogo, columns });
      if (!Object.keys(payload).length) {
        skipped += 1;
        continue;
      }

      const { error } = await supabase.from(PLAYERS_TABLE).update(payload).eq("id", player.id);
      if (error) {
        console.log(`[etl:full] Skipping ${name}: ${error.message}`);
        skipped += 1;
        continue;
      }
      updated += 1;
    }

    console.log(
      `[etl:full] Batch ${Math.floor(i / BATCH_SIZE) + 1} done. Updated=${updated}, Skipped=${skipped}`
    );
    await sleep(BATCH_DELAY_MS);
  }

  console.log(`[etl:full] Completed. Updated=${updated}, Skipped=${skipped}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[etl:full] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
