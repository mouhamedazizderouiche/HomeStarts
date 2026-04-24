const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const axios = backendRequire("axios");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const SPORTSDB_BASE_URL = "https://www.thesportsdb.com/api/v1/json/3";

const BATCH_SIZE = 20;
const BATCH_DELAY_MS = 2000;
const REQUEST_DELAY_MS = 5000; // <=12 requests/min
const MAX_PLAYERS = Number(process.env.IMAGE_ENRICH_MAX_PLAYERS || 5000);
const FETCH_PAGE_SIZE = Number(process.env.IMAGE_ENRICH_PAGE_SIZE || 1000);
const COOLDOWN_ON_429_MS = Number(process.env.IMAGE_ENRICH_429_COOLDOWN_MS || 65000);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});
const sportsDb = axios.create({
  baseURL: SPORTSDB_BASE_URL,
  timeout: 15000
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (v) => String(v || "").replace(/\s+/g, " ").trim();
const normalizeText = (v) =>
  cleanText(v)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const loadCandidates = async (offset = 0, limit = FETCH_PAGE_SIZE) => {
  const from = offset;
  const to = offset + limit - 1;
  const { data, error } = await supabase
    .from(PLAYERS_TABLE)
    .select("id,name,image_url")
    .ilike("image_url", "%ui-avatars%")
    .range(from, to);
  if (error) {
    throw new Error(error.message);
  }
  return Array.isArray(data) ? data : [];
};

const run = async () => {
  let offset = 0;
  const cache = new Map();
  let updated = 0;
  let checked = 0;
  let cooldownUntil = 0;
  let total429 = 0;

  console.log('[etl:images] Starting paginated enrichment...');
  while (true) {
    const players = await loadCandidates(offset, FETCH_PAGE_SIZE);
    if (!players || players.length === 0) {
      break;
    }
    console.log(`[etl:images] Fetched page offset=${offset} size=${players.length}`);

    for (let i = 0; i < players.length; i += BATCH_SIZE) {
      const batch = players.slice(i, i + BATCH_SIZE);
    for (const player of batch) {
      checked += 1;
      const key = normalizeText(player.name);
      let imageUrl = cache.get(key) || "";

      if (!imageUrl) {
        try {
          const now = Date.now();
          if (cooldownUntil > now) {
            await sleep(cooldownUntil - now);
          }
          await sleep(REQUEST_DELAY_MS);
          const response = await sportsDb.get("/searchplayers.php", {
            params: { p: player.name }
          });
          const list = Array.isArray(response.data?.player) ? response.data.player : [];
          const hit = list.find((item) => normalizeText(item?.strPlayer) === key) || list[0] || null;
          imageUrl = cleanText(hit?.strCutout || hit?.strThumb || hit?.strRender || "");
          cache.set(key, imageUrl);
        } catch (error) {
          if (error?.response?.status === 429) {
            total429 += 1;
            cooldownUntil = Date.now() + COOLDOWN_ON_429_MS;
            console.log(
              `[etl:images] 429 for ${player.name}. Cooling down ${Math.round(
                COOLDOWN_ON_429_MS / 1000
              )}s (total 429=${total429}).`
            );
          }
          console.log(`[etl:images] Skip ${player.name}: ${error.message}`);
          cache.set(key, "");
          continue;
        }
      }

      if (!imageUrl) {
        continue;
      }

      const { error: upErr } = await supabase
        .from(PLAYERS_TABLE)
        .update({ image_url: imageUrl })
        .eq("id", player.id);

      if (!upErr) {
        updated += 1;
      } else {
        console.warn(`[etl:images] Failed to update ${player.id}: ${upErr.message}`);
      }
    }
      console.log(
        `[etl:images] Batch ${Math.floor(i / BATCH_SIZE) + 1} done. Checked=${checked}, Updated=${updated}`
      );
      await sleep(BATCH_DELAY_MS);
    }

    offset += FETCH_PAGE_SIZE;
  }

  console.log(`[etl:images] Completed. Checked=${checked}, Updated=${updated}`);
  if (total429 > 0) {
    console.log(`[etl:images] Rate-limit hits: ${total429}`);
  }
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[etl:images] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
