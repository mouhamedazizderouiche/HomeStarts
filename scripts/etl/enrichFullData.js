const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const BATCH_SIZE = 50;
const MAX_PLAYERS = Number(process.env.FULL_DATA_MAX_PLAYERS || 0);
const RETRY_COUNT = 3;
const RETRY_DELAY_MS = 1200;
const BATCH_DELAY_MS = 1500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const toInt = (value) => {
  const parsed = Number.parseInt(String(value || "").replace(/[^\d-]/g, ""), 10);
  return Number.isFinite(parsed) ? parsed : null;
};

const detectColumns = async (table, candidates) => {
  const out = new Set();
  for (const column of candidates) {
    try {
      const { error } = await supabase.from(table).select(column).limit(1);
      if (!error) out.add(column);
    } catch (_error) {
      // ignore
    }
  }
  return out;
};

const retry = async (fn, label) => {
  let lastError = null;
  for (let attempt = 1; attempt <= RETRY_COUNT; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      console.log(`[enrich:full-data] Retry ${attempt}/${RETRY_COUNT} for ${label}: ${error.message}`);
      await sleep(RETRY_DELAY_MS * attempt);
    }
  }
  throw lastError;
};

const getFlagUrl = (code) => `https://flagcdn.com/64x48/${String(code || "un").toLowerCase()}.png`;
const getPlayerImage = (transfermarktId, name) =>
  transfermarktId
    ? `https://img.transfermarkt.com/portrait/normal/${transfermarktId}.jpg`
    : `https://ui-avatars.com/api/?name=${encodeURIComponent(name || "Unknown Player")}`;
const getClubLogo = (clubTmId) =>
  clubTmId ? `https://tmssl.akamaized.net/images/wappen/normal/${clubTmId}.png` : "";

const enrichPlayers = async (playersCols) => {
  const selectCols = ["id", "name", "transfermarkt_id", "club_id", "nationality_code", "image_url"].filter((c) =>
    playersCols.has(c)
  );
  if (!selectCols.includes("id") || !selectCols.includes("name")) {
    console.log("[enrich:full-data] Skipping players enrichment: id/name not available.");
    return { updated: 0, scanned: 0 };
  }

  let from = 0;
  const page = 1000;
  let done = false;
  let scanned = 0;
  let updated = 0;

  while (!done) {
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select(selectCols.join(","))
      .range(from, from + page - 1);
    if (error) {
      throw new Error(error.message);
    }
    const rows = Array.isArray(data) ? data : [];
    if (!rows.length) {
      done = true;
      continue;
    }
    const scopedRows =
      MAX_PLAYERS > 0 && scanned + rows.length > MAX_PLAYERS
        ? rows.slice(0, Math.max(0, MAX_PLAYERS - scanned))
        : rows;
    if (!scopedRows.length) {
      done = true;
      continue;
    }

    scanned += scopedRows.length;
    for (let i = 0; i < scopedRows.length; i += BATCH_SIZE) {
      const batch = scopedRows.slice(i, i + BATCH_SIZE);
      for (const row of batch) {
        const payload = {};
        const tm = toInt(row.transfermarkt_id);
        const image = getPlayerImage(tm, row.name);
        if (playersCols.has("image_url") && image && image !== row.image_url) {
          payload.image_url = image;
        }
        if (!Object.keys(payload).length) continue;

        await retry(
          async () => {
            const { error: updateError } = await supabase
              .from(PLAYERS_TABLE)
              .update(payload)
              .eq("id", row.id);
            if (updateError) throw new Error(updateError.message);
          },
          `player:${row.id}`
        );
        updated += 1;
      }
      console.log(`[enrich:full-data] Players batch ${Math.floor(i / BATCH_SIZE) + 1} in page updated.`);
      await sleep(BATCH_DELAY_MS);
    }

    from += page;
    if (MAX_PLAYERS > 0 && scanned >= MAX_PLAYERS) {
      done = true;
    }
  }

  return { updated, scanned };
};

const enrichClubs = async () => {
  const clubCols = await detectColumns("clubs", ["id", "name", "transfermarkt_id", "logo_url"]);
  if (!clubCols.has("id") || !clubCols.has("transfermarkt_id") || !clubCols.has("logo_url")) {
    console.log("[enrich:full-data] Skipping clubs enrichment: clubs table/columns unavailable.");
    return { updated: 0, scanned: 0 };
  }

  const { data, error } = await supabase.from("clubs").select("id,name,transfermarkt_id,logo_url");
  if (error) throw new Error(error.message);
  const clubs = Array.isArray(data) ? data : [];

  let updated = 0;
  for (let i = 0; i < clubs.length; i += BATCH_SIZE) {
    const batch = clubs.slice(i, i + BATCH_SIZE);
    for (const club of batch) {
      const logo = getClubLogo(toInt(club.transfermarkt_id));
      if (!logo || logo === club.logo_url) continue;
      await retry(
        async () => {
          const { error: updateError } = await supabase.from("clubs").update({ logo_url: logo }).eq("id", club.id);
          if (updateError) throw new Error(updateError.message);
        },
        `club:${club.id}`
      );
      updated += 1;
    }
    console.log(`[enrich:full-data] Clubs batch ${Math.floor(i / BATCH_SIZE) + 1} done.`);
    await sleep(BATCH_DELAY_MS);
  }

  return { updated, scanned: clubs.length };
};

const enrichCountries = async () => {
  const countryCols = await detectColumns("countries", ["code", "name", "flag_url"]);
  if (!countryCols.has("code") || !countryCols.has("flag_url")) {
    console.log("[enrich:full-data] Skipping countries enrichment: countries table/columns unavailable.");
    return { updated: 0, scanned: 0 };
  }

  const { data, error } = await supabase.from("countries").select("code,flag_url");
  if (error) throw new Error(error.message);
  const countries = Array.isArray(data) ? data : [];

  let updated = 0;
  for (let i = 0; i < countries.length; i += BATCH_SIZE) {
    const batch = countries.slice(i, i + BATCH_SIZE);
    for (const country of batch) {
      const code = cleanText(country.code).toLowerCase() || "un";
      const flag = getFlagUrl(code);
      if (flag === country.flag_url) continue;
      await retry(
        async () => {
          const { error: updateError } = await supabase
            .from("countries")
            .update({ code, flag_url: flag })
            .eq("code", country.code);
          if (updateError) throw new Error(updateError.message);
        },
        `country:${country.code}`
      );
      updated += 1;
    }
    console.log(`[enrich:full-data] Countries batch ${Math.floor(i / BATCH_SIZE) + 1} done.`);
    await sleep(BATCH_DELAY_MS);
  }

  return { updated, scanned: countries.length };
};

const run = async () => {
  const playersCols = await detectColumns(PLAYERS_TABLE, [
    "id",
    "name",
    "transfermarkt_id",
    "club_id",
    "nationality_code",
    "image_url"
  ]);

  const players = await enrichPlayers(playersCols);
  const clubs = await enrichClubs();
  const countries = await enrichCountries();

  console.log("[enrich:full-data] Completed", {
    players,
    clubs,
    countries
  });
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[enrich:full-data] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
