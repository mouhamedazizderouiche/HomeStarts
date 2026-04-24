const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");
const { normalizeCountry, getCountryCode } = require("../../backend/utils/countryUtils");

dotenv.config({
  path: path.resolve(__dirname, "..", "..", "backend", ".env")
});

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const COUNTRIES_TABLE = process.env.SUPABASE_COUNTRIES_TABLE || "countries";
const PAGE_SIZE = 1000;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const validCode = (value) => /^[a-z]{2}$/.test(String(value || "").toLowerCase());
const flagUrl = (code) => `https://flagcdn.com/64x48/${code}.png`;

const detectColumns = async (tableName, candidates) => {
  const columns = new Set();
  for (const col of candidates) {
    try {
      const { error } = await supabase.from(tableName).select(col).limit(1);
      if (!error) columns.add(col);
    } catch (_error) {
      // ignore
    }
  }
  return columns;
};

const fetchPlayers = async (selectColumns, hasIsActive) => {
  const rows = [];
  let from = 0;
  while (true) {
    let query = supabase
      .from(PLAYERS_TABLE)
      .select(selectColumns.join(","))
      .order("id", { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (hasIsActive) {
      query = query.eq("is_active", true);
    }
    const { data, error } = await query;
    if (error) {
      throw new Error(error.message);
    }
    const batch = Array.isArray(data) ? data : [];
    if (!batch.length) {
      break;
    }
    rows.push(...batch);
    from += PAGE_SIZE;
    if (batch.length < PAGE_SIZE) {
      break;
    }
  }
  return rows;
};

const run = async () => {
  const playerCols = await detectColumns(PLAYERS_TABLE, ["id", "nationality", "nationality_code", "is_active"]);
  if (!playerCols.has("id")) {
    throw new Error("players.id is required");
  }
  const selectCols = ["id"];
  if (playerCols.has("nationality")) selectCols.push("nationality");
  if (playerCols.has("nationality_code")) selectCols.push("nationality_code");
  if (!selectCols.includes("nationality") && !selectCols.includes("nationality_code")) {
    throw new Error("players must include nationality or nationality_code");
  }

  console.log("[countries:rebuild] Fetching active players...");
  const rows = await fetchPlayers(selectCols, playerCols.has("is_active"));
  const grouped = new Map();

  for (const row of rows) {
    const rawName = cleanText(row.nationality);
    const normalizedName = normalizeCountry(rawName) || rawName;
    const rawCode = cleanText(row.nationality_code).toLowerCase();
    const inferredCode = String(getCountryCode(normalizedName) || "").toLowerCase();
    const code = validCode(rawCode) ? rawCode : inferredCode;
    if (!validCode(code) || code === "un") {
      continue;
    }
    const name = normalizedName || code.toUpperCase();
    const key = `${code}|${name}`;
    const item = grouped.get(key) || { code, name, player_count: 0 };
    item.player_count += 1;
    grouped.set(key, item);
  }

  const countries = [...grouped.values()].sort((a, b) => b.player_count - a.player_count);
  if (!countries.length) {
    console.log("[countries:rebuild] No countries to upsert.");
    return;
  }

  const countriesCols = await detectColumns(COUNTRIES_TABLE, [
    "code",
    "iso_code",
    "name",
    "flag_url",
    "player_count",
    "updated_at"
  ]);
  const codeColumn = countriesCols.has("code") ? "code" : countriesCols.has("iso_code") ? "iso_code" : "";
  if (!codeColumn || !countriesCols.has("name")) {
    throw new Error("countries table requires name + code/iso_code");
  }

  const payload = countries.map((item) => {
    const row = {
      name: item.name,
      flag_url: flagUrl(item.code)
    };
    row[codeColumn] = item.code;
    if (countriesCols.has("player_count")) {
      row.player_count = item.player_count;
    }
    if (countriesCols.has("updated_at")) {
      row.updated_at = new Date().toISOString();
    }
    return row;
  });

  const conflict = codeColumn;
  const { error } = await supabase.from(COUNTRIES_TABLE).upsert(payload, { onConflict: conflict });
  if (error) {
    throw new Error(error.message);
  }

  console.log(`[countries:rebuild] Players scanned: ${rows.length}`);
  console.log(`[countries:rebuild] Countries upserted: ${payload.length}`);
  console.log(`[countries:rebuild] Top sample: ${payload.slice(0, 5).map((c) => `${c.name} (${c.player_count || 0})`).join(", ")}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[countries:rebuild] Fatal:", error.message);
    process.exit(1);
  });
}

module.exports = { run };

