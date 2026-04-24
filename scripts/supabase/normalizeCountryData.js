const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(
  path.resolve(__dirname, "..", "..", "backend", "package.json")
);

const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");
const { normalizeCountry, getCountryCode } = require("../../backend/utils/countryUtils");

dotenv.config({
  path: path.resolve(__dirname, "..", "..", "backend", ".env")
});

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";

const PAGE_SIZE = 1000;
const WRITE_BATCH_SIZE = 250;

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false }
    })
  : null;

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const avatar = (name) =>
  `https://ui-avatars.com/api/?name=${encodeURIComponent(name || "Unknown Player")}&background=0F172A&color=E2E8F0`;

const getTableColumns = async () => {
  const candidates = [
    "id",
    "name",
    "nationality",
    "nationality_code",
    "club",
    "current_club",
    "is_active",
    "player_status",
    "image_url",
    "updated_at"
  ];
  const columns = new Set();

  for (const column of candidates) {
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).select(column).limit(1);
      if (!error) {
        columns.add(column);
      }
    } catch (_error) {
      // noop
    }
  }
  return columns;
};

const fetchAllRows = async (selectColumns) => {
  const rows = [];
  let from = 0;

  while (true) {
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select(selectColumns.join(","))
      .order("id", { ascending: true })
      .range(from, to);

    if (error) {
      throw new Error(error.message);
    }

    if (!Array.isArray(data) || data.length === 0) {
      break;
    }

    rows.push(...data);
    from += PAGE_SIZE;
    if (data.length < PAGE_SIZE) {
      break;
    }
  }

  return rows;
};

const run = async () => {
  if (!hasSupabaseEnv) {
    console.log("[normalize-country] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
    return;
  }

  const columns = await getTableColumns();
  const selectColumns = ["id"];
  for (const col of ["name", "nationality", "nationality_code", "club", "current_club", "is_active", "player_status", "image_url", "updated_at"]) {
    if (columns.has(col)) {
      selectColumns.push(col);
    }
  }

  if (!columns.has("id")) {
    throw new Error("players.id column is required.");
  }

  console.log("[normalize-country] Fetching players...");
  const rows = await fetchAllRows(selectColumns);

  let invalidCount = 0;
  let normalizedCountryCount = 0;
  let updatedImagesCount = 0;
  const updates = [];

  for (const row of rows) {
    const name = cleanText(row.name);
    const club = cleanText(row.current_club || row.club);
    const existingCode = cleanText(row.nationality_code).toLowerCase();
    const countryFromCode = normalizeCountry(existingCode);
    const normalizedCountry = normalizeCountry(countryFromCode || cleanText(row.nationality));
    const normalizedCode = String(getCountryCode(normalizedCountry) || existingCode || "un").toLowerCase();
    const hasValidCountry = normalizedCountry && normalizedCode && normalizedCode.length === 2;
    const isBroken =
      !name ||
      !club ||
      /unknown|without club|free agent/i.test(club) ||
      !hasValidCountry;

    if (isBroken) {
      invalidCount += 1;
    }

    const payload = { id: row.id };
    let changed = false;

    if (columns.has("nationality")) {
      payload.nationality = normalizedCountry || row.nationality || "";
      changed = changed || cleanText(payload.nationality) !== cleanText(row.nationality);
    }
    if (columns.has("nationality_code")) {
      payload.nationality_code = normalizedCode;
      changed = changed || cleanText(payload.nationality_code) !== cleanText(row.nationality_code).toLowerCase();
    }
    if (columns.has("is_active")) {
      payload.is_active = !isBroken;
      changed = changed || Boolean(payload.is_active) !== Boolean(row.is_active);
    }
    if (columns.has("player_status")) {
      if (isBroken && cleanText(row.player_status).toLowerCase() !== "verified") {
        payload.player_status = "uncertain";
        changed = true;
      }
    }
    if (columns.has("image_url") && !cleanText(row.image_url)) {
      payload.image_url = avatar(name);
      changed = true;
      updatedImagesCount += 1;
    }
    if (columns.has("name")) {
      const cleanedName = cleanText(name.replace(/\s*\(\d{4,}\)\s*$/g, ""));
      payload.name = cleanedName || name;
      changed = changed || cleanText(payload.name) !== cleanText(row.name);
    }
    if (columns.has("updated_at")) {
      payload.updated_at = new Date().toISOString();
      changed = true;
    }

    if (cleanText(payload.nationality) && cleanText(payload.nationality) !== cleanText(row.nationality)) {
      normalizedCountryCount += 1;
    }

    if (changed) {
      updates.push(payload);
    }
  }

  console.log(`[normalize-country] Applying ${updates.length} updates...`);
  for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
    const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
    const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, { onConflict: "id" });
    if (error) {
      console.log("[normalize-country] Skipping batch:", error.message);
    }
  }

  console.log(`[normalize-country] total players: ${rows.length}`);
  console.log(`[normalize-country] normalized countries: ${normalizedCountryCount}`);
  console.log(`[normalize-country] invalid players marked inactive: ${invalidCount}`);
  console.log(`[normalize-country] fallback images set: ${updatedImagesCount}`);
  console.log(`[normalize-country] updated rows: ${updates.length}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[normalize-country] Fatal:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
