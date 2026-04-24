const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({
  path: path.resolve(__dirname, "..", "..", "backend", ".env")
});

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const CLUBS_TABLE = process.env.SUPABASE_CLUBS_TABLE || "clubs";
const PAGE_SIZE = 1000;
const WRITE_BATCH_SIZE = 300;
const MAX_PLAYERS = Number(process.env.BACKFILL_ASSETS_MAX_PLAYERS || 0);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const isAvatar = (value) => /ui-avatars\.com/i.test(String(value || ""));
const isHttp = (value) => /^https?:\/\//i.test(String(value || "").trim());
const extractIdHint = (value) => {
  const text = decodeURIComponent(String(value || ""));
  const match = text.match(/\((\d{4,})\)/);
  const parsed = Number(match?.[1] || 0);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
};

const detectColumns = async (tableName, candidates) => {
  const columns = new Set();
  try {
    const { data, error } = await supabase.rpc("get_columns", { table_name: tableName });
    if (!error && Array.isArray(data)) {
      const tableColumns = new Set(data.map((row) => String(row?.column_name || "").trim()).filter(Boolean));
      for (const candidate of candidates) {
        if (tableColumns.has(candidate)) {
          columns.add(candidate);
        }
      }
      return columns;
    }
  } catch (_error) {
    // fallback probes
  }
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

const fetchAll = async (tableName, select) => {
  const rows = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from(tableName)
      .select(select)
      .order("id", { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
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

const flushUpdatesById = async (tableName, payload) => {
  for (let i = 0; i < payload.length; i += WRITE_BATCH_SIZE) {
    const batch = payload.slice(i, i + WRITE_BATCH_SIZE);
    for (const row of batch) {
      const { id, ...fields } = row;
      const { error } = await supabase.from(tableName).update(fields).eq("id", id);
      if (error) {
        console.log(`[assets:backfill] Skipping ${tableName} row ${id}: ${error.message}`);
      }
    }
  }
};

const flushUpsertsById = async (tableName, payload) => {
  for (let i = 0; i < payload.length; i += WRITE_BATCH_SIZE) {
    const batch = payload.slice(i, i + WRITE_BATCH_SIZE);
    const { error } = await supabase.from(tableName).upsert(batch, { onConflict: "id" });
    if (error) {
      console.log(`[assets:backfill] Skipping ${tableName} batch ${Math.floor(i / WRITE_BATCH_SIZE) + 1}: ${error.message}`);
    } else if (i % (WRITE_BATCH_SIZE * 10) === 0) {
      console.log(`[assets:backfill] ${tableName} upsert progress: ${Math.min(i + WRITE_BATCH_SIZE, payload.length)}/${payload.length}`);
    }
  }
};

const run = async () => {
  const playerCols = await detectColumns(PLAYERS_TABLE, [
    "id",
    "name",
    "transfermarkt_id",
    "image_url",
    "updated_at"
  ]);
  if (!playerCols.has("id") || !playerCols.has("image_url")) {
    console.log("[assets:backfill] Players table missing required columns, skipping players.");
  } else {
    const selectCols = ["id", "image_url"];
    if (playerCols.has("transfermarkt_id")) selectCols.push("transfermarkt_id");
    if (playerCols.has("name")) selectCols.push("name");
    let players = await fetchAll(PLAYERS_TABLE, selectCols.join(","));
    if (MAX_PLAYERS > 0) {
      players = players.slice(0, MAX_PLAYERS);
    }
    const updates = [];
    for (const row of players) {
      const tmId =
        Number(row.transfermarkt_id || 0) ||
        extractIdHint(row.name) ||
        extractIdHint(row.image_url);
      if (!Number.isFinite(tmId) || tmId <= 0) continue;
      const current = String(row.image_url || "").trim();
      if (isHttp(current) && !isAvatar(current)) continue;
      const payload = {
        id: row.id,
        image_url: `https://img.transfermarkt.com/portrait/normal/${tmId}.jpg`
      };
      if (playerCols.has("updated_at")) {
        payload.updated_at = new Date().toISOString();
      }
      updates.push(payload);
    }
    if (updates.length) {
      await flushUpsertsById(PLAYERS_TABLE, updates);
    }
    console.log(`[assets:backfill] Players scanned: ${players.length}`);
    console.log(`[assets:backfill] Player images updated: ${updates.length}`);
  }

  const clubCols = await detectColumns(CLUBS_TABLE, ["id", "transfermarkt_id", "logo_url", "updated_at"]);
  if (!clubCols.has("id") || !clubCols.has("transfermarkt_id") || !clubCols.has("logo_url")) {
    console.log("[assets:backfill] Clubs table missing required columns, skipping clubs.");
  } else {
    const clubs = await fetchAll(CLUBS_TABLE, "id,transfermarkt_id,logo_url");
    const updates = [];
    for (const row of clubs) {
      const tmId = Number(row.transfermarkt_id || 0);
      if (!Number.isFinite(tmId) || tmId <= 0) continue;
      const current = String(row.logo_url || "").trim();
      if (isHttp(current) && !isAvatar(current)) continue;
      const payload = {
        id: row.id,
        logo_url: `https://tmssl.akamaized.net/images/wappen/normal/${tmId}.png`
      };
      if (clubCols.has("updated_at")) {
        payload.updated_at = new Date().toISOString();
      }
      updates.push(payload);
    }
    if (updates.length) {
      await flushUpdatesById(CLUBS_TABLE, updates);
    }
    console.log(`[assets:backfill] Clubs scanned: ${clubs.length}`);
    console.log(`[assets:backfill] Club logos updated: ${updates.length}`);
  }
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[assets:backfill] Fatal:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
