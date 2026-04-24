const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(
  path.resolve(__dirname, "..", "..", "backend", "package.json")
);

const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({
  path: path.resolve(__dirname, "..", "..", "backend", ".env"),
});

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";

const PAGE_SIZE = 1000;
const WRITE_BATCH_SIZE = 250;
const ACTIVE_SEASON_MIN = 2023;

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeSource = (value) => cleanText(value).toLowerCase();

const normalizeClubName = (value) =>
  cleanText(value)
    .replace(/\(\s*[\u2013-]\s*\d{4}\s*\)/g, "")
    .replace(/\(\s*[\u2013-]\s*\d{2,4}\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();

const getTableColumns = async (tableName) => {
  const candidates = [
    "id",
    "current_club",
    "club",
    "last_season",
    "is_national_team_player",
    "is_active",
    "player_status",
    "data_sources",
    "last_seen_at",
    "updated_at",
  ];

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
    console.log("[clean] Skipping step: could not detect schema columns.");
  }
  return columns;
};

const includesWikipedia = (row) => {
  const dataSources = Array.isArray(row.data_sources) ? row.data_sources : [];
  return dataSources.some((item) => normalizeSource(item) === "wikipedia");
};

const computePlayerStatus = ({ verified, isActive }) => {
  if (verified) return "verified";
  if (isActive) return "active";
  return "uncertain";
};

const fetchAllPlayers = async (columns) => {
  const selectable = [
    "id",
    "current_club",
    "club",
    "last_season",
    "is_national_team_player",
    "is_active",
    "player_status",
    "data_sources",
    "last_seen_at",
    "updated_at",
  ].filter((column) => columns.has(column));

  if (!selectable.includes("id")) {
    throw new Error("Required column players.id is missing.");
  }

  const rows = [];
  let from = 0;
  let done = false;

  while (!done) {
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select(selectable.join(","))
      .order("id", { ascending: true })
      .range(from, to);

    if (error) {
      throw new Error(`Failed to fetch players: ${error.message}`);
    }

    if (!data || data.length === 0) {
      done = true;
      continue;
    }

    rows.push(...data);
    from += PAGE_SIZE;
    if (data.length < PAGE_SIZE) {
      done = true;
    }
  }

  return rows;
};

const run = async () => {
  if (!hasSupabaseEnv) {
    console.log("[clean] Skipping step: missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
    return;
  }

  const columns = await getTableColumns(PLAYERS_TABLE);
  if (!columns.size) {
    console.log("[clean] Skipping step: no schema metadata available.");
    return;
  }

  console.log("[clean] Fetching players...");
  let allPlayers = [];
  try {
    allPlayers = await fetchAllPlayers(columns);
  } catch (error) {
    console.log("[clean] Skipping step:", error.message);
    return;
  }

  const updates = [];
  let removedPlayers = 0;

  for (const row of allPlayers) {
    const club = cleanText(
      (columns.has("current_club") ? row.current_club : "") ||
        (columns.has("club") ? row.club : "") ||
        ""
    );
    const cleanedClub = normalizeClubName(club);
    const lastSeason = Number(columns.has("last_season") ? row.last_season || 0 : 0);
    const isNationalTeamPlayer = Boolean(
      columns.has("is_national_team_player") ? row.is_national_team_player : false
    );
    const existingIsActive = Boolean(columns.has("is_active") ? row.is_active : true);

    const isClubInvalid = !cleanedClub || /unknown|without club|free agent/i.test(cleanedClub);
    const isActive = !isClubInvalid && (lastSeason >= ACTIVE_SEASON_MIN || isNationalTeamPlayer);
    if (existingIsActive && !isActive) {
      removedPlayers += 1;
    }

    const verified = columns.has("data_sources") ? includesWikipedia(row) : false;
    const playerStatus = computePlayerStatus({ verified, isActive });

    const payload = { id: row.id };
    if (columns.has("current_club")) payload.current_club = cleanedClub || null;
    if (columns.has("club")) payload.club = cleanedClub || null;
    if (columns.has("is_active")) payload.is_active = isActive;
    if (columns.has("player_status")) payload.player_status = playerStatus;
    if (columns.has("last_seen_at")) {
      payload.last_seen_at = isActive
        ? row.last_seen_at || new Date().toISOString()
        : row.last_seen_at || null;
    }
    if (columns.has("updated_at")) payload.updated_at = new Date().toISOString();

    let changed = false;
    if (columns.has("current_club")) {
      changed = changed || cleanText(row.current_club) !== cleanText(payload.current_club);
    }
    if (columns.has("club")) {
      changed = changed || cleanText(row.club) !== cleanText(payload.club);
    }
    if (columns.has("is_active")) {
      changed = changed || Boolean(row.is_active) !== Boolean(payload.is_active);
    }
    if (columns.has("player_status")) {
      changed = changed || cleanText(row.player_status) !== cleanText(payload.player_status);
    }
    if (columns.has("last_seen_at")) {
      changed = changed || cleanText(row.last_seen_at) !== cleanText(payload.last_seen_at);
    }

    if (changed) {
      updates.push(payload);
    }
  }

  console.log(`[clean] Applying ${updates.length} updates...`);
  for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
    const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, {
        onConflict: "id",
      });
      if (error) {
        console.log("[clean] Skipping step:", error.message);
        continue;
      }
    } catch (error) {
      console.log("[clean] Skipping step:", error.message);
      continue;
    }
  }

  const activePlayers = allPlayers.filter((row) => {
    const club = cleanText(
      (columns.has("current_club") ? row.current_club : "") ||
        (columns.has("club") ? row.club : "") ||
        ""
    );
    const cleanedClub = normalizeClubName(club);
    const lastSeason = Number(columns.has("last_season") ? row.last_season || 0 : 0);
    const isNationalTeamPlayer = Boolean(
      columns.has("is_national_team_player") ? row.is_national_team_player : false
    );
    return (
      cleanedClub &&
      !/unknown|without club|free agent/i.test(cleanedClub) &&
      (lastSeason >= ACTIVE_SEASON_MIN || isNationalTeamPlayer)
    );
  }).length;

  const verifiedPlayers = allPlayers.filter((row) =>
    columns.has("data_sources") ? includesWikipedia(row) : false
  ).length;

  console.log(`[clean] total players: ${allPlayers.length}`);
  console.log(`[clean] active players: ${activePlayers}`);
  console.log(`[clean] verified players: ${verifiedPlayers}`);
  console.log(`[clean] removed players (set inactive): ${removedPlayers}`);
  console.log(`[clean] updated rows: ${updates.length}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[clean] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
