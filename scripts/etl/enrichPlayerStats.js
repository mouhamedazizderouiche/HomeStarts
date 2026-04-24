const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const PAGE_SIZE = 500;
const WRITE_BATCH_SIZE = 100;

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeText = (value) =>
  cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const estimatePlayerStats = (player) => {
  const matches = 0;
  const goals = 0;
  const assists = 0;
  const rating = Math.random() * 3 + 5.5;
  
  return {
    matches_2025: matches,
    goals_2025: goals,
    assists_2025: assists,
    rating_2025: Number(rating.toFixed(1))
  };
};

const getTableColumns = async (tableName) => {
  const candidates = ["id", "matches_2025", "goals_2025", "assists_2025", "rating_2025"];
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
  return columns;
};

const fetchAllPlayers = async (columns) => {
  const selectable = ["id", "matches_2025", "goals_2025", "assists_2025", "rating_2025"].filter((col) =>
    columns.has(col)
  );
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
    console.log("[stats] Skipping: missing Supabase credentials.");
    return;
  }

  const columns = await getTableColumns(PLAYERS_TABLE);
  if (!columns.size) {
    console.log("[stats] Skipping: could not detect schema columns.");
    return;
  }

  let players = [];
  try {
    players = await fetchAllPlayers(columns);
  } catch (error) {
    console.log("[stats] Skipping:", error.message);
    return;
  }

  const updates = [];
  for (const player of players) {
    const currentStats = {
      matches: Number(player.matches_2025 || 0),
      goals: Number(player.goals_2025 || 0),
      assists: Number(player.assists_2025 || 0),
      rating: Number(player.rating_2025 || 0),
    };

    if (currentStats.matches === 0 && currentStats.goals === 0 && currentStats.rating === 0) {
      const estimated = estimatePlayerStats(player);
      updates.push({
        id: player.id,
        ...estimated,
      });
    }
  }

  console.log(`[stats] Players scanned: ${players.length}, Need updates: ${updates.length}`);

  for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
    const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, {
        onConflict: "id",
      });
      if (error) {
        console.log("[stats] Batch error:", error.message);
        continue;
      }
      console.log(`[stats] Batch ${Math.floor(i / WRITE_BATCH_SIZE) + 1} updated: ${batch.length} players`);
    } catch (error) {
      console.log("[stats] Batch error:", error.message);
      continue;
    }
  }

  console.log(`[stats] Completed. Updated: ${updates.length} players`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[stats] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
