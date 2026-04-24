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

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const toNumber = (value) => {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

const computeFormScore = ({ goals2025, assists2025, rating2025 }) => {
  const goalsPart = clamp(toNumber(goals2025), 0, 30) * 0.4;
  const assistsPart = clamp(toNumber(assists2025), 0, 20) * 0.3;
  const ratingPart = clamp(toNumber(rating2025), 0, 10) * 0.3;
  const rawScore = goalsPart + assistsPart + ratingPart;
  const maxRawScore = 30 * 0.4 + 20 * 0.3 + 10 * 0.3;
  const normalized = clamp((rawScore / maxRawScore) * 10, 0, 10);
  return Number(normalized.toFixed(2));
};

const computeTopScore = ({ isNationalTeamPlayer, formScore, marketValue }) => {
  const nationalTeamBonus = isNationalTeamPlayer ? 50 : 0;
  const formPart = toNumber(formScore) * 10;
  const marketValuePart = toNumber(marketValue) / 1_000_000;
  return Number((nationalTeamBonus + formPart + marketValuePart).toFixed(2));
};

const getTableColumns = async (tableName) => {
  const candidates = [
    "id",
    "goals_2025",
    "assists_2025",
    "rating_2025",
    "form_score",
    "top_score",
    "market_value",
    "is_national_team_player",
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
    console.log("[scores] Skipping step: could not detect schema columns.");
  }
  return columns;
};

const fetchAllPlayers = async (columns) => {
  const selectable = [
    "id",
    "goals_2025",
    "assists_2025",
    "rating_2025",
    "form_score",
    "top_score",
    "market_value",
    "is_national_team_player",
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
    console.log("[scores] Skipping step: missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
    return;
  }

  const columns = await getTableColumns(PLAYERS_TABLE);
  if (!columns.size) {
    console.log("[scores] Skipping step: no schema metadata available.");
    return;
  }

  if (!columns.has("form_score") && !columns.has("top_score")) {
    console.log("[scores] Skipping step: form_score/top_score columns not present.");
    return;
  }

  let players = [];
  try {
    players = await fetchAllPlayers(columns);
  } catch (error) {
    console.log("[scores] Skipping step:", error.message);
    return;
  }

  const nowIso = new Date().toISOString();
  const updates = [];

  for (const player of players) {
    const formScore = computeFormScore({
      goals2025: columns.has("goals_2025") ? player.goals_2025 : 0,
      assists2025: columns.has("assists_2025") ? player.assists_2025 : 0,
      rating2025: columns.has("rating_2025") ? player.rating_2025 : 0,
    });
    const topScore = computeTopScore({
      isNationalTeamPlayer: columns.has("is_national_team_player")
        ? Boolean(player.is_national_team_player)
        : false,
      formScore,
      marketValue: columns.has("market_value") ? player.market_value : 0,
    });

    const payload = { id: player.id };
    let changed = false;

    if (columns.has("form_score")) {
      payload.form_score = formScore;
      changed = changed || Number(player.form_score || 0) !== formScore;
    }
    if (columns.has("top_score")) {
      payload.top_score = topScore;
      changed = changed || Number(player.top_score || 0) !== topScore;
    }
    if (columns.has("last_seen_at")) {
      payload.last_seen_at = player.last_seen_at || nowIso;
      changed = changed || !player.last_seen_at;
    }
    if (columns.has("updated_at")) {
      payload.updated_at = nowIso;
    }

    if (changed) {
      updates.push(payload);
    }
  }

  for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
    const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, {
        onConflict: "id",
      });
      if (error) {
        console.log("[scores] Skipping step:", error.message);
        continue;
      }
    } catch (error) {
      console.log("[scores] Skipping step:", error.message);
      continue;
    }
  }

  console.log(`[scores] players scanned: ${players.length}`);
  console.log(`[scores] players updated: ${updates.length}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[scores] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run, computeFormScore, computeTopScore };
