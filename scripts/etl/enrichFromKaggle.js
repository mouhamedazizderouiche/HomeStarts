const path = require("path");
const fs = require("fs");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const axios = backendRequire("axios");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const BATCH_SIZE = 250;
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

const toNumber = (value) => {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

// Load Kaggle player-scores CSV if available
const loadKaggleScores = () => {
  const csvPath = path.resolve(__dirname, "..", "..", "data", "kaggle_player_scores.csv");
  if (!fs.existsSync(csvPath)) {
    console.log("[kaggle] CSV not found at", csvPath);
    return new Map();
  }

  const content = fs.readFileSync(csvPath, "utf8");
  const lines = content.split("\n");
  const headers = lines[0].split(",").map((h) => h.trim().toLowerCase());
  const nameIdx = headers.indexOf("player_name") >= 0 ? headers.indexOf("player_name") : headers.indexOf("name");
  const goalsIdx = headers.indexOf("goals");
  const assistsIdx = headers.indexOf("assists");
  const ratingIdx = headers.indexOf("rating");
  const matchesIdx = headers.indexOf("matches");

  const scoreMap = new Map();
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const parts = lines[i].split(",");
    if (nameIdx < 0 || !parts[nameIdx]) continue;

    const playerName = cleanText(parts[nameIdx]).replace(/"/g, "");
    const goals = toNumber(parts[goalsIdx]);
    const assists = toNumber(parts[assistsIdx]);
    const rating = toNumber(parts[ratingIdx]);
    const matches = toNumber(parts[matchesIdx]);

    scoreMap.set(normalizeText(playerName), { goals, assists, rating, matches });
  }

  console.log(`[kaggle] Loaded ${scoreMap.size} player scores from CSV`);
  return scoreMap;
};

const fetchAllPlayers = async () => {
  const rows = [];
  let from = 0;
  let done = false;

  while (!done) {
    const to = from + BATCH_SIZE - 1;
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select("id,name,goals_2025,assists_2025,rating_2025,matches_2025")
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
    from += BATCH_SIZE;
    if (data.length < BATCH_SIZE) {
      done = true;
    }
  }

  return rows;
};

const run = async () => {
  if (!hasSupabaseEnv) {
    console.log("[kaggle] Skipping: missing Supabase credentials.");
    return;
  }

  const kaggleScores = loadKaggleScores();
  if (!kaggleScores.size) {
    console.log("[kaggle] No Kaggle data to enrich.");
    return;
  }

  let players = [];
  try {
    players = await fetchAllPlayers();
  } catch (error) {
    console.log("[kaggle] Skipping:", error.message);
    return;
  }

  const updates = [];
  for (const player of players) {
    const scoreData = kaggleScores.get(normalizeText(player.name));
    if (!scoreData) continue;

    const currentGoals = Number(player.goals_2025 || 0);
    const currentAssists = Number(player.assists_2025 || 0);
    const currentRating = Number(player.rating_2025 || 0);
    const currentMatches = Number(player.matches_2025 || 0);

    // Only update if Kaggle has better data
    const hasImprovement =
      (scoreData.goals > 0 && currentGoals === 0) ||
      (scoreData.assists > 0 && currentAssists === 0) ||
      (scoreData.rating > currentRating && scoreData.rating > 0);

    if (hasImprovement) {
      updates.push({
        id: player.id,
        goals_2025: scoreData.goals > 0 ? scoreData.goals : currentGoals,
        assists_2025: scoreData.assists > 0 ? scoreData.assists : currentAssists,
        rating_2025: scoreData.rating > 0 ? scoreData.rating : currentRating,
        matches_2025: scoreData.matches > 0 ? scoreData.matches : currentMatches,
      });
    }
  }

  console.log(`[kaggle] Players scanned: ${players.length}, Can improve: ${updates.length}`);

  for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
    const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, {
        onConflict: "id",
      });
      if (error) {
        console.log("[kaggle] Batch error:", error.message);
        continue;
      }
      console.log(`[kaggle] Batch ${Math.floor(i / WRITE_BATCH_SIZE) + 1} updated: ${batch.length} players`);
    } catch (error) {
      console.log("[kaggle] Batch error:", error.message);
      continue;
    }
  }

  console.log(`[kaggle] Completed. Improved: ${updates.length} players`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[kaggle] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
