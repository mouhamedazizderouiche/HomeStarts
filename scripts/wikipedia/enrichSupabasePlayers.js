const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const DEFAULT_INPUT_JSON = path.resolve(
  __dirname,
  "..",
  "..",
  "data",
  "wikipedia",
  `national-team-players-${new Date().toISOString().slice(0, 10)}.json`
);

const INPUT_JSON = process.env.WIKI_INPUT_JSON || DEFAULT_INPUT_JSON;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const PAGE_SIZE = 1000;

const REQUIRED_ENV = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    throw new Error(`Missing ${key} in backend/.env`);
  }
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();

const normalizeName = (value) =>
  cleanText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\p{L}\p{N}\s]/gu, "")
    .toLowerCase();

const normalizeDate = (value) => {
  const raw = cleanText(value);
  if (!raw) {
    return "";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
};

const levenshteinDistance = (a, b) => {
  if (a === b) {
    return 0;
  }
  if (!a.length) {
    return b.length;
  }
  if (!b.length) {
    return a.length;
  }

  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i += 1) {
    matrix[i][0] = i;
  }
  for (let j = 0; j <= b.length; j += 1) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= a.length; i += 1) {
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }

  return matrix[a.length][b.length];
};

const similarityScore = (a, b) => {
  if (!a || !b) {
    return 0;
  }
  if (a === b) {
    return 1;
  }
  const distance = levenshteinDistance(a, b);
  return 1 - distance / Math.max(a.length, b.length);
};

const loadScrapedPlayers = async () => {
  const raw = await fs.readFile(INPUT_JSON, "utf8");
  const parsed = JSON.parse(raw);
  const players = Array.isArray(parsed.players) ? parsed.players : [];
  return players;
};

const fetchAllExistingPlayers = async () => {
  const all = [];
  let from = 0;
  let done = false;

  while (!done) {
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select("id,name,date_of_birth,nationality_code,transfermarkt_id,data_sources,current_club,club")
      .range(from, to);

    if (error) {
      throw new Error(`Failed loading existing players: ${error.message}`);
    }

    if (!data || data.length === 0) {
      done = true;
      continue;
    }

    all.push(...data);
    from += PAGE_SIZE;
    if (data.length < PAGE_SIZE) {
      done = true;
    }
  }

  return all.map((row) => ({
    ...row,
    normalized_name: normalizeName(row.name),
    normalized_dob: normalizeDate(row.date_of_birth)
  }));
};

const mergeDataSources = (existingSources) => {
  const merged = new Set(Array.isArray(existingSources) ? existingSources : []);
  merged.add("wikipedia");
  return [...merged];
};

const findBestMatch = (incomingPlayer, existingPlayers) => {
  const targetName = normalizeName(incomingPlayer.name);
  const targetDob = normalizeDate(incomingPlayer.dob_iso || incomingPlayer.dob);
  const preferredPool = existingPlayers.filter(
    (row) => cleanText(row.nationality_code).toLowerCase() === incomingPlayer.nationality_code
  );
  const pool = preferredPool.length ? preferredPool : existingPlayers;

  let best = null;
  let bestScore = 0;

  for (const candidate of pool) {
    const nameScore = similarityScore(targetName, candidate.normalized_name);
    const dobScore = targetDob && candidate.normalized_dob && targetDob === candidate.normalized_dob ? 1 : 0;
    const totalScore = nameScore * 0.85 + dobScore * 0.15;

    if (totalScore > bestScore) {
      best = candidate;
      bestScore = totalScore;
    }
  }

  if (!best) {
    return null;
  }

  const exactName = best.normalized_name === targetName;
  const dobMatch = targetDob && best.normalized_dob && targetDob === best.normalized_dob;

  if (exactName && (dobMatch || !targetDob || !best.normalized_dob)) {
    return best;
  }
  if (bestScore >= 0.96) {
    return best;
  }
  if (bestScore >= 0.91 && dobMatch) {
    return best;
  }
  return null;
};

const buildUpdatePayload = (incomingPlayer, existingPlayer) => ({
  current_club: cleanText(incomingPlayer.current_club),
  club: cleanText(incomingPlayer.current_club),
  is_active: true,
  is_national_team_player: true,
  caps: incomingPlayer.caps,
  goals: incomingPlayer.goals,
  nationality_code: incomingPlayer.nationality_code,
  data_sources: mergeDataSources(existingPlayer.data_sources),
  updated_at: new Date().toISOString()
});

const buildInsertPayload = (incomingPlayer) => ({
  transfermarkt_id: cleanText(incomingPlayer.transfermarkt_id || "") || null,
  name: cleanText(incomingPlayer.name),
  position: cleanText(incomingPlayer.position) || "Unknown",
  date_of_birth: normalizeDate(incomingPlayer.dob_iso || incomingPlayer.dob) || null,
  current_club: cleanText(incomingPlayer.current_club),
  club: cleanText(incomingPlayer.current_club),
  nationality_code: incomingPlayer.nationality_code,
  is_active: true,
  is_national_team_player: true,
  caps: incomingPlayer.caps,
  goals: incomingPlayer.goals,
  data_sources: ["wikipedia"],
  updated_at: new Date().toISOString()
});

const run = async () => {
  const scrapedPlayers = await loadScrapedPlayers();
  const existingPlayers = await fetchAllExistingPlayers();

  let updatedCount = 0;
  let insertedCount = 0;
  let notFoundCount = 0;

  for (const player of scrapedPlayers) {
    const normalizedName = normalizeName(player.name);
    if (!normalizedName) {
      notFoundCount += 1;
      continue;
    }

    const matched = findBestMatch(player, existingPlayers);

    if (matched) {
      const payload = buildUpdatePayload(player, matched);
      const { error } = await supabase.from(PLAYERS_TABLE).update(payload).eq("id", matched.id);
      if (error) {
        console.error(`[wiki:enrich] Update failed for ${player.name}: ${error.message}`);
        notFoundCount += 1;
        continue;
      }

      updatedCount += 1;
      matched.current_club = payload.current_club;
      matched.club = payload.club;
      matched.data_sources = payload.data_sources;
      continue;
    }

    const insertPayload = buildInsertPayload(player);
    const conflictTarget = insertPayload.transfermarkt_id
      ? "transfermarkt_id"
      : insertPayload.date_of_birth
        ? "name,date_of_birth"
        : "name";
    const { error, data } = await supabase
      .from(PLAYERS_TABLE)
      .upsert(insertPayload, { onConflict: conflictTarget })
      .select("id,name,date_of_birth,nationality_code,transfermarkt_id,data_sources,current_club,club")
      .single();

    if (error) {
      console.error(`[wiki:enrich] Insert failed for ${player.name}: ${error.message}`);
      notFoundCount += 1;
      continue;
    }

    insertedCount += 1;
    if (data) {
      existingPlayers.push({
        ...data,
        normalized_name: normalizeName(data.name),
        normalized_dob: normalizeDate(data.date_of_birth)
      });
    }
  }

  console.log(`[wiki:enrich] Updated: ${updatedCount}`);
  console.log(`[wiki:enrich] Inserted: ${insertedCount}`);
  console.log(`[wiki:enrich] Not found / failed: ${notFoundCount}`);
  console.log(
    `[wiki:enrich] Summary: ${scrapedPlayers.length} scraped players processed from ${INPUT_JSON}`
  );
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[wiki:enrich] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
