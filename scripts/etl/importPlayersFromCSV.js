const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const zlib = require("zlib");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { parse } = backendRequire("csv-parse");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";

const PLAYERS_CSV_PATH = process.env.PLAYERS_CSV_PATH || process.env.TRANSFERMARKT_PLAYERS_CSV_PATH || "";
const WIKI_INPUT_JSON = process.env.WIKI_INPUT_JSON || "";
const ACTIVE_MIN_SEASON = 2024;
const WRITE_BATCH_SIZE = Number(process.env.IMPORT_WRITE_BATCH_SIZE || 1000);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeText = (value) =>
  cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
const toInteger = (value) => {
  const parsed = Number.parseInt(String(value || "").replace(/[^\d-]/g, ""), 10);
  return Number.isFinite(parsed) ? parsed : null;
};
const toNumber = (value) => {
  const parsed = Number(String(value || "").replace(/[^\d.-]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
};
const toIsoDate = (value) => {
  const raw = cleanText(value);
  if (!raw) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
};

const pick = (row, keys) => {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && cleanText(row[key]) !== "") {
      return row[key];
    }
  }
  return "";
};

const avatar = (name) =>
  `https://ui-avatars.com/api/?name=${encodeURIComponent(name || "Unknown Player")}&background=0F172A&color=E2E8F0`;

const COUNTRY_CODES = {
  tunisia: "tn",
  morocco: "ma",
  france: "fr",
  brazil: "br",
  brasil: "br",
  algeria: "dz",
  senegal: "sn",
  nigeria: "ng",
  egypt: "eg",
  "ivory coast": "ci",
  ghana: "gh",
  cameroon: "cm",
  mali: "ml",
  argentina: "ar",
  portugal: "pt",
  spain: "es",
  germany: "de",
  japan: "jp",
  "south korea": "kr",
  "saudi arabia": "sa"
};

const getCountryCode = (nationality) => (COUNTRY_CODES[normalizeText(nationality)] || "").toLowerCase();

const csvPathCandidates = [
  PLAYERS_CSV_PATH,
  path.resolve(__dirname, "..", "..", "data", "players.csv"),
  path.resolve(__dirname, "..", "..", "data", "player_profiles.csv"),
  path.resolve(__dirname, "..", "..", "data", "player_scores.csv"),
  path.resolve(__dirname, "..", "..", "data", "players.csv.gz")
].filter(Boolean);

const findExistingCsvPath = async () => {
  for (const candidate of csvPathCandidates) {
    try {
      await fsp.access(candidate);
      return candidate;
    } catch (_error) {
      // try next
    }
  }
  return "";
};

const getColumns = async (tableName) => {
  const expected = [
    "name",
    "nationality",
    "nationality_code",
    "current_club",
    "club",
    "position",
    "dob",
    "date_of_birth",
    "market_value",
    "transfermarkt_id",
    "last_season",
    "is_active",
    "is_national_team_player",
    "player_status",
    "image_url",
    "data_sources",
    "updated_at",
    "created_at"
  ];

  const columns = new Set();
  for (const column of expected) {
    try {
      const { error } = await supabase.from(tableName).select(column).limit(1);
      if (!error) {
        columns.add(column);
      }
    } catch (_error) {
      // ignore probe failures
    }
  }
  return columns.size ? columns : new Set(expected);
};

const loadWikipediaMap = async () => {
  if (!WIKI_INPUT_JSON) {
    return { byNameDob: new Map(), byName: new Map() };
  }
  try {
    const content = await fsp.readFile(WIKI_INPUT_JSON, "utf8");
    const parsed = JSON.parse(content);
    const players = Array.isArray(parsed.players) ? parsed.players : [];
    const byNameDob = new Map();
    const byName = new Map();

    for (const player of players) {
      const name = cleanText(player.name);
      if (!name) {
        continue;
      }
      const normalizedName = normalizeText(name);
      const dob = toIsoDate(player.dob_iso || player.dob);
      if (dob) {
        byNameDob.set(`${normalizedName}|${dob}`, player);
      }
      if (!byName.has(normalizedName)) {
        byName.set(normalizedName, player);
      }
    }
    return { byNameDob, byName };
  } catch (error) {
    console.log("[import-csv] Skipping wikipedia enrichment:", error.message);
    return { byNameDob: new Map(), byName: new Map() };
  }
};

const parseCsvRows = async (csvPath) => {
  const isGzip = csvPath.toLowerCase().endsWith(".gz");
  const source = fs.createReadStream(csvPath);
  const input = isGzip ? source.pipe(zlib.createGunzip()) : source;

  return new Promise((resolve, reject) => {
    const rows = [];
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_column_count: true,
      trim: true
    });

    input.on("error", reject);
    parser.on("error", reject);
    parser.on("readable", () => {
      let record = parser.read();
      while (record) {
        rows.push(record);
        record = parser.read();
      }
    });
    parser.on("end", () => resolve(rows));

    input.pipe(parser);
  });
};

const countByCountry = async (country) => {
  try {
    const { count, error } = await supabase
      .from(PLAYERS_TABLE)
      .select("*", { count: "exact", head: true })
      .ilike("nationality", country);
    if (error) {
      return 0;
    }
    return Number(count || 0);
  } catch (_error) {
    return 0;
  }
};

const flushBatch = async (batch, columns) => {
  if (!batch.length) {
    return 0;
  }

  let writeResult = null;
  if (columns.has("transfermarkt_id")) {
    writeResult = await supabase.from(PLAYERS_TABLE).upsert(batch, {
      onConflict: "transfermarkt_id"
    });
  } else if (columns.has("name") && columns.has("dob")) {
    writeResult = await supabase.from(PLAYERS_TABLE).upsert(batch, {
      onConflict: "name,dob"
    });
  } else if (columns.has("name") && columns.has("date_of_birth")) {
    writeResult = await supabase.from(PLAYERS_TABLE).upsert(batch, {
      onConflict: "name,date_of_birth"
    });
  } else {
    writeResult = await supabase.from(PLAYERS_TABLE).insert(batch);
  }

  if (writeResult?.error) {
    const msg = String(writeResult.error.message || "");
    if (msg.includes("no unique or exclusion constraint matching the ON CONFLICT specification")) {
      const insertResult = await supabase.from(PLAYERS_TABLE).insert(batch);
      if (!insertResult.error) {
        return batch.length;
      }
      console.log("[import-csv] Skipping batch:", insertResult.error.message);
      return 0;
    }
    console.log("[import-csv] Skipping batch:", msg);
    return 0;
  }
  return batch.length;
};

const run = async () => {
  const startedAt = Date.now();
  const csvPath = await findExistingCsvPath();
  if (!csvPath) {
    throw new Error(
      `No local CSV source found. Set PLAYERS_CSV_PATH. Checked: ${csvPathCandidates.join(", ")}`
    );
  }

  console.log(`[import-csv] Reading CSV: ${csvPath}`);
  const rows = await parseCsvRows(csvPath);
  console.log(`[import-csv] CSV rows loaded: ${rows.length}`);

  const columns = await getColumns(PLAYERS_TABLE);
  const wiki = await loadWikipediaMap();

  let seasonFieldDetected = 0;
  for (const row of rows.slice(0, 2000)) {
    if (toInteger(pick(row, ["last_season", "season", "latest_season", "last_active_season"]))) {
      seasonFieldDetected += 1;
    }
  }
  const enforceSeasonFilter = seasonFieldDetected > 20;

  let skippedNoClub = 0;
  let skippedInactive = 0;
  let preparedCount = 0;
  let upserted = 0;
  const batch = [];

  for (const row of rows) {
    const name = cleanText(pick(row, ["player_name", "name", "player", "full_name", "player_slug"]));
    const nationality = cleanText(
      pick(row, ["citizenship", "nationality", "country_of_citizenship", "country", "nation"])
    );
    const currentClub = cleanText(
      pick(row, ["current_club_name", "current_club", "club", "club_name", "team_name", "team"])
    );
    const lastSeason = toInteger(pick(row, ["last_season", "season", "latest_season", "last_active_season"]));

    if (!name || !nationality) {
      continue;
    }

    if (!currentClub || /retired|unknown/i.test(currentClub)) {
      skippedNoClub += 1;
      continue;
    }

    if (enforceSeasonFilter && (!lastSeason || lastSeason < ACTIVE_MIN_SEASON)) {
      skippedInactive += 1;
      continue;
    }

    const transfermarktId = cleanText(pick(row, ["player_id", "transfermarkt_id", "id"]));
    const dob = toIsoDate(pick(row, ["date_of_birth", "dob", "birth_date"]));
    const normalizedName = normalizeText(name);
    const wikiMatch =
      (dob && wiki.byNameDob.get(`${normalizedName}|${dob}`)) ||
      wiki.byName.get(normalizedName) ||
      null;

    const payload = {
      name,
      nationality,
      nationality_code: getCountryCode(nationality),
      current_club: cleanText(wikiMatch?.current_club || currentClub),
      club: cleanText(wikiMatch?.current_club || currentClub),
      position: cleanText(pick(row, ["main_position", "position", "sub_position", "pos"])) || "Unknown",
      market_value: toNumber(pick(row, ["market_value", "market_value_in_eur", "market_value_eur"])),
      transfermarkt_id:
        transfermarktId || `tm_${normalizedName}_${dob || "na"}_${normalizeText(currentClub)}`,
      last_season: lastSeason || ACTIVE_MIN_SEASON,
      is_active: true,
      is_national_team_player: Boolean(wikiMatch),
      player_status: wikiMatch ? "verified" : "active",
      image_url: avatar(name),
      data_sources: wikiMatch ? ["transfermarkt", "wikipedia"] : ["transfermarkt"],
      updated_at: new Date().toISOString()
    };

    if (columns.has("dob")) {
      payload.dob = dob;
    } else if (columns.has("date_of_birth")) {
      payload.date_of_birth = dob;
    }

    const projected = {};
    for (const [key, value] of Object.entries(payload)) {
      if (columns.has(key)) {
        projected[key] = value;
      }
    }

    batch.push(projected);
    preparedCount += 1;

    if (batch.length >= WRITE_BATCH_SIZE) {
      upserted += await flushBatch(batch, columns);
      batch.length = 0;
    }
  }

  if (batch.length) {
    upserted += await flushBatch(batch, columns);
  }

  console.log(`[import-csv] Active players prepared: ${preparedCount}`);
  console.log(`[import-csv] Skipped empty/retired club rows: ${skippedNoClub}`);
  console.log(`[import-csv] Skipped by season filter: ${skippedInactive}`);

  const [totalCount, tnCount, maCount, frCount, brCount] = await Promise.all([
    supabase
      .from(PLAYERS_TABLE)
      .select("*", { count: "exact", head: true })
      .then((r) => Number(r.count || 0))
      .catch(() => 0),
    countByCountry("Tunisia"),
    countByCountry("Morocco"),
    countByCountry("France"),
    countByCountry("Brazil")
  ]);

  const seconds = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`[import-csv] Upserted rows: ${upserted}`);
  console.log(`[import-csv] Total players in DB: ${totalCount}`);
  console.log(`[import-csv] Tunisia players: ${tnCount}`);
  console.log(`[import-csv] Morocco players: ${maCount}`);
  console.log(`[import-csv] France players: ${frCount}`);
  console.log(`[import-csv] Brazil players: ${brCount}`);
  console.log(`[import-csv] Import runtime: ${seconds}s`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[import-csv] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
