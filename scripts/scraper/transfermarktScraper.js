const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const axios = backendRequire("axios");
const cheerio = backendRequire("cheerio");
const dotenv = backendRequire("dotenv");
const pLimitImport = backendRequire("p-limit");
const { createClient } = backendRequire("@supabase/supabase-js");

const { normalizeCountry, getCountryCode } = require("../../backend/utils/countryUtils");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const BASE = "https://www.transfermarkt.com";
const OUTPUT_DIR = path.resolve(__dirname, "..", "..", "data", "transfermarkt");
const PROFILE_DELAY_MS = Number(process.env.TRANSFERMARKT_PROFILE_DELAY_MS || 3000);
const PROFILE_CONCURRENCY = Number(process.env.TRANSFERMARKT_PROFILE_CONCURRENCY || 2);
const MAX_RETRIES = Number(process.env.TRANSFERMARKT_MAX_RETRIES || 3);
const REQUEST_TIMEOUT_MS = Number(process.env.TRANSFERMARKT_TIMEOUT_MS || 30000);

const pLimit = typeof pLimitImport === "function" ? pLimitImport : pLimitImport.default;
const profileLimit = pLimit(Math.max(1, PROFILE_CONCURRENCY));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const hasSupabase = Boolean(SUPABASE_URL && SUPABASE_KEY);
const supabase = hasSupabase
  ? createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } })
  : null;

const USER_AGENT =
  process.env.TRANSFERMARKT_USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const client = axios.create({
  timeout: REQUEST_TIMEOUT_MS,
  headers: {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
  }
});

const COUNTRY_MAP = {
  173: { name: "Tunisia", iso2: "tn" },
  107: { name: "Morocco", iso2: "ma" },
  4: { name: "Algeria", iso2: "dz" },
  149: { name: "Senegal", iso2: "sn" },
  124: { name: "Nigeria", iso2: "ng" },
  2: { name: "Egypt", iso2: "eg" },
  38: { name: "Cote d'Ivoire", iso2: "ci" },
  26: { name: "Brazil", iso2: "br" },
  50: { name: "France", iso2: "fr" },
  54: { name: "Ghana", iso2: "gh" },
  31: { name: "Cameroon", iso2: "cm" }
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clean = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalize = (value) => clean(value).toLowerCase();
const toInt = (value) => {
  const n = Number.parseInt(String(value || "").replace(/[^\d-]/g, ""), 10);
  return Number.isFinite(n) ? n : null;
};

const parseMarketValueToEuros = (raw) => {
  const text = clean(raw).toLowerCase().replace(/\s+/g, "");
  if (!text) return null;
  const match = text.match(/€?([\d.,]+)([mk]|bn)?/i);
  if (!match) return null;
  const numberText = match[1];
  const unit = String(match[2] || "").toLowerCase();
  let normalized = numberText;
  if (unit) {
    normalized = numberText.includes(",") && numberText.includes(".")
      ? numberText.replace(/\./g, "").replace(",", ".")
      : numberText.replace(",", ".");
  } else {
    if (numberText.includes(",") && numberText.includes(".")) {
      normalized = numberText.replace(/\./g, "").replace(",", ".");
    } else if (numberText.includes(",")) {
      normalized = numberText.replace(",", ".");
    } else if (numberText.includes(".")) {
      const parts = numberText.split(".");
      const last = parts[parts.length - 1];
      normalized = last.length <= 2 ? numberText : numberText.replace(/\./g, "");
    }
  }
  const numeric = Number.parseFloat(normalized);
  if (!Number.isFinite(numeric)) return null;
  if (unit === "m") return Math.round(numeric * 1_000_000);
  if (unit === "k") return Math.round(numeric * 1_000);
  if (unit === "bn") return Math.round(numeric * 1_000_000_000);
  return Math.round(numeric);
};

const parseDate = (raw) => {
  const text = clean(raw);
  if (!text) return null;
  const direct = new Date(text);
  if (!Number.isNaN(direct.getTime())) {
    return direct.toISOString().slice(0, 10);
  }
  const alt = text.match(/(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})/);
  if (!alt) return null;
  const day = Number(alt[1]);
  const month = Number(alt[2]);
  const year = Number(alt[3].length === 2 ? `20${alt[3]}` : alt[3]);
  const d = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
};

const parseHeightCm = (raw) => {
  const text = clean(raw).toLowerCase();
  const cm = text.match(/(\d{2,3})\s*cm/);
  if (cm) return `${cm[1]} cm`;
  const meters = text.match(/(\d(?:[.,]\d{1,2}))\s*m/);
  if (!meters) return null;
  const n = Number.parseFloat(meters[1].replace(",", "."));
  if (!Number.isFinite(n)) return null;
  return `${Math.round(n * 100)} cm`;
};

const normalizeNationalityValue = (raw) => {
  const text = clean(raw);
  if (!text) return { nationality: null, nationality_code: null };

  const direct = normalizeCountry(text);
  const directCode = String(getCountryCode(direct) || "").toLowerCase();
  if (directCode) {
    return { nationality: direct, nationality_code: directCode };
  }

  const pieces = text
    .split(/[\/,;|]+/)
    .map((p) => clean(p))
    .filter(Boolean);
  for (const piece of pieces) {
    const n = normalizeCountry(piece);
    const c = String(getCountryCode(n) || "").toLowerCase();
    if (c) {
      return { nationality: n, nationality_code: c };
    }
  }

  const words = text.split(/\s+/).map((w) => clean(w)).filter(Boolean);
  for (const word of words) {
    const n = normalizeCountry(word);
    const c = String(getCountryCode(n) || "").toLowerCase();
    if (c) {
      return { nationality: n, nationality_code: c };
    }
  }

  return { nationality: text, nationality_code: null };
};

const parsePlayerIdFromHref = (href) => {
  const match = String(href || "").match(/\/spieler\/(\d+)/i);
  return match ? toInt(match[1]) : null;
};

const parseClubIdFromHref = (href) => {
  const match = String(href || "").match(/\/verein\/(\d+)/i);
  return match ? toInt(match[1]) : null;
};

let profileRequestGate = Promise.resolve();
let lastProfileRequestAt = 0;

const scheduleProfileSlot = async () => {
  let release;
  const next = new Promise((resolve) => {
    release = resolve;
  });
  const previous = profileRequestGate;
  profileRequestGate = previous.then(() => next);
  await previous;

  const now = Date.now();
  const wait = Math.max(0, PROFILE_DELAY_MS - (now - lastProfileRequestAt));
  if (wait > 0) {
    await sleep(wait);
  }
  lastProfileRequestAt = Date.now();
  release();
};

const fetchWithRetry = async (url, { isProfile = false } = {}) => {
  let lastError = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      if (isProfile) {
        await scheduleProfileSlot();
      }
      const response = await client.get(url);
      return String(response.data || "");
    } catch (error) {
      lastError = error;
      const status = error?.response?.status;
      const shouldRetry = attempt < MAX_RETRIES;
      console.log(
        `[transfermarkt] request failed (${attempt}/${MAX_RETRIES}) ${url} status=${
          status || "n/a"
        } message=${error.message}`
      );
      if (!shouldRetry) {
        break;
      }
      const backoffMs = 1000 * attempt + Math.floor(Math.random() * 500);
      await sleep(backoffMs);
    }
  }
  throw lastError || new Error(`Request failed for ${url}`);
};

const detectTableColumns = async (tableName, candidates) => {
  if (!hasSupabase) {
    return new Set(candidates);
  }
  const out = new Set();
  for (const col of candidates) {
    try {
      const { error } = await supabase.from(tableName).select(col).limit(1);
      if (!error) out.add(col);
    } catch (_error) {
      // no-op
    }
  }
  return out;
};

const parsePlayerRowsFromCountryPage = (html, fallbackCountry = "") => {
  const $ = cheerio.load(html);
  const destinationCountry = clean(
    $("select[name='land'] option[selected]").first().text() ||
      $("select[name='land'] option[selected='selected']").first().text() ||
      fallbackCountry ||
      ""
  );
  const rows = [];

  $("table.items tbody tr.odd, table.items tbody tr.even").each((_, tr) => {
    const row = $(tr);
    const playerAnchor = row.find("td a[href*='/profil/spieler/']").first();
    const profilePath = clean(playerAnchor.attr("href"));
    const playerName = clean(playerAnchor.attr("title") || playerAnchor.text());
    const transfermarktId = parsePlayerIdFromHref(profilePath);
    if (!playerName || !transfermarktId || !profilePath) {
      return;
    }

    const clubAnchor = row.find("td a[href*='/startseite/verein/']").first();
    const currentClubName = clean(clubAnchor.attr("title") || clubAnchor.text());
    const clubTransfermarktId = parseClubIdFromHref(clubAnchor.attr("href") || "");

    const leagueAnchor = row.find("td a[href*='/wettbewerb/']").first();
    const leagueName = clean(leagueAnchor.attr("title") || leagueAnchor.text());

    const clubCountry = destinationCountry || null;
    const marketValueRaw = clean(row.find("td.rechts.hauptlink").last().text());

    rows.push({
      player_name: playerName,
      transfermarkt_id: transfermarktId,
      player_profile_url: profilePath.startsWith("http") ? profilePath : `${BASE}${profilePath}`,
      current_club_name: currentClubName || null,
      club_transfermarkt_id: clubTransfermarktId || null,
      league_name: leagueName || null,
      club_country: clubCountry || null,
      market_value_raw: marketValueRaw || null,
      market_value: parseMarketValueToEuros(marketValueRaw)
    });
  });

  return rows;
};

const scrapeDiasporaRows = async (landId) => {
  const url = `${BASE}/statistik/legionaere?land_id=${landId}`;
  const html = await fetchWithRetry(url);
  const $ = cheerio.load(html);
  const nationalityName = clean($(`select[name='land_id'] option[value='${landId}']`).first().text());

  const byCountryLinks = [
    ...new Set(
      $("table.items tbody tr a[href*='/spieler-statistik/legionaere/statistik/stat/land_id/']")
        .map((_, a) => clean($(a).attr("href")))
        .get()
        .filter((href) => href.includes(`/land_id/${landId}/land/`))
    )
  ];

  const rowsById = new Map();
  for (const relHref of byCountryLinks) {
    const pageUrl = relHref.startsWith("http") ? relHref : `${BASE}${relHref}`;
    try {
      const pageHtml = await fetchWithRetry(pageUrl);
      const pageCountry = clean(
        cheerio.load(pageHtml)("select[name='land'] option[selected]").first().text()
      );
      const players = parsePlayerRowsFromCountryPage(pageHtml, pageCountry);
      for (const p of players) {
        if (!rowsById.has(p.transfermarkt_id)) {
          rowsById.set(p.transfermarkt_id, p);
        }
      }
    } catch (error) {
      console.log(`[transfermarkt] failed country-page scrape ${pageUrl}: ${error.message}`);
    }
  }

  // Fallback: if country pages failed, try base page directly (legacy layout)
  if (!rowsById.size) {
    for (const p of parsePlayerRowsFromCountryPage(html)) {
      rowsById.set(p.transfermarkt_id, p);
    }
  }

  return { url, nationality_name: nationalityName || null, rows: [...rowsById.values()] };
};

const extractInfoboxValue = ($, labelRegex) => {
  let value = "";
  $("li.data-header__label, span.info-table__content--regular").each((_, el) => {
    const label = clean($(el).text());
    if (!labelRegex.test(label)) {
      return;
    }
    const parent = $(el).closest("li, tr, div");
    const candidate = clean(parent.text().replace(label, ""));
    if (candidate && !value) {
      value = candidate;
    }
  });
  if (value) {
    return value;
  }

  $("tr").each((_, tr) => {
    const left = clean($(tr).find("th,td").first().text());
    if (!labelRegex.test(left)) {
      return;
    }
    const right = clean($(tr).find("td").last().text());
    if (right && !value) {
      value = right;
    }
  });
  return value;
};

const cleanHeaderName = (name) =>
  clean(
    String(name || "")
      .replace(/^[^\w#]*#\s*\d+\s*/i, "")
      .replace(/^#\s*\d+\s*/i, "")
      .replace(/\s+/g, " ")
  );

const scrapePlayerProfile = async (playerFromList) => {
  const html = await fetchWithRetry(playerFromList.player_profile_url, { isProfile: true });
  const $ = cheerio.load(html);

  const fullName = cleanHeaderName(
    $("h1.data-header__headline-wrapper").first().text() ||
      $("h1[itemprop='name']").first().text() ||
      playerFromList.player_name
  );
  const labels = $("span.info-table__content--regular")
    .map((_, el) => clean($(el).text()).replace(/:$/, ""))
    .get();
  const values = $("span.info-table__content--bold")
    .map((_, el) => clean($(el).text()))
    .get();
  const profileMap = new Map();
  for (let i = 0; i < Math.min(labels.length, values.length); i += 1) {
    if (labels[i]) {
      profileMap.set(normalize(labels[i]), values[i]);
    }
  }

  const pick = (...keys) => {
    for (const key of keys) {
      const hit = profileMap.get(normalize(key));
      if (hit) return hit;
    }
    return "";
  };

  const dateOfBirthRaw = pick("Date of birth/Age", "Date of birth", "Born");
  const positionRaw = pick("Position");
  const position = clean(positionRaw.replace(/^Attack\s*-\s*/i, "").replace(/^Midfield\s*-\s*/i, ""));
  const height = parseHeightCm(pick("Height"));
  const preferredFoot = clean(pick("Foot"));
  const jerseyRaw = pick("Shirt number", "Jersey", "Number");
  const jerseyNumber = toInt(jerseyRaw);
  const nationalityRaw = clean(pick("Citizenship", "Nationality"));
  const currentClub = clean(pick("Current club")) || playerFromList.current_club_name || null;
  const contractExpiry = parseDate(pick("Contract expires", "Contract until"));

  const marketValueRaw = clean(
    $("a.data-header__market-value-wrapper").first().text() ||
      $("div.data-header__box--small").find("a").first().text() ||
      playerFromList.market_value_raw ||
      ""
  );
  const imageCandidate =
    $("meta[property='og:image']").attr("content") ||
    $("img.data-header__profile-image").attr("src") ||
    "";

  const nationalityNormalized = normalizeNationalityValue(nationalityRaw || "");
  const nationality = nationalityNormalized.nationality;
  const nationalityCode = nationalityNormalized.nationality_code;

  return {
    transfermarkt_id: playerFromList.transfermarkt_id,
    full_name: fullName || playerFromList.player_name,
    date_of_birth: parseDate(dateOfBirthRaw),
    position: position || null,
    nationality: nationality || nationalityRaw || null,
    nationality_code: nationalityCode,
    height: height || null,
    preferred_foot: preferredFoot || null,
    jersey_number: jerseyNumber,
    current_club: currentClub,
    contract_expiry: contractExpiry,
    market_value: parseMarketValueToEuros(marketValueRaw) || playerFromList.market_value || null,
    player_image_url: clean(imageCandidate) || null
  };
};

const ensureDir = async (dir) => {
  await fs.mkdir(dir, { recursive: true });
};

const saveRawSnapshot = async (payload, countryLabel) => {
  await ensureDir(OUTPUT_DIR);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const safeCountry = normalize(countryLabel || "unknown").replace(/[^a-z0-9_-]/g, "-");
  const file = path.join(OUTPUT_DIR, `transfermarkt-diaspora-${safeCountry}-${stamp}.json`);
  await fs.writeFile(file, JSON.stringify(payload, null, 2), "utf8");
  return file;
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
};

const upsertClubs = async (players, clubColumns) => {
  const clubsByName = new Map();
  for (const p of players) {
    const name = clean(p.current_club_name || p.current_club || "");
    if (!name) continue;
    if (!clubsByName.has(normalize(name))) {
      clubsByName.set(normalize(name), {
        name,
        country: clean(p.club_country || ""),
        transfermarkt_id: p.club_transfermarkt_id || null
      });
    }
  }

  const clubNames = [...clubsByName.values()].map((c) => c.name);
  if (!clubNames.length) {
    return new Map();
  }

  const idMap = new Map();
  for (const namesChunk of chunk(clubNames, 200)) {
    const { data, error } = await supabase.from("clubs").select("id,name").in("name", namesChunk);
    if (error) {
      throw new Error(`clubs select failed: ${error.message}`);
    }
    for (const row of Array.isArray(data) ? data : []) {
      idMap.set(normalize(row.name), row.id);
    }
  }

  const inserts = [];
  for (const club of clubsByName.values()) {
    if (!idMap.has(normalize(club.name))) {
      const payload = {};
      if (clubColumns.has("name")) payload.name = club.name;
      if (clubColumns.has("country")) payload.country = club.country || null;
      if (clubColumns.has("transfermarkt_id")) payload.transfermarkt_id = club.transfermarkt_id || null;
      if (clubColumns.has("logo_url")) payload.logo_url = null;
      if (clubColumns.has("created_at")) payload.created_at = new Date().toISOString();
      inserts.push(payload);
    }
  }

  for (const payload of inserts) {
    try {
      const { error } = await supabase.from("clubs").insert(payload);
      if (error) {
        console.log(`[transfermarkt] club insert skipped: ${error.message}`);
      }
    } catch (error) {
      console.log(`[transfermarkt] club insert error: ${error.message}`);
    }
  }

  for (const namesChunk of chunk(clubNames, 200)) {
    const { data, error } = await supabase.from("clubs").select("id,name").in("name", namesChunk);
    if (error) {
      throw new Error(`clubs refresh failed: ${error.message}`);
    }
    for (const row of Array.isArray(data) ? data : []) {
      idMap.set(normalize(row.name), row.id);
    }
  }

  return idMap;
};

const upsertPlayers = async (players, clubIdByName, playerColumns) => {
  const ids = [...new Set(players.map((p) => p.transfermarkt_id).filter(Boolean))];
  const existing = new Set();
  for (const idsChunk of chunk(ids, 200)) {
    const { data, error } = await supabase
      .from("players")
      .select("transfermarkt_id")
      .in("transfermarkt_id", idsChunk);
    if (error) {
      throw new Error(`players select failed: ${error.message}`);
    }
    for (const row of Array.isArray(data) ? data : []) {
      existing.add(Number(row.transfermarkt_id));
    }
  }

  let inserted = 0;
  let updated = 0;

  const payloads = [];
  for (const player of players) {
    if (!player.transfermarkt_id) {
      continue;
    }
    const clubName = clean(player.current_club_name || player.current_club || "");
    const clubId = clubIdByName.get(normalize(clubName)) || null;
    const payload = {};
    if (playerColumns.has("transfermarkt_id")) payload.transfermarkt_id = player.transfermarkt_id;
    if (playerColumns.has("name")) payload.name = clean(player.full_name || player.player_name || "");
    if (playerColumns.has("nationality")) payload.nationality = clean(player.nationality || "");
    if (playerColumns.has("nationality_code")) payload.nationality_code = clean(player.nationality_code || "");
    if (playerColumns.has("position")) payload.position = clean(player.position || "") || null;
    if (playerColumns.has("date_of_birth")) payload.date_of_birth = player.date_of_birth || null;
    if (playerColumns.has("height")) payload.height = player.height || null;
    if (playerColumns.has("preferred_foot")) payload.preferred_foot = player.preferred_foot || null;
    if (playerColumns.has("jersey_number")) payload.jersey_number = player.jersey_number || null;
    if (playerColumns.has("current_club_id")) payload.current_club_id = clubId;
    if (playerColumns.has("market_value")) payload.market_value = player.market_value || null;
    if (playerColumns.has("contract_expires")) payload.contract_expires = player.contract_expiry || null;
    if (playerColumns.has("image_url")) payload.image_url = player.player_image_url || null;
    if (playerColumns.has("is_active")) payload.is_active = true;
    if (playerColumns.has("data_sources")) payload.data_sources = ["transfermarkt"];
    if (playerColumns.has("created_at")) payload.created_at = new Date().toISOString();
    if (playerColumns.has("updated_at")) payload.updated_at = new Date().toISOString();

    if (existing.has(Number(player.transfermarkt_id))) {
      updated += 1;
    } else {
      inserted += 1;
    }
    payloads.push(payload);
  }

  for (const payloadChunk of chunk(payloads, 200)) {
    const { error } = await supabase.from("players").upsert(payloadChunk, {
      onConflict: "transfermarkt_id"
    });
    if (error) {
      throw new Error(`players upsert failed: ${error.message}`);
    }
  }

  return { inserted, updated };
};

const parseArgs = () => {
  const args = process.argv.slice(2);
  const out = { landId: 173, dryRun: false, maxPlayers: 0 };
  for (const arg of args) {
    if (arg.startsWith("--land_id=")) out.landId = toInt(arg.split("=")[1]) || 173;
    if (arg.startsWith("--max_players=")) out.maxPlayers = toInt(arg.split("=")[1]) || 0;
    if (arg === "--dry-run") out.dryRun = true;
  }
  return out;
};

const run = async () => {
  const cfg = parseArgs();
  let country = COUNTRY_MAP[cfg.landId] || {
    name: `land_id_${cfg.landId}`,
    iso2: null
  };

  console.log(`[transfermarkt] start land_id=${cfg.landId} country=${country.name} dryRun=${cfg.dryRun}`);
  const stats = {
    totalPlayersFound: 0,
    playersProcessed: 0,
    inserted: 0,
    updated: 0,
    errors: 0
  };

  const diaspora = await scrapeDiasporaRows(cfg.landId);
  if (diaspora.nationality_name) {
    const normalizedName = normalizeCountry(diaspora.nationality_name);
    country = {
      name: normalizedName || country.name,
      iso2: String(getCountryCode(normalizedName || diaspora.nationality_name) || country.iso2 || "").toLowerCase() || null
    };
  }
  stats.totalPlayersFound = diaspora.rows.length;
  console.log(`[transfermarkt] diaspora players found: ${stats.totalPlayersFound}`);

  const uniqueById = new Map();
  for (const row of diaspora.rows) {
    if (!row.transfermarkt_id) continue;
    if (!uniqueById.has(row.transfermarkt_id)) {
      uniqueById.set(row.transfermarkt_id, row);
    }
  }
  let basePlayers = [...uniqueById.values()];
  if (cfg.maxPlayers > 0) {
    basePlayers = basePlayers.slice(0, cfg.maxPlayers);
  }

  const profileResults = await Promise.all(
    basePlayers.map((row) =>
      profileLimit(async () => {
        try {
          const profile = await scrapePlayerProfile(row);
          stats.playersProcessed += 1;
          return {
            ...row,
            ...profile,
            nationality_code: profile.nationality_code || country.iso2 || null
          };
        } catch (error) {
          stats.errors += 1;
          console.log(
            `[transfermarkt] profile error id=${row.transfermarkt_id} name=${row.player_name}: ${error.message}`
          );
          return {
            ...row,
            nationality_code: country.iso2 || null
          };
        }
      })
    )
  );

  const rawPayload = {
    source: "transfermarkt",
    scraped_at: new Date().toISOString(),
    country: country.name,
    land_id: cfg.landId,
    players: profileResults
  };
  const rawFile = await saveRawSnapshot(rawPayload, country.name);
  console.log(`[transfermarkt] raw snapshot saved: ${rawFile}`);

  if (cfg.dryRun) {
    console.log("[transfermarkt] dry-run enabled, skipping Supabase upsert.");
    console.log(`[transfermarkt] players processed: ${stats.playersProcessed}`);
    console.log(`[transfermarkt] errors: ${stats.errors}`);
    return;
  }

  if (!hasSupabase) {
    console.log("[transfermarkt] missing SUPABASE_URL or SUPABASE_KEY/SUPABASE_SERVICE_ROLE_KEY, skipping DB.");
    return;
  }

  const clubColumns = await detectTableColumns("clubs", [
    "id",
    "name",
    "country",
    "logo_url",
    "transfermarkt_id",
    "created_at"
  ]);
  const playerColumns = await detectTableColumns("players", [
    "transfermarkt_id",
    "name",
    "nationality",
    "nationality_code",
    "position",
    "date_of_birth",
    "height",
    "preferred_foot",
    "jersey_number",
    "current_club_id",
    "market_value",
    "contract_expires",
    "image_url",
    "is_active",
    "data_sources",
    "created_at",
    "updated_at"
  ]);

  try {
    const clubIdByName = await upsertClubs(profileResults, clubColumns);
    const writeStats = await upsertPlayers(profileResults, clubIdByName, playerColumns);
    stats.inserted = writeStats.inserted;
    stats.updated = writeStats.updated;
  } catch (error) {
    stats.errors += 1;
    console.log(`[transfermarkt] database error: ${error.message}`);
  }

  console.log(`[transfermarkt] total players found: ${stats.totalPlayersFound}`);
  console.log(`[transfermarkt] players processed: ${stats.playersProcessed}`);
  console.log(`[transfermarkt] inserted: ${stats.inserted}`);
  console.log(`[transfermarkt] updated: ${stats.updated}`);
  console.log(`[transfermarkt] errors: ${stats.errors}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[transfermarkt] fatal:", error.message);
    process.exit(1);
  });
}

module.exports = {
  scrapeDiasporaRows,
  scrapePlayerProfile,
  parseMarketValueToEuros,
  run
};
