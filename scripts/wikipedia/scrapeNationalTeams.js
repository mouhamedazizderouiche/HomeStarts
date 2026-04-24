const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");
const countries = require("./countries");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const axios = backendRequire("axios");
const cheerio = backendRequire("cheerio");

const REQUEST_DELAY_MS = 3000;
const USER_AGENT =
  process.env.WIKIPEDIA_USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const BASE_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki";
const OUTPUT_JSON =
  process.env.WIKI_OUTPUT_JSON ||
  path.resolve(__dirname, "..", "..", "data", "wikipedia", `national-team-players-${new Date().toISOString().slice(0, 10)}.json`);
const COUNTRY_LIMIT = Number(process.env.WIKI_COUNTRY_LIMIT || 0);
const COUNTRY_FILTER = String(process.env.WIKI_COUNTRIES || "")
  .split(",")
  .map((item) => item.trim().toLowerCase())
  .filter(Boolean);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const cleanText = (value) =>
  String(value || "")
    .replace(/\[[^\]]*]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeName = (value) =>
  cleanText(value)
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ");

const parseNumberOrNull = (value) => {
  const extracted = String(value || "")
    .replace(/[^\d-]/g, "")
    .trim();
  if (!extracted) {
    return null;
  }
  const parsed = Number.parseInt(extracted, 10);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseDob = (value) => {
  const text = cleanText(value);
  if (!text) {
    return { dob: "", dob_iso: null };
  }

  const fromTimeTag = text.match(/\d{4}-\d{2}-\d{2}/);
  if (fromTimeTag) {
    return { dob: text, dob_iso: fromTimeTag[0] };
  }

  const parsed = new Date(text.replace(/\(.*?\)/g, "").trim());
  if (!Number.isNaN(parsed.getTime())) {
    return { dob: text, dob_iso: parsed.toISOString().slice(0, 10) };
  }

  return { dob: text, dob_iso: null };
};

const mapHeaders = ($, $table) => {
  const headerCells = $table
    .find("tr")
    .first()
    .find("th")
    .toArray()
    .map((cell) => cleanText($(cell).text()));

  if (!headerCells.length) {
    return null;
  }

  const headerMap = {};
  headerCells.forEach((header, index) => {
    const normalized = header.toLowerCase();
    if (/(player|name)/.test(normalized)) {
      headerMap.name = index;
    }
    if (/(position|pos\.)/.test(normalized)) {
      headerMap.position = index;
    }
    if (/(date of birth|birth|dob)/.test(normalized)) {
      headerMap.dob = index;
    }
    if (/caps?/.test(normalized)) {
      headerMap.caps = index;
    }
    if (/goals?/.test(normalized)) {
      headerMap.goals = index;
    }
    if (/club/.test(normalized)) {
      headerMap.current_club = index;
    }
  });

  if (headerMap.name === undefined || headerMap.current_club === undefined) {
    return null;
  }
  return headerMap;
};

const classifySquadTable = ($, $table) => {
  const headerText = cleanText(
    $table
      .find("tr")
      .first()
      .find("th")
      .toArray()
      .map((cell) => $(cell).text())
      .join(" | ")
  ).toLowerCase();

  const looksLikeSquadTable =
    /(player|name)/.test(headerText) &&
    /club/.test(headerText) &&
    /caps?/.test(headerText) &&
    /goals?/.test(headerText);

  if (!looksLikeSquadTable) {
    return "ignore";
  }

  if (/latest call-up|latest call up|recent call-up|recent call up/.test(headerText)) {
    return "recent_callups";
  }

  return "current_squad";
};

const extractPlayerName = ($cell) => {
  const linkedName = cleanText(
    $cell.find("a")
      .filter((_, link) => !cleanText(link.attribs?.title || "").toLowerCase().includes("national football team"))
      .first()
      .text()
  );

  return normalizeName(linkedName || cleanText($cell.text()));
};

const extractRowsFromTable = ({ $, $table, headerMap, country }) => {
  const players = [];

  $table.find("tr").slice(1).each((_, row) => {
    const cells = $(row).find("th, td");
    if (!cells.length) {
      return;
    }

    const readCell = (index) => {
      if (index === undefined || index < 0 || index >= cells.length) {
        return "";
      }
      return cleanText($(cells[index]).text());
    };

    const nameCell = headerMap.name !== undefined ? $(cells[headerMap.name]) : $(cells[0]);
    const name = extractPlayerName(nameCell);
    const position = readCell(headerMap.position);
    const dobRaw = readCell(headerMap.dob);
    const caps = parseNumberOrNull(readCell(headerMap.caps));
    const goals = parseNumberOrNull(readCell(headerMap.goals));
    const currentClub = readCell(headerMap.current_club);
    const { dob, dob_iso: dobIso } = parseDob(dobRaw);

    if (!name || !currentClub) {
      return;
    }

    players.push({
      name,
      position: position || "Unknown",
      dob,
      dob_iso: dobIso,
      caps,
      goals,
      current_club: currentClub,
      nationality_code: country.iso2,
      is_national_team_player: true,
      is_active: true
    });
  });

  return players;
};

const dedupePlayers = (players) => {
  const seen = new Set();
  return players.filter((player) => {
    const key = `${player.nationality_code}|${player.name.toLowerCase()}|${player.dob_iso || player.dob || ""}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
};

const scrapeCountry = async (country) => {
  const url = `${BASE_WIKIPEDIA_URL}/${country.slug}_national_football_team`;
  const response = await axios.get(url, {
    headers: { "User-Agent": USER_AGENT },
    timeout: 25000,
    responseType: "arraybuffer"
  });
  const html = Buffer.from(response.data).toString("utf8");
  const $ = cheerio.load(html);
  const currentTables = [];
  const recentTables = [];
  $("table.wikitable").each((_, table) => {
    const tableType = classifySquadTable($, $(table));
    if (tableType === "current_squad") {
      currentTables.push($(table));
    }
    if (tableType === "recent_callups") {
      recentTables.push($(table));
    }
  });

  const parseFromTables = (tables, sourceSection) =>
    tables.flatMap(($table) => {
      const headerMap = mapHeaders($, $table);
      if (!headerMap) {
        return [];
      }
      return extractRowsFromTable({ $, $table, headerMap, country }).map((row) => ({
        ...row,
        source_section: sourceSection,
        source_url: url
      }));
    });

  const currentSquad = parseFromTables(currentTables, "current_squad");
  const recentCallUps = parseFromTables(recentTables, "recent_callups");
  const players = dedupePlayers([...currentSquad, ...recentCallUps]);

  return {
    country: country.name,
    slug: country.slug,
    nationality_code: country.iso2,
    source_url: url,
    current_squad_count: currentSquad.length,
    recent_callups_count: recentCallUps.length,
    total_unique_players: players.length,
    players
  };
};

const run = async () => {
  const outputDir = path.dirname(OUTPUT_JSON);
  await fs.mkdir(outputDir, { recursive: true });

  const targetCountries = countries
    .filter((country) => {
      if (!COUNTRY_FILTER.length) {
        return true;
      }
      return COUNTRY_FILTER.includes(country.iso2) || COUNTRY_FILTER.includes(country.slug.toLowerCase());
    })
    .slice(0, COUNTRY_LIMIT > 0 ? COUNTRY_LIMIT : countries.length);

  const allCountryResults = [];
  const startedAt = new Date().toISOString();

  for (let index = 0; index < targetCountries.length; index += 1) {
    const country = targetCountries[index];
    try {
      console.log(`[wiki:scrape] (${index + 1}/${targetCountries.length}) ${country.name}...`);
      const result = await scrapeCountry(country);
      allCountryResults.push(result);
      console.log(
        `[wiki:scrape] ${country.name}: ${result.total_unique_players} players (${result.current_squad_count} current, ${result.recent_callups_count} recent)`
      );
    } catch (error) {
      console.error(`[wiki:scrape] Failed for ${country.name}: ${error.message}`);
      allCountryResults.push({
        country: country.name,
        slug: country.slug,
        nationality_code: country.iso2,
        source_url: `${BASE_WIKIPEDIA_URL}/${country.slug}_national_football_team`,
        error: error.message,
        players: []
      });
    }

    if (index < targetCountries.length - 1) {
      await sleep(REQUEST_DELAY_MS);
    }
  }

  const flatPlayers = allCountryResults.flatMap((entry) => entry.players || []);
  const output = {
    generated_at: new Date().toISOString(),
    started_at: startedAt,
    request_delay_ms: REQUEST_DELAY_MS,
    countries_requested: targetCountries.length,
    countries_successful: allCountryResults.filter((c) => !c.error).length,
    total_players: flatPlayers.length,
    countries: allCountryResults,
    players: flatPlayers
  };

  await fs.writeFile(OUTPUT_JSON, JSON.stringify(output, null, 2), "utf8");
  console.log(`[wiki:scrape] Done. Saved ${flatPlayers.length} players to ${OUTPUT_JSON}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[wiki:scrape] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
