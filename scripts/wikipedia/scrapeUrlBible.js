const fs = require("fs/promises");
const path = require("path");
const { createRequire } = require("module");
const urlBible = require("./urlBible");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const axios = backendRequire("axios");
const cheerio = backendRequire("cheerio");

const REQUEST_DELAY_MS = Number(process.env.WIKI_BIBLE_DELAY_MS || 2000);
const MAX_ITEMS = Number(process.env.WIKI_BIBLE_MAX_ITEMS || 0);
const MODE = String(process.env.WIKI_BIBLE_MODE || "all").toLowerCase();
const OUTPUT_JSON =
  process.env.WIKI_BIBLE_OUTPUT ||
  path.resolve(__dirname, "..", "..", "data", "wikipedia", `wiki-bible-${new Date().toISOString().slice(0, 10)}.json`);

const USER_AGENT =
  process.env.WIKIPEDIA_USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const client = axios.create({
  timeout: 25000,
  headers: {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9"
  }
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clean = (v) => String(v || "").replace(/\s+/g, " ").trim();

const extractNationalTeamPlayers = ($, iso2, sourceUrl) => {
  const players = [];
  $("table.wikitable").each((_, table) => {
    const headers = $(table)
      .find("tr")
      .first()
      .find("th")
      .toArray()
      .map((th) => clean($(th).text()).toLowerCase());

    const nameIdx = headers.findIndex((h) => /(player|name)/.test(h));
    const clubIdx = headers.findIndex((h) => /club/.test(h));
    const posIdx = headers.findIndex((h) => /(position|pos)/.test(h));
    if (nameIdx < 0 || clubIdx < 0) return;

    $(table)
      .find("tr")
      .slice(1)
      .each((_, row) => {
        const cells = $(row).find("th,td");
        const name = clean($(cells[nameIdx]).text());
        const club = clean($(cells[clubIdx]).text());
        const position = posIdx >= 0 ? clean($(cells[posIdx]).text()) : "";
        if (!name || !club) return;
        players.push({ name, current_club: club, position, nationality_code: iso2, source_url: sourceUrl });
      });
  });
  return players;
};

const extractCategoryLinks = ($) => {
  const links = new Set();
  $("#mw-pages a").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (!href.startsWith("/wiki/") || href.includes(":") || href.includes("(disambiguation)")) return;
    links.add(`https://en.wikipedia.org${href}`);
  });
  return [...links];
};

const extractPlayerInfobox = ($, url) => {
  const info = {
    url,
    name: clean($("h1#firstHeading").text()),
    image_url: "",
    birth_date: "",
    position: "",
    current_club: ""
  };

  const imageSrc = $("table.infobox img").first().attr("src") || "";
  if (imageSrc) info.image_url = imageSrc.startsWith("//") ? `https:${imageSrc}` : imageSrc;

  $("table.infobox tr").each((_, tr) => {
    const key = clean($(tr).find("th").first().text()).toLowerCase();
    const val = clean($(tr).find("td").first().text());
    if (!key || !val) return;
    if (/date of birth|born/.test(key) && !info.birth_date) info.birth_date = val;
    if (/position/.test(key) && !info.position) info.position = val;
    if (/current team|club/.test(key) && !info.current_club) info.current_club = val;
  });

  return info;
};

const extractFbrefRows = ($, url) => {
  const rows = [];
  $("table.stats_table tbody tr").each((_, tr) => {
    const row = $(tr);
    const name = clean(row.find("th[data-stat='player'] a").text() || row.find("th[data-stat='player']").text());
    if (!name) return;
    const minutes = clean(row.find("td[data-stat='minutes_90s']").text() || row.find("td[data-stat='minutes']").text());
    const goals = clean(row.find("td[data-stat='goals']").text());
    const assists = clean(row.find("td[data-stat='assists']").text());
    rows.push({ name, goals, assists, minutes, source_url: url });
  });
  return rows;
};

const scrapeUrl = async (entry) => {
  const response = await client.get(entry.url);
  const $ = cheerio.load(response.data || "");

  if (entry.type === "type1") {
    return {
      ...entry,
      players: extractNationalTeamPlayers($, entry.iso2, entry.url)
    };
  }

  if (entry.type === "type2") {
    const rows = [];
    $("table.wikitable tbody tr").each((_, tr) => {
      const cells = $(tr).find("th,td");
      const name = clean($(cells[0]).text());
      const caps = clean($(cells[1]).text());
      if (name) rows.push({ name, caps, source_url: entry.url });
    });
    return { ...entry, players: rows };
  }

  if (entry.type === "type3") {
    return { ...entry, player_links: extractCategoryLinks($) };
  }

  if (entry.type === "type4") {
    return { ...entry, profile: extractPlayerInfobox($, entry.url) };
  }

  if (entry.type === "type5") {
    return { ...entry, stats: extractFbrefRows($, entry.url) };
  }

  return { ...entry, data: [] };
};

const buildWorklist = () => {
  const list = [];
  if (MODE === "all" || MODE === "type1") {
    for (const item of urlBible.type1_national_teams) list.push({ type: "type1", ...item });
  }
  if (MODE === "all" || MODE === "type2") {
    for (const url of urlBible.type2_international_lists) list.push({ type: "type2", url });
  }
  if (MODE === "all" || MODE === "type3") {
    for (const url of urlBible.type3_categories) list.push({ type: "type3", url });
  }
  if (MODE === "all" || MODE === "type5") {
    for (const url of urlBible.type5_fbref) list.push({ type: "type5", url });
  }
  return MAX_ITEMS > 0 ? list.slice(0, MAX_ITEMS) : list;
};

const run = async () => {
  const worklist = buildWorklist();
  const out = [];
  console.log(`[wiki:bible] Starting mode=${MODE}, targets=${worklist.length}`);

  for (let i = 0; i < worklist.length; i += 1) {
    const entry = worklist[i];
    try {
      const result = await scrapeUrl(entry);
      out.push(result);
      console.log(`[wiki:bible] (${i + 1}/${worklist.length}) OK ${entry.type} ${entry.url}`);
    } catch (error) {
      out.push({ ...entry, error: error.message });
      console.log(`[wiki:bible] (${i + 1}/${worklist.length}) FAIL ${entry.url} -> ${error.message}`);
    }
    if (i < worklist.length - 1) await sleep(REQUEST_DELAY_MS);
  }

  if (MODE === "type4" && !worklist.length) {
    console.log("[wiki:bible] type4 mode expects direct player urls via custom integration.");
  }

  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.writeFile(
    OUTPUT_JSON,
    JSON.stringify(
      {
        generated_at: new Date().toISOString(),
        mode: MODE,
        request_delay_ms: REQUEST_DELAY_MS,
        items: out
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(`[wiki:bible] Saved ${out.length} entries -> ${OUTPUT_JSON}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[wiki:bible] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
