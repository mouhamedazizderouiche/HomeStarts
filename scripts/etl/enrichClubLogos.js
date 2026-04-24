const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const axios = backendRequire("axios");
const cheerio = backendRequire("cheerio");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";
const CLUBS_TABLE = process.env.SUPABASE_CLUBS_TABLE || "clubs";
const BATCH_SIZE = 100;
const REQUEST_DELAY_MS = 1500;
const WRITE_BATCH_SIZE = 50;

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeText = (value) =>
  cleanText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
const isHttp = (value) => /^https?:\/\//i.test(String(value || "").trim());

let lastRequestAt = 0;

// Search Wikipedia for club logo
const fetchClubLogoWikipedia = async (clubName) => {
  const elapsed = Date.now() - lastRequestAt;
  if (elapsed < REQUEST_DELAY_MS) {
    await sleep(REQUEST_DELAY_MS - elapsed);
  }

  try {
    const searchUrl = `https://en.wikipedia.org/wiki/${encodeURIComponent(cleanText(clubName).replace(/\s+/g, "_"))}`;
    const response = await axios.get(searchUrl, {
      timeout: 10000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      },
    });

    lastRequestAt = Date.now();
    const $ = cheerio.load(response.data || "");

    // Try to find club logo/crest
    let logo = $("table.infobox img").first().attr("src") || "";
    if (!logo) {
      logo = $("div.mw-parser-output img").first().attr("src") || "";
    }

    if (logo) {
      logo = logo.startsWith("//") ? `https:${logo}` : logo;
      if (isHttp(logo)) {
        return logo;
      }
    }
    return "";
  } catch (error) {
    lastRequestAt = Date.now();
    return "";
  }
};

// Search Transfermarkt for club logo
const fetchClubLogoTransfermarkt = async (clubName) => {
  const elapsed = Date.now() - lastRequestAt;
  if (elapsed < REQUEST_DELAY_MS) {
    await sleep(REQUEST_DELAY_MS - elapsed);
  }

  try {
    const searchUrl = `https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query=${encodeURIComponent(clubName)}`;
    const response = await axios.get(searchUrl, {
      timeout: 10000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      },
    });

    lastRequestAt = Date.now();
    const $ = cheerio.load(response.data || "");

    // Look for club logo in search results
    const logoUrl = $("img.tm-lazy-img").first().attr("data-src") || $("img").first().attr("src") || "";
    if (isHttp(logoUrl) && logoUrl.includes("wappen")) {
      return logoUrl;
    }
    return "";
  } catch (error) {
    lastRequestAt = Date.now();
    return "";
  }
};

// Query existing clubs table for logos
const getClubLogosFromTable = async () => {
  if (!supabase) return new Map();

  try {
    const { data, error } = await supabase
      .from(CLUBS_TABLE)
      .select("name,logo_url")
      .not("logo_url", "is", null)
      .limit(5000);

    if (error || !Array.isArray(data)) return new Map();

    const logoMap = new Map();
    for (const club of data) {
      if (club.name && club.logo_url && isHttp(club.logo_url)) {
        logoMap.set(normalizeText(club.name), club.logo_url);
      }
    }
    return logoMap;
  } catch (error) {
    return new Map();
  }
};

const run = async () => {
  if (!hasSupabaseEnv) {
    console.log("[club-logos] Skipping: missing Supabase credentials.");
    return;
  }

  console.log("[club-logos] Loading existing club logos from database...");
  const clubLogos = await getClubLogosFromTable();
  console.log(`[club-logos] Found ${clubLogos.size} clubs with logos in database`);

  // Get players with missing/fallback club logos
  let from = 0;
  let done = false;
  let processedCount = 0;
  let improvedCount = 0;

  while (!done && processedCount < 1000) {
    const to = from + BATCH_SIZE - 1;
    const { data, error } = await supabase
      .from(PLAYERS_TABLE)
      .select("id,name,club,current_club,club_logo_url")
      .limit(BATCH_SIZE)
      .range(from, to);

    if (error || !data || data.length === 0) {
      done = true;
      continue;
    }

    const updates = [];
    for (const player of data) {
      const clubName = cleanText(player.current_club || player.club || "");
      if (!clubName) continue;

      const clubKey = normalizeText(clubName);
      let logoUrl = null;

      // Check if we have it cached
      if (clubLogos.has(clubKey)) {
        logoUrl = clubLogos.get(clubKey);
      } else if (!player.club_logo_url || player.club_logo_url.includes("dicebear")) {
        // Try to fetch real logo
        console.log(`[club-logos] Searching logo for: ${clubName}`);
        logoUrl =
          (await fetchClubLogoTransfermarkt(clubName)) || (await fetchClubLogoWikipedia(clubName));

        if (logoUrl) {
          clubLogos.set(clubKey, logoUrl);
        }
      }

      if (logoUrl && isHttp(logoUrl) && !logoUrl.includes("dicebear")) {
        updates.push({
          id: player.id,
          club_logo_url: logoUrl,
        });
        improvedCount++;
      }
    }

    // Write batch
    if (updates.length > 0) {
      for (let i = 0; i < updates.length; i += WRITE_BATCH_SIZE) {
        const batch = updates.slice(i, i + WRITE_BATCH_SIZE);
        try {
          const { error } = await supabase.from(PLAYERS_TABLE).upsert(batch, {
            onConflict: "id",
          });
          if (!error) {
            console.log(`[club-logos] Updated ${batch.length} player club logos`);
          }
        } catch (e) {
          console.log("[club-logos] Update error:", e.message);
        }
      }
    }

    processedCount += data.length;
    from += BATCH_SIZE;
    if (data.length < BATCH_SIZE) {
      done = true;
    }
  }

  console.log(`[club-logos] Completed. Processed: ${processedCount}, Improved: ${improvedCount}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[club-logos] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
