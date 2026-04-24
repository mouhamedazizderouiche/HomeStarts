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
const BATCH_SIZE = 50;
const REQUEST_DELAY_MS = 2000;
const WRITE_BATCH_SIZE = 25;
const MAX_ITERATIONS = 500; // Process up to 25k players

const hasSupabaseEnv = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabase = hasSupabaseEnv
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    })
  : null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const cleanText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const isHttp = (value) => /^https?:\/\//i.test(String(value || "").trim());
const isPlaceholder = (url) =>
  url && (url.includes("dicebear") || url.includes("ui-avatars") || url.includes("avatar"));

let lastRequestAt = 0;

// Enhanced Transfermarkt player image search
const fetchTransfermarktPlayerImage = async (playerName) => {
  const elapsed = Date.now() - lastRequestAt;
  if (elapsed < REQUEST_DELAY_MS) {
    await sleep(REQUEST_DELAY_MS - elapsed);
  }

  try {
    const searchUrl = `https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query=${encodeURIComponent(playerName)}`;
    const response = await axios.get(searchUrl, {
      timeout: 10000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      },
    });

    lastRequestAt = Date.now();
    const $ = cheerio.load(response.data || "");

    // Extract player image from search results
    let imageUrl = $("img.tm-lazy-img").first().attr("data-src") || $("img").first().attr("src") || "";

    if (isHttp(imageUrl) && imageUrl.includes("transfermarkt")) {
      return imageUrl;
    }

    // Try alternate selectors
    imageUrl = $("td.hauptlink img").first().attr("src") || "";
    if (isHttp(imageUrl)) {
      return imageUrl;
    }

    return "";
  } catch (error) {
    lastRequestAt = Date.now();
    return "";
  }
};

// Wikipedia player image search
const fetchWikipediaPlayerImage = async (playerName) => {
  const elapsed = Date.now() - lastRequestAt;
  if (elapsed < REQUEST_DELAY_MS) {
    await sleep(REQUEST_DELAY_MS - elapsed);
  }

  try {
    const wikiUrl = `https://en.wikipedia.org/wiki/${encodeURIComponent(cleanText(playerName).replace(/\s+/g, "_"))}`;
    const response = await axios.get(wikiUrl, {
      timeout: 10000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      },
    });

    lastRequestAt = Date.now();
    const $ = cheerio.load(response.data || "");

    // Get infobox image
    let imageUrl = $("table.infobox img").first().attr("src") || "";

    if (imageUrl) {
      imageUrl = imageUrl.startsWith("//") ? `https:${imageUrl}` : imageUrl;
      if (isHttp(imageUrl) && !imageUrl.includes("blank")) {
        return imageUrl;
      }
    }

    return "";
  } catch (error) {
    lastRequestAt = Date.now();
    return "";
  }
};

// Get players with placeholder images
const getPlayersWithPlaceholders = async (limit) => {
  const candidates = ["id", "name", "image_url", "image"];
  const available = new Set();

  for (const col of candidates) {
    try {
      const { error } = await supabase.from(PLAYERS_TABLE).select(col).limit(1);
      if (!error) {
        available.add(col);
      }
    } catch (_) {
      // ignore
    }
  }

  const selectCols = Array.from(available).join(",");
  const { data, error } = await supabase
    .from(PLAYERS_TABLE)
    .select(selectCols)
    .not("image_url", "like", "%transfermarkt%")
    .not("image_url", "like", "%wikimedia%")
    .limit(limit);

  return !error && Array.isArray(data) ? data : [];
};

const run = async () => {
  if (!hasSupabaseEnv) {
    console.log("[player-images] Skipping: missing Supabase credentials.");
    return;
  }

  let totalProcessed = 0;
  let totalImproved = 0;

  for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
    const players = await getPlayersWithPlaceholders(BATCH_SIZE);

    if (!players.length) {
      console.log("[player-images] No more players with placeholder images.");
      break;
    }

    const updates = [];

    for (const player of players) {
      const playerName = cleanText(player.name);
      if (!playerName) continue;

      console.log(`[player-images] [${totalProcessed + 1}] Searching: ${playerName}`);

      let imageUrl =
        (await fetchTransfermarktPlayerImage(playerName)) || (await fetchWikipediaPlayerImage(playerName));

      if (imageUrl && isHttp(imageUrl) && !isPlaceholder(imageUrl)) {
        updates.push({
          id: player.id,
          image_url: imageUrl,
          image: imageUrl,
          photo: imageUrl,
        });
        totalImproved++;
        console.log(`[player-images] ✓ Found image: ${imageUrl.substring(0, 60)}`);
      }

      totalProcessed++;
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
            console.log(`[player-images] Batch updated: ${batch.length} players`);
          }
        } catch (e) {
          console.log("[player-images] Batch error:", e.message);
        }
      }
    }

    console.log(
      `[player-images] Iteration ${iteration + 1}: Processed=${totalProcessed}, Improved=${totalImproved}`
    );
  }

  console.log(`[player-images] Completed. Total processed: ${totalProcessed}, Improved: ${totalImproved}`);
};

if (require.main === module) {
  run().catch((error) => {
    console.error("[player-images] Fatal error:", error.message);
    process.exit(1);
  });
}

module.exports = { run };
