const path = require("path");
const { createRequire } = require("module");

const backendRequire = createRequire(path.resolve(__dirname, "..", "..", "backend", "package.json"));
const dotenv = backendRequire("dotenv");
const { createClient } = backendRequire("@supabase/supabase-js");

dotenv.config({ path: path.resolve(__dirname, "..", "..", "backend", ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || "players";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const BATCH_SIZE = 200;

const extractTmFromRow = (row) => {
  const sName = String(row?.name || "");
  const sImage = String(row?.image_url || "");

  // 1) Look for a portrait URL pattern used by Transfermarkt
  const portraitMatch = sImage.match(/portrait\/header\/(?:default\/)?(\d{3,7})\.jpg/i);
  if (portraitMatch && Number.isFinite(Number(portraitMatch[1]))) return Number(portraitMatch[1]);

  // 2) Look for any 3-7 digit number in image_url (some hints contain ids)
  const imgNum = sImage.match(/(\d{3,7})/);
  if (imgNum && Number.isFinite(Number(imgNum[1]))) return Number(imgNum[1]);

  // 3) Look for parentheses in name like "Player Name (388220)"
  const nameParen = sName.match(/\((\d{3,7})\)/);
  if (nameParen && Number.isFinite(Number(nameParen[1]))) return Number(nameParen[1]);

  // 4) Fallback: any standalone 3-7 digit number in the name
  const nameNum = sName.match(/(\d{3,7})/);
  if (nameNum && Number.isFinite(Number(nameNum[1]))) return Number(nameNum[1]);

  return 0;
};

const run = async () => {
  console.log("[etl:tm-images] Starting Transfermarkt image backfill...");

  const { data: candidates, error } = await supabase
    .from(PLAYERS_TABLE)
    .select("id,name,image_url")
    .ilike("image_url", "%ui-avatars%")
    .limit(2000);

  if (error) {
    console.error("[etl:tm-images] Failed to load candidates:", error.message);
    process.exit(1);
  }

  const rows = Array.isArray(candidates) ? candidates : [];
  console.log(`[etl:tm-images] Candidates with avatar fallback: ${rows.length}`);

  let updated = 0;
  let found = 0;
  let skipped = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const updates = [];
    for (const p of batch) {
      const tmId = extractTmFromRow(p);
      if (!tmId) {
        skipped += 1;
        continue;
      }
      found += 1;
      const url = `https://img.transfermarkt.com/portrait/header/default/${tmId}.jpg`;
      updates.push({ id: p.id, image_url: url });
    }

    let batchUpdated = 0;
    for (const u of updates) {
      const { error: upErr } = await supabase.from(PLAYERS_TABLE).update({ image_url: u.image_url }).eq("id", u.id);
      if (!upErr) {
        updated += 1;
        batchUpdated += 1;
      } else {
        console.warn(`[etl:tm-images] Failed to update ${u.id}: ${upErr.message}`);
      }
    }
    console.log(`[etl:tm-images] Batch ${Math.floor(i / BATCH_SIZE) + 1} done. BatchUpdated=${batchUpdated} TotalUpdated=${updated}`);
  }

  console.log(`[etl:tm-images] Completed. Updated=${updated} Found=${found} Skipped=${skipped}`);
};

if (require.main === module) {
  run().catch((err) => {
    console.error('[etl:tm-images] Fatal error:', err.message);
    process.exit(1);
  });
}

module.exports = { run };