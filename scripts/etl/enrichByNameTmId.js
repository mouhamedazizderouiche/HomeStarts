const path = require('path');
const { createRequire } = require('module');
const backendRequire = createRequire(path.resolve(__dirname, '..', '..', 'backend', 'package.json'));
const dotenv = backendRequire('dotenv');
const { createClient } = backendRequire('@supabase/supabase-js');

dotenv.config({ path: path.resolve(__dirname, '..', '..', 'backend', '.env') });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLAYERS_TABLE = process.env.SUPABASE_PLAYERS_TABLE || 'players';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const BATCH = Number(process.env.ETL_TM_BATCH || 1000);
const START_OFFSET = Number(process.env.START_OFFSET || 0);
const FETCH_RETRIES = Number(process.env.ETL_FETCH_RETRIES || 3);
const FETCH_RETRY_MS = Number(process.env.ETL_FETCH_RETRY_MS || 2000);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchPageWithRetry(from, to) {
  let attempt = 0;
  while (attempt < FETCH_RETRIES) {
    attempt += 1;
    try {
      const { data, error } = await supabase
        .from(PLAYERS_TABLE)
        .select('id,name,image_url')
        .ilike('image_url','%ui-avatars%')
        .range(from, to);
      if (error) throw error;
      return data || [];
    } catch (err) {
      console.warn(`[etl:tm-name] Fetch attempt ${attempt} failed for range ${from}-${to}: ${err.message}`);
      if (attempt >= FETCH_RETRIES) throw err;
      await sleep(FETCH_RETRY_MS * attempt);
    }
  }
  return [];
}

(async function main(){
  console.log('[etl:tm-name] Starting targeted TM update...');
  let offset = START_OFFSET || 0;
  let totalUpdated = 0;
  try {
    while (true) {
      const from = offset;
      const to = offset + BATCH - 1;
      const data = await fetchPageWithRetry(from, to);
      if (!data || data.length === 0) break;

      const updates = [];
      for (const p of data) {
        const m = String(p.name || '').match(/\((\d{3,8})\)/);
        if (m) {
          const id = m[1];
          const url = `https://img.transfermarkt.com/portrait/header/default/${id}.jpg`;
          updates.push({ id: p.id, image_url: url });
        }
      }

      for (const u of updates) {
        const { error: upErr } = await supabase.from(PLAYERS_TABLE).update({ image_url: u.image_url }).eq('id', u.id);
        if (!upErr) totalUpdated += 1;
        else console.warn('[etl:tm-name] Update failed', u.id, upErr.message);
      }

      console.log(`[etl:tm-name] Page offset=${offset} processed. PageUpdated=${updates.length} TotalUpdated=${totalUpdated}`);
      offset += BATCH;
    }
    console.log(`[etl:tm-name] Completed. TotalUpdated=${totalUpdated}`);
  } catch (err) {
    console.error('[etl:tm-name] Fatal error:', err.message);
    process.exit(1);
  }
})();
