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

(async function main(){
  const { data, error } = await supabase
    .from(PLAYERS_TABLE)
    .select('id,name,image_url')
    .ilike('image_url', '%ui-avatars%')
    .limit(10);

  if (error) {
    console.error('Query failed:', error.message);
    process.exit(1);
  }
  console.log('Sample placeholders:', data || []);
})();
