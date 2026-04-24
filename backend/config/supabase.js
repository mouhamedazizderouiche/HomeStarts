const { createClient } = require("@supabase/supabase-js");

const hasSupabaseConfig = () =>
  Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);

const getSupabaseClient = () => {
  if (!hasSupabaseConfig()) {
    return null;
  }

  return createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false }
  });
};

module.exports = {
  hasSupabaseConfig,
  getSupabaseClient
};
