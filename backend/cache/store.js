const NodeCache = require("node-cache");

const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 21600);

const cache = new NodeCache({
  stdTTL: CACHE_TTL_SECONDS,
  checkperiod: 120
});

const cacheKeys = {
  playersByCountry: (country) => `players_${String(country || "").trim()}`,
  playerById: (id) => `player_${String(id || "").trim()}`,
  playerIndexById: (id) => `player_index_${String(id || "").trim()}`
};

module.exports = {
  cache,
  cacheKeys,
  CACHE_TTL_SECONDS
};
