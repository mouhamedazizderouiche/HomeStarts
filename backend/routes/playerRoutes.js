const express = require("express");
const {
  getPlayersByCountry,
  getTopPlayersByCountry,
  getCalendarByCountry,
  getMatchesCalendar,
  getPlayerById,
  getPlayerNextMatch,
  getMatchesByClub,
  getNationalTeamByCountry,
  getCountries
} = require("../services/playerService");

const router = express.Router();

const parseDiaspora = (value) => String(value || "").toLowerCase() === "true";
const withTiming = async (label, fn) => {
  const started = Date.now();
  try {
    return await fn();
  } finally {
    const ms = Date.now() - started;
    console.log(`${label}: ${ms}ms`);
  }
};

const handlePlayersByCountry = async (req, res) => {
  const { country, status, diaspora } = req.query;
  const diasporaMode = parseDiaspora(diaspora);

  if (status && !["verified", "active"].includes(String(status))) {
    return res.status(400).json({ error: "status must be one of: verified, active" });
  }

  try {
    const result = await withTiming("route:getPlayersByCountry", async () => {
      const selectedCountry = String(country || "").trim();
      return getPlayersByCountry(selectedCountry, String(status || ""), diasporaMode);
    });
    console.log("Players fetched:", Array.isArray(result?.players) ? result.players.length : 0);
    return res.json(result);
  } catch (error) {
    console.error("[HomeStars] /players failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database",
      players: [],
      total: 0
    });
  }
};

const handleTopPlayers = async (req, res) => {
  const { country, diaspora } = req.query;
  const diasporaMode = parseDiaspora(diaspora);

  if (!country) {
    return res.status(400).json({ error: "Query parameter 'country' is required." });
  }

  try {
    const result = await withTiming("route:getTopPlayers", async () =>
      getTopPlayersByCountry(country.trim(), diasporaMode)
    );
    console.log("Top players fetched:", Array.isArray(result?.players) ? result.players.length : 0);
    return res.json(result);
  } catch (error) {
    console.error("[HomeStars] /players/top failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database",
      players: [],
      total: 0
    });
  }
};

const handlePlayerById = async (req, res) => {
  const { id } = req.params;

  try {
    const player = await getPlayerById(id);
    if (!player) {
      return res.status(404).json({
        error: "Player not found.",
        id
      });
    }

    return res.json(player);
  } catch (error) {
    console.error(`[HomeStars] player lookup failed (${id}):`, error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
};

const handleCalendar = async (req, res) => {
  const { country, diaspora } = req.query;
  const diasporaMode = parseDiaspora(diaspora);

  if (!country) {
    return res.status(400).json({ error: "Query parameter 'country' is required." });
  }

  try {
    const calendar = await withTiming("route:getCalendarByCountry", async () =>
      getCalendarByCountry(country.trim(), diasporaMode)
    );
    console.log("Calendar matches fetched:", Array.isArray(calendar) ? calendar.length : 0);
    return res.json(calendar);
  } catch (error) {
    console.error("[HomeStars] /calendar failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database",
      matches: []
    });
  }
};

const handleMatchesByClub = async (req, res) => {
  const { club } = req.query;
  if (!club) {
    return res.status(400).json({ error: "Query parameter 'club' is required." });
  }
  try {
    const matches = await getMatchesByClub(club);
    return res.json(matches);
  } catch (error) {
    console.error("[HomeStars] /matches failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database",
      matches: []
    });
  }
};

const handleMatchesCalendar = async (req, res) => {
  const { country = "", club = "", diaspora } = req.query;
  const diasporaMode = parseDiaspora(diaspora);
  try {
    const calendar = await withTiming("route:getMatchesCalendar", async () =>
      getMatchesCalendar({
        country: String(country || "").trim(),
        club: String(club || "").trim(),
        diaspora: diasporaMode
      })
    );
    console.log(
      "Calendar grouped days:",
      calendar && typeof calendar === "object" ? Object.keys(calendar).length : 0
    );
    return res.json(calendar);
  } catch (error) {
    console.error("[HomeStars] /matches/calendar failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
};

const handlePlayerNextMatch = async (req, res) => {
  const { id } = req.params;
  try {
    const nextMatch = await getPlayerNextMatch(id);
    if (!nextMatch) {
      return res.status(404).json({ error: "Player or next match not found.", id });
    }
    return res.json(nextMatch);
  } catch (error) {
    console.error("[HomeStars] /players/:id/next-match failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
};

const handleNationalTeam = async (req, res) => {
  const { country } = req.params;
  if (!country) {
    return res.status(400).json({ error: "Country param is required." });
  }
  try {
    const result = await getNationalTeamByCountry(country);
    return res.json(result);
  } catch (error) {
    console.error("[HomeStars] /national-teams failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
};

router.get("/players", handlePlayersByCountry);
router.get("/api/players", handlePlayersByCountry);
router.get("/players/top", handleTopPlayers);
router.get("/api/players/top", handleTopPlayers);
router.get("/api/top", handleTopPlayers);

router.get("/player/:id", handlePlayerById);
router.get("/api/players/:id", handlePlayerById);
router.get("/api/players/:id/next-match", handlePlayerNextMatch);
router.get("/api/national-teams/:country", handleNationalTeam);

router.get("/calendar", handleCalendar);
router.get("/api/calendar", handleCalendar);
router.get("/api/matches", handleMatchesByClub);
router.get("/api/matches/calendar", handleMatchesCalendar);

router.get("/countries", async (_req, res) => {
  try {
    const countries = await withTiming("route:getCountries", async () => getCountries());
    console.log("Countries fetched:", Array.isArray(countries) ? countries.length : 0);
    return res.json(countries);
  } catch (error) {
    console.error("[HomeStars] /countries failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
});

router.get("/api/countries", async (_req, res) => {
  try {
    const countries = await withTiming("route:getCountriesApi", async () => getCountries());
    console.log("Countries fetched:", Array.isArray(countries) ? countries.length : 0);
    return res.json(countries);
  } catch (error) {
    console.error("[HomeStars] /api/countries failed:", error.message);
    return res.status(500).json({
      error: "Database query failed.",
      source: "database"
    });
  }
});

module.exports = router;
