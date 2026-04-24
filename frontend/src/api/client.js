import axios from "axios";

const client = axios.create({
  baseURL: "http://localhost:5000",
  timeout: 7000
});

const initialsAvatar = (value) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
    String(value || "Unknown")
  )}&backgroundColor=1A56A0&textColor=ffffff`;

const normalizePlayer = (player = {}) => {
  const clubName =
    typeof player.club === "string"
      ? player.club
      : player.club?.name || player.club_data?.name || player.current_club || "Unknown Club";
  const clubLogo =
    player.club_logo_url ||
    player.club?.logo_url ||
    player.club_data?.logo_url ||
    initialsAvatar(clubName);
  const countryName = player.country?.name || player.nationality || "";
  const countryCode = String(player.country?.code || player.nationality_code || "un").toLowerCase();
  const flagUrl = player.flag_url || player.country?.flag_url || `https://flagcdn.com/64x48/${countryCode}.png`;

  return {
    ...player,
    club: clubName,
    current_club: player.current_club || clubName,
    club_logo_url: clubLogo,
    image: player.image || player.image_url || initialsAvatar(player.name || "Unknown Player"),
    image_url: player.image_url || player.image || initialsAvatar(player.name || "Unknown Player"),
    flag_url: flagUrl,
    country: {
      name: countryName,
      code: countryCode,
      flag_url: flagUrl
    }
    ,
    stats: {
      matches: Number(player.matches_2025 ?? player.matches ?? player.matches_total ?? player.matches_2024 ?? 0),
      goals: Number(player.goals_2025 ?? player.goals ?? player.goals_total ?? 0),
      assists: Number(player.assists_2025 ?? player.assists ?? player.assists_total ?? 0),
      rating: Number(player.rating_2025 ?? player.rating ?? player.avg_rating ?? 0)
    },
    primary_nationality: player.primary_nationality || countryName || "",
    dual_nationality: Array.isArray(player.dual_nationality) ? player.dual_nationality : [],
    origin_countries: Array.isArray(player.origin_countries) ? player.origin_countries : [],
    market_value: Number(player.market_value || 0),
    transfermarkt_url: String(player.transfermarkt_url || "").trim()
  };
};

export const fetchCountries = async () => {
  const { data } = await client.get("/api/countries");
  return data;
};

export const fetchPlayersByCountry = async (country, status = "", diaspora = false) => {
  const params = { country };
  if (status) {
    params.status = status;
  }
  if (diaspora) {
    params.diaspora = "true";
  }
  const { data } = await client.get("/api/players", { params });
  return {
    ...data,
    players: Array.isArray(data?.players) ? data.players.map(normalizePlayer) : []
  };
};

export const fetchTopPlayersByCountry = async (country, diaspora = false) => {
  const params = diaspora ? { country, diaspora: "true" } : { country };
  const { data } = await client.get("/api/players/top", { params });
  return {
    ...data,
    players: Array.isArray(data?.players) ? data.players.map(normalizePlayer) : []
  };
};

export const fetchCalendarByCountry = async (country, diaspora = false) => {
  const params = diaspora ? { country, diaspora: "true" } : { country };
  const { data } = await client.get("/api/calendar", { params });
  return data;
};

export const fetchPlayerById = async (id) => {
  const { data } = await client.get(`/api/players/${id}`);
  return normalizePlayer(data);
};

export const fetchPlayerNextMatch = async (id) => {
  const { data } = await client.get(`/api/players/${id}/next-match`);
  return data;
};

export const fetchMatchesByClub = async (club) => {
  const { data } = await client.get("/api/matches", { params: { club } });
  return data;
};

export const fetchMatchesCalendar = async (params = {}) => {
  const { data } = await client.get("/api/matches/calendar", { params });
  return data;
};

export default client;
