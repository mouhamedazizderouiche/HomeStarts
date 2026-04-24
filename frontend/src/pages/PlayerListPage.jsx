import { useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { fetchCountries, fetchPlayersByCountry, fetchTopPlayersByCountry } from "../api/client";
import PlayerCard from "../components/PlayerCard";
import TopPlayersSection from "../components/TopPlayersSection";

function PlayerListPage() {
  const { country } = useParams();
  const decodedCountry = decodeURIComponent(country || "");
  const [statusFilter, setStatusFilter] = useState("all");
  const [diasporaMode, setDiasporaMode] = useState(false);
  const [clubSearch, setClubSearch] = useState("");
  const [positionFilter, setPositionFilter] = useState("all");
  const fallbackFlagSvg =
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='64' height='48' viewBox='0 0 64 48'%3E%3Crect width='64' height='48' fill='%230f172a'/%3E%3Ctext x='32' y='29' text-anchor='middle' fill='%23e2e8f0' font-size='20' font-family='Arial'%3E%3F%3C/text%3E%3C/svg%3E";

  const countriesQuery = useQuery({
    queryKey: ["countries"],
    queryFn: fetchCountries,
    placeholderData: keepPreviousData
  });

  const playersQuery = useQuery({
    queryKey: ["players", decodedCountry, statusFilter, diasporaMode],
    queryFn: () => fetchPlayersByCountry(decodedCountry, statusFilter === "verified" ? "verified" : "", diasporaMode),
    enabled: Boolean(decodedCountry),
    placeholderData: keepPreviousData
  });

  const topPlayersQuery = useQuery({
    queryKey: ["topPlayers", decodedCountry, diasporaMode],
    queryFn: () => fetchTopPlayersByCountry(decodedCountry, diasporaMode),
    enabled: Boolean(decodedCountry),
    placeholderData: keepPreviousData
  });

  const loading = countriesQuery.isLoading || playersQuery.isLoading || topPlayersQuery.isLoading;
  const error = playersQuery.isError ? "Could not load players for this country." : "";
  const countryInfo = useMemo(() => {
    const countriesData = Array.isArray(countriesQuery.data) ? countriesQuery.data : [];
    const selectedCountry = countriesData.find(
      (item) => String(item.name || "").toLowerCase() === decodedCountry.toLowerCase()
    );
    return selectedCountry || { name: decodedCountry, code: "un", flag_url: "https://flagcdn.com/64x48/un.png" };
  }, [countriesQuery.data, decodedCountry]);

  const allPlayers = useMemo(() => {
    const playersData = playersQuery.data;
    const normalizedPlayers = Array.isArray(playersData)
      ? playersData
      : Array.isArray(playersData?.players)
        ? playersData.players
        : [];
    return normalizedPlayers.filter((player) => {
      const status = String(player.player_status || "active").toLowerCase();
      const club = String(player.current_club || player.club || "").trim();
      if (!club || club.toLowerCase() === "unknown club" || status === "uncertain") {
        return false;
      }
      if (statusFilter === "verified") {
        return status === "verified";
      }
      return true;
    });
  }, [playersQuery.data, statusFilter]);

  const filteredPlayers = useMemo(() => {
    const clubTerm = clubSearch.trim().toLowerCase();
    return allPlayers.filter((player) => {
      const position = String(player.position || "").toLowerCase();
      const club = String(player.club || player.current_club || "").toLowerCase();
      if (positionFilter !== "all" && position !== positionFilter) {
        return false;
      }
      if (clubTerm && !club.includes(clubTerm)) {
        return false;
      }
      return true;
    });
  }, [allPlayers, clubSearch, positionFilter]);

  const topPlayers = useMemo(() => {
    const data = topPlayersQuery.data;
    if (Array.isArray(data?.players)) {
      return data.players;
    }
    return Array.isArray(data) ? data : [];
  }, [topPlayersQuery.data]);

  const positions = useMemo(() => {
    const values = [...new Set(allPlayers.map((p) => String(p.position || "").trim()).filter(Boolean))];
    return values.sort((a, b) => a.localeCompare(b));
  }, [allPlayers]);

  return (
    <main className="mx-auto min-h-screen w-full max-w-7xl px-4 py-8 sm:px-6">
      <section className="relative overflow-hidden rounded-3xl border border-white/10 bg-gradient-to-br from-[#2a0006] via-[#12040b] to-[#070a14] p-5 shadow-soft sm:p-6">
        <div className="pointer-events-none absolute -right-16 -top-20 h-56 w-56 rounded-full bg-[#ff29391a] blur-3xl" />
        <div className="pointer-events-none absolute -left-16 -bottom-20 h-56 w-56 rounded-full bg-[#00ff4115] blur-3xl" />
        <div className="relative z-10 flex flex-wrap items-start justify-between gap-4">
          <div>
            <Link
              to="/"
              className="inline-flex rounded-full border border-slate-600 px-3 py-1 text-xs text-slate-300 transition hover:border-stadium hover:text-stadium"
            >
              Back to countries
            </Link>
            <h1 className="mt-3 flex items-center gap-3 text-3xl font-bold text-white">
              {String(countryInfo?.flag_url || "").startsWith("http") ? (
                <img
                  src={countryInfo.flag_url}
                  alt={`${decodedCountry} flag`}
                  className="h-7 w-10 rounded object-cover"
                  onError={(event) => {
                    event.currentTarget.src = fallbackFlagSvg;
                  }}
                />
              ) : (
                <span aria-hidden="true">{countryInfo?.flag || "?"}</span>
              )}
              <span>{decodedCountry} National Team and Diaspora Tracker</span>
            </h1>
            <p className="mt-1 text-sm text-slate-300">{filteredPlayers.length} players loaded</p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setStatusFilter("all")}
              className={`rounded-full border px-3 py-1 text-xs ${statusFilter === "all" ? "border-stadium/70 bg-stadium/10 text-stadium" : "border-white/15 bg-white/5 text-slate-300"}`}
            >
              All Active
            </button>
            <button
              type="button"
              onClick={() => setStatusFilter("verified")}
              className={`rounded-full border px-3 py-1 text-xs ${statusFilter === "verified" ? "border-stadium/70 bg-stadium/10 text-stadium" : "border-white/15 bg-white/5 text-slate-300"}`}
            >
              Verified Only
            </button>
            <button
              type="button"
              onClick={() => setDiasporaMode((value) => !value)}
              className={`rounded-full border px-3 py-1 text-xs ${diasporaMode ? "border-stadium/70 bg-stadium/10 text-stadium" : "border-white/15 bg-white/5 text-slate-300"}`}
            >
              Diaspora {diasporaMode ? "On" : "Off"}
            </button>
          </div>
        </div>
      </section>

      <div className="mt-6 grid gap-6 lg:grid-cols-[270px_1fr]">
        <aside className="rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm">
          <h2 className="mb-4 text-lg font-semibold text-white">Filters</h2>

          <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-slate-400">
            Position
          </label>
          <select
            value={positionFilter}
            onChange={(e) => setPositionFilter(e.target.value)}
            className="mb-4 w-full rounded-lg border border-white/15 bg-white/[0.03] px-3 py-2 text-sm text-slate-100"
          >
            <option value="all">All Positions</option>
            {positions.map((position) => (
              <option key={position} value={position.toLowerCase()}>
                {position}
              </option>
            ))}
          </select>

          <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-slate-400">
            Club
          </label>
          <input
            type="text"
            value={clubSearch}
            onChange={(e) => setClubSearch(e.target.value)}
            placeholder="Search club..."
            className="mb-4 w-full rounded-lg border border-white/15 bg-white/[0.03] px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500"
          />

          <Link
            to={`/calendar?country=${encodeURIComponent(decodedCountry)}`}
            className="inline-flex rounded-full border border-stadium/50 bg-stadium/10 px-3 py-1 text-xs font-semibold text-stadium transition-all duration-200 hover:shadow-neon"
          >
            View Country Calendar
          </Link>
        </aside>

        <section>
          <TopPlayersSection players={topPlayers} loading={loading} />

          {loading ? (
            <p className="rounded-xl border border-slate-700 bg-panel p-4 text-sm text-slate-300">Loading players...</p>
          ) : null}

          {error ? (
            <p className="rounded-xl border border-red-500/50 bg-red-950/40 p-4 text-sm text-red-200">{error}</p>
          ) : null}

          {!loading && !error && filteredPlayers.length === 0 ? (
            <p className="rounded-xl border border-slate-700 bg-panel p-4 text-sm text-slate-300">
              No players found for {decodedCountry}.
            </p>
          ) : null}

          {!loading && !error && filteredPlayers.length > 0 ? (
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
              {filteredPlayers.map((player) => (
                <PlayerCard key={player.id} player={player} />
              ))}
            </div>
          ) : null}
        </section>
      </div>
    </main>
  );
}

export default PlayerListPage;
