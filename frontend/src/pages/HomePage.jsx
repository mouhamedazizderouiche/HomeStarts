import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { fetchCountries, fetchMatchesCalendar, fetchTopPlayersByCountry } from "../api/client";
import CountryCard from "../components/CountryCard";
import SearchBar from "../components/SearchBar";
import SkeletonLoader from "../components/SkeletonLoader";

const trendingCountries = ["Tunisia", "Morocco", "Senegal", "Brazil", "France"];

function HomePage() {
  const [search, setSearch] = useState("");

  const countriesQuery = useQuery({
    queryKey: ["countries"],
    queryFn: fetchCountries,
    placeholderData: keepPreviousData
  });

  const countries = useMemo(() => {
    const raw = Array.isArray(countriesQuery.data) ? countriesQuery.data : [];
    return raw.map((item) => ({
      ...item,
      code: String(item.code || "un").toLowerCase(),
      player_count: Number(item.player_count || item.count || 0),
      flag_url: item.flag_url || `https://flagcdn.com/64x48/${String(item.code || "un").toLowerCase()}.png`
    }));
  }, [countriesQuery.data]);

  const featuredCountry = useMemo(() => {
    if (!countries.length) {
      return "Tunisia";
    }
    const first = countries.find((item) => trendingCountries.includes(item.name));
    return first?.name || countries[0].name || "Tunisia";
  }, [countries]);

  const topPlayersQuery = useQuery({
    queryKey: ["topPlayers", featuredCountry],
    queryFn: () => fetchTopPlayersByCountry(featuredCountry),
    enabled: Boolean(featuredCountry),
    placeholderData: keepPreviousData
  });

  const matchesQuery = useQuery({
    queryKey: ["matchesCalendar", featuredCountry],
    queryFn: () => fetchMatchesCalendar({ country: featuredCountry }),
    enabled: Boolean(featuredCountry),
    placeholderData: keepPreviousData
  });

  const loading = countriesQuery.isLoading || topPlayersQuery.isLoading || matchesQuery.isLoading;
  const error = countriesQuery.isError ? "Could not load countries." : "";
  const topPlayers = Array.isArray(topPlayersQuery.data?.players) ? topPlayersQuery.data.players.slice(0, 5) : [];
  const upcomingMatches = useMemo(() => {
    const grouped = matchesQuery.data && typeof matchesQuery.data === "object" ? matchesQuery.data : {};
    return Object.entries(grouped)
      .flatMap(([date, matches]) =>
        (Array.isArray(matches) ? matches : []).map((item) => ({ ...item, calendar_date: date }))
      )
      .slice(0, 5);
  }, [matchesQuery.data]);

  const filteredCountries = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return countries;
    return countries.filter((country) => country.name.toLowerCase().includes(term));
  }, [countries, search]);

  return (
    <main className="mx-auto min-h-screen w-full max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <section className="relative overflow-hidden rounded-3xl border border-white/10 bg-gradient-to-br from-[#0d1b34] via-[#10223f] to-[#0b152b] p-6 shadow-soft sm:p-10">
        <div className="absolute -right-14 -top-14 h-52 w-52 rounded-full bg-stadium/20 blur-3xl" />
        <div className="absolute -bottom-20 -left-20 h-64 w-64 rounded-full bg-blue-500/20 blur-3xl" />
        <div className="relative z-10 max-w-3xl">
          <p className="text-xs uppercase tracking-[0.25em] text-stadium">HomeStars V2.0</p>
          <h1 className="mt-3 text-4xl font-extrabold leading-tight text-white sm:text-5xl">
            Follow your stars
          </h1>
          <p className="mt-2 text-xl font-semibold text-slate-200">Track your nation&apos;s football talent worldwide.</p>
          <p className="mt-4 max-w-2xl text-sm text-slate-300">
            A scouting-first platform to discover active players, form, and upcoming matches from one clean source.
          </p>
          <div className="mt-6 flex flex-wrap items-center gap-3">
            <Link
              to={featuredCountry ? `/players/${encodeURIComponent(featuredCountry)}` : "/"}
              className="rounded-full border border-stadium/60 bg-stadium/20 px-5 py-2 text-sm font-semibold text-stadium transition-all duration-200 hover:shadow-neon"
            >
              Track Players Now
            </Link>
            <Link
              to="/calendar"
              className="rounded-full border border-white/20 bg-white/5 px-5 py-2 text-sm font-semibold text-slate-100 transition-all duration-200 hover:border-stadium/50"
            >
              Open Match Calendar
            </Link>
          </div>
        </div>
      </section>

      <section className="mt-8 rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-lg font-semibold text-white">Explore by Nation</h2>
          <span className="text-xs text-slate-400">{countries.length} countries</span>
        </div>
        <SearchBar value={search} onChange={setSearch} placeholder="Search country" />
      </section>

      <section className="mt-8 grid gap-6 xl:grid-cols-[1.6fr_1fr]">
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-lg font-semibold text-white">All Nations</h2>
            <p className="text-xs text-slate-400">Sorted by player count</p>
          </div>

          {loading && (
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {Array.from({ length: 12 }).map((_, i) => (
                <SkeletonLoader key={i} className="h-24 border border-white/10" />
              ))}
            </div>
          )}

          {error && <p className="text-sm text-red-300">{error}</p>}

          {!loading && !error && (
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {filteredCountries.map((country) => (
                <CountryCard key={country.name} country={country} />
              ))}
            </div>
          )}
        </div>

        <div className="space-y-6">
          <section className="rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg font-semibold text-white">Top Players</h2>
              <span className="text-xs text-slate-400">{featuredCountry}</span>
            </div>
            <div className="space-y-3">
              {topPlayers.length === 0 && <p className="text-sm text-slate-400">No top players yet.</p>}
              {topPlayers.map((player) => (
                <Link
                  to={`/player/${player.id}`}
                  key={player.id}
                  className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/[0.03] p-3 transition-all duration-200 hover:border-stadium/60"
                >
                  <img
                    src={player.image_url || player.image}
                    alt={player.name}
                    className="h-10 w-10 rounded-full object-cover"
                    loading="lazy"
                    onError={(e) => {
                      e.currentTarget.src = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(player.name || "Unknown"))}&backgroundColor=1A56A0&textColor=ffffff`;
                    }}
                  />
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-white">{player.name}</p>
                    <p className="truncate text-xs text-slate-400">{player.club}</p>
                  </div>
                  <span className="rounded-full border border-stadium/50 bg-stadium/10 px-2 py-1 text-xs font-semibold text-stadium">
                    {Number(player.form_score || 0).toFixed(1)}
                  </span>
                </Link>
              ))}
            </div>
          </section>

          <section className="rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg font-semibold text-white">Upcoming Matches</h2>
              <span className="text-xs text-slate-400">next {upcomingMatches.length}</span>
            </div>
            <div className="space-y-3">
              {upcomingMatches.length === 0 && <p className="text-sm text-slate-400">No upcoming matches.</p>}
              {upcomingMatches.map((match, index) => (
                <article key={`${match.id || index}-${match.match_date}`} className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                  <p className="truncate text-sm font-semibold text-white">
                    {match.home_team || match.club} vs {match.away_team || match.opponent}
                  </p>
                  <p className="mt-1 text-xs text-slate-300">{new Date(match.match_date).toLocaleString()}</p>
                  <p className="mt-1 text-xs text-stadium">{match.competition || "Competition"}</p>
                </article>
              ))}
            </div>
          </section>
        </div>
      </section>
    </main>
  );
}

export default HomePage;

