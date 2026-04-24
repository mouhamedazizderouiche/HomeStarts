import { useMemo } from "react";
import { Link, useParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { fetchMatchesByClub, fetchPlayerById, fetchPlayerNextMatch } from "../api/client";
import FormChart from "../components/FormChart";

const fallbackAvatar = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
    String(name || "Unknown")
  )}&backgroundColor=1A56A0&textColor=ffffff`;

const formatMatchDateTime = (matchDate) => {
  if (!matchDate) return "";
  const date = new Date(matchDate);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
};

const formatMarketValue = (value) => {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return "-";
  }
  if (amount >= 1_000_000) {
    return `EUR ${(amount / 1_000_000).toFixed(1)}M`;
  }
  return `EUR ${(amount / 1_000).toFixed(0)}K`;
};

function PlayerDetailPage() {
  const { id } = useParams();
  const playerQuery = useQuery({
    queryKey: ["player", id],
    queryFn: () => fetchPlayerById(id),
    enabled: Boolean(id),
    placeholderData: keepPreviousData
  });

  const nextMatchQuery = useQuery({
    queryKey: ["playerNextMatch", id],
    queryFn: () => fetchPlayerNextMatch(id),
    enabled: Boolean(id),
    placeholderData: keepPreviousData
  });

  const clubMatchesQuery = useQuery({
    queryKey: ["clubMatches", playerQuery.data?.current_club || ""],
    queryFn: () => fetchMatchesByClub(playerQuery.data?.current_club || ""),
    enabled: Boolean(playerQuery.data?.current_club),
    placeholderData: keepPreviousData
  });

  const player = useMemo(() => {
    if (!playerQuery.data) {
      return null;
    }
    return {
      ...playerQuery.data,
      next_match: nextMatchQuery.data || playerQuery.data?.next_match || null
    };
  }, [playerQuery.data, nextMatchQuery.data]);

  const clubMatches = useMemo(() => {
    const matches = clubMatchesQuery.data;
    return Array.isArray(matches) ? matches.slice(0, 3) : [];
  }, [clubMatchesQuery.data]);

  const loading = playerQuery.isLoading || nextMatchQuery.isLoading;
  const error = playerQuery.isError ? "Could not load player details." : "";

  if (loading) {
    return (
      <main className="mx-auto min-h-screen w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="rounded-xl border border-slate-700 bg-panel p-4 text-sm text-slate-300">
          Loading player profile...
        </p>
      </main>
    );
  }

  if (error || !player) {
    return (
      <main className="mx-auto min-h-screen w-full max-w-6xl px-4 py-8 sm:px-6">
        <Link to="/" className="inline-flex rounded-full border border-slate-600 px-3 py-1 text-xs text-slate-300">
          Back to home
        </Link>
        <p className="mt-4 rounded-xl border border-red-500/50 bg-red-950/40 p-4 text-sm text-red-200">
          {error || "Player not found."}
        </p>
      </main>
    );
  }

  return (
    <main className="mx-auto min-h-screen w-full max-w-6xl px-4 py-8 sm:px-6">
      <Link
        to={`/players/${encodeURIComponent(player.country?.name || player.nationality || "")}`}
        className="inline-flex rounded-full border border-slate-600 px-3 py-1 text-xs text-slate-300 transition hover:border-stadium hover:text-stadium"
      >
        Back to player list
      </Link>

      <section className="group relative mt-6 overflow-hidden rounded-3xl border border-white/10 bg-gradient-to-br from-[#0f1f3a] via-[#122542] to-[#0f172a] p-6 shadow-soft">
        <div className="pointer-events-none absolute -right-16 -top-16 h-48 w-48 rounded-full bg-stadium/10 blur-3xl" />
        <div className="pointer-events-none absolute -bottom-20 -left-20 h-56 w-56 rounded-full bg-cyan-500/10 blur-3xl" />

        <div className="relative z-10 flex flex-col gap-6 md:flex-row md:items-end">
          <img
            src={player.image_url || player.image || fallbackAvatar(player.name)}
            alt={player.name}
            className="h-32 w-32 rounded-2xl border border-white/20 object-cover"
            onError={(e) => {
              e.currentTarget.src = fallbackAvatar(player.name);
            }}
          />

          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="truncate text-4xl font-extrabold text-white">{player.name}</h1>
              <img
                src={player.flag_url || "https://flagcdn.com/64x48/un.png"}
                alt={`${player.country?.name || player.nationality || "country"} flag`}
                className="h-6 w-9 rounded object-cover"
                onError={(e) => {
                  e.currentTarget.src = "https://flagcdn.com/64x48/un.png";
                }}
              />
            </div>

            <div className="mt-2 flex items-center gap-2 text-slate-300">
              <img
                src={player.club_logo_url || fallbackAvatar(player.club)}
                alt={`${player.club} logo`}
                className="h-6 w-6 rounded-full object-cover"
                onError={(e) => {
                  e.currentTarget.src = fallbackAvatar(player.club);
                }}
              />
              <span>{player.club}</span>
            </div>

            <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
              <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
                <p className="text-xs text-slate-300">Matches</p>
                <p className="mt-1 text-xl font-bold text-stadium">{Math.round(Number(player?.stats?.matches || 0))}</p>
              </div>
              <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
                <p className="text-xs text-slate-300">Goals</p>
                <p className="mt-1 text-xl font-bold text-stadium">{Math.round(Number(player?.stats?.goals || 0))}</p>
              </div>
              <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
                <p className="text-xs text-slate-300">Assists</p>
                <p className="mt-1 text-xl font-bold text-stadium">{Math.round(Number(player?.stats?.assists || 0))}</p>
              </div>
              <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
                <p className="text-xs text-slate-300">Rating</p>
                <p className="mt-1 text-xl font-bold text-stadium">
                  {Number(player?.stats?.rating || 0).toFixed(1)}
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="mt-8 grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-5 backdrop-blur-sm">
          <h2 className="text-xl font-semibold text-white">Profile Info</h2>
          <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3">
              <p className="text-slate-400">Primary Nationality</p>
              <p className="mt-1 font-semibold text-white">{player.primary_nationality || "-"}</p>
            </div>
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3">
              <p className="text-slate-400">Age</p>
              <p className="mt-1 font-semibold text-white">{player.age || "-"}</p>
            </div>
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3">
              <p className="text-slate-400">Position</p>
              <p className="mt-1 font-semibold text-white">{player.position || "-"}</p>
            </div>
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3">
              <p className="text-slate-400">Market Value</p>
              <p className="mt-1 font-semibold text-white">{formatMarketValue(player.market_value)}</p>
            </div>
          </div>

          {Array.isArray(player.dual_nationality) && player.dual_nationality.length > 0 ? (
            <div className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] p-3">
              <p className="text-slate-400">Dual Nationality</p>
              <p className="mt-1 text-white">{player.dual_nationality.join(", ")}</p>
            </div>
          ) : null}

          {player.transfermarkt_url ? (
            <a
              href={player.transfermarkt_url}
              target="_blank"
              rel="noreferrer"
              className="mt-4 inline-flex rounded-full border border-stadium/50 bg-stadium/10 px-3 py-1 text-xs font-semibold text-stadium transition hover:shadow-neon"
            >
              Open Transfermarkt Profile
            </a>
          ) : null}
        </div>

        <div>
          <FormChart
            rating={player?.stats?.rating || 0}
            matches={player?.stats?.matches || 0}
            goals={player?.stats?.goals || 0}
            assists={player?.stats?.assists || 0}
          />
        </div>
      </section>

      <section className="mt-8 grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-5 backdrop-blur-sm">
          <h2 className="text-xl font-semibold text-white">Next Match</h2>
          <div className="mt-4 rounded-xl border border-white/10 bg-white/[0.03] p-4">
            {player.next_match ? (
              <>
                <p className="text-sm text-stadium">{player.next_match.competition || "Upcoming"}</p>
                <p className="mt-2 text-2xl font-bold text-white">
                  {player.next_match.home_team || player.club} vs{" "}
                  {player.next_match.away_team || player.next_match.opponent}
                </p>
                <p className="mt-2 text-sm text-slate-300">
                  {formatMatchDateTime(player.next_match.match_date || player.next_match.date)}
                </p>
              </>
            ) : (
              <p className="text-sm text-slate-400">No upcoming match</p>
            )}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-5 backdrop-blur-sm">
          <h2 className="text-xl font-semibold text-white">📊 Season Stats</h2>
          <div className="mt-4 grid grid-cols-2 gap-2 text-sm">
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3 text-center">
              <p className="text-slate-400">Season</p>
              <p className="mt-1 font-bold text-stadium">2024/25</p>
            </div>
            <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3 text-center">
              <p className="text-slate-400">Status</p>
              <p className="mt-1 font-bold text-cyan-400">Active</p>
            </div>
          </div>
        </div>
      </section>
        <h2 className="text-xl font-semibold text-white">Next 3 Matches</h2>
        <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {clubMatches.length === 0 ? <p className="text-sm text-slate-400">No additional fixtures in range.</p> : null}
          {clubMatches.map((match) => (
            <article key={match.id} className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
              <p className="text-sm font-semibold text-white">
                {match.home_team || match.club} vs {match.away_team || match.opponent}
              </p>
              <p className="mt-1 text-xs text-slate-300">{formatMatchDateTime(match.match_date || match.date)}</p>
              <p className="mt-1 text-xs text-stadium">{match.competition || "Competition"}</p>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

export default PlayerDetailPage;
