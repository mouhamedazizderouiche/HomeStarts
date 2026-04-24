import { Link } from "react-router-dom";

const fallbackAvatar = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
    String(name || "Unknown")
  )}&backgroundColor=1A56A0&textColor=ffffff`;

const formatMarketValue = (value) => {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return null;
  }
  if (amount >= 1_000_000) {
    return `EUR ${(amount / 1_000_000).toFixed(1)}M`;
  }
  return `EUR ${(amount / 1_000).toFixed(0)}K`;
};

function TopPlayersSection({ players = [], loading = false }) {
  return (
    <section className="mb-8 rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
      <div className="mb-4 flex items-center justify-between gap-2">
        <h2 className="text-lg font-semibold text-white">Top Players Today</h2>
        <span className="text-xs text-slate-400">Top 5</span>
      </div>

      {loading ? (
        <div className="grid gap-3 sm:grid-cols-2">
          {Array.from({ length: 8 }).map((_, index) => (
            <div key={index} className="h-20 animate-pulse rounded-xl bg-slate-700/40" />
          ))}
        </div>
      ) : null}

      {!loading && players.length === 0 ? (
        <p className="rounded-xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-300">
          No top players available yet.
        </p>
      ) : null}

      {!loading && players.length > 0 ? (
        <div className="grid gap-3 sm:grid-cols-2">
          {players.slice(0, 5).map((player) => {
            const status = String(player.player_status || "active");
            const statusLabel = status === "verified" ? "VERIFIED" : "ACTIVE";
            const statusClasses =
              status === "verified"
                ? "border-[#00FF41]/60 bg-[#00FF41]/10 text-[#00FF41]"
                : "border-sky-400/60 bg-sky-400/10 text-sky-300";
            const valueLabel = formatMarketValue(player.market_value);
            const stats = player.stats || { matches: 0, goals: 0, assists: 0, rating: 0 };

            return (
              <Link
                to={`/player/${player.id}`}
                key={player.id}
                className="rounded-xl border border-white/10 bg-white/[0.03] p-3 transition-all duration-200 hover:-translate-y-1 hover:border-stadium/60 hover:shadow-neon"
              >
                <div className="flex items-center gap-3">
                  <div className="flex h-12 w-12 shrink-0 items-center justify-center overflow-hidden rounded-full border border-white/15 bg-[#1A56A0] text-sm font-semibold text-white">
                    <img
                      src={player.image_url || player.image || fallbackAvatar(player.name)}
                      alt={player.name}
                      className="h-12 w-12 rounded-full object-cover object-center"
                      onError={(e) => {
                        e.currentTarget.src = fallbackAvatar(player.name);
                      }}
                    />
                  </div>
                  <div className="min-w-0 flex-1">
                    <h3 className="truncate text-sm font-semibold leading-tight text-white">{player.name}</h3>
                    <div className="mt-1 flex items-center gap-2">
                      <img
                        src={player.club_logo_url || fallbackAvatar(player.club)}
                        alt={`${player.club} logo`}
                        className="h-4 w-4 rounded-full object-cover object-center"
                        onError={(e) => {
                          e.currentTarget.src = fallbackAvatar(player.club);
                        }}
                      />
                      <p className="truncate text-xs text-slate-300">{player.club}</p>
                      <img
                        src={player.flag_url || "https://flagcdn.com/64x48/un.png"}
                        alt={`${player.name} nationality`}
                        className="h-3 w-5 rounded object-cover"
                        onError={(e) => {
                          e.currentTarget.src = "https://flagcdn.com/64x48/un.png";
                        }}
                      />
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      <span
                        className={`inline-flex rounded-full border px-2 py-1 text-[10px] font-semibold ${statusClasses}`}
                      >
                        {statusLabel}
                      </span>
                      {valueLabel ? (
                        <span className="inline-flex rounded-full border border-emerald-400/50 bg-emerald-400/10 px-2 py-1 text-[10px] font-semibold text-emerald-300">
                          {valueLabel}
                        </span>
                      ) : null}
                    </div>
                    <p className="mt-2 text-[11px] text-slate-400">
                      M: {Number(stats.matches || 0)} | G: {Number(stats.goals || 0)} | A:{" "}
                      {Number(stats.assists || 0)} | R: {Number(stats.rating || 0).toFixed(1)}
                    </p>
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}

export default TopPlayersSection;
