import React, { memo } from "react";
import { Link } from "react-router-dom";

const formatNextMatch = (matchDate) => {
  if (!matchDate) {
    return { dayLabel: "No upcoming match", timeLabel: "" };
  }
  const match = new Date(matchDate);
  if (Number.isNaN(match.getTime())) {
    return { dayLabel: "No upcoming match", timeLabel: "" };
  }

  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const startOfMatch = new Date(match.getFullYear(), match.getMonth(), match.getDate());
  const dayDiff = Math.round((startOfMatch.getTime() - startOfToday.getTime()) / 86400000);

  let dayLabel = match.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric"
  });
  if (dayDiff === 0) {
    dayLabel = "Today";
  } else if (dayDiff === 1) {
    dayLabel = "Tomorrow";
  }

  const timeLabel = match.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit"
  });

  return { dayLabel, timeLabel };
};

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

function PlayerCard({ player }) {
  const status = String(player.player_status || "active");
  const statusLabel = status === "verified" ? "VERIFIED" : "ACTIVE";
  const statusClasses =
    status === "verified"
      ? "border-[#00FF41]/60 bg-[#00FF41]/10 text-[#00FF41]"
      : "border-sky-400/60 bg-sky-400/10 text-sky-300";
  const nextMatch = player.next_match || null;
  const nextMatchDate = formatNextMatch(nextMatch?.match_date || nextMatch?.date);
  const valueLabel = formatMarketValue(player.market_value);
  const stats = player.stats || { matches: 0, goals: 0, assists: 0, rating: 0 };

  return (
    <Link
      to={`/player/${player.id}`}
      className="group relative rounded-2xl border border-slate-700 bg-gradient-to-br from-slate-800/40 via-slate-900/30 to-slate-900/40 p-4 shadow-soft backdrop-blur-sm transition-all duration-200 hover:-translate-y-1 hover:border-stadium/70 hover:shadow-neon hover:from-slate-800/50 hover:via-slate-800/40 hover:to-slate-900/50"
    >
      <div className="flex flex-col gap-3">
        <div className="flex items-start gap-3">
          <div className="relative h-16 w-16 shrink-0 overflow-hidden rounded-xl border-2 border-white/15 bg-gradient-to-br from-[#2563EB] to-[#1A56A0] text-lg font-semibold text-white shadow-lg">
            <img
              src={player.image_url || player.image || fallbackAvatar(player.name)}
              alt={player.name}
              className="h-16 w-16 rounded-xl object-cover object-center"
              loading="lazy"
              onError={(e) => {
                const initials = String(player.name || "XX")
                  .split(" ")
                  .map((w) => w[0])
                  .slice(0, 2)
                  .join("")
                  .toUpperCase();
                e.currentTarget.style.display = "none";
                e.currentTarget.parentNode.style.background =
                  "linear-gradient(to bottom right, #2563EB, #1A56A0)";
                e.currentTarget.parentNode.style.display = "flex";
                e.currentTarget.parentNode.style.alignItems = "center";
                e.currentTarget.parentNode.style.justifyContent = "center";
                e.currentTarget.parentNode.style.fontSize = "18px";
                e.currentTarget.parentNode.style.fontWeight = "600";
                e.currentTarget.parentNode.style.color = "white";
                e.currentTarget.parentNode.innerText = initials || "XX";
              }}
            />
            <div
              className={`absolute -top-1 -right-1 rounded-full border px-2 py-0.5 text-xs font-bold ${statusClasses} shadow-lg`}
            >
              {statusLabel}
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <div className="mb-1 flex items-center gap-2">
              <h3 className="text-sm font-bold text-white">{player.name}</h3>
              <img
                src={player.flag_url || player.flag || "https://flagcdn.com/64x48/un.png"}
                alt={`${player.name} nationality`}
                className="h-4 w-6 shrink-0 rounded object-cover"
                loading="lazy"
                onError={(e) => {
                  e.currentTarget.src = "https://flagcdn.com/64x48/un.png";
                }}
              />
            </div>

            <div className="mb-2 flex items-center gap-2">
              <img
                src={player.club_logo_url || fallbackAvatar(player.club)}
                alt={`${player.club} logo`}
                className="h-4 w-4 shrink-0 rounded-full bg-slate-700 object-cover object-center"
                loading="lazy"
                onError={(e) => {
                  e.currentTarget.src = fallbackAvatar(player.club);
                }}
              />
              <p className="truncate text-xs font-medium text-slate-300">{player.club}</p>
            </div>

            <span className="inline-flex rounded-full border border-stadium/40 bg-stadium/10 px-2 py-0.5 text-xs font-medium text-stadium">
              {player.position || "N/A"}
            </span>
          </div>
        </div>

        <div className="grid grid-cols-4 gap-2 rounded-lg border border-white/10 bg-white/[0.02] p-2">
          <div className="text-center">
            <p className="text-xs font-semibold text-slate-400">MATCHES</p>
            <p className="text-sm font-bold text-white">{stats.matches || "0"}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-semibold text-slate-400">GOALS</p>
            <p className="text-sm font-bold text-cyan-400">{stats.goals || "0"}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-semibold text-slate-400">ASSISTS</p>
            <p className="text-sm font-bold text-emerald-400">{stats.assists || "0"}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-semibold text-slate-400">RATING</p>
            <p className="text-sm font-bold text-amber-400">{stats.rating ? stats.rating.toFixed(1) : "-"}</p>
          </div>
        </div>

        {valueLabel ? (
          <div className="rounded-lg border border-emerald-400/30 bg-emerald-400/10 px-3 py-1 text-xs font-semibold text-emerald-300">
            {valueLabel}
          </div>
        ) : null}

        {nextMatch ? (
          <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs">
            <p className="truncate font-semibold text-white">Next: {nextMatch.opponent}</p>
            <p className="mt-1 text-slate-300">
              {nextMatchDate.dayLabel} {nextMatchDate.timeLabel ? `| ${nextMatchDate.timeLabel}` : ""}
            </p>
            <p className="mt-1 truncate text-xs text-stadium">{nextMatch.competition || "Scheduled Match"}</p>
          </div>
        ) : null}
      </div>
    </Link>
  );
}

export default memo(PlayerCard);
