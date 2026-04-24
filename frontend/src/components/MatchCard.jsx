function MatchCard({ match }) {
  const formattedDate = new Date(match.date).toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric"
  });

  const homeTeam = match.home_team || match.team || "";
  const awayTeam = match.away_team || match.opponent || "";
  const homeLogoUrl = match.home_logo_url || match.club_logo_url || "";
  const awayLogoUrl = match.away_logo_url || "";

  const TeamLogo = ({ url, name }) => (
    <div className="flex items-center gap-2">
      {url ? (
        <img
          src={url}
          alt={name}
          className="h-8 w-8 rounded-full object-cover"
          onError={(e) => {
            e.target.style.display = "none";
          }}
        />
      ) : (
        <div className="h-8 w-8 rounded-full bg-slate-600 flex items-center justify-center text-xs font-bold text-white">
          {String(name || "").substring(0, 2).toUpperCase()}
        </div>
      )}
      <span className="text-sm font-medium text-white max-w-xs truncate">{name}</span>
    </div>
  );

  return (
    <div className="rounded-2xl border border-slate-700 bg-panel p-4 shadow-soft">
      <div className="flex items-center justify-between gap-2">
        <TeamLogo url={homeLogoUrl} name={homeTeam} />
        <span className="text-xs text-slate-400 font-semibold">vs</span>
        <TeamLogo url={awayLogoUrl} name={awayTeam} />
      </div>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <span className="rounded-full border border-slate-600 px-2 py-1 text-xs text-slate-300">
          {formattedDate}
        </span>
        {match.competition && (
          <span className="rounded-full border border-stadium/40 bg-stadium/10 px-2 py-1 text-xs text-stadium">
            {match.competition}
          </span>
        )}
      </div>
    </div>
  );
}

export default MatchCard;
