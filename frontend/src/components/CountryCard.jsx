import { Link } from "react-router-dom";

function CountryCard({ country }) {
  const countryCode = (country.code || "").toLowerCase() || "un";
  const fallbackFlag = "https://flagcdn.com/64x48/un.png";
  const flagImage = country.flag_url?.startsWith("http")
    ? country.flag_url
    : `https://flagcdn.com/64x48/${countryCode}.png`;

  return (
    <Link
      to={`/players/${encodeURIComponent(country.name)}`}
      className="group rounded-2xl border p-4 backdrop-blur-sm transition-all duration-200 ease-in-out hover:-translate-y-1"
      style={{
        background: "rgba(255,255,255,0.05)",
        borderColor: "rgba(255,255,255,0.1)",
        boxShadow: "0 0 0 rgba(0,255,65,0)"
      }}
      onMouseEnter={(event) => {
        event.currentTarget.style.boxShadow = "0 0 20px rgba(0,255,65,0.3)";
      }}
      onMouseLeave={(event) => {
        event.currentTarget.style.boxShadow = "0 0 0 rgba(0,255,65,0)";
      }}
    >
      <div className="flex items-center gap-3">
        <img
          src={flagImage}
          alt={`${country.name} flag`}
          className="h-8 w-12 rounded object-cover"
          loading="lazy"
          onError={(event) => {
            event.currentTarget.src = "https://flagcdn.com/64x48/un.png";
          }}
        />
        <div className="min-w-0 flex-1">
          <h3 className="truncate text-sm font-semibold text-white">{country.name}</h3>
          <p className="text-xs text-[#E2E8F0]/80">{country.player_count || country.count || 0} players tracked</p>
        </div>
      </div>
    </Link>
  );
}

export default CountryCard;
