import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { fetchCountries, fetchMatchesCalendar } from "../api/client";

const buildMonthDays = (baseDate) => {
  const year = baseDate.getFullYear();
  const month = baseDate.getMonth();
  const first = new Date(year, month, 1);
  const last = new Date(year, month + 1, 0);
  const prefix = (first.getDay() + 6) % 7;
  const total = prefix + last.getDate();
  const cells = [];

  for (let i = 0; i < total; i += 1) {
    const dayNumber = i - prefix + 1;
    if (dayNumber < 1 || dayNumber > last.getDate()) {
      cells.push(null);
    } else {
      cells.push(new Date(year, month, dayNumber));
    }
  }

  while (cells.length % 7 !== 0) {
    cells.push(null);
  }

  return cells;
};

function CalendarPage() {
  const [searchParams] = useSearchParams();
  const initialCountryFromQuery = searchParams.get("country") || "";
  const [selectedCountry, setSelectedCountry] = useState(initialCountryFromQuery);
  const [clubFilter, setClubFilter] = useState("");
  const [cursorMonth, setCursorMonth] = useState(() => new Date());

  const countriesQuery = useQuery({
    queryKey: ["countries"],
    queryFn: fetchCountries,
    placeholderData: keepPreviousData
  });

  const matchesQuery = useQuery({
    queryKey: ["matchesCalendar", selectedCountry, clubFilter],
    queryFn: () => {
      const params = {};
      if (selectedCountry) params.country = selectedCountry;
      if (clubFilter) params.club = clubFilter;
      return fetchMatchesCalendar(params);
    },
    placeholderData: keepPreviousData
  });

  const countries = Array.isArray(countriesQuery.data) ? countriesQuery.data : [];
  const matchesByDate = matchesQuery.data && typeof matchesQuery.data === "object" ? matchesQuery.data : {};
  const loading = matchesQuery.isLoading;
  const error = countriesQuery.isError
    ? "Could not load countries."
    : matchesQuery.isError
      ? "Could not load calendar matches."
      : "";

  useEffect(() => {
    if (!selectedCountry && countries[0]?.name) {
      setSelectedCountry(countries[0].name);
    }
  }, [selectedCountry, countries]);

  const monthCells = useMemo(() => buildMonthDays(cursorMonth), [cursorMonth]);

  const getMatchesForDay = (dateObj) => {
    if (!dateObj) return [];
    const key = dateObj.toISOString().slice(0, 10);
    return Array.isArray(matchesByDate[key]) ? matchesByDate[key] : [];
  };

  const groupedList = useMemo(() => {
    return Object.entries(matchesByDate)
      .sort((a, b) => new Date(a[0]) - new Date(b[0]))
      .slice(0, 14);
  }, [matchesByDate]);

  return (
    <main className="mx-auto min-h-screen w-full max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <Link to="/" className="inline-flex rounded-full border border-slate-600 px-3 py-1 text-xs text-slate-300">
            Back
          </Link>
          <h1 className="mt-3 text-3xl font-bold text-white">Match Calendar</h1>
        </div>
        <div className="flex flex-wrap gap-2">
          <select
            value={selectedCountry}
            onChange={(e) => setSelectedCountry(e.target.value)}
            className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-slate-200"
          >
            <option value="">All Countries</option>
            {countries.map((country) => (
              <option key={country.name} value={country.name} className="bg-slate-900">
                {country.name}
              </option>
            ))}
          </select>
          <input
            value={clubFilter}
            onChange={(e) => setClubFilter(e.target.value)}
            placeholder="Filter by club"
            className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-slate-200 placeholder:text-slate-500"
          />
        </div>
      </div>

      {error && <p className="mb-4 rounded-xl border border-red-500/50 bg-red-950/40 p-4 text-sm text-red-200">{error}</p>}

      <section className="rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
        <div className="mb-4 flex items-center justify-between">
          <button
            type="button"
            onClick={() => setCursorMonth((prev) => new Date(prev.getFullYear(), prev.getMonth() - 1, 1))}
            className="rounded-lg border border-white/15 px-3 py-1 text-xs text-slate-300"
          >
            Prev
          </button>
          <h2 className="text-lg font-semibold text-white">
            {cursorMonth.toLocaleDateString(undefined, { month: "long", year: "numeric" })}
          </h2>
          <button
            type="button"
            onClick={() => setCursorMonth((prev) => new Date(prev.getFullYear(), prev.getMonth() + 1, 1))}
            className="rounded-lg border border-white/15 px-3 py-1 text-xs text-slate-300"
          >
            Next
          </button>
        </div>

        <div className="grid grid-cols-7 gap-2 text-center text-xs text-slate-400">
          {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((day) => (
            <div key={day}>{day}</div>
          ))}
        </div>

        <div className="mt-2 grid grid-cols-7 gap-2">
          {monthCells.map((cell, index) => {
            const dayMatches = getMatchesForDay(cell);
            return (
              <div key={`${cell ? cell.toISOString() : 'empty'}-${index}`} className="min-h-[96px] rounded-xl border border-white/10 bg-white/[0.03] p-2">
                {cell && (
                  <>
                    <p className="text-xs font-semibold text-slate-200">{cell.getDate()}</p>
                    <div className="mt-1 space-y-1">
                      {dayMatches.slice(0, 2).map((match, idx) => (
                        <div key={`${match.id || idx}`} className="rounded-md bg-stadium/10 px-2 py-1 text-[10px] text-stadium">
                          <div className="flex items-center gap-1">
                            <img
                              src={match.home_club_logo || `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.home_team || match.club || "Home"))}&backgroundColor=1A56A0&textColor=ffffff`}
                              alt="home logo"
                              className="h-3 w-3 rounded-full object-cover"
                              onError={(e) => {
                                e.currentTarget.src = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.home_team || match.club || "Home"))}&backgroundColor=1A56A0&textColor=ffffff`;
                              }}
                            />
                            <img
                              src={match.away_club_logo || `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.away_team || match.opponent || "Away"))}&backgroundColor=1A56A0&textColor=ffffff`}
                              alt="away logo"
                              className="h-3 w-3 rounded-full object-cover"
                              onError={(e) => {
                                e.currentTarget.src = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.away_team || match.opponent || "Away"))}&backgroundColor=1A56A0&textColor=ffffff`;
                              }}
                            />
                            <span>{match.home_team || match.club} vs {match.away_team || match.opponent}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </>
                )}
              </div>
            );
          })}
        </div>
      </section>

      <section className="mt-8 rounded-2xl border border-white/10 bg-white/[0.04] p-4 backdrop-blur-sm sm:p-5">
        <h2 className="text-lg font-semibold text-white">Upcoming fixtures list</h2>
        {loading && <p className="mt-3 text-sm text-slate-400">Loading...</p>}
        {!loading && groupedList.length === 0 && <p className="mt-3 text-sm text-slate-400">No matches found.</p>}
        <div className="mt-4 space-y-4">
          {groupedList.map(([dateKey, matches]) => (
            <div key={dateKey}>
              <h3 className="sticky top-2 z-10 rounded-lg border border-white/10 bg-slate-900/90 px-3 py-2 text-sm font-semibold text-stadium">
                {new Date(dateKey).toLocaleDateString(undefined, {
                  weekday: "long",
                  month: "short",
                  day: "numeric"
                })}
              </h3>
              <div className="mt-2 space-y-2">
                {(Array.isArray(matches) ? matches : []).map((match, idx) => (
                  <article key={`${match.id || idx}-${match.match_date}`} className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                    <div className="flex items-center gap-2">
                      <img
                        src={match.home_club_logo || `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.home_team || match.club || "Home"))}&backgroundColor=1A56A0&textColor=ffffff`}
                        alt="home logo"
                        className="h-5 w-5 rounded-full object-cover"
                        onError={(e) => {
                          e.currentTarget.src = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.home_team || match.club || "Home"))}&backgroundColor=1A56A0&textColor=ffffff`;
                        }}
                      />
                      <img
                        src={match.away_club_logo || `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.away_team || match.opponent || "Away"))}&backgroundColor=1A56A0&textColor=ffffff`}
                        alt="away logo"
                        className="h-5 w-5 rounded-full object-cover"
                        onError={(e) => {
                          e.currentTarget.src = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(String(match.away_team || match.opponent || "Away"))}&backgroundColor=1A56A0&textColor=ffffff`;
                        }}
                      />
                      <p className="text-sm font-semibold text-white">
                        {match.home_team || match.club} vs {match.away_team || match.opponent}
                      </p>
                    </div>
                    <p className="mt-1 text-xs text-slate-300">{new Date(match.match_date).toLocaleString()}</p>
                    <p className="mt-1 text-xs text-stadium">{match.competition || "Competition"}</p>
                  </article>
                ))}
              </div>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}

export default CalendarPage;

