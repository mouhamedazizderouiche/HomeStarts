# HomeStarts# HomeStars MVP

HomeStars is a dark-themed football scouting web app where users can:

- Select a country
- View all players of that nationality
- Open player details and upcoming matches

## Tech Stack

- Frontend: React + Vite + Tailwind CSS + React Router + Axios
- Backend: Node.js + Express
- Data: football-data.org (main) + TheSportsDB (backup/enrichment)

## Project Structure

```text
HomeStars/
  backend/
    cache/
      store.js
    data/
      countries.js
    routes/
      playerRoutes.js
    services/
      footballApi.js
      playerService.js
      sportsdbApi.js
    package.json
    server.js
  frontend/
    src/
      api/
        client.js
      components/
        CountryCard.jsx
        MatchCard.jsx
        PlayerCard.jsx
        SearchBar.jsx
      pages/
        HomePage.jsx
        PlayerDetailPage.jsx
        PlayerListPage.jsx
      App.jsx
      index.css
      main.jsx
    index.html
    package.json
    postcss.config.js
    tailwind.config.js
    vite.config.js
```

## Backend Routes

- `GET /players?country=Tunisia`
- `GET /player/:id`
- `GET /countries` (used by frontend for country cards)

## Real Data Setup (Dual API)

1. Create a key on [football-data.org](https://www.football-data.org/).
2. Copy `backend/.env.example` to `backend/.env`.
3. Set `FOOTBALL_API_KEY` in `backend/.env`.
4. Optionally tune:
   - `FOOTBALL_COMPETITIONS` (default: `PL,PD,SA,FL1,BL1,DED,PPL,BSA`)
   - `CACHE_TTL_SECONDS` (default 6 hours)
   - `PLAYERS_PER_COUNTRY_LIMIT`
   - `NEXT_MATCHES_COUNT`
   - `TEAM_SQUADS_WARMUP_LIMIT`, `TEAM_SQUADS_ON_DEMAND_LIMIT`, `TEAM_API_THROTTLE_MS`

### Providers

- Main provider: `https://api.football-data.org/v4/`
- Backup/enrichment provider: `https://www.thesportsdb.com/api/v1/json/123/`

### Caching

- Country player list cache key: `players_<country>`
- Player detail cache key: `player_<id>`
- Player lookup cache key: `player_index_<id>`
- Competition teams cache key: `competition_teams_<code>` (24h)
- Team squad cache key: `team_squad_<teamId>` (12h)
- Default TTL: `21600` seconds (6 hours)

## Run Locally

Open two terminals from `HomeStars` root.

### Terminal 1: Backend

```bash
cd backend
npm install
npm run dev
```

Backend runs on `http://localhost:5000`.

### Terminal 2: Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on `http://localhost:5173`.
