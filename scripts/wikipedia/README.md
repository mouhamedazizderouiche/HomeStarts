# Wikipedia National Team Enrichment

## Step 1: Scrape Wikipedia to JSON

Run from `backend/`:

```bash
npm run wiki:scrape-national-teams
```

Optional env vars:

- `WIKI_COUNTRY_LIMIT=3` to run only first N countries.
- `WIKI_COUNTRIES=tn,ma,dz` to run only selected countries by ISO or slug.
- `WIKI_OUTPUT_JSON=C:/work/HomeStars/data/wikipedia/national-team-players.json`
- `WIKIPEDIA_USER_AGENT=...`

Output JSON structure:

- `players[]`: `name`, `position`, `dob`, `dob_iso`, `caps`, `goals`, `current_club`, `nationality_code`, `is_national_team_player`, `is_active`
- `countries[]`: per-country scrape stats

## Step 2: Enrich Supabase players table

1. Add env values in `backend/.env`:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_PLAYERS_TABLE=players` (optional)
   - `WIKI_INPUT_JSON` (optional)
2. Ensure `data_sources` exists:
   - Run `scripts/wikipedia/sql/add_data_sources_column.sql` in Supabase SQL editor.
3. Run:

```bash
npm run wiki:enrich-supabase
```

Behavior:

- Fuzzy matches by normalized name + DOB preference.
- If matched: updates `current_club`, `club`, `is_active`, `is_national_team_player`, `caps`, `goals`, `data_sources`.
- If not matched: inserts a new row with `data_sources=['wikipedia']`.
- Never deletes players.
