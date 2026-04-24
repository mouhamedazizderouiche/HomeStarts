# Supabase Data Pipeline Operations

## 1) Prepare Schema (once)

Run these SQL files in Supabase SQL editor:

- `scripts/wikipedia/sql/add_data_sources_column.sql`
- `scripts/supabase/sql/add_player_status_columns.sql`
- `scripts/supabase/sql/add_scoring_activity_columns.sql`
- `scripts/supabase/sql/add_matches_table.sql`

## 2) Configure Env

Set in `backend/.env`:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_PLAYERS_TABLE=players`
- `SUPABASE_MATCHES_TABLE=matches` (optional)
- `FOOTBALL_DATA_API_KEY` (for primary match sync source)

## 3) Clean Players

From `backend/`:

```bash
npm run supabase:clean-players
```

This script:

- never deletes rows
- cleans clubs (removes `(– 2022)` style suffixes)
- sets `is_active` using:
  - `last_season >= 2023` OR `is_national_team_player = true`
  - plus valid club (not unknown)
- sets `player_status` (`verified`, `active`, `uncertain`)
- updates `last_seen_at` when appropriate

## 4) Update Scores

From `backend/`:

```bash
npm run supabase:update-scores
```

This script computes:

- `form_score` (0-10 normalized)
- `top_score`

## 5) Sync Matches (7-14 days)

From `backend/`:

```bash
npm run sync-matches
```

This script:

- uses football-data.org (primary)
- falls back to TheSportsDB only if needed
- applies 6-second delay between external requests
- upserts by `club + match_date`
- keeps fixtures only in years `2024-2026`

