CREATE TABLE IF NOT EXISTS public.matches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  club text,
  opponent text,
  home_team text,
  away_team text,
  match_date timestamptz,
  competition text,
  home_away text,
  home_score int,
  away_score int,
  score text,
  stadium text,
  status text,
  created_at timestamptz DEFAULT NOW()
);

ALTER TABLE public.matches
ADD COLUMN IF NOT EXISTS score text;

ALTER TABLE public.matches
ADD COLUMN IF NOT EXISTS home_team text,
ADD COLUMN IF NOT EXISTS away_team text,
ADD COLUMN IF NOT EXISTS home_score int,
ADD COLUMN IF NOT EXISTS away_score int;

CREATE INDEX IF NOT EXISTS idx_matches_club ON public.matches (club);
CREATE INDEX IF NOT EXISTS idx_matches_date ON public.matches (match_date);

CREATE UNIQUE INDEX IF NOT EXISTS uq_matches_club_date
ON public.matches (club, match_date);
