ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS rating_2025 numeric DEFAULT 0;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS form_score numeric DEFAULT 0;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS top_score numeric DEFAULT 0;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS last_seen_at timestamptz;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS last_match_date date;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS dual_nationality text[] DEFAULT ARRAY[]::text[];

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS origin_countries text[] DEFAULT ARRAY[]::text[];

CREATE INDEX IF NOT EXISTS idx_players_country_top_score
ON public.players (nationality, top_score DESC);

CREATE INDEX IF NOT EXISTS idx_players_form_score
ON public.players (form_score DESC);

CREATE INDEX IF NOT EXISTS idx_players_last_seen_at
ON public.players (last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_players_last_match_date
ON public.players (last_match_date DESC);
