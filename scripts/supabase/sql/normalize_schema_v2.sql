BEGIN;

CREATE TABLE IF NOT EXISTS public.countries (
  code text PRIMARY KEY,
  name text NOT NULL,
  flag_url text,
  created_at timestamptz DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.clubs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  transfermarkt_id integer UNIQUE,
  country_code text REFERENCES public.countries(code),
  logo_url text,
  created_at timestamptz DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.players (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  transfermarkt_id integer UNIQUE,
  birth_date date,
  position text,
  market_value integer,
  nationality_code text REFERENCES public.countries(code),
  club_id uuid REFERENCES public.clubs(id),
  image_url text,
  last_season integer,
  created_at timestamptz DEFAULT NOW(),
  CONSTRAINT unique_player_identity UNIQUE (name, birth_date, club_id)
);

CREATE TABLE IF NOT EXISTS public.national_teams (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  country_code text REFERENCES public.countries(code),
  name text,
  transfermarkt_id integer UNIQUE,
  created_at timestamptz DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.national_team_players (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  player_id uuid REFERENCES public.players(id) ON DELETE CASCADE,
  national_team_id uuid REFERENCES public.national_teams(id) ON DELETE CASCADE,
  UNIQUE(player_id, national_team_id)
);

CREATE TABLE IF NOT EXISTS public.matches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  home_club_id uuid REFERENCES public.clubs(id),
  away_club_id uuid REFERENCES public.clubs(id),
  match_date timestamptz,
  competition text,
  score text,
  created_at timestamptz DEFAULT NOW(),
  CONSTRAINT unique_match UNIQUE (home_club_id, away_club_id, match_date)
);

ALTER TABLE public.players ADD COLUMN IF NOT EXISTS transfermarkt_id integer;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS birth_date date;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS position text;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS market_value integer;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS nationality_code text;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS club_id uuid;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS image_url text;
ALTER TABLE public.players ADD COLUMN IF NOT EXISTS last_season integer;

ALTER TABLE public.clubs ADD COLUMN IF NOT EXISTS transfermarkt_id integer;
ALTER TABLE public.clubs ADD COLUMN IF NOT EXISTS country_code text;
ALTER TABLE public.clubs ADD COLUMN IF NOT EXISTS logo_url text;

ALTER TABLE public.countries ADD COLUMN IF NOT EXISTS flag_url text;

ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS home_club_id uuid;
ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS away_club_id uuid;
ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS competition text;
ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS score text;

CREATE INDEX IF NOT EXISTS idx_players_club ON public.players(club_id);
CREATE INDEX IF NOT EXISTS idx_players_country ON public.players(nationality_code);
CREATE INDEX IF NOT EXISTS idx_clubs_country ON public.clubs(country_code);
CREATE INDEX IF NOT EXISTS idx_matches_date ON public.matches(match_date);

CREATE UNIQUE INDEX IF NOT EXISTS uq_players_transfermarkt_id ON public.players(transfermarkt_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_clubs_transfermarkt_id ON public.clubs(transfermarkt_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_players_identity ON public.players(name, birth_date, club_id);

DELETE FROM public.players a
USING public.players b
WHERE a.id < b.id
  AND a.name = b.name
  AND COALESCE(a.birth_date::text, '') = COALESCE(b.birth_date::text, '')
  AND COALESCE(a.club_id::text, '') = COALESCE(b.club_id::text, '');

UPDATE public.countries
SET code = lower(code)
WHERE code <> lower(code);

UPDATE public.players
SET nationality_code = lower(nationality_code)
WHERE nationality_code IS NOT NULL
  AND nationality_code <> lower(nationality_code);

UPDATE public.clubs
SET country_code = lower(country_code)
WHERE country_code IS NOT NULL
  AND country_code <> lower(country_code);

UPDATE public.players
SET image_url = 'https://img.transfermarkt.com/portrait/normal/' || transfermarkt_id || '.jpg'
WHERE transfermarkt_id IS NOT NULL
  AND (image_url IS NULL OR image_url = '' OR image_url LIKE '%ui-avatars%');

UPDATE public.clubs
SET logo_url = 'https://tmssl.akamaized.net/images/wappen/normal/' || transfermarkt_id || '.png'
WHERE transfermarkt_id IS NOT NULL
  AND (logo_url IS NULL OR logo_url = '');

UPDATE public.countries
SET flag_url = 'https://flagcdn.com/64x48/' || lower(code) || '.png'
WHERE code IS NOT NULL
  AND (flag_url IS NULL OR flag_url = '' OR flag_url LIKE '%/un.png');

COMMIT;
