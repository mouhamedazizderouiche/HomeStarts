ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS data_sources text[] DEFAULT ARRAY[]::text[];

UPDATE public.players
SET data_sources = CASE
  WHEN data_sources IS NULL OR array_length(data_sources, 1) IS NULL THEN ARRAY['transfermarkt']::text[]
  ELSE data_sources
END;
