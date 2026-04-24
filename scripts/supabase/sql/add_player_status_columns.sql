ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS current_club text;

ALTER TABLE public.players
ADD COLUMN IF NOT EXISTS player_status text DEFAULT 'active';

UPDATE public.players
SET current_club = COALESCE(NULLIF(current_club, ''), club)
WHERE current_club IS NULL OR current_club = '';

UPDATE public.players
SET player_status = COALESCE(player_status, 'active')
WHERE player_status IS NULL OR player_status = '';

CREATE INDEX IF NOT EXISTS idx_players_is_active ON public.players (is_active);
CREATE INDEX IF NOT EXISTS idx_players_player_status ON public.players (player_status);
CREATE INDEX IF NOT EXISTS idx_players_current_club ON public.players (current_club);
