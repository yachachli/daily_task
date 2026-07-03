-- Portuguese translation columns. Nullable, so existing rows and code are
-- unaffected (CLAUDE.md: new columns must be nullable or defaulted).
-- MUST be run against Neon BEFORE deploying the bestbet_backend homepage
-- functions from the portuguese-support-backend branch (their SELECT names
-- analysis_pt explicitly) and before setting TRANSLATE_LANGUAGES=es,pt.
ALTER TABLE v2_nba_daily_bets  ADD COLUMN IF NOT EXISTS analysis_pt TEXT;
ALTER TABLE v2_mlb_daily_bets  ADD COLUMN IF NOT EXISTS analysis_pt TEXT;
ALTER TABLE v2_nfl_daily_bets  ADD COLUMN IF NOT EXISTS analysis_pt TEXT;
ALTER TABLE v2_wnba_daily_bets ADD COLUMN IF NOT EXISTS analysis_pt TEXT;
ALTER TABLE predictions        ADD COLUMN IF NOT EXISTS analysis_pt TEXT;
