CREATE TABLE IF NOT EXISTS public.nba_game_predictions (
    id BIGSERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    predicted_winner TEXT,
    predicted_spread NUMERIC(8, 3),
    vegas_spread NUMERIC(5, 1),
    vegas_home_moneyline INTEGER,
    vegas_away_moneyline INTEGER,
    home_win_prob NUMERIC(8, 6),
    away_win_prob NUMERIC(8, 6),
    confidence TEXT,
    home_adj_em NUMERIC(10, 4),
    away_adj_em NUMERIC(10, 4),
    matchup_pace NUMERIC(10, 4),
    short_answer TEXT,
    long_answer TEXT,
    head_to_head TEXT,
    factors JSONB NOT NULL DEFAULT '[]'::jsonb,
    home_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    away_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    game_payload JSONB NOT NULL,
    source_payload JSONB NOT NULL,
    source_endpoint TEXT NOT NULL,
    github_run_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_nba_game_predictions UNIQUE (prediction_date, home_team, away_team),
    CONSTRAINT ck_nba_game_predictions_teams_diff CHECK (home_team <> away_team)
);

CREATE INDEX IF NOT EXISTS idx_nba_game_predictions_date
ON public.nba_game_predictions (prediction_date DESC);
