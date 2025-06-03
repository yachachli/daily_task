-- name: MlbPlayers :many
SELECT * FROM mlb_players;

-- name: MlbTeams :many
SELECT * FROM mlb_teams;

-- name: MlbCopyAnalysis :copyfrom
INSERT INTO v2_mlb_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);
