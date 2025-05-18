-- name: AllPlayers :many
SELECT * FROM mlb_players;

-- name: AllTeams :many
SELECT * FROM mlb_teams;

-- -- name: CopyAnalysis :copyfrom
-- INSERT INTO v2_mlb_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);