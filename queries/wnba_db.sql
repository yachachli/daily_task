-- name: WnbaPlayersWithTeam :many
SELECT P.*, T.team_abv FROM wnba_players P
INNER JOIN wnba_teams T ON P.team_id = T.id;

-- name: WnbaTeams :many
SELECT * FROM wnba_teams;

-- name: WnbaCopyAnalysis :copyfrom
INSERT INTO v2_wnba_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);