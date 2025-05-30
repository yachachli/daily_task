-- name: NflPlayersWithTeam :many
SELECT P.*, T.team_code as team_abv FROM v3_nfl_players P
INNER JOIN v3_nfl_teams T ON P.team_id = T.id;

-- name: NflTeams :many
SELECT * FROM v3_nfl_teams;

-- name: NflCopyAnalysis :copyfrom
INSERT INTO v2_nfl_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);