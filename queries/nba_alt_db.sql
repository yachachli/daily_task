-- name: NbaAltPlayersWithTeam :many
SELECT P.*, T.team_abv FROM nba_players P
INNER JOIN nba_teams T ON P.team_id = T.id;

-- name: NbaAltTeams :many
SELECT * FROM nba_teams;

-- name: NbaAltCopyAnalysis :copyfrom
INSERT INTO v2_nba_alt_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);
