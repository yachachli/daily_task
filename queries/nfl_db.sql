-- name: NflPlayersWithTeam :many
SELECT P.*, T.team_code as team_abv FROM v3_nfl_players P
INNER JOIN v3_nfl_teams T ON P.team_id = T.id;

-- name: NflTeams :many
SELECT * FROM v3_nfl_teams;

-- name: NflCopyAnalysis :copyfrom
INSERT INTO v2_nfl_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);

-- name: NflDedupeRecentAnalysis :execrows
WITH ranked AS (
    SELECT
        id,
        row_number() OVER (
            PARTITION BY
                game_time,
                game_tag,
                (analysis->'input'->>'player_id')::int,
                analysis->'input'->>'stat',
                (analysis->'input'->>'line')::numeric
            ORDER BY created_at DESC, id DESC
        ) AS rn
    FROM public.v2_nfl_daily_bets
    WHERE created_at >= now() - make_interval(days => $1)
)
DELETE FROM public.v2_nfl_daily_bets b
USING ranked r
WHERE b.id = r.id AND r.rn > 1;

-- name: NflRecentAnalysisKeys :many
SELECT
    game_time,
    game_tag,
    (analysis->'input'->>'player_id')::int AS player_id,
    analysis->'input'->>'stat' AS stat,
    (analysis->'input'->>'line')::numeric AS line
FROM public.v2_nfl_daily_bets
WHERE created_at >= now() - make_interval(days => $1);

-- name: NflUpsertAnalysis :one
WITH inserted AS (
    INSERT INTO public.v2_nfl_daily_bets (analysis, price, game_time, game_tag)
    SELECT sqlc.arg(analysis_json), sqlc.arg(price), sqlc.arg(game_time), sqlc.arg(game_tag)
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.v2_nfl_daily_bets
        WHERE
            game_time = sqlc.arg(game_time)
            AND game_tag = sqlc.arg(game_tag)
            AND (analysis->'input'->>'player_id')::int =
                (sqlc.arg(analysis_json)::json->'input'->>'player_id')::int
            AND analysis->'input'->>'stat' = (sqlc.arg(analysis_json)::json->'input'->>'stat')
            AND (analysis->'input'->>'line')::numeric =
                (sqlc.arg(analysis_json)::json->'input'->>'line')::numeric
    )
    RETURNING 1
)
SELECT count(*) FROM inserted;
