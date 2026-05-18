-- name: WnbaPlayersWithTeam :many
SELECT P.*, T.team_abv FROM wnba_players P
INNER JOIN wnba_teams T ON P.team_id = T.id;

-- name: WnbaTeams :many
SELECT * FROM wnba_teams;

-- name: WnbaCopyAnalysis :copyfrom
INSERT INTO v2_wnba_daily_bets (analysis, price, game_time, game_tag) VALUES ($1, $2, $3, $4);

-- name: WnbaDedupeRecentAnalysis :execrows
WITH ranked AS (
    SELECT
        id,
        row_number() OVER (
            PARTITION BY
                game_time,
                game_tag,
                (analysis->'input'->>'player_id')::bigint,
                analysis->'input'->>'stat',
                (analysis->'input'->>'line')::numeric
            ORDER BY created_at DESC, id DESC
        ) AS rn
    FROM public.v2_wnba_daily_bets
    WHERE created_at >= now() - make_interval(days => $1)
)
DELETE FROM public.v2_wnba_daily_bets b
USING ranked r
WHERE b.id = r.id AND r.rn > 1;

-- name: WnbaRecentAnalysisKeys :many
SELECT
    game_time,
    game_tag,
    (analysis->'input'->>'player_id')::bigint AS player_id,
    analysis->'input'->>'stat' AS stat,
    (analysis->'input'->>'line')::numeric AS line
FROM public.v2_wnba_daily_bets
WHERE created_at >= now() - make_interval(days => $1);

-- name: WnbaUpsertAnalysis :one
WITH inserted AS (
    INSERT INTO public.v2_wnba_daily_bets (analysis, price, game_time, game_tag)
    SELECT sqlc.arg(analysis_json), sqlc.arg(price), sqlc.arg(game_time), sqlc.arg(game_tag)
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.v2_wnba_daily_bets
        WHERE
            game_time = sqlc.arg(game_time)
            AND game_tag = sqlc.arg(game_tag)
            AND (analysis->'input'->>'player_id')::bigint =
                (sqlc.arg(analysis_json)::json->'input'->>'player_id')::bigint
            AND analysis->'input'->>'stat' = (sqlc.arg(analysis_json)::json->'input'->>'stat')
            AND (analysis->'input'->>'line')::numeric =
                (sqlc.arg(analysis_json)::json->'input'->>'line')::numeric
    )
    RETURNING 1
)
SELECT count(*) FROM inserted;
