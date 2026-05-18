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
                (analysis->'input'->>'player_id')::int,
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

-- name: WnbaUpsertAnalysis :one
WITH updated AS (
    UPDATE public.v2_wnba_daily_bets
    SET
        analysis = $1,
        price = $2,
        game_time = $3,
        game_tag = $4,
        created_at = now()
    WHERE
        game_time = $3
        AND game_tag = $4
        AND (analysis->'input'->>'player_id')::int =
            ($1::json->'input'->>'player_id')::int
        AND analysis->'input'->>'stat' = ($1::json->'input'->>'stat')
        AND (analysis->'input'->>'line')::numeric =
            ($1::json->'input'->>'line')::numeric
    RETURNING 1
), inserted AS (
    INSERT INTO public.v2_wnba_daily_bets (analysis, price, game_time, game_tag)
    SELECT $1, $2, $3, $4
    WHERE NOT EXISTS (SELECT 1 FROM updated)
    RETURNING 1
)
SELECT (SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted);
