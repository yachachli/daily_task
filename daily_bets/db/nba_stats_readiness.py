"""Verify that recent NBA player history is ready before publishing app props."""
from datetime import date

# A game preview's freshness record verifies the schedule (including off-days).
# Check the committed boxes separately so a date alone cannot pass the gate.
READINESS_SQL = """
WITH preview AS (
    SELECT EXISTS (
        SELECT 1 FROM public.nba_game_predictions
        WHERE prediction_date BETWEEN $1::date + 1 AND $1::date + 3
          AND game_payload->'model_context'->'data_freshness'->>'status' = 'complete'
          AND game_payload->'model_context'->'data_freshness'->>'checked_through' = $1::date::text
    ) AS schedule_verified
),
finals AS (
    SELECT game_id, home_score, away_score
    FROM public.nba_game_results
    WHERE game_date = $1 AND status = 'completed'
),
boxes AS (
    SELECT game_id, lower(home_away) AS side, sum(points) AS points,
           sum(minutes_played) AS minutes,
           bool_and(points IS NOT NULL AND minutes_played IS NOT NULL
                    AND rebounds IS NOT NULL AND assists IS NOT NULL
                    AND steals IS NOT NULL AND blocks IS NOT NULL
                    AND turnovers IS NOT NULL AND three_point_fg_made IS NOT NULL) AS stats_complete
    FROM public.nba_player_game_stats
    WHERE game_date = $1
    GROUP BY 1, 2
)
SELECT preview.schedule_verified,
    ARRAY(
        SELECT g.game_id::text FROM finals g
        LEFT JOIN boxes h ON h.game_id = g.game_id AND h.side = 'home'
        LEFT JOIN boxes a ON a.game_id = g.game_id AND a.side = 'away'
        WHERE h.points IS DISTINCT FROM g.home_score OR a.points IS DISTINCT FROM g.away_score
           OR coalesce(h.minutes, 0) < 225 OR coalesce(a.minutes, 0) < 225
           OR h.stats_complete IS DISTINCT FROM true OR a.stats_complete IS DISTINCT FROM true
        ORDER BY g.game_id
    ) AS incomplete_games
FROM preview
"""


async def require_verified_stats(pool, through: date) -> None:
    async with pool.acquire() as connection:
        readiness = await connection.fetchrow(READINESS_SQL, through, timeout=20)
    if not readiness["schedule_verified"] or readiness["incomplete_games"]:
        games = ", ".join(readiness["incomplete_games"]) or "none identified"
        raise RuntimeError(
            f"NBA_STATS_NOT_READY: prior slate {through}; "
            f"schedule verified: {readiness['schedule_verified']}; "
            f"incomplete box scores: {games}"
        )
