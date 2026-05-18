from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

    ConnectionLike = (
        asyncpg.Connection[asyncpg.Record]
        | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]
    )


ExistingBetKey = tuple[datetime, str, int, str, Decimal]

RECENT_ANALYSIS_KEYS_SQL = """
SELECT
    game_time,
    game_tag,
    (analysis->'input'->>'player_id')::int AS player_id,
    analysis->'input'->>'stat' AS stat,
    (analysis->'input'->>'line')::numeric AS line
FROM {table_name}
WHERE created_at >= now() - make_interval(days => $1)
"""


def make_existing_bet_key(
    game_time: datetime,
    game_tag: str,
    player_id: int,
    stat: str,
    line: float | int | Decimal,
) -> ExistingBetKey:
    return (game_time, game_tag, player_id, stat, Decimal(str(line)))


async def fetch_recent_analysis_keys(
    conn: ConnectionLike, table_name: str, *, days: int = 1
) -> set[ExistingBetKey]:
    records = await conn.fetch(
        RECENT_ANALYSIS_KEYS_SQL.format(table_name=table_name),
        days,
    )
    return {
        make_existing_bet_key(
            row["game_time"],
            row["game_tag"],
            row["player_id"],
            row["stat"],
            row["line"],
        )
        for row in records
    }
