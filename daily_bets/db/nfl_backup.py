from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    import asyncpg

    ConnectionLike = (
        asyncpg.Connection[asyncpg.Record]
        | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]
    )
else:
    ConnectionLike = object


async def dedupe_backup(conn: ConnectionLike) -> None:
    """Delete duplicate rows in public.v2_nfl_daily_bets_backup keeping latest created_at.

    Uses ctid to deterministically keep one row per id when created_at ties.
    """
    await conn.execute(
        """
        WITH ranked AS (
          SELECT ctid, id,
                 ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY created_at DESC, ctid DESC
                 ) AS rn
          FROM public.v2_nfl_daily_bets_backup
        )
        DELETE FROM public.v2_nfl_daily_bets_backup b
        USING ranked r
        WHERE b.ctid = r.ctid
          AND r.rn > 1;
        """
    )


async def ensure_unique_index(conn: ConnectionLike) -> None:
    """Ensure a unique index exists on backup table id column."""
    await conn.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM   pg_indexes
            WHERE  schemaname = 'public'
            AND    indexname = 'ux_v2_nfl_daily_bets_backup_id'
          ) THEN
            EXECUTE 'CREATE UNIQUE INDEX ux_v2_nfl_daily_bets_backup_id ON public.v2_nfl_daily_bets_backup (id)';
          END IF;
        END$$;
        """
    )


async def sync_recent_to_backup(conn: ConnectionLike, *, days: int = 14) -> int:
    """Insert recent rows from primary into backup, skipping existing ids.

    Returns number of rows inserted (best-effort parse of command tag).
    """
    command_tag = await conn.execute(
        """
        INSERT INTO public.v2_nfl_daily_bets_backup (id, analysis, created_at, price, game_time, game_tag)
        SELECT b.id, b.analysis, b.created_at, b.price, b.game_time, b.game_tag
        FROM public.v2_nfl_daily_bets b
        WHERE b.created_at > NOW() - ($1::interval)
        ON CONFLICT (id) DO NOTHING
        """,
        f"{days} days",
    )
    # asyncpg returns tags like "INSERT 0 <n>"
    parts = command_tag.split()
    if parts and parts[-1].isdigit():
        return int(parts[-1])
    return 0


async def run_backup_maintenance(conn: ConnectionLike, *, days: int = 14) -> int:
    """Run dedupe, ensure unique index, and sync recent rows.

    Returns number of rows inserted during sync.
    """
    await dedupe_backup(conn)
    await ensure_unique_index(conn)
    return await sync_recent_to_backup(conn, days=days)


