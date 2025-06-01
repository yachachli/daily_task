import asyncio
import typing as t

from daily_bets.db import delete_old_bets as db
from daily_bets.db_pool import DBConnection, DBPool, db_pool


async def run_with_pool(
    pool: DBPool, func: t.Callable[[DBConnection], t.Awaitable[None]]
):
    async with pool.acquire() as conn:
        await func(conn)


async def main():
    pool = await db_pool()
    if not pool:
        print("Failed to create database pool.")
        return

    _ = await asyncio.gather(
        run_with_pool(pool, db.delete_old_nba_bets),
        run_with_pool(pool, db.delete_old_nfl_bets),
        run_with_pool(pool, db.delete_old_mlb_bets),
        run_with_pool(pool, db.delete_old_wnba_bets),
    )


if __name__ == "__main__":
    asyncio.run(main())
