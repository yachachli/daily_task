import asyncio
import typing as t

from daily_bets.db import delete_old_bets as db
from daily_bets.logger import logger, setup_logging
from daily_bets.db_pool import DBConnection, DBPool, db_pool


async def run_with_pool(
    pool: DBPool, func: t.Callable[[DBConnection], t.Awaitable[int]]
):
    total = 0
    async with pool.acquire() as conn:
        total += await func(conn)
    return total

async def main():
    pool = await db_pool()
    if not pool:
        print("Failed to create database pool.")
        return

    (
        nba_rows,
        nfl_rows,
        mlb_rows,
        wnba_rows,
    ) = await asyncio.gather(
        run_with_pool(pool, db.delete_old_nba_bets),
        run_with_pool(pool, db.delete_old_nfl_bets),
        run_with_pool(pool, db.delete_old_mlb_bets),
        run_with_pool(pool, db.delete_old_wnba_bets),
    )
    logger.info(f"Deleted {nba_rows} NBA rows")
    logger.info(f"Deleted {nfl_rows} NFL rows")
    logger.info(f"Deleted {mlb_rows} MLB rows")
    logger.info(f"Deleted {wnba_rows} WNBA rows")
    logger.info(f"Total rows deleted: {nba_rows + nfl_rows + mlb_rows + wnba_rows}")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
