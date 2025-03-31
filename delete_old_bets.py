from asyncpg import Pool
from daily_bets.db import db_pool
import asyncio


async def delete_nba(pool: Pool):
    async with pool.acquire() as conn:
        await conn.execute("""
        DELETE FROM v2_nba_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def delete_nfl(pool: Pool):
    async with pool.acquire() as conn:
        await conn.execute("""
        DELETE FROM v2_nfl_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def main():
    pool = await db_pool()
    asyncio.gather(
        delete_nba(pool),
        delete_nfl(pool),
    )


if __name__ == "__main__":
    asyncio.run(main())
