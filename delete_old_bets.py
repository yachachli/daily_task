import asyncio

from daily_bets.db_pool import DBPool, db_pool

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


async def delete_nba(pool: DBPool):
    async with pool.acquire() as conn:
        await conn.execute("""
        DELETE FROM v2_nba_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def delete_nfl(pool: DBPool):
    async with pool.acquire() as conn:
        await conn.execute("""
        DELETE FROM v2_nfl_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def main():
    pool = await db_pool()
    if not pool:
        print("Failed to create database pool.")
        return

    await asyncio.gather(
        delete_nba(pool),
        delete_nfl(pool),
    )


if __name__ == "__main__":
    asyncio.run(main())
