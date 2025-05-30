import asyncio

from daily_bets.db_pool import DBPool, db_pool

try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    pass


async def delete_nba(pool: DBPool):
    async with pool.acquire() as conn:
        _ = await conn.execute("""
        DELETE FROM v2_nba_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def delete_nfl(pool: DBPool):
    async with pool.acquire() as conn:
        _ = await conn.execute("""
        DELETE FROM v2_nfl_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def delete_mlb(pool: DBPool):
    async with pool.acquire() as conn:
        _ = await conn.execute("""
        DELETE FROM v2_mlb_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';""")


async def main():
    pool = await db_pool()
    if not pool:
        print("Failed to create database pool.")
        return

    _ = await asyncio.gather(
        delete_nba(pool),
        delete_nfl(pool),
    )


if __name__ == "__main__":
    asyncio.run(main())
