try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    print("Failed to load `dotenv`, proceding with existing env vars")

import asyncio

from daily_bets.db_pool import db_pool
from daily_bets.logger import setup_logging
from daily_bets.nfl import nfl


async def main():
    setup_logging()

    pool = await db_pool()

    if not pool:
        print("Failed to create database pool.")
        return

    await nfl.run(pool)
    # _ = await asyncio.gather(mlb.run(pool), nba.run(pool))


if __name__ == "__main__":
    asyncio.run(main())
