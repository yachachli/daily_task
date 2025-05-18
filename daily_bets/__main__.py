try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("Failed to load `dotenv`, proceding with existing env vars")

import asyncio

from daily_bets.db_pool import db_pool
from daily_bets.logger import setup_logging
from daily_bets.mlb import mlb


async def main():
    setup_logging()

    pool = await db_pool()

    if not pool:
        print("Failed to create database pool.")
        return

    await mlb.run(pool)

    # if len(sys.argv) < 2:
    #     logger.info("Running daily bets analysis with all stats")
    #     await nba.run(pool, list(nba.MARKET_TO_STAT.keys()))
    #     return

    # stat = sys.argv[1]
    # if stat not in nba.MARKET_TO_STAT:
    #     logger.error(f"{stat} not one of {nba.MARKET_TO_STAT.keys()}")
    #     return

    # logger.info(f"Running daily bets analysis with stat {stat}")
    # await nba.run(pool, [stat])
    # await nfl.run(pool)


if __name__ == "__main__":
    asyncio.run(main())
