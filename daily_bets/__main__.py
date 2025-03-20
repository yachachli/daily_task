import asyncio

from daily_bets.nba import nba
from daily_bets.db import db_pool
from daily_bets.logger import setup_logging, logger
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    logger.warning("Failed to load `dotenv`, proceding with existing env vars")


async def main():
    setup_logging()
    pool = await db_pool()

    if len(sys.argv) < 2:
        logger.info("Running daily bets analysis with all stats")
        await nba.run(pool, list(nba.MARKET_TO_STAT.keys()))
        return

    stat = sys.argv[1]
    if stat not in nba.MARKET_TO_STAT:
        logger.error(f"{stat} not one of {nba.MARKET_TO_STAT.keys()}")
        return

    await nba.run(pool, [stat])
    # await nfl.run(pool)


if __name__ == "__main__":
    asyncio.run(main())
