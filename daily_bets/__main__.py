import asyncio

from daily_bets import nba
from daily_bets.db import db_pool
from daily_bets.logger import setup_logging, logger


try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    logger.warning("Failed to load `dotenv`, proceding with existing env vars")


async def main():
    setup_logging()
    pool = await db_pool()
    logger.info("Running daily bets analysis")

    await nba.run(pool)


if __name__ == "__main__":
    asyncio.run(main())
