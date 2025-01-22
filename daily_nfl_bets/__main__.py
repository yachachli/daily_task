import asyncio
from daily_nfl_bets.nfl import nfl
from daily_nfl_bets.db import db_pool
from daily_nfl_bets.logger import setup_logging, logger

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("Failed to load `dotenv`, proceeding with existing env vars")

async def main():
    setup_logging()
    pool = await db_pool()
    logger.info("Running daily NFL bets analysis")
    await nfl.run(pool)

if __name__ == "__main__":
    asyncio.run(main())
