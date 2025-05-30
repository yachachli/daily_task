try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    print("Failed to load `dotenv`, proceding with existing env vars")

import asyncio
import sys

from daily_bets.analysis import mlb, nba, nfl
from daily_bets.db_pool import db_pool
from daily_bets.logger import logger, setup_logging


async def main():
    setup_logging()

    pool = await db_pool()

    if not pool:
        print("Failed to create database pool.")
        return

    if len(sys.argv) < 2:
        logger.info("No arguments provided, running all analyses")
        _ = await asyncio.gather(nfl.run(pool), mlb.run(pool), nba.run(pool))

    match sys.argv[1]:
        case "nfl":
            await nfl.run(pool)
        case "mlb":
            await mlb.run(pool)
        case "nba":
            await nba.run(pool)
        case _:
            logger.error(
                f"Invalid argument: {sys.argv[1]}. Expected: 'nfl', 'mlb', 'nba' or nothing to run all"
            )


if __name__ == "__main__":
    asyncio.run(main())
