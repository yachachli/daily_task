import asyncpg
import httpx
import json
from datetime import datetime, timedelta, timezone
import logging
import asyncio
import pprint

from daily_bets import nba

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s",
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    logging.warning("Failed to load `dotenv`, proceding with existing env vars")


async def main():
    logging.info("Running daily bets analysis")

    await nba.run()


asyncio.run(main())
