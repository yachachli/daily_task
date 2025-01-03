import asyncpg
from os import environ
from typing import TYPE_CHECKING

from daily_bets.logger import logger

if TYPE_CHECKING:
    DBPool = asyncpg.Pool[asyncpg.Record]
else:
    DBPool = asyncpg.Pool


async def db_pool():
    logger.info("Creating database connection pool")

    pool: DBPool = await asyncpg.create_pool(
        database=environ["DB_NAME"],
        user=environ["DB_USER"],
        password=environ["DB_PASS"],
        host=environ["DB_HOST"],
    )  # type: ignore
    return pool
