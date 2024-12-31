import asyncpg
from os import environ


async def db_connect():
    return await asyncpg.connect(
        database=environ["DB_NAME"],
        user=environ["DB_USER"],
        password=environ["DB_PASS"],
        host=environ["DB_HOST"],
    )
