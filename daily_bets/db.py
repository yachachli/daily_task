import json
from os import environ
import typing

import asyncpg

from daily_bets.logger import logger


def encode_jsonb(data: typing.Any):
    return b"\x01" + json.dumps(data).encode()


def decode_jsonb(data: bytes):
    return json.loads(data[1:].decode())


def encode_json(data: typing.Any):
    return json.dumps(data).encode()


def decode_json(data: bytes):
    return json.loads(data.decode())


async def init_connection(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "jsonb",
        encoder=encode_jsonb,
        decoder=decode_jsonb,
        schema="pg_catalog",
        format="binary",
    )
    await conn.set_type_codec(
        "json",
        encoder=encode_json,
        decoder=decode_json,
        schema="pg_catalog",
        format="binary",
    )


async def db_pool():
    logger.info("Creating database connection pool")

    pool = await asyncpg.create_pool(
        database=environ["DB_NAME"],
        user=environ["DB_USER"],
        password=environ["DB_PASS"],
        host=environ["DB_HOST"],
        init=init_connection,
    )

    if not pool:
        raise Exception("Failed to create database pool.")

    return pool
