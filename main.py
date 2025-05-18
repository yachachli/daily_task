import asyncio

# from daily_bets.db import db_pool
from dotenv import load_dotenv

load_dotenv()


async def main():
    ...
    # pool = await db_pool()
    #
    # async with pool.acquire() as conn:
    #     res = await conn.copy_records_to_table(
    #         "v2_nba_daily_bets",
    #         columns=[
    #             "analysis",
    #             "price",
    #         ],
    #         records=[("{}", 9999)],
    #     )


if __name__ == "__main__":
    asyncio.run(main())
