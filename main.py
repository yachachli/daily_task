import asyncio

from daily_bets.db import db_pool

from dotenv import load_dotenv

load_dotenv()


async def main():
    # async with httpx.AsyncClient() as client:
    #     url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
    #     r = await client.get(url)
    #     print(r.text)
    pool = await db_pool()

    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            "v2_nba_daily_bets",
            columns=["player_id", "team_id", "opponent_id", "stat", "line", "analysis"],
            records=[
                (
                    99999,
                    99999,
                    99999,
                    "points",
                    99.99,
                    "{}",
                )
            ],
        )


if __name__ == "__main__":
    asyncio.run(main())
