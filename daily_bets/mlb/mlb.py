#!/usr/bin/env python3
import os
import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from asyncpg import Pool
from pydantic import BaseModel

from daily_bets.db import db_pool    # your asyncpg pool factory

# ───────── Configuration ─────────
API_KEY      = os.environ["API_KEY"]
SPORT_KEY    = "baseball_mlb"
REGION       = "us_dfs"
ANALYSIS_URL = os.environ["MLB_ANALYSIS_API_URL"]

MARKET_TO_STAT = {
    "batter_home_runs":      "home runs",
    "batter_hits":           "hits",
    "batter_rbis":           "rbi",
    "batter_hits_runs_rbis": "hits + rbi",
}

class BetAnalysisInput(BaseModel):
    player_id:    int
    team_code:    str
    stat:         str
    line:         float
    opponent_abv: str

async def fetch_tomorrow_events(client: httpx.AsyncClient):
    resp = await client.get(
        f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events",
        params={"apiKey": API_KEY}
    )
    resp.raise_for_status()
    return resp.json()

async def fetch_odds(client: httpx.AsyncClient, event_id: str):
    resp = await client.get(
        f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds",
        params={
            "apiKey":     API_KEY,
            "regions":    REGION,
            "markets":    ",".join(MARKET_TO_STAT.keys()),
            "oddsFormat": "decimal",
        }
    )
    resp.raise_for_status()
    return resp.json()

async def run(pool: Pool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    records = []

    async with httpx.AsyncClient(timeout=30) as client:
        events = await fetch_tomorrow_events(client)

        # single DB connection for name→ID lookups
        async with pool.acquire() as db_conn:
            for game in events:
                game_dt = datetime.fromisoformat(
                    game["commence_time"].replace("Z", "+00:00")
                ).date()
                if game_dt != tomorrow:
                    continue

                odds = await fetch_odds(client, game["id"])

                # **this was missing**: pick out Underdog
                ug = next(
                    (b for b in odds.get("bookmakers", [])
                     if b["title"].lower() == "underdog"),
                    None
                )
                if not ug:
                    continue

                tag = f"{game['away_team']}@{game['home_team']}"
                for m in ug["markets"]:
                    stat = MARKET_TO_STAT.get(m["key"])
                    if not stat:
                        continue

                    for o in m["outcomes"]:
                        # lookup the real numeric player_id
                        row = await db_conn.fetchrow(
                            "SELECT player_id FROM mlb_players WHERE long_name = $1",
                            o["description"]
                        )
                        if not row:
                            continue
                        pid = row["player_id"]

                        payload = BetAnalysisInput(
                            player_id    = pid,
                            team_code    = game["home_team"],
                            stat         = stat,
                            line         = o["point"],
                            opponent_abv = game["away_team"],
                        )
                        resp = await client.post(ANALYSIS_URL, json=payload.model_dump())
                        resp.raise_for_status()

                        commence_dt = datetime.fromisoformat(
                            game["commence_time"].replace("Z", "+00:00")
                        )

                        records.append((
                            resp.text,
                            o["price"],
                            commence_dt,
                            tag
                        ))
                        if len(records) >= 20:
                            break
                    if len(records) >= 20:
                        break
                if len(records) >= 20:
                    break

    if not records:
        print("No Underdog bets found for tomorrow.")
        return

    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            "v2_mlb_daily_bets",
            columns=["analysis","price","game_time","game_tag"],
            records=records
        )
    print(f"Inserted {len(records)} records into v2_mlb_daily_bets")

async def main():
    # load .env and kick off everything in one loop
    import dotenv
    dotenv.load_dotenv()
    pool = await db_pool()
    await run(pool)

if __name__ == "__main__":
    asyncio.run(main())
