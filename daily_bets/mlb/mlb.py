#!/usr/bin/env python3
import os
import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from asyncpg import Pool

from db import db_pool       # <-- your existing db.py
from pydantic import BaseModel

# ───────── Configuration ─────────
API_KEY        = os.environ["API_KEY"]
SPORT_KEY      = "baseball_mlb"
REGION         = "us_dfs"
ANALYSIS_URL   = os.environ["MLB_ANALYSIS_API_URL"]

# OddsAPI market → BetAnalysisInput.stat
MARKET_TO_STAT = {
    "batter_home_runs":      "home runs",
    "batter_hits":           "hits",
    "batter_rbis":           "rbi",
    "batter_hits_runs_rbis": "hits + rbi",
}

# ───────── Pydantic Input Model ─────────
class BetAnalysisInput(BaseModel):
    player_id:    int
    team_code:    str
    stat:         str
    line:         float
    opponent_abv: str

# ───────── Helpers ─────────
async def fetch_tomorrow_events(client: httpx.AsyncClient):
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events"
    resp = await client.get(url, params={"apiKey": API_KEY})
    resp.raise_for_status()
    return resp.json()

async def fetch_odds(client: httpx.AsyncClient, event_id: str):
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "apiKey":     API_KEY,
        "regions":    REGION,
        "markets":    ",".join(MARKET_TO_STAT.keys()),
        "oddsFormat": "decimal",
    }
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

# ───────── Main Runner ─────────
async def run(pool: Pool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    records: list[tuple[str,float,str,str]] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        events = await fetch_tomorrow_events(client)

        for game in events:
            game_dt = datetime.fromisoformat(game["commence_time"].replace("Z","+00:00")).date()
            if game_dt != tomorrow:
                continue

            odds = await fetch_odds(client, game["id"])
            ug = next((b for b in odds.get("bookmakers", []) if b["title"].lower()=="underdog"), None)
            if not ug:
                continue

            tag = f"{game['away_team']}@{game['home_team']}"
            for m in ug["markets"]:
                stat = MARKET_TO_STAT.get(m["key"])
                if not stat:
                    continue
                for o in m["outcomes"]:
                    # call your HTTP analysis endpoint
                    payload = BetAnalysisInput(
                        player_id    = int(o["description"]),  # or lookup if needed
                        team_code    = game["home_team"],
                        stat         = stat,
                        line         = o["point"],
                        opponent_abv = game["away_team"],
                    )
                    res = await client.post(ANALYSIS_URL, json=payload.model_dump())
                    res.raise_for_status()
                    analysis_json = res.text

                    records.append((
                        analysis_json,
                        o["price"],
                        game["commence_time"],
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

    # bulk‐insert via Neon pool
    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            "v2_mlb_daily_bets",
            columns=["analysis","price","game_time","game_tag"],
            records=records
        )
    print(f"Inserted {len(records)} records into v2_mlb_daily_bets")

# ───────── Entrypoint ─────────
if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    asyncio.run(
        run(
            asyncio.run(db_pool())
        )
    )
