import os
import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from asyncpg import Pool
from pydantic import BaseModel

from daily_bets.db import db_pool  

# ───────── Configuration ─────────
API_KEY      = os.environ["API_KEY"]
SPORT_KEY    = "baseball_mlb"
REGION       = "us_dfs"
ANALYSIS_URL = os.environ["MLB_ANALYSIS_API_URL"]

# ⇣ map Underdog “market_key” → friendly stat string
MARKET_TO_STAT = {
    "batter_home_runs":              "home runs",
    "batter_hits":                   "hits",
    "batter_rbis":                   "rbi",
    "batter_hits_runs_rbis":         "hits + rbi",
    "batter_total_bases":            "total bases",
    "batter_runs_scored":            "runs",
    "batter_singles":                "singles",
    "batter_doubles":                "doubles",
    "batter_triples":                "triples",
    "batter_total_bases_alternate":  "total bases",
    "batter_home_runs_alternate":    "home runs",
    "batter_hits_alternate":         "hits",
    "batter_rbis_alternate":         "rbi",
}

MAX_BETS_PER_GAME = 100           # ← overall cap per matchup

class BetAnalysisInput(BaseModel):
    player_id:    int
    team_code:    str
    stat:         str
    line:         float
    opponent_abv: str


# ───────── API helpers ─────────
async def fetch_tomorrow_events(client: httpx.AsyncClient):
    r = await client.get(
        f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events",
        params={"apiKey": API_KEY},
    )
    r.raise_for_status()
    return r.json()


async def fetch_odds(client: httpx.AsyncClient, event_id: str):
    r = await client.get(
        f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds",
        params={
            "apiKey":     API_KEY,
            "regions":    REGION,
            "markets":    ",".join(MARKET_TO_STAT.keys()),
            "oddsFormat": "decimal",
        },
    )
    r.raise_for_status()
    return r.json()


# ───────── Main run loop ─────────
async def run(pool: Pool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    records: list[tuple[str, float, datetime, str]] = []

    async with httpx.AsyncClient(timeout=30) as client:
        events = await fetch_tomorrow_events(client)

        # single DB connection for player-ID lookups
        async with pool.acquire() as db_conn:
            for game in events:
                game_dt = datetime.fromisoformat(
                    game["commence_time"].replace("Z", "+00:00")
                ).date()
                if game_dt != tomorrow:
                    continue

                odds = await fetch_odds(client, game["id"])

                # pick out Underdog bookmaker
                ug = next(
                    (
                        b
                        for b in odds.get("bookmakers", [])
                        if b["title"].lower() == "underdog"
                    ),
                    None,
                )
                if not ug:
                    continue

                tag             = f"{game['away_team']}@{game['home_team']}"
                bets_this_game  = 0
                extra_queue: list[tuple[dict, str]] = []   # (outcome, stat)

                # ── 1️⃣  mandatory sweep: one per market ──────────────────
                for m in ug["markets"]:
                    stat = MARKET_TO_STAT.get(m["key"])
                    if not stat:
                        continue

                    outcomes = m.get("outcomes", [])
                    if not outcomes:       # market returned empty – skip
                        continue

                    first_outcome, *rest = outcomes
                    if await append_bet(
                        first_outcome,
                        stat,
                        db_conn,
                        client,
                        game,
                        tag,
                        records,
                    ):
                        bets_this_game += 1

                    # stash remaining outcomes for the top-up phase
                    extra_queue.extend((o, stat) for o in rest)

                # ── 2️⃣  top-up until 100 bets ───────────────────────────
                for o, stat in extra_queue:
                    if bets_this_game >= MAX_BETS_PER_GAME:
                        break
                    if await append_bet(
                        o,
                        stat,
                        db_conn,
                        client,
                        game,
                        tag,
                        records,
                    ):
                        bets_this_game += 1

                # …then proceed to next game (loop continues)

    if not records:
        print("No Underdog bets found for tomorrow.")
        return

    # bulk-insert
    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            "v2_mlb_daily_bets",
            columns=["analysis", "price", "game_time", "game_tag"],
            records=records,
        )
    print(f"Inserted {len(records)} records into v2_mlb_daily_bets")


# ───────── helper that does: DB lookup → backend call → append ─────────
async def append_bet(
    outcome: dict,
    stat: str,
    db_conn,
    client,
    game: dict,
    tag: str,
    records: list,
) -> bool:
    """Return True if the bet was successfully analysed & added."""
    row = await db_conn.fetchrow(
        "SELECT player_id FROM mlb_players WHERE long_name = $1",
        outcome["description"],
    )
    if not row:
        return False

    payload = BetAnalysisInput(
        player_id=row["player_id"],
        team_code=game["home_team"],
        stat=stat,
        line=outcome["point"],
        opponent_abv=game["away_team"],
    )
    resp = await client.post(ANALYSIS_URL, json=payload.model_dump())
    resp.raise_for_status()

    commence_dt = datetime.fromisoformat(game["commence_time"].replace("Z", "+00:00"))
    records.append((resp.text, outcome["price"], commence_dt, tag))
    return True


# ───────── entry-point ─────────
async def main():
    import dotenv

    dotenv.load_dotenv()
    pool = await db_pool()
    await run(pool)

if __name__ == "__main__":
    asyncio.run(main())
