import asyncio
import os
import json
import httpx
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Union
from itertools import batched

from daily_nfl_bets.db import DBPool
from daily_nfl_bets.logger import logger

@dataclass
class SportEvent:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str
# comment for checking pr

@dataclass
class NflPlayer:
    id: str
    name: str
    team_id: str


@dataclass
class NflTeam:
    id: str
    name: str
    code: str


@dataclass
class Outcome:
    name: str
    description: str
    price: float
    point: float


@dataclass
class Market:
    key: str
    last_update: str
    outcomes: List[Outcome]


@dataclass
class Bookmaker:
    key: str
    title: str
    markets: List[Market]


@dataclass
class Game:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str
    bookmakers: List[Bookmaker]


MARKET_TO_STAT: Dict[str, str] = {
    "player_field_goals": "field goals",
    "player_kicking_points": "kicking points",
    "player_pass_attempts": "pass attempts",
    "player_pass_attempts_alternate": "pass attempts",
    "player_pass_interceptions": "pass ints",
    "player_pass_tds": "pass tds",
    "player_pass_tds_alternate": "pass tds",
    "player_pass_yds": "pass yards",
    "player_pass_yds_alternate": "pass yards",
    "player_pats": "extra points",
    "player_reception_yds": "rec yards",
    "player_receptions": "receptions",
    "player_rush_attempts": "rush carries",
    "player_rush_reception_yds": "rush + rec yards",
    "player_rush_yds": "rush yards",
    "player_sacks": "sacks",
    "player_tds_over": "pass + rush + rec tds",
    "player_anytime_td": "pass + rush + rec tds",
    "player_reception_yds_alternate": "rec yards",
    "player_rush_reception_yds_alternate": "rush + rec yards",
    "player_rush_reception_tds_alternate": "rush + rec tds",
    "player_rush_reception_tds": "rush + rec tds",
    "player_pass_interceptions_alternate": "pass ints",
    "player_pats_alternate": "extra points",
    "player_rush_attempts_alternate": "rush carries",
    "player_rush_yds_alternate": "rush yards",
}


async def load_nfl_players_from_db(pool: DBPool) -> Dict[str, NflPlayer]:
    query = """
        SELECT id AS id, name, team_id
        FROM v3_nfl_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
        return {
            row["name"].strip().lower(): NflPlayer(
                id=row["id"], name=row["name"], team_id=row["team_id"]
            )
            for row in rows
        }


async def load_nfl_teams_from_db(pool: DBPool) -> Dict[str, NflTeam]:
    query = """
        SELECT id, name, team_code AS code
        FROM v3_nfl_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
        return {row["id"]: NflTeam(id=row["id"], name=row["name"], code=row["code"]) for row in rows}


async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    nfl_players: Dict[str, NflPlayer],
    nfl_teams: Dict[str, NflTeam],
):
    logger.info(f"Fetching bets for game: {event}")

    # Determine team codes
    home_team = event.home_team.strip().lower()
    away_team = event.away_team.strip().lower()

    home_team_code = next((t.code for t in nfl_teams.values() if t.name.lower() == home_team), None)
    away_team_code = next((t.code for t in nfl_teams.values() if t.name.lower() == away_team), None)

    if not home_team_code or not away_team_code:
        logger.warning(f"Missing team codes for {home_team=} or {away_team=}")
        return []

    # Fetch odds for the game
    odds_url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/{event.id}/odds"
    params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",
        "markets": ",".join(MARKET_TO_STAT.keys()),
        "oddsFormat": "decimal",
    }

    resp = await client.get(odds_url, params=params)
    resp.raise_for_status()
    game_data = resp.json()

    results = []

    for bookmaker in game_data.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            stat_type = MARKET_TO_STAT.get(market["key"])
            if not stat_type:
                continue

            for outcome in market["outcomes"]:
                player_name = outcome["description"].strip().lower()
                player = nfl_players.get(player_name)

                if not player:
                    logger.warning(f"Player not found: {player_name}")
                    continue

                result = {
                    "player_id": player.id,
                    "team_code": home_team_code if player.team_id == home_team_code else away_team_code,
                    "stat": stat_type,
                    "line": outcome["point"],
                    "opponent": away_team_code if player.team_id == home_team_code else home_team_code,
                }
                results.append(result)

    return results


async def run(pool: DBPool):
    logger.info("Starting NFL analysis")

    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=7) - timedelta(seconds=1)

    events_url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events"
    params = {
        "apiKey": os.environ["API_KEY"],
        "commenceTimeFrom": day_start.isoformat().replace("+00:00", "Z"),
        "commenceTimeTo": day_end.isoformat().replace("+00:00", "Z"),
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(events_url, params=params)
        resp.raise_for_status()
        events = [SportEvent(**ev) for ev in resp.json()]

        if not events:
            logger.info("No NFL events found")
            return

        nfl_players, nfl_teams = await asyncio.gather(
            load_nfl_players_from_db(pool), load_nfl_teams_from_db(pool)
        )

        for event in events:
            game_results = await fetch_game_bets(client, event, nfl_players, nfl_teams)

            for bet in game_results:
                logger.info(f"Bet generated: {bet}")
                await save_bet(pool, bet)


async def save_bet(pool: DBPool, bet: Dict[str, Any]):
    query = """
        INSERT INTO nfl_daily_bets (player_id, team_code, stat, line, opponent, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
    """
    async with pool.acquire() as conn:
        await conn.execute(
            query,
            bet["player_id"],
            bet["team_code"],
            bet["stat"],
            bet["line"],
            bet["opponent"],
        )
