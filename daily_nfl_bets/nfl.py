import asyncio
import os
import json
import httpx
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Union, Iterable, Callable, Awaitable
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
    """
    Returns a dict mapping LOWERCASE player_name -> NflPlayer
    from your v3_nfl_players table.
    """
    query = """
        SELECT id AS id, name, team_id
        FROM v3_nfl_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    players = {}
    for row in rows:
        # Store by lowercased name
        normalized = row["name"].strip().lower()
        players[normalized] = NflPlayer(
            id=row["id"], 
            name=row["name"], 
            team_id=row["team_id"]
        )
    return players


async def load_nfl_teams_from_db(pool: DBPool) -> Dict[str, NflTeam]:
    """
    Returns a dict mapping team_id -> NflTeam
    from your v3_nfl_teams table.
    """
    query = """
        SELECT id, name, team_code AS code
        FROM v3_nfl_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    teams = {}
    for row in rows:
        t_id = row["id"]
        teams[t_id] = NflTeam(
            id=row["id"],
            name=row["name"],
            code=row["code"],
        )
    return teams


async def fetch_player_by_name(pool: DBPool, player_name: str) -> Union[NflPlayer, None]:
    """
    Fetch player information from the database using the player's name, ignoring case.
    """
    query = """
        SELECT id, name, team_id
        FROM v3_nfl_players
        WHERE LOWER(name) = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, player_name)
        if row:
            return NflPlayer(id=row["id"], name=row["name"], team_id=row["team_id"])
    return None


async def analyze_bet(
    pool: DBPool,
    client: httpx.AsyncClient,
    bet: Dict[str, Any],
    api_url: str
) -> Dict[str, Any]:
    """
    Calls the NFL Analysis API with the bet request data and returns the entire JSON response.
    """
    request_json = {
        "player_id": bet["player_id"],
        "team_code": bet["team_code"],
        "stat": bet["stat"],
        "line": bet["line"],
        "opponent": bet["opponent"],
    }

    logger.info(f"Calling backend for bet analysis: {request_json}")

    headers = {"Content-Type": "application/json"}
    response = await client.post(api_url, json=request_json, headers=headers)
    response.raise_for_status()

    response_data = response.json()
    logger.info(f"Backend success for {request_json['player_id']}: {response_data}")
    return response_data


async def save_bet(
    pool: DBPool, 
    bet: Dict[str, Any], 
    response_data: Dict[str, Any]
):
    """
    Inserts a row into nfl_daily_bets with the entire response_data in bet_grade.
    """
    query = """
        INSERT INTO nfl_daily_bets (
            player_id, 
            team_code, 
            stat, 
            line, 
            opponent, 
            bet_grade, 
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
    """
    # We store the entire JSON as text in bet_grade:
    bet_grade_str = json.dumps(response_data)

    async with pool.acquire() as conn:
        await conn.execute(
            query,
            str(bet["player_id"]),
            bet["team_code"],
            bet["stat"],
            bet["line"],
            bet["opponent"],
            bet_grade_str,
        )

    logger.info(f"Saved bet. player_id={bet['player_id']} stat={bet['stat']} line={bet['line']}")


async def analyze_and_save_bet(
    pool: DBPool,
    bet: Dict[str, Any],
    client: httpx.AsyncClient,
    api_url: str
):
    """
    Wrapper to call analyze_bet and then save_bet.
    """
    try:
        response_data = await analyze_bet(pool, client, bet, api_url)
        await save_bet(pool, bet, response_data)
    except Exception as e:
        logger.error(f"Failed to analyze and save bet: {bet} => {e}")


async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    nfl_players: Dict[str, NflPlayer],
    nfl_teams: Dict[str, NflTeam],
    pool: DBPool,
    api_url: str,
):
    """
    For a given NFL game, fetch the player-level markets from the Odds API,
    map them to players in DB, call the NFL Analysis API, and store results.
    """
    logger.info(f"Fetching bets for game: {event}")

    home_team = event.home_team.strip().lower()
    away_team = event.away_team.strip().lower()

    # Find the team_codes for the home & away teams
    home_team_code = next((t.code for t in nfl_teams.values() if t.name.lower() == home_team), None)
    away_team_code = next((t.code for t in nfl_teams.values() if t.name.lower() == away_team), None)

    if not home_team_code or not away_team_code:
        logger.warning(f"Missing team codes for {home_team=} or {away_team=}")
        return

    # Fetch odds for this event
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

    tasks = []

    for bookmaker in game_data.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            stat_type = MARKET_TO_STAT.get(market["key"])
            if not stat_type:
                continue

            for outcome in market.get("outcomes", []):
                player_name = outcome["description"].strip().lower()
                # Lookup the player from DB dictionary
                player = nfl_players.get(player_name)
                if not player:
                    logger.warning(f"Player not found in DB: {player_name}")
                    continue

                bet_info = {
                    "player_id": player.id,
                    "team_code": home_team_code if player.team_id == home_team_code else away_team_code,
                    "stat": stat_type,
                    "line": outcome["point"],
                    "opponent": away_team_code if player.team_id == home_team_code else home_team_code,
                }

                # We'll create a task for concurrency
                tasks.append(analyze_and_save_bet(pool, bet_info, client, api_url))

    # Concurrency: gather all tasks for this event
    if tasks:
        # batch them if you like, e.g. groups of 10:
        # results = []
        # for chunk in batched(tasks, 10):
        #     results.extend(await asyncio.gather(*chunk, return_exceptions=True))
        # else:
        await asyncio.gather(*tasks, return_exceptions=True)


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
        # Fetch upcoming NFL events
        resp = await client.get(events_url, params=params)
        resp.raise_for_status()
        events_data = resp.json()

        if not events_data:
            logger.info("No NFL events found")
            return

        # Convert event dicts to SportEvent objects
        events = [SportEvent(**ev) for ev in events_data]

        # Load DB dictionaries
        nfl_players, nfl_teams = await asyncio.gather(
            load_nfl_players_from_db(pool), 
            load_nfl_teams_from_db(pool)
        )

        # For each event, fetch bets concurrently
        api_url = os.environ["NFL_ANALYSIS_API_URL"]
        tasks = []
        for event in events:
            tasks.append(fetch_game_bets(client, event, nfl_players, nfl_teams, pool, api_url))

        # Gather all event-level tasks in parallel if desired
        await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("NFL analysis complete.")
