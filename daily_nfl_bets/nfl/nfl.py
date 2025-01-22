import asyncio
import os
from datetime import datetime, timedelta, timezone
import typing as t

import httpx
from httpx._types import QueryParamTypes

from daily_bets.db import DBPool
from daily_bets.logger import logger
from daily_bets.utils import batch_calls  # your existing batch_calls function

from daily_nfl_bets.nfl.models import (
    SportEvent,
    NflPlayer,
    NflTeam,
    BetAnalysis,
    bet_analysis_from_json,
    # If you have a separate bet_analysis_to_tuple, you can remove it or adapt it.
)

# For mapping keys -> "stat" strings:
MARKET_TO_STAT = {
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


async def load_nfl_players_from_db(pool: DBPool) -> dict[str, NflPlayer]:
    """
    Returns a dict mapping LOWERCASE player_name -> NflPlayer
    from your v3_nfl_players table.
    """
    query = """
        SELECT 
            id AS id,         -- This is the primary key from your table
            team_id,          -- Possibly the 'code' or some ID for the player's team
            name
        FROM v3_nfl_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    players: dict[str, NflPlayer] = {}
    for row in rows:
        normalized = row["name"].strip().lower()
        players[normalized] = NflPlayer(
            # db_id=str(row["id"]),   # or just row["id"] if you prefer
            id=str(row["id"]),      # whichever ID you want to pass to the API
            team_id=str(row["team_id"]),
            name=row["name"],
        )
    return players


async def load_nfl_teams_from_db(pool: DBPool) -> dict[str, NflTeam]:
    """
    Returns a dict mapping team_id -> NflTeam
    from your v3_nfl_teams table.
    """
    query = """
        SELECT 
            id, 
            name, 
            team_code AS code
        FROM v3_nfl_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    teams: dict[str, NflTeam] = {}
    for row in rows:
        t_id = str(row["id"])
        teams[t_id] = NflTeam(
            id=t_id,
            name=row["name"],
            code=row["code"],
        )
    return teams


async def fetch_sport(
    client: httpx.AsyncClient, 
    sport: str, 
    url: str, 
    params: QueryParamTypes
) -> list[SportEvent]:
    """
    Fetch events from The Odds API. Return a list of SportEvent objects.
    """
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        logger.warning(f"No {sport} events found in this date range.")
        return []
    else:
        logger.info(f"Found {len(data)} {sport} events.")
        return [SportEvent(**ev) for ev in data]


async def analyze_bet(
    client: httpx.AsyncClient,
    player: NflPlayer,
    home_code: str,
    away_code: str,
    stat_type: str,
    line_val: float,
    over_under: str,
) -> BetAnalysis:
    """
    Calls your NFL analysis backend with the prepared request
    and returns a BetAnalysis object.
    """
    # Decide which team_code is opponent
    opponent_code = away_code if player.team_id == home_code else home_code

    request_json = {
        "player_id": player.id,    # Might be your 'player_id'
        "team_code": player.team_id,  
        "stat": stat_type,
        "line": line_val,
        "opponent": opponent_code,
        "over_under": over_under.lower(),
    }

    api_url = os.environ["NFL_ANALYSIS_API_URL"]
    headers = {"Content-Type": "application/json"}

    logger.info(f"Calling NFL backend for {player.name}: {request_json}")
    r = await client.post(api_url, json=request_json, headers=headers)
    r.raise_for_status()

    raw_data = r.json()
    logger.info(f"Backend success for {player.name}: {raw_data}")

    bet_analysis = bet_analysis_from_json(raw_data)
    return bet_analysis


async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    i: int,
    nfl_player_dict: dict[str, NflPlayer],
    nfl_teams_dict: dict[str, NflTeam],
) -> list[t.Union[BetAnalysis, Exception]]:
    """
    Fetch single-game odds from The Odds API,
    then analyze each player's bet with your NFL backend in batches of 10.
    """
    logger.info(
        f"[{i}] {event.sport_key} | {event.away_team} @ {event.home_team} | {event.commence_time} | {event.id}"
    )

    # Find home_code & away_code by matching event.home_team/event.away_team
    home_code = None
    away_code = None

    # This is naive: we search v3_nfl_teams by name ignoring case
    # If your table stores "Philadelphia Eagles" in the name col, it will match
    for t_id, team in nfl_teams_dict.items():
        if team.name.lower() == event.home_team.lower():
            home_code = team.code
        if team.name.lower() == event.away_team.lower():
            away_code = team.code

    if not home_code or not away_code:
        logger.warning(
            f"Could not find codes for home={event.home_team} or away={event.away_team}"
        )
        return []

    # Fetch single-event odds:
    single_odds_url = f"https://api.the-odds-api.com/v4/sports/{event.sport_key}/events/{event.id}/odds"
    odds_params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",
        "markets": ",".join(MARKET_TO_STAT.keys()),
        "oddsFormat": "decimal",
    }

    odds_resp = await client.get(single_odds_url, params=odds_params)
    odds_resp.raise_for_status()
    game_data = odds_resp.json()

    if "bookmakers" not in game_data:
        logger.warning(f"No bookmakers found for event {event.id}")
        return []

    # We'll store the tasks input for batch_calls
    tasks_input: list[tuple[str, float, float, str, str]] = []

    # We'll define a closure that references home_code, away_code, etc.
    async def analyze_outcome(
        player_name_raw: str,
        line_val: float,
        price_val: float,
        over_under_str: str,
        stat_type_str: str
    ) -> BetAnalysis:
        normalized_name = player_name_raw.lower()
        player = nfl_player_dict.get(normalized_name)
        if not player:
            msg = f"Player not found in DB: {player_name_raw}"
            logger.warning(msg)
            raise ValueError(msg)

        return await analyze_bet(
            client,
            player,
            home_code,
            away_code,
            stat_type_str,
            line_val,
            over_under_str,
        )

    # Extract each outcome
    for bookmaker in game_data.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            stat_type_str = MARKET_TO_STAT.get(market["key"])
            if stat_type_str is None:
                logger.warning(f"Unknown market key: {market['key']}")
                continue

            for outcome in market.get("outcomes", []):
                player_name_raw = outcome["description"]   # ex: "Jalen Hurts"
                line_val = outcome["point"]                # ex: 230.5
                price_val = outcome["price"]               # ex: 1.87
                over_under_str = outcome["name"]           # ex: "Over"
                tasks_input.append(
                    (player_name_raw, line_val, price_val, over_under_str, stat_type_str)
                )

    if not tasks_input:
        logger.info(f"No outcomes to analyze for event {event.id}")
        return []

    # Now batch_calls with batch_size=10
    return await batch_calls(tasks_input, analyze_outcome, batch_size=200)


async def run(pool: DBPool):
    logger.info("Starting NFL analysis")

    # We'll go 7 days out
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=7) - timedelta(seconds=1)

    # The odds API endpoint for NFL
    events_url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events"
    params_events = {
        "apiKey": os.environ["API_KEY"],
        "commenceTimeFrom": day_start.isoformat().replace("+00:00", "Z"),
        "commenceTimeTo": day_end.isoformat().replace("+00:00", "Z"),
    }

    # We'll fetch all events
    async with httpx.AsyncClient(timeout=30) as client:
        events = await fetch_sport(client, "americanfootball_nfl", events_url, params_events)

        if not events:
            logger.error(f"No events for NFL in {day_start} to {day_end}")
            return

        # Load players and teams from DB
        nfl_player_dict, nfl_teams_dict = await asyncio.gather(
            load_nfl_players_from_db(pool),
            load_nfl_teams_from_db(pool),
        )

        all_analysis: list[t.Union[BetAnalysis, Exception]] = []

        # For each event, gather results from fetch_game_bets
        for i, event in enumerate(events, start=1):
            results = await fetch_game_bets(
                client, event, i, nfl_player_dict, nfl_teams_dict
            )
            if results:
                all_analysis.extend(results)

        # Separate successes from exceptions
        successes = [r for r in all_analysis if not isinstance(r, Exception)]
        logger.info(f"Finished analyzing NFL. Good results = {len(successes)} / {len(all_analysis)} total")

    # Now insert them into `nfl_daily_bets`:
    # columns = [player_id, team_code, stat, line, opponent, bet_grade]
    # We store the entire JSON in `bet_grade`.

    async with pool.acquire() as conn:
        # 1) Build the list of records (the tuples).
        records: list[tuple[str, str, str, str, float, str]] = []

        for bet_obj in successes:
            # bet_obj is a BetAnalysis
            import json
            
            # Convert these fields as needed:
            player_id = str(bet_obj.player_team_info.team_id)
            team_code = bet_obj.player_team_info.team_code
            opponent_code = bet_obj.defense_data.team_code
            stat = bet_obj.stat_type
            line_val = bet_obj.threshold  # or bet_obj.bet_number
            # Full JSON in bet_grade
            bet_grade_json = json.dumps(bet_obj, default=lambda x: x.__dict__)

            # Append the tuple in the order matching your columns
            records.append(
                (player_id, team_code, opponent_code, stat, line_val, bet_grade_json)
            )

        # 2) Now we have `records`, so we can call `copy_records_to_table`.
        copy_res = await conn.copy_records_to_table(
            "nfl_daily_bets",
            columns=["player_id", "team_code", "opponent", "stat", "line", "bet_grade"],
            records=records,
        )
        logger.info(copy_res)
        logger.info(f"Inserted {len(records)} rows into nfl_daily_bets.")

