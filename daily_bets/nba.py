import asyncio
import json
import logging
import pprint
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx

from daily_bets.db import db_connect


async def load_nba_players_from_db():
    """
    Load data from the 'nba_players' table.
    We'll create a dictionary keyed by the LOWERCASE player_name -> row info.
    Columns in 'nba_players': id, name, position, team_id, player_pic, player_id
    """
    conn = await db_connect()

    query = """
        SELECT id, name, position, team_id, player_pic, player_id
        FROM nba_players
    """
    rows = await conn.fetch(query)

    player_dict = {}
    for row in rows:
        db_id, name, position, team_id, player_pic, external_player_id = tuple(row)
        normalized_name = name.strip().lower()
        player_dict[normalized_name] = {
            "db_id": db_id,
            "player_id": str(external_player_id).strip() if external_player_id else "",
            "team_id": team_id,
            "full_name": name.strip(),
            "position": position.strip() if position else "",
            "player_pic": player_pic.strip() if player_pic else "",
        }

    return player_dict


async def load_nba_teams_from_db():
    """
    Load data from the 'nba_teams' table.
    Suppose it has columns: id, name, team_city, team_abv, conference

    We'll create a dict keyed by the numeric ID (the 'id' column),
    storing basic info including the abbreviation.
    """
    conn = await db_connect()

    query = """
        SELECT id, name, team_city, team_abv, conference
        FROM nba_teams
    """
    rows = await conn.fetch(query)

    teams_dict = {}
    for row in rows:
        t_id, t_name, t_city, t_abv, conf = tuple(row)
        teams_dict[t_id] = {
            "name": t_name.strip(),
            "team_city": t_city.strip(),
            "team_abv": t_abv.strip().upper(),
            "conference": conf.strip() if conf else "",
        }

    return teams_dict


def build_team_fullname_map(teams_dict):
    """
    Creates a lookup dict that maps the full name (city + ' ' + name) in lowercase
    to the team's abbreviation. For example, 'charlotte hornets' -> 'CHA'.
    """
    full_map = {}
    for t_id, team_info in teams_dict.items():
        city = team_info["team_city"]
        name = team_info["name"]
        abv = team_info["team_abv"]
        full_team_str = f"{city} {name}".strip().lower()
        full_map[full_team_str] = abv
    return full_map


async def analyze_bet(
    client: httpx.AsyncClient,
    outcome: dict,
    home_team_abv: str,
    away_team_abv: str,
    nba_player_dict: dict[str, dict],
    nba_teams_dict: dict[str, dict],
):
    player_name_raw = outcome.get("description", "").strip()
    normalized_name = player_name_raw.lower()

    player_info = nba_player_dict.get(normalized_name)
    if not player_info:
        logging.info(f"  - Player not found in DB: {player_name_raw}")
        return {"error": f"Player not found in DB: {player_name_raw}"}

    line = outcome.get("point", 0.0)
    over_under = outcome.get("name", "Over")  # "Over" or "Under"

    # Get the player's team abbreviation
    team_id = player_info.get("team_id")
    if team_id and team_id in nba_teams_dict:
        player_team_abv = nba_teams_dict[team_id]["team_abv"]
    else:
        logging.warning(f"team abv not found {team_id=} {outcome=}")
        player_team_abv = "???"

    # Determine Opponent
    if player_team_abv == home_team_abv:
        opponent_abv = away_team_abv
    elif player_team_abv == away_team_abv:
        opponent_abv = home_team_abv
    else:
        logging.warning(
            f"team abv not found {player_team_abv=} {home_team_abv=} {away_team_abv=} {outcome=}"
        )
        opponent_abv = "???"

    request_json = {
        "player_id": player_info["player_id"],
        "team_code": player_team_abv,
        "stat": "points",
        "line": line,
        "opponent": opponent_abv,
        "over_under": over_under.lower(),  # "over" or "under"
    }

    logging.info(f"  -> Calling backend for {player_name_raw}: {request_json}")
    try:
        apiUrl = "https://analyze-nba-player-over-under-vilhfa3ama-uc.a.run.app"
        headers = {"Content-Type": "application/json"}
        r = await client.post(apiUrl, json=request_json, headers=headers)

        if r.status_code == 200:
            responseData = r.json()
            logging.info(f"    Backend success: {responseData}")
            return responseData
        else:
            logging.warning(f"    Backend error {r.status_code}: {r.text}")
            return {"error": r.text}
    except Exception as e:
        logging.error(
            f"    Exception calling backend: {e=}, {json.dumps(request_json)}"
        )
        return {"exception": str(e)}

async def fetch_game_bets(client: httpx.AsyncClient,
 event: dict,
 i: int,
 team_fullname_map,
 nba_player_dict,
 nba_teams_dict):
    logging.info(f"hello?? {event=} {i=}")
    event_id = event["id"]
    away_team_str = event["away_team"]     # e.g. "Charlotte Hornets"
    home_team_str = event["home_team"]     # e.g. "Chicago Bulls"
    commence_time = event["commence_time"]
    sport_key = event["sport_key"]

    logging.info(f"{i}. {sport_key} | {away_team_str} @ {home_team_str} | commence_time={commence_time} | event_id={event_id}")

    # Convert home/away team names to abbreviations
    if away_team_str not in team_fullname_map.keys():
        logging.warning(f"Team not found {away_team_str}")
    if home_team_str not in team_fullname_map.keys():
        logging.warning(f"Team not found {home_team_str}")

    away_team_abv = team_fullname_map.get(away_team_str.lower(), "???")
    home_team_abv = team_fullname_map.get(home_team_str.lower(), "???")

    single_odds_url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    odds_params = {
        "apiKey": API_KEY,
        "regions": "us_dfs",  # or "us"
        "markets": "player_points",
        "oddsFormat": "decimal"
    }

    resp_odds = await client.get(single_odds_url, params=odds_params)
    resp_odds.raise_for_status()
    odds_data = resp_odds.json()

    backend_results = []
    batch = []

    if "bookmakers" not in odds_data:
        logging.warning(f"No bookmakers data found for {sport_key=} {event_id=}")
    async with httpx.AsyncClient(timeout=30) as client:
        for bookmaker in odds_data["bookmakers"]:
            markets = bookmaker.get("markets", [])
            for market_obj in markets:
                if market_obj["key"] != "player_points":
                    continue
                outcomes = market_obj.get("outcomes", [])

                for outcome in outcomes:
                    batch.append(analyze_bet(client, outcome, home_team_abv, away_team_abv, nba_player_dict, nba_teams_dict))
                    if len(batch) >= 10:
                        backend_results.extend(await asyncio.gather(*batch))
                        batch = []
                if len(batch) >= 0:
                    backend_results.extend(await asyncio.gather(*batch))
                    batch = []
    return backend_results, odds_data


async def run(): ...
