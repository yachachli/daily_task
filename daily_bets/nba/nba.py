import asyncio
import os
import typing as t
from datetime import datetime, timedelta, timezone

import httpx
from httpx._types import QueryParamTypes

from daily_bets.db import DBPool
from daily_bets.logger import logger
from daily_bets.nba.models import (
    BetAnalysis,
    Game,
    NbaPlayer,
    NbaTeam,
    Outcome,
    SportEvent,
    bet_analysis_from_json,
    bet_analysis_to_tuple,
    build_team_fullname_map,
    game_from_json,
    load_nba_players_from_db,
    load_nba_teams_from_db,
)
from daily_bets.utils import batch_calls

T = t.TypeVar("T")
R = t.TypeVar("R")


MARKET_TO_STAT = {
    "player_assists": "assists",
    "player_assists_alternate": "assists",
    "player_points": "points",
    "player_points_alternate": "points",
    "player_points_assists": "points + assists",
    "player_points_assists_alternate": "points + assists",
    "player_rebounds": "rebounds",
    "player_rebounds_alternate": "rebounds",
    "player_points_rebounds": "points + rebounds",
    "player_points_rebounds_alternate": "points + rebounds",
    "player_points_rebounds_assists": "points + rebounds + assists",
    "player_points_rebounds_assists_alternate": "points + rebounds + assists",
    "player_rebounds_assists": "rebounds + assists",
    "player_rebounds_assists_alternate": "rebounds + assists",
    "player_threes": "threes",
    "player_threes_alternate": "threes",
    "player_blocks": "blocks",
    "player_blocks_alternate": "blocks",
    "player_steals_alternate": "steals",
    "player_steals": "steals",
    "player_turnovers": "turnovers",
    "player_turnovers_alternate": "turnovers",
}


async def analyze_bet(
    client: httpx.AsyncClient,
    outcome: Outcome,
    home_team_abv: str,
    away_team_abv: str,
    stat_type: str,
    nba_player_dict: dict[str, NbaPlayer],
    nba_teams_dict: dict[str, NbaTeam],
    analysis_cache: dict[tuple[str, float, float], BetAnalysis],
):
    player_name_raw = outcome.description
    normalized_name = player_name_raw.lower()

    player = nba_player_dict.get(normalized_name)
    if not player:
        logger.error(f"Player not found in DB: {player=}")
        raise ValueError(f"Player not found in DB: {player=}")

    line = outcome.point
    over_under = outcome.name
    price = outcome.price

    # Get the player's team abbreviation
    if player.team_id not in nba_teams_dict:
        logger.error(
            f"Team not found in DB: {player.team_id=}", extra={"player": player}
        )
        raise ValueError(f"Team not found in DB: {player.team_id=}")

    bet_key = (outcome.description, outcome.price, outcome.point)
    if (existing := analysis_cache.get(bet_key)) is not None:
        logger.info(f"Found existing bet with key {bet_key=}. Skipping analysis")
        return existing

    team_abv = player_team_abv = nba_teams_dict[player.team_id].abv
    if team_abv == home_team_abv:
        opponent_abv = away_team_abv
    elif team_abv == away_team_abv:
        opponent_abv = home_team_abv
    else:
        logger.error(
            f"team abv not found {player_team_abv=} {home_team_abv=} {away_team_abv=} {outcome=}"
        )
        raise ValueError(
            f"team abv not found {player_team_abv=} {home_team_abv=} {away_team_abv=} {outcome=}"
        )

    request_json: dict[str, t.Any] = {
        "player_id": player.id,
        "team_code": player_team_abv,
        "stat": stat_type,
        "line": line,
        "opponent": opponent_abv,
        "over_under": over_under.lower(),
    }

    logger.info(f"Calling backend for {player_name_raw}: {request_json}")

    apiUrl = os.environ["NBA_ANALYSIS_API_URL"]
    headers = {"Content-Type": "application/json"}
    r = await client.post(apiUrl, json=request_json, headers=headers)
    r.raise_for_status()

    response_data = r.json()
    logger.info(f"Backend success: {response_data=}")
    bet_analysis = bet_analysis_from_json(response_data)
    bet_analysis.price_val = price
    logger.info(f"Parse backend: {bet_analysis=}")

    analysis_cache[bet_key] = bet_analysis
    return bet_analysis


async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    i: int,
    team_fullname_map: dict[str, str],
    nba_player_dict: dict[str, NbaPlayer],
    nba_teams_dict: dict[str, NbaTeam],
):
    logger.info(f"Fetching bets for {event=} {i=}")

    logger.info(
        f"{i}. {event.sport_key} | {event.away_team} @ {event.home_team} | commence_time={event.commence_time} | event_id={event.id}"
    )

    # Convert home/away team names to abbreviations
    if event.away_team not in team_fullname_map.keys():
        logger.warning(f"Team not found {event.away_team}")
    if event.home_team not in team_fullname_map.keys():
        logger.warning(f"Team not found {event.home_team}")

    away_team_abv = team_fullname_map.get(event.away_team.lower(), "???")
    home_team_abv = team_fullname_map.get(event.home_team.lower(), "???")

    single_odds_url = f"https://api.the-odds-api.com/v4/sports/{event.sport_key}/events/{event.id}/odds"
    logger.info("{single_odds_url=}")
    odds_params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",  # or "us"
        "markets": ",".join(MARKET_TO_STAT.keys()),
        "oddsFormat": "decimal",
    }

    resp_odds = await client.get(single_odds_url, params=odds_params)
    resp_odds.raise_for_status()
    odds_data = resp_odds.json()

    game = game_from_json(odds_data)
    logger.info(f"{game=}")

    backend_results: list[BetAnalysis | Exception] = []

    # desc, price, point
    analysis_cache: dict[tuple[str, float, float], BetAnalysis] = {}

    def analyze_bet_inner(outcome: Outcome, stat: str):
        return analyze_bet(
            client,
            outcome,
            home_team_abv,
            away_team_abv,
            stat,
            nba_player_dict,
            nba_teams_dict,
            analysis_cache,
        )

    async with httpx.AsyncClient(timeout=30) as client:
        for bookmaker in game.bookmakers:
            for market in bookmaker.markets:
                logger.info(f"{market=}")
                stat_type = MARKET_TO_STAT.get(market.key)
                if stat_type is None:
                    logger.warning(f"Unknown market key {market.key=}")
                    continue
                outcomes = market.outcomes

                backend_results.extend(
                    await batch_calls(
                        map(lambda o: (o, stat_type), outcomes),
                        analyze_bet_inner,
                        100,
                    )
                )
    return backend_results, game


async def fetch_sport(
    client: httpx.AsyncClient, sport: str, url: str, params: QueryParamTypes
):
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    events_list = [SportEvent(**data) for data in resp.json()]

    if not events_list:
        logger.warning(f"No {sport} events found in this date range.")
    else:
        logger.info(f"Found {len(events_list)} {sport} events.")
        # Tag each event with the sport

    return events_list


async def run(pool: DBPool):
    logger.info("Starting NBA analysis")
    sport = "basketball_nba"
    # "americanfootball_nfl"

    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=2) - timedelta(seconds=1)

    async with httpx.AsyncClient(timeout=30) as client:
        get_events_url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
        params_events = {
            "apiKey": os.environ["API_KEY"],
            "commenceTimeFrom": day_start.isoformat().replace("+00:00", "Z"),
            "commenceTimeTo": day_end.isoformat().replace("+00:00", "Z"),
        }
        logger.info("fetching events")
        all_events = await fetch_sport(client, sport, get_events_url, params_events)
        for result in all_events:
            logger.info(f"{result=}")

    if not all_events:
        logger.error(
            f"No events at all for NBA between {day_start} and {day_end}. Exiting."
        )
        return
    else:
        logger.info(f"Got {len(all_events)} events")

    nba_player_dict, nba_teams_dict = await asyncio.gather(
        load_nba_players_from_db(pool), load_nba_teams_from_db(pool)
    )
    team_fullname_map = build_team_fullname_map(nba_teams_dict)

    all_games: list[Game] = []
    backend_results: list[BetAnalysis | Exception] = []

    logger.info(f"Now fetching single-event odds for {len(all_events)} events...")

    async with httpx.AsyncClient(timeout=30) as client:
        for i, event in enumerate(all_events, start=1):
            backend_results_batch, game = await fetch_game_bets(
                client, event, i, team_fullname_map, nba_player_dict, nba_teams_dict
            )
            backend_results.extend(backend_results_batch)
            all_games.append(game)

    # logger.info("--- All Single-Event Odds Data ---")
    logger.info(f"Got {len(all_games)} odds data events")

    # logger.info("--- Backend Results ---")
    logger.info(f"Got {len(backend_results)} analysis results")

    successes: list[BetAnalysis] = []
    for result in backend_results:
        if isinstance(result, Exception):
            logger.error(result)
        else:
            successes.append(result)

    async with pool.acquire() as conn:
        res = await conn.copy_records_to_table(
            "v2_nba_daily_bets",
            columns=["player_id", "team_id", "opponent_id", "stat", "line", "price", "analysis"],
            records=list(map(bet_analysis_to_tuple, successes)),
        )
        logger.info(res)
        #price
