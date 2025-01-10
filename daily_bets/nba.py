import asyncio
import json
import os

import httpx
import typing as t
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from httpx._types import QueryParamTypes
from itertools import batched

from daily_bets.db import DBPool
from daily_bets.logger import logger

T = t.TypeVar("T")
R = t.TypeVar("R")


class DataclassEncoder(json.JSONEncoder):
    def default(self, o: t.Any):
        if hasattr(o, "__dataclass_fields__"):  # checks if the object is a dataclass
            return asdict(o)  # convert dataclass to dict
        return super().default(o)


def json_dumps_dataclass(obj: t.Any):
    return json.dumps(obj, cls=DataclassEncoder, indent=4)


@dataclass
class SportEvent:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str


@dataclass
class NbaPlayer:
    db_id: str
    id: str
    team_id: str
    name: str
    position: str
    player_pic: str


@dataclass
class NbaTeam:
    id: str
    name: str
    city: str
    abv: str
    conference: str


@dataclass
class PlayerTeamInfo:
    Team_ID: int
    Team_Name: str
    Team_City: str
    Team_Abv: str
    Conference: str
    ppg: float
    oppg: float
    wins: int
    loss: int
    division: str
    team_bpg: float
    team_spg: float
    team_apg: float
    team_fga: float
    team_fgm: float
    team_fta: float
    team_tov: float
    pace: float
    def_rtg: float


@dataclass
class PlayerData:
    ID: int
    Player_ID: int
    Season_ID: int
    Games_Played: int
    Points_Per_Game: float
    Rebounds_Per_Game: float
    Assists_Per_Game: float
    Steals_Per_Game: float
    Blocks_Per_Game: float
    Turnovers_Per_Game: float
    Field_Goal_Percentage: float
    Three_Point_Percentage: float
    Free_Throw_Percentage: float
    Minutes_Per_Game: float
    Offensive_Rebounds_Per_Game: float
    Defensive_Rebounds_Per_Game: float
    Field_Goals_Made_Per_Game: float
    Field_Goals_Attempted_Per_Game: float
    Three_Pointers_Made_Per_Game: float
    Three_Pointers_Attempted_Per_Game: float
    Free_Throws_Made_Per_Game: float
    Free_Throws_Attempted_Per_Game: float


@dataclass
class OpponentStats:
    Team_ID: int
    Team_Name: str
    Team_City: str
    Team_Abv: str
    Conference: str
    ppg: float
    oppg: float
    wins: int
    loss: int
    division: str
    team_bpg: float
    team_spg: float
    team_apg: float
    team_fga: float
    team_fgm: float
    team_fta: float
    team_tov: float
    pace: float
    def_rtg: float


@dataclass
class GraphData:
    label: str
    value: float


@dataclass
class Graph:
    version: int
    data: list[GraphData]
    title: str
    threshold: float


@dataclass
class BetAnalysis:
    player_name: str
    player_position: str
    player_team_info: PlayerTeamInfo
    stat_type: str
    over_under: str
    bet_grade: str
    hit_rate: str
    original_bet_query: str
    threshold: float
    short_answer: str
    long_answer: str
    insights: list[str]
    user_prompt: str
    bet_number: float
    player_data: PlayerData
    opponent_stats: OpponentStats
    over_under_analysis: str
    bet_recommendation: str
    graphs: list[Graph]
    error: bool


def bet_analysis_from_json(data: dict[str, t.Any]) -> BetAnalysis:
    # Unpack the data directly for nested classes
    player_team_info = PlayerTeamInfo(**data["player_team_info"])

    player_data = PlayerData(**data["player_data"])
    opponent_stats = OpponentStats(**data["opponent_stats"])

    # Convert graphs
    graphs = [
        Graph(
            version=graph_data["version"],
            data=[GraphData(**g) for g in graph_data["data"]],
            title=graph_data["title"],
            threshold=graph_data["threshold"],
        )
        for graph_data in data["graphs"]
    ]

    # Return BetInfo object by unpacking top-level data
    return BetAnalysis(
        **{
            key: value
            for key, value in data.items()
            if key
            not in ["player_team_info", "player_data", "opponent_stats", "graphs"]
        },
        player_team_info=player_team_info,
        player_data=player_data,
        opponent_stats=opponent_stats,
        graphs=graphs,
    )


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
    outcomes: list[Outcome]


@dataclass
class Bookmaker:
    key: str
    title: str
    markets: list[Market]


@dataclass
class Game:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str
    bookmakers: list[Bookmaker]


def game_from_json(game_data: dict[str, t.Any]) -> Game:
    """Converts json into an `Outcome`."""

    # Convert the bookmakers section to Bookmaker instances
    bookmakers = [
        Bookmaker(
            key=bookmaker["key"],
            title=bookmaker["title"],
            markets=[
                Market(
                    key=market["key"],
                    last_update=market["last_update"],
                    outcomes=[Outcome(**outcome) for outcome in market["outcomes"]],
                )
                for market in bookmaker["markets"]
            ],
        )
        for bookmaker in game_data["bookmakers"]
    ]

    # Create and return the Game dataclass
    return Game(
        id=game_data["id"],
        sport_key=game_data["sport_key"],
        sport_title=game_data["sport_title"],
        commence_time=game_data["commence_time"],
        home_team=game_data["home_team"],
        away_team=game_data["away_team"],
        bookmakers=bookmakers,
    )


async def load_nba_players_from_db(pool: DBPool):
    """Returns a dict mapping LOWERCASE `player_name` to `NbaPlayer`."""
    query = """
        SELECT id as db_id, player_id as id, team_id, name, position, player_pic
        FROM nba_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        player_dict: dict[str, NbaPlayer] = {}
        for row in rows:
            row = dict(row)
            normalized_name: str = row["name"].strip().lower()
            player_dict[normalized_name] = NbaPlayer(**row)

    return player_dict


async def load_nba_teams_from_db(pool: DBPool):
    """Returns a dict mapping `team_id` to `NbaTeam`."""
    query = """
        SELECT id, name, team_city as city, team_abv as abv, conference
        FROM nba_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        teams_dict: dict[str, NbaTeam] = {}
        for row in rows:
            row = dict(row)
            t_id = row["id"]
            teams_dict[t_id] = NbaTeam(**row)

    return teams_dict


def build_team_fullname_map(teams_dict: dict[str, NbaTeam]):
    """Returns a dict mapping full name (city + ' ' + name) in lowercase to the team's abbreviation.

    For example, 'charlotte hornets' -> 'CHA'.
    """
    full_map: dict[str, str] = {}
    for _, team in teams_dict.items():
        full_team_str = f"{team.city} {team.name}".strip().lower()
        full_map[full_team_str] = team.abv
    return full_map


MARKET_TO_STAT: dict[str, str] = {
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
    nba_player_dict: dict[str, NbaPlayer],
    nba_teams_dict: dict[str, NbaTeam],
    stat_type: str,
):
    player_name_raw = outcome.description
    normalized_name = player_name_raw.lower()

    player = nba_player_dict.get(normalized_name)
    if not player:
        logger.error(f"Player not found in DB: {player=}")
        raise ValueError(f"Player not found in DB: {player=}")

    line = outcome.point
    # TODO: Over shouldn't be the default
    over_under = outcome.name

    # Get the player's team abbreviation
    if player.team_id not in nba_teams_dict:
        logger.error(
            f"Team not found in DB: {player.team_id=}", extra={"player": player}
        )
        raise ValueError(f"Team not found in DB: {player.team_id=}")

    # Determine Opponent
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
    logger.info(f"Parse backend: {bet_analysis=}")
    bet_analysis.raw_json = response_data # type: ignore
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
    all_markets = ",".join(MARKET_TO_STAT.keys())
    odds_params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",  # or "us"
        "markets": all_markets,
        "oddsFormat": "decimal",
    }

    resp_odds = await client.get(single_odds_url, params=odds_params)
    resp_odds.raise_for_status()
    odds_data = resp_odds.json()

    game = game_from_json(odds_data)
    logger.info(f"{game=}")

    backend_results: list[BetAnalysis | Exception] = []

    # def analyze_bet_inner(outcome: Outcome, stat: str):
    #     return analyze_bet(
    #         client,
    #         outcome,
    #         home_team_abv,
    #         away_team_abv,
    #         nba_player_dict,
    #         nba_teams_dict,
    #         stat_type=stat,
    #     )
    async def analyze_bet_for_this_market(outcome: Outcome, stat_type: str):
        return await analyze_bet(
            client,
            outcome,
            home_team_abv,
            away_team_abv,
            nba_player_dict,
            nba_teams_dict,
            stat_type=stat_type,
        )

    async with httpx.AsyncClient(timeout=30) as client:
        for bookmaker in game.bookmakers:
            for market in bookmaker.markets:
                logger.info(f"{market=}")
                # 1) Pick the stat_type for this market
                stat_type = MARKET_TO_STAT.get(market.key)
                if not stat_type:
                    # Market key isn't in MARKET_TO_STAT; skip it
                    continue

                # 2) We only need to pass `Outcome` objects to batch_calls,
                #    but inside each call, we use `stat_type` from outer scope.
                outcomes = market.outcomes

                # Define a simple function that captures `stat_type` in its closure
                async def analyze_outcome(outcome: Outcome):
                    return await analyze_bet_for_this_market(outcome, stat_type) # type: ignore

                # 3) Pass that function to batch_calls, so each outcome calls
                #    analyze_outcome(outcome) with the correct stat_type.
                backend_results.extend(
                    await batch_calls(
                        outcomes,
                        analyze_outcome,  # no lambda
                        10,
                    )
                )
    return backend_results, game


async def batch_calls(
    datas: t.Iterable[T],
    func: t.Callable[..., t.Awaitable[R]],
    batch_size: int,
) -> list[R | Exception]:
    results: list[R | Exception] = []
    for chunk in batched(datas, batch_size):
        results.extend(
            await asyncio.gather(
                *(func(params) for params in chunk), return_exceptions=True
            )  # type: ignore Wants BaseException but doesn't work with Exception for some reason
        )
    return results


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

    logger.info(f"{day_start=}, {day_end=}")

    async with httpx.AsyncClient(timeout=30) as client:
        get_events_url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
        params_events = {
            "apiKey": os.environ["API_KEY"],
            "commenceTimeFrom": day_start.isoformat().replace("+00:00", "Z"),
            "commenceTimeTo": day_end.isoformat().replace("+00:00", "Z"),
        }
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

    logger.info("--- All Single-Event Odds Data ---")
    logger.info(json_dumps_dataclass(all_games))

    logger.info("--- Backend Results ---")
    for result in backend_results:
        if isinstance(result, Exception):
            logger.error(result)
        else:
            logger.info(json_dumps_dataclass(result))

    insert_query = """
        INSERT INTO daily_nba_bets (
            player_id,
            team_id,
            opponent_id,
            stat_type,
            line,
            bet_grade
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
    """

    async with pool.acquire() as conn:
        for bet_result in backend_results:
            # Skip exceptions
            if isinstance(bet_result, Exception):
                continue

            bet: BetAnalysis = bet_result
            
            # We can store these from your BetAnalysis fields
            player_id = str(bet.player_data.Player_ID)  # or bet.player_name...
            team_id = str(bet.player_team_info.Team_ID)
            opp_id = str(bet.opponent_stats.Team_ID)
            stat_type = bet.stat_type
            line_val = bet.bet_number  # or bet.threshold

            # We want the *entire* backend JSON, which we attached as .raw_json
            full_json = getattr(bet, "raw_json", None)  
            if not full_json:
                # e.g. maybe you handle the case where we can't store
                # If it's missing, skip or store an empty {}
                full_json = {}

            # Insert the row
            row = await conn.fetchrow(
                insert_query,
                player_id,
                team_id,
                opp_id,
                stat_type,
                line_val,
                json.dumps(full_json),  # Convert dict -> JSON string
            )

            new_id = row["id"] # type: ignore
            logger.info(f"Inserted daily_nba_bets row ID={new_id} for {bet.player_name}")
