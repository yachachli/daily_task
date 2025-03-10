import typing as t
from dataclasses import dataclass

from daily_bets.db import DBPool
from daily_bets.utils import json_dumps_dataclass


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
    price_val: float  # <---- NEW FIELD FOR PRICE


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
        price_val=data.get("price_val", 1.73),  # defual to 1.73
    )


def bet_analysis_to_tuple(
    bet_analysis: BetAnalysis,
) -> tuple[int, int, int, str, float, float, str]:
    return (
        bet_analysis.player_data.Player_ID,
        bet_analysis.player_team_info.Team_ID,
        bet_analysis.opponent_stats.Team_ID,
        bet_analysis.stat_type,
        bet_analysis.threshold,
        getattr(bet_analysis, "price_val", 0.0),  # price (default to 0.0 if missing)
        json_dumps_dataclass(bet_analysis),
    )


@dataclass
class Outcome:
    name: str
    """Over or under"""
    description: str
    """Player name"""
    price: float
    """Multiplier"""
    point: float
    """Stat Line"""


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
        SELECT id as db_id, player_id, team_id, name, position, player_pic
        FROM nba_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        player_dict: dict[str, NbaPlayer] = {}
        for row in rows:
            row = dict(row)
            normalized_name: str = row["name"].strip().lower()
            player_dict[normalized_name] = NbaPlayer(
                db_id=row["db_id"],  # Keep this if needed for DB reference
                id=row["player_id"],  # Ensure this is the correct player_id
                team_id=row["team_id"],
                name=row["name"],
                position=row["position"],
                player_pic=row["player_pic"],
            )

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
