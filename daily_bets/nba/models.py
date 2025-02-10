import json
import typing as t
from dataclasses import dataclass

from pydantic import BaseModel

from daily_bets.db import DBPool


@dataclass
class SportEvent:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str


class Injury(BaseModel):
    injDate: str
    """YYYYMMDD E.x. '20240805'"""
    description: str
    designation: str
    # "Injured Reserve"
    injReturnDate: str
    """YYYYMMDD E.x. '20240805'"""


class BetAnalysisInput(BaseModel):
    player_id: int
    team_code: str
    line: float
    stat: str
    opponent_abv: str


class GraphV1Data(BaseModel):
    value: float
    label: str


class GraphV1(BaseModel):
    version: t.Literal[1]
    title: str
    data: list[GraphV1Data]
    threshold: float | None


class BetAnalysis(BaseModel):
    over_under: t.Literal["over"] | t.Literal["under"] | None
    grade: str
    league: t.Literal["NBA"] | t.Literal["NFL"]
    injury: Injury | None
    insights: list[str]
    input: BetAnalysisInput
    short_answer: str
    long_answer: str
    player_position: str
    graphs: list[GraphV1]


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


class NbaPlayer(BaseModel):
    id: int
    name: str
    position: str | None = None
    team_id: int | None = None
    player_pic: str | None = None
    player_id: int
    injury: list[Injury] | None


class NbaSeasons(BaseModel):
    id: int
    season_year: str


class NbaTeam(BaseModel):
    id: int
    name: str
    team_city: str
    team_abv: str
    conference: str
    ppg: float
    oppg: float
    wins: float
    loss: float
    division: str
    team_bpg: float
    team_spg: float
    team_apg: float
    team_fga: float
    team_fgm: float
    team_fta: float
    pace: float
    def_rtg: float


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
        SELECT *
        FROM nba_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        player_dict: dict[str, NbaPlayer] = {}
        for row in rows:
            row = dict(row)
            normalized_name: str = row["name"].strip().lower()
            if isinstance(row["injury"], str):
                row["injury"] = json.loads(row["injury"])

            player_dict[normalized_name] = NbaPlayer.model_validate(row)

    return player_dict


async def load_nba_teams_from_db(pool: DBPool):
    """Returns a dict mapping `team_id` to `NbaTeam`."""
    query = """
        SELECT *
        FROM nba_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

        teams_dict: dict[str, NbaTeam] = {}
        for row in rows:
            row = dict(row)
            t_id = row["id"]
            teams_dict[t_id] = NbaTeam.model_validate(row)

    return teams_dict


def build_team_fullname_map(teams_dict: dict[str, NbaTeam]):
    """Returns a dict mapping full name (city + ' ' + name) in lowercase to the team's abbreviation.

    For example, 'charlotte hornets' -> 'CHA'.
    """
    full_map: dict[str, str] = {}
    for _, team in teams_dict.items():
        full_team_str = f"{team.team_city} {team.name}".strip().lower()
        full_map[full_team_str] = team.team_abv
    return full_map
