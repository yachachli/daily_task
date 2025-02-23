import typing as t

from pydantic import BaseModel
from asyncpg import Pool


class SportEvent(BaseModel):
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


class Outcome(BaseModel):
    name: str
    """Over or under"""
    description: str
    """Player name"""
    price: float
    """Multiplier"""
    point: float
    """Stat Line"""


class Market(BaseModel):
    key: str
    last_update: str
    outcomes: list[Outcome]


class Bookmaker(BaseModel):
    key: str
    title: str
    markets: list[Market]


class Game(BaseModel):
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


async def load_nba_players_from_db(pool: Pool):
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
            player_dict[normalized_name] = NbaPlayer.model_validate(row)

    return player_dict


async def load_nba_teams_from_db(pool: Pool):
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


def build_nba_team_fullname_map(teams_dict: dict[str, NbaTeam]):
    """Returns a dict mapping full name (city + ' ' + name) in lowercase to the team's abbreviation.

    For example, 'charlotte hornets' -> 'CHA'.
    """
    full_map: dict[str, str] = {}
    for _, team in teams_dict.items():
        full_team_str = f"{team.team_city} {team.name}".strip().lower()
        full_map[full_team_str] = team.team_abv
    return full_map


class NflPlayer(BaseModel):
    id: int
    team_id: int
    name: str
    height: str
    position: str
    injuries: Injury


class NflTeam(BaseModel):
    id: int
    name: str
    team_code: str
    wins: int
    losses: int
    ties: int
    points_for: int
    points_against: int
    total_tackles: int
    fumbles_lost: int
    defensive_touchdowns: int
    fumbles_recovered: int
    solo_tackles: int
    defensive_interceptions: int
    qb_hits: int
    tackles_for_loss: int
    pass_deflections: int
    sacks: int
    fumbles: int
    passing_td_allowed: int
    passing_yards_allowed: int
    rushing_yards_allowed: int
    rushing_td_allowed: int | None
