import typing as t

from pydantic import BaseModel


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
    date: str


class GraphV1(BaseModel):
    version: t.Literal[1]
    title: str
    data: list[GraphV1Data]
    threshold: float | None


class BetAnalysis(BaseModel):
    over_under: t.Literal["over"] | t.Literal["under"] | None
    grade: int
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
    player_id: int
    name: str
    position: str | None = None
    player_pic: str | None = None
    injury: list[Injury] | None
    team_id: int
    team_abv: str


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
