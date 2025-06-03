import datetime
import typing as t

import msgspec


class BetAnalysisInput(msgspec.Struct):
    player_id: int
    team_code: str
    line: float
    stat: str
    opponent_abv: str


class Injury(msgspec.Struct):
    injDate: str
    """YYYYMMDD E.x. '20240805'"""
    description: str
    designation: str
    # "Injured Reserve"
    injReturnDate: str
    """YYYYMMDD E.x. '20240805'"""


class GraphV1Data(msgspec.Struct):
    value: float
    label: str
    date: str


class GraphV1(msgspec.Struct):
    version: t.Literal[1]
    title: str
    data: list[GraphV1Data]
    threshold: float | None


class BetAnalysis(msgspec.Struct):
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


class CopyAnalysisParams(msgspec.Struct):
    analysis: dict[str, t.Any]
    price: float | None
    game_time: datetime.datetime | None
    game_tag: str | None
