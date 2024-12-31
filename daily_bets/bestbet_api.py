import typing as t
import httpx


class Bet(t.TypedDict):
    player_id: str  # or just int if you know it's always an int
    team_code: str
    stat: str  # "points", "rebounds", etc.
    line: float
    opponent: str
    over_under: t.Literal["over"] | t.Literal["under"]


async def analyze_bet(client: httpx.AsyncClient, data: Bet) -> dict: ...
