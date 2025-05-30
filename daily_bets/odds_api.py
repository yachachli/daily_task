import typing as t

import httpx
import msgspec
from neverraise import ResultAsync

from daily_bets.env import Env
from daily_bets.errors import DecodeError, HttpError


class SportEvent(msgspec.Struct, frozen=True):
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str


class Outcome(msgspec.Struct, frozen=True):
    name: str
    """Over or under"""
    description: str
    """Player name"""
    price: float
    """Multiplier"""
    point: float
    """Line"""


class Market(msgspec.Struct, frozen=True):
    key: str
    last_update: str
    outcomes: list[Outcome]


class Bookmaker(msgspec.Struct, frozen=True):
    key: str
    title: str
    markets: list[Market]


class Game(msgspec.Struct, frozen=True):
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str
    bookmakers: list[Bookmaker]


def fetch_tomorrow_events(
    client: httpx.AsyncClient,
    sport_key: str,
) -> ResultAsync[list[SportEvent], HttpError | DecodeError]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
    return (
        ResultAsync.from_coro(
            client.get(
                url,
                params={"apiKey": Env.API_KEY},
            ),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: res.raise_for_status(),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: msgspec.json.decode(res.text, type=list[SportEvent]),
            lambda e: DecodeError(e),
        )
    )


def fetch_game(
    client: httpx.AsyncClient,
    sport_key: str,
    event_id: str,
    region: str,
    markets: t.Iterable[str],
    odds_format: str = "decimal",
) -> ResultAsync[Game, HttpError | DecodeError]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": Env.API_KEY,
        "regions": region,
        "markets": ",".join(markets),
        "oddsFormat": odds_format,
    }
    return (
        ResultAsync.from_coro(
            client.get(url, params=params),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: res.raise_for_status(),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: msgspec.json.decode(res.text, type=Game),
            lambda e: DecodeError(e),
        )
    )
