import typing as t

import httpx
import msgspec
from neverraise import ErrAsync, ResultAsync

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
    
    # Validate API key is present
    if not Env.API_KEY or not Env.API_KEY.strip():
        return ErrAsync(HttpError(ValueError("API_KEY environment variable is missing or empty")))
    
    async def _fetch_with_error_details():
        response = await client.get(
            url,
            params={"apiKey": Env.API_KEY},
        )
        # Capture response details for 401 errors before raising
        if response.status_code == 401:
            response_text = response.text[:500] if response.text else "No response body"
            raise httpx.HTTPStatusError(
                f"401 Unauthorized - API key may be invalid, expired, or revoked. Response: {response_text}",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        return response
    
    return (
        ResultAsync.from_coro(
            _fetch_with_error_details(),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: msgspec.json.decode(res.text, type=list[SportEvent]),
            lambda e: DecodeError(e),
        )
    )


def _decode_game(text: str) -> Game:
    """Decode an event-odds payload, dropping outcomes that carry no line.

    Yes/no markets (e.g. Dabble's `player_anytime_td`) return outcomes without
    `point`. Decoding them straight into `Outcome` rejects the whole game, which
    silently drops every prop from every book for that event.
    """
    raw = msgspec.json.decode(text, type=dict[str, t.Any])
    bookmakers: list[dict[str, t.Any]] = raw.get("bookmakers") or []
    for bookmaker in bookmakers:
        markets: list[dict[str, t.Any]] = bookmaker.get("markets") or []
        for market in markets:
            outcomes: list[dict[str, t.Any]] = market.get("outcomes") or []
            market["outcomes"] = [o for o in outcomes if o.get("point") is not None]
    return msgspec.convert(raw, type=Game)


def fetch_game(
    client: httpx.AsyncClient,
    sport_key: str,
    event_id: str,
    region: str,
    markets: t.Iterable[str],
    odds_format: str = "decimal",
) -> ResultAsync[Game, HttpError | DecodeError]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    
    # Validate API key is present
    if not Env.API_KEY or not Env.API_KEY.strip():
        return ErrAsync(HttpError(ValueError("API_KEY environment variable is missing or empty")))
    
    params = {
        "apiKey": Env.API_KEY,
        "regions": region,
        "markets": ",".join(markets),
        "oddsFormat": odds_format,
    }
    
    async def _fetch_with_error_details():
        response = await client.get(url, params=params)
        # Capture response details for 401 errors before raising
        if response.status_code == 401:
            response_text = response.text[:500] if response.text else "No response body"
            raise httpx.HTTPStatusError(
                f"401 Unauthorized - API key may be invalid, expired, or revoked. Response: {response_text}",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        return response
    
    return (
        ResultAsync.from_coro(
            _fetch_with_error_details(),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: _decode_game(res.text),
            lambda e: DecodeError(e),
        )
    )
