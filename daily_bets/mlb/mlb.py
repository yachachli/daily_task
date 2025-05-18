import typing as t
from datetime import datetime, timedelta, timezone

import httpx
import msgspec
from neverraise import Err, Ok, ErrAsync, ResultAsync
from dateutil.parser import parse as parse_datetime

from daily_bets.db import mlb
from daily_bets.db_pool import DBPool
from daily_bets.env import Env
from daily_bets.logger import logger
from daily_bets.models import BetAnalysisInput
from daily_bets.odds_api import fetch_game, fetch_tomorrow_events, Outcome, SportEvent
from daily_bets.utils import batch_calls_result_async, normalize_name

SPORT_KEY = "baseball_mlb"
REGION = "us_dfs"

MARKET_TO_STAT = {
    "batter_home_runs": "home runs",
    "batter_hits": "hits",
    "batter_rbis": "rbi",
    "batter_hits_runs_rbis": "hits + rbi",
}

TEAM_NAME_TO_ABV = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Cleveland Indians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WAS",
}


class MlbMap:
    _player_name_and_team_abv_to_player_id: dict[tuple[str, str | None], int]
    _team_name_to_abv: dict[str, str]

    def __init__(
        self, players: dict[tuple[str, str | None], int], teams: dict[str, str]
    ):
        self._player_name_and_team_abv_to_player_id = players
        self._team_name_to_abv = teams

    @classmethod
    async def from_db(cls, pool: DBPool) -> t.Self:
        async with pool.acquire() as conn:
            mlb_players = await mlb.all_players(conn)
            mlb_teams = await mlb.all_teams(conn)

        players: dict[tuple[str, str | None], int] = {}
        for mlb_player in mlb_players:
            assert (
                normalize_name(mlb_player.long_name),
                mlb_player.team_abv,
            ) not in players, f"{mlb_player.long_name} {mlb_player.team_abv}"

            players[(normalize_name(mlb_player.long_name), mlb_player.team_abv)] = (
                mlb_player.player_id
            )
        assert len(players) == len(mlb_players)

        team_name_to_abv = {
            normalize_name(f"{t.team_city} {t.team_name}"): t.team_abv
            for t in mlb_teams
        }
        for k, v in TEAM_NAME_TO_ABV.items():
            if k not in team_name_to_abv:
                team_name_to_abv[normalize_name(k)] = v

        return cls(
            players,
            team_name_to_abv,
        )

    def player_name_to_player_id(
        self, player_name: str, team_abv: str | None
    ) -> int | None:
        return self._player_name_and_team_abv_to_player_id.get(
            (normalize_name(player_name), team_abv)
        )

    def team_full_name_to_abv(self, team_full_name: str) -> str | None:
        return self._team_name_to_abv.get(normalize_name(team_full_name))


class NoTeamFoundError(Exception): ...


class NoPlayerFoundError(Exception): ...


class HttpError(Exception): ...


class DecodeError(Exception): ...


def handle_outcome(
    mlb_map: MlbMap,
    event: SportEvent,
    outcome: Outcome,
    stat: str,
    client: httpx.AsyncClient,
    # ) -> tuple[str, float] | None:
) -> ResultAsync[
    tuple[str, float], NoTeamFoundError | NoPlayerFoundError | HttpError | DecodeError
]:
    logger.info(f"    Handling outcome: {outcome.description} {stat} {outcome.point}")
    team_abv_player: str | None
    team_abv_opponent: str | None

    team_abv_home = mlb_map.team_full_name_to_abv(event.home_team)
    team_abv_away = mlb_map.team_full_name_to_abv(event.away_team)

    if not team_abv_home or not team_abv_away:
        return ErrAsync(
            NoTeamFoundError(
                f"Not able to find team {event.home_team!r} {team_abv_home!r} or {event.away_team!r} {team_abv_away!r}"
            )
        )

    player_id = mlb_map.player_name_to_player_id(
        outcome.description,
        team_abv_home,
    )
    if player_id:
        team_abv_player = team_abv_home
        team_abv_opponent = team_abv_away
    else:
        player_id = mlb_map.player_name_to_player_id(
            outcome.description,
            team_abv_away,
        )
        if not player_id:
            return ErrAsync(
                NoPlayerFoundError(
                    f"No player found for {outcome.description} on team {team_abv_home} or {team_abv_away}"
                )
            )
        team_abv_player = team_abv_away
        team_abv_opponent = team_abv_home

    payload = BetAnalysisInput(
        player_id=player_id,
        team_code=team_abv_player,
        opponent_abv=team_abv_opponent,
        stat=stat,
        line=outcome.point,
    )
    return (
        ResultAsync.from_coro(
            client.post(
                Env.MLB_ANALYSIS_API_URL,
                content=msgspec.json.encode(payload),
                headers={"Content-Type": "application/json"},
            ),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: res.raise_for_status(),
            lambda e: HttpError(e),
        )
        .try_catch(
            lambda res: res.text,
            lambda e: DecodeError(e),
        )
        .map(lambda text: (text, outcome.price))
    )


async def run(pool: DBPool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    records: list[tuple[str, float, datetime, str]] = []
    logger.info(f"Fetching tomorrow's MLB events: {tomorrow}")

    logger.info("Fetching MLB map from db")
    mlb_map = await MlbMap.from_db(pool)

    async with httpx.AsyncClient(timeout=30.0) as client:
        match await fetch_tomorrow_events(client, SPORT_KEY):
            case Ok(events):
                logger.info(f"Fetched {len(events)} events")
            case Err(e):
                logger.error(f"Error fetching tomorrow's MLB events: {e}")
                return

        Err("asdf")

        for event in events:
            logger.info(f"Processing event: {event.home_team} vs {event.away_team}")
            game_dt = datetime.fromisoformat(
                event.commence_time.replace("Z", "+00:00")
            ).date()
            if game_dt - tomorrow > timedelta(days=1):
                continue

            match await fetch_game(
                client, SPORT_KEY, event.id, REGION, MARKET_TO_STAT.keys()
            ):
                case Ok(game):
                    logger.info(
                        f"Fetched game: {game.home_team} vs {game.away_team} bookmakers {len(game.bookmakers)}"
                    )
                case Err() as e:
                    logger.error(f"Error fetching game: {e}")
                    return

            tag = f"{event.away_team}@{event.home_team}"

            for bookmaker in game.bookmakers:
                logger.info(
                    f"Bookmaker: {bookmaker.title} markets {len(bookmaker.markets)}"
                )
                for market in bookmaker.markets:
                    logger.info(f"  Market: {market.key}")
                    stat = MARKET_TO_STAT.get(market.key)
                    if not stat:
                        continue
                    analysis_jsons = await batch_calls_result_async(
                        [
                            (mlb_map, event, outcome, stat, client)
                            for outcome in market.outcomes
                        ],
                        handle_outcome,
                        batch_size=10,
                    )
                    for res in analysis_jsons:
                        match res:
                            case Ok((analysis_json, price)):
                                records.append(
                                    (
                                        analysis_json,
                                        price,
                                        parse_datetime(event.commence_time),
                                        tag,
                                    )
                                )
                            case Err(e):
                                logger.error(f"Error handling outcome: {e}")

    # bulk‐insert via Neon pool
    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            "v2_mlb_daily_bets",
            columns=["analysis", "price", "game_time", "game_tag"],
            records=records,
        )
    print(f"Inserted {len(records)} records into v2_mlb_daily_bets")
