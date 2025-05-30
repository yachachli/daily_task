import typing as t
from datetime import date, datetime, timedelta, timezone

import httpx
import msgspec.json
from dateutil.parser import parse as parse_datetime
from msgspec import DecodeError
from neverraise import Err, ErrAsync, Ok, ResultAsync

from daily_bets.db import nfl_db as db
from daily_bets.db_pool import DBPool
from daily_bets.env import Env
from daily_bets.errors import HttpError, NoPlayerFoundError, NoTeamFoundError
from daily_bets.logger import logger
from daily_bets.models import (
    BetAnalysisInput,
)
from daily_bets.odds_api import Outcome, SportEvent, fetch_game, fetch_tomorrow_events
from daily_bets.utils import batch_calls_result_async, normalize_name

MARKET_TO_STAT = {
    "player_field_goals": "field goals",
    "player_kicking_points": "kicking points",
    "player_pass_attempts": "pass attempts",
    "player_pass_attempts_alternate": "pass attempts",
    "player_pass_interceptions": "pass ints",
    "player_pass_tds": "pass tds",
    "player_pass_tds_alternate": "pass tds",
    "player_pass_yds": "pass yards",
    "player_pass_yds_alternate": "pass yards",
    "player_pats": "extra points",
    "player_reception_yds": "rec yards",
    "player_receptions": "receptions",
    "player_rush_attempts": "rush carries",
    "player_rush_reception_yds": "rush + rec yards",
    "player_rush_yds": "rush yards",
    "player_sacks": "sacks",
    "player_tds_over": "pass + rush + rec tds",
    "player_anytime_td": "pass + rush + rec tds",
    "player_reception_yds_alternate": "rec yards",
    "player_rush_reception_yds_alternate": "rush + rec yards",
    "player_rush_reception_tds_alternate": "rush + rec tds",
    "player_rush_reception_tds": "rush + rec tds",
    "player_pass_interceptions_alternate": "pass ints",
    "player_pats_alternate": "extra points",
    "player_rush_attempts_alternate": "rush carries",
    "player_rush_yds_alternate": "rush yards",
}

SPORT_KEY = "americanfootball_nfl"
REGION = "us_dfs"


class NflMap:
    _player_name_and_team_abv_to_player_id: dict[tuple[str, str], int]
    _team_name_to_abv: dict[str, str]

    def __init__(self, players: dict[tuple[str, str], int], teams: dict[str, str]):
        self._player_name_and_team_abv_to_player_id = players
        self._team_name_to_abv = teams

    @classmethod
    async def from_db(cls, pool: DBPool) -> t.Self:
        async with pool.acquire() as conn:
            nfl_players = await db.nfl_players_with_team(conn)
            nfl_teams = await db.nfl_teams(conn)

        players: dict[tuple[str, str], int] = {}
        for nfl_player in nfl_players:
            assert (
                normalize_name(nfl_player.name),
                nfl_player.team_abv,
            ) not in players, f"{nfl_player.name} {nfl_player.team_abv}"

            players[(normalize_name(nfl_player.name), nfl_player.team_abv)] = (
                nfl_player.id
            )
        assert len(players) == len(nfl_players)

        team_name_to_abv = {normalize_name(t.name): t.team_code for t in nfl_teams}

        return cls(
            players,
            team_name_to_abv,
        )

    def player_name_to_player_id(self, player_name: str, team_abv: str) -> int | None:
        return self._player_name_and_team_abv_to_player_id.get(
            (normalize_name(player_name), team_abv)
        )

    def team_full_name_to_abv(self, team_full_name: str) -> str | None:
        return self._team_name_to_abv.get(normalize_name(team_full_name))


def do_analysis(
    nfl_map: NflMap,
    client: httpx.AsyncClient,
    event: SportEvent,
    outcome: Outcome,
    stat: str,
) -> ResultAsync[
    db.NflCopyAnalysisParams,
    NoTeamFoundError | NoPlayerFoundError | HttpError | DecodeError,
]:
    team_abv_player: str | None
    team_abv_opponent: str | None

    team_abv_home = nfl_map.team_full_name_to_abv(event.home_team)
    team_abv_away = nfl_map.team_full_name_to_abv(event.away_team)

    if not team_abv_home or not team_abv_away:
        return ErrAsync(
            NoTeamFoundError(
                f"Not able to find team {event.home_team!r} {team_abv_home!r} or {event.away_team!r} {team_abv_away!r}"
            )
        )

    game_tag = f"{team_abv_away}@{team_abv_home}"

    logger.info(
        f"    Handling outcome: {outcome.description} {stat} {outcome.point} {game_tag}"
    )

    # figure out which team the player is on
    player_id = nfl_map.player_name_to_player_id(
        outcome.description,
        team_abv_home,
    )
    if player_id:
        team_abv_player = team_abv_home
        team_abv_opponent = team_abv_away
    else:
        player_id = nfl_map.player_name_to_player_id(
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
                Env.NFL_ANALYSIS_API_URL,
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
        .map(
            lambda analysis: db.NflCopyAnalysisParams(
                analysis=analysis,
                price=outcome.price,
                game_time=parse_datetime(event.commence_time),
                game_tag=game_tag,
            )
        )
    )


async def get_analysis_params(
    client: httpx.AsyncClient, tomorrow: date
) -> list[tuple[SportEvent, Outcome, str]]:
    params: set[tuple[SportEvent, Outcome, str]] = set()

    match await fetch_tomorrow_events(client, SPORT_KEY):
        case Ok(events):
            ...
        case Err() as e:
            logger.error(f"Error fetching tomorrow's MLB events: {e!r}")
            return []

    for event in events:
        logger.info(f"Processing event: {event.home_team} vs {event.away_team}")
        game_dt = datetime.fromisoformat(
            event.commence_time.replace("Z", "+00:00")
        ).date()
        if game_dt - tomorrow > timedelta(days=1):
            continue

        # fmt: off
        match await fetch_game(client, SPORT_KEY, event.id, REGION, MARKET_TO_STAT.keys()): 
            case Ok(game): logger.info( f"  Fetched game: {game.home_team} vs {game.away_team} bookmakers {len(game.bookmakers)}")  # noqa: E701
            case Err() as e:
                logger.error(f"Error fetching game: {e!r}")
                return []
        # fmt: on

        for bookmaker in game.bookmakers:
            logger.info(
                f"    Bookmaker: {bookmaker.title} markets {len(bookmaker.markets)}"
            )
            for market in bookmaker.markets:
                logger.info(f"      Market: {market.key}")
                stat = MARKET_TO_STAT.get(market.key)
                if not stat:
                    continue
                for outcome in market.outcomes:
                    params.add((event, outcome, stat))

    return list(params)


async def run(pool: DBPool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    copy_params: list[db.NflCopyAnalysisParams] = []
    logger.info(f"Fetching tomorrow's MLB events: {tomorrow}")

    logger.info("Fetching MLB map from db")
    nfl_map = await NflMap.from_db(pool)

    async with httpx.AsyncClient(timeout=30.0) as client:
        analysis_params = await get_analysis_params(client, tomorrow)

        logger.info(f"Processing {len(analysis_params)} analysis params")
        analysis_jsons = await batch_calls_result_async(
            [
                (
                    nfl_map,
                    client,
                    event,
                    outcome,
                    stat,
                )
                for event, outcome, stat in analysis_params
            ],
            do_analysis,
            batch_size=10,
        )
        for res in analysis_jsons:
            # fmt: off
            match res:
                case Ok(analysis_params): copy_params.append(analysis_params)  # noqa: E701
                case Err(e): logger.error(f"Error handling outcome: {e!r}")  # noqa: E701
            # fmt: on

    async with pool.acquire() as conn:
        _ = await conn.copy_records_to_table(
            "v2_mlb_daily_bets",
            columns=["analysis", "price", "game_time", "game_tag"],
            records=[
                (param.analysis, param.price, param.game_time, param.game_tag)
                for param in copy_params
            ],
        )
        # await mlb_db.copy_analysis(conn, params=copy_params)
    print(f"Inserted {len(copy_params)} records")
