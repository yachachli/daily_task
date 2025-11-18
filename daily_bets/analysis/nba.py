import asyncio
import typing as t
from datetime import date, datetime, timedelta, timezone

import httpx
import msgspec.json
from dateutil.parser import parse as parse_datetime
from msgspec import DecodeError
from neverraise import Err, ErrAsync, Ok, ResultAsync

from daily_bets.db import nba_db as db
from daily_bets.db_pool import DBPool
from daily_bets.env import Env
from daily_bets.errors import NoPlayerFoundError, NoTeamFoundError
from daily_bets.logger import logger
from daily_bets.models import (
    BetAnalysisInput,
)
from daily_bets.db import nba_backup
from daily_bets.odds_api import (
    HttpError,
    Outcome,
    SportEvent,
    fetch_game,
    fetch_tomorrow_events,
)
from daily_bets.utils import batch_calls_result_async, normalize_name

MARKET_TO_STAT = {
    "player_assists": "assists",
    "player_assists_alternate": "assists",
    "player_points": "points",
    "player_points_alternate": "points",
    "player_points_assists": "points + assists",
    "player_points_assists_alternate": "points + assists",
    "player_rebounds": "rebounds",
    "player_rebounds_alternate": "rebounds",
    "player_points_rebounds": "points + rebounds",
    "player_points_rebounds_alternate": "points + rebounds",
    "player_points_rebounds_assists": "points + rebounds + assists",
    "player_points_rebounds_assists_alternate": "points + rebounds + assists",
    "player_rebounds_assists": "rebounds + assists",
    "player_rebounds_assists_alternate": "rebounds + assists",
    "player_threes": "threes",
    "player_threes_alternate": "threes",
    "player_blocks": "blocks",
    "player_blocks_alternate": "blocks",
    "player_steals_alternate": "steals",
    "player_steals": "steals",
    "player_turnovers": "turnovers",
    "player_turnovers_alternate": "turnovers",
}

SPORT_KEY = "basketball_nba"
REGION = "us_dfs"
INCLUDE_ALTERNATE_MARKETS = False


class NbaMap:
    _player_name_team_abv_to_player: dict[tuple[str, str], db.NbaPlayersWithTeamRow]
    """('harry giles iii', 'CHA') -> NbaPlayersWithTeamRow"""
    _team_name_to_abv: dict[str, str]
    """'charlotte hornets' -> 'CHA'"""

    def __init__(
        self,
        players: dict[tuple[str, str], db.NbaPlayersWithTeamRow],
        teams: dict[str, str],
    ):
        self._player_name_team_abv_to_player = players
        self._team_name_to_abv = teams

    def team_full_name_to_abv(self, name: str) -> str | None:
        return self._team_name_to_abv.get(normalize_name(name))

    def player_name_to_player_id(
        self, name: str, team_abv: str
    ) -> db.NbaPlayersWithTeamRow | None:
        return self._player_name_team_abv_to_player.get(
            (normalize_name(name), team_abv)
        )

    @classmethod
    async def from_db(cls, pool: DBPool) -> t.Self:
        [players, teams] = await asyncio.gather(
            NbaMap._load_nba_players_from_db(pool),
            NbaMap._load_nba_teams_from_db(pool),
        )
        return cls(players, teams)

    @staticmethod
    async def _load_nba_players_from_db(pool: DBPool):
        async with pool.acquire() as conn:
            players = await db.nba_players_with_team(conn)

        player_dict: dict[tuple[str, str], db.NbaPlayersWithTeamRow] = {}
        for player in players:
            name = normalize_name(player.name)
            abv = player.team_abv
            player_dict[(name, abv)] = player

        return player_dict

    @staticmethod
    async def _load_nba_teams_from_db(pool: DBPool) -> dict[str, str]:
        async with pool.acquire() as conn:
            teams = await db.nba_teams(conn)

        team_name_to_abv = {
            normalize_name(f"{t.team_city} {t.name}"): t.team_abv for t in teams
        }

        return team_name_to_abv


def do_analysis(
    nba_map: NbaMap,
    client: httpx.AsyncClient,
    event: SportEvent,
    outcome: Outcome,
    stat: str,
) -> ResultAsync[
    db.NbaCopyAnalysisParams,
    NoTeamFoundError | NoPlayerFoundError | HttpError | DecodeError,
]:
    team_abv_player: str | None
    team_abv_opponent: str | None

    team_abv_home = nba_map.team_full_name_to_abv(event.home_team)
    team_abv_away = nba_map.team_full_name_to_abv(event.away_team)

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
    player = nba_map.player_name_to_player_id(
        outcome.description,
        team_abv_home,
    )
    if player:
        team_abv_player = team_abv_home
        team_abv_opponent = team_abv_away
    else:
        player = nba_map.player_name_to_player_id(
            outcome.description,
            team_abv_away,
        )
        if not player:
            return ErrAsync(
                NoPlayerFoundError(
                    f"No player found for {outcome.description} on team {team_abv_home} or {team_abv_away}"
                )
            )

        team_abv_player = team_abv_away
        team_abv_opponent = team_abv_home

    payload = BetAnalysisInput(
        player_id=player.player_id,
        team_code=team_abv_player,
        opponent_abv=team_abv_opponent,
        stat=stat,
        line=outcome.point,
    )

    return (
        ResultAsync.from_coro(
            client.post(
                Env.NBA_ANALYSIS_API_URL,
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
            lambda analysis: db.NbaCopyAnalysisParams(
                analysis=analysis,
                price=outcome.price,
                game_time=parse_datetime(event.commence_time),
                game_tag=game_tag,
            )
        )
    )


async def get_analysis_params(
    client: httpx.AsyncClient, end_date: date
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
        if game_dt > end_date:
            continue

        # fmt: off
        markets_to_fetch = (
            list(MARKET_TO_STAT.keys())
            if INCLUDE_ALTERNATE_MARKETS
            else [k for k in MARKET_TO_STAT.keys() if not k.endswith("_alternate")]
        )
        match await fetch_game(client, SPORT_KEY, event.id, REGION, markets_to_fetch): 
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
                # Optionally skip alternate markets at processing time too
                if not INCLUDE_ALTERNATE_MARKETS and market.key.endswith("_alternate"):
                    continue
                stat = MARKET_TO_STAT.get(market.key)
                if not stat:
                    continue
                for outcome in market.outcomes:
                    params.add((event, outcome, stat))

    return list(params)


async def run(pool: DBPool):
    end_date = (datetime.now(timezone.utc) + timedelta(days=3)).date()
    copy_params: list[db.NbaCopyAnalysisParams] = []
    logger.info(f"Fetching tomorrow's NBA events: {end_date}")

    logger.info("Fetching NBA map from db")
    nba_map = await NbaMap.from_db(pool)

    async with httpx.AsyncClient(timeout=30.0) as client:
        analysis_params = await get_analysis_params(client, end_date)
        logger.info(f"Processing {len(analysis_params)} analysis params")
        analysis_jsons = await batch_calls_result_async(
            [
                (
                    nba_map,
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
        copy_count = await db.nba_copy_analysis(conn, params=copy_params)
        try:
            backup_inserted = await nba_backup.run_backup_maintenance(conn, days=14)
        except Exception as e:
            logger.error(f"Backup maintenance failed: {e!r}")
            backup_inserted = 0
    print(f"Inserted {copy_count} records; backup sync inserted {backup_inserted}")
