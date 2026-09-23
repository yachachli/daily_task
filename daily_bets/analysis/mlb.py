import json
import typing as t
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import httpx
import msgspec
from dateutil.parser import parse as parse_datetime
from neverraise import Err, ErrAsync, Ok, ResultAsync

from daily_bets.analysis.existing_bets import make_existing_bet_key
from daily_bets.db import mlb_backup
from daily_bets.db import mlb_db as db
from daily_bets.db_pool import DBPool
from daily_bets.env import Env
from daily_bets.errors import (
    SkipBetError,
    DecodeError,
    HttpError,
    NoPlayerFoundError,
    NoTeamFoundError,
)
from daily_bets.logger import logger
from daily_bets.models import BetAnalysisInput
from daily_bets.odds_api import (
    Outcome,
    SportEvent,
    fetch_game,
    fetch_tomorrow_events,
)
from daily_bets.utils import batch_calls_result_async, normalize_name


class _Recommendation(msgspec.Struct):
    """Just the field we need from the backend response (extra keys ignored)."""

    over_under: str | None = None


def line_key(
    event: SportEvent, outcome: Outcome, stat: str
) -> tuple[str, str, str, float]:
    return (event.id, outcome.description, stat, float(outcome.point))


#: Both offered sides of one line, keyed by "over" / "under".
Sides = dict[str, Outcome]


SPORT_KEY = "baseball_mlb"
REGION = "us_dfs"
MAX_BETS_PER_TEAM_PER_GAME = 20

MARKET_TO_STAT = {
    "batter_home_runs": "home runs",
    "batter_hits": "hits",
    "batter_rbis": "rbi",
    # batter_hits_runs_rbis is really hits + runs + RBIs; it was mislabeled
    # "hits + rbi" and is excluded from discovery at product request (Sept 2026).
}

#: Pitching markets, analysed by v2_mlb_pitcher_pou rather than the hitter
#: endpoint. Kept in a separate map, and fetched in a separate request, for two
#: reasons: the stat decides which backend analyses it, and a market key the
#: books do not offer must not be able to take the hitter slate down with it --
#: fetch_game failing returns [] and abandons every event.
#:
#: Keep the stat names in sync with MARKETS in
#: bestbet_backend/src/mlb_pitcher_pou/api_calls.py.
PITCHER_MARKET_TO_STAT = {
    "pitcher_strikeouts": "pitcher strikeouts",
    "pitcher_outs": "pitcher outs",
    "pitcher_hits_allowed": "hits allowed",
    "pitcher_walks": "walks allowed",
    "pitcher_earned_runs": "earned runs allowed",
}

#: A game fields one starter a side, so this is a handful of rows per team --
#: nothing like the nine-plus hitters MAX_BETS_PER_TEAM_PER_GAME exists to trim.
#: Counted separately so a team's hitters cannot crowd its starter off the
#: board: appended after them, pitcher outcomes would be the first trimmed.
MAX_PITCHER_BETS_PER_TEAM_PER_GAME = 8


def is_pitcher_stat(stat: str) -> bool:
    return stat in set(PITCHER_MARKET_TO_STAT.values())


def analysis_url_for(stat: str) -> str:
    """The POU endpoint that can analyse this stat."""
    return (
        Env.MLB_PITCHER_ANALYSIS_API_URL
        if is_pitcher_stat(stat)
        else Env.MLB_ANALYSIS_API_URL
    )

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
            mlb_players = await db.mlb_players(conn)
            mlb_teams = await db.mlb_teams(conn)

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


def resolve_player_context(
    mlb_map: MlbMap,
    event: SportEvent,
    player_name: str,
) -> tuple[int, str, str, str] | None:
    team_abv_home = mlb_map.team_full_name_to_abv(event.home_team)
    team_abv_away = mlb_map.team_full_name_to_abv(event.away_team)

    if not team_abv_home or not team_abv_away:
        return None

    game_tag = f"{team_abv_away}@{team_abv_home}"

    player_id = mlb_map.player_name_to_player_id(player_name, team_abv_home)
    if player_id:
        return (player_id, team_abv_home, team_abv_away, game_tag)

    player_id = mlb_map.player_name_to_player_id(player_name, team_abv_away)
    if player_id:
        return (player_id, team_abv_away, team_abv_home, game_tag)

    return None


def _pick_offered_side(
    analysis_text: str, posted: Outcome, sides: Sides | None
) -> Outcome:
    rec = msgspec.json.decode(analysis_text, type=_Recommendation).over_under
    rec = rec.strip().lower() if rec else None
    if rec is None or not sides:
        return posted
    chosen = sides.get(rec)
    if chosen is None:
        raise SkipBetError(
            f"Backend recommends {rec} but the book offers only "
            f"{'/'.join(sorted(sides))} for {posted.description} {posted.point}"
        )
    if chosen is not posted:
        logger.info(
            f"    Flipped to offered {rec} side for {posted.description} {posted.point} "
            f"(price {posted.price} -> {chosen.price})"
        )
    return chosen


def do_analysis(
    mlb_map: MlbMap,
    client: httpx.AsyncClient,
    event: SportEvent,
    outcome: Outcome,
    stat: str,
    sides: Sides | None = None,
) -> ResultAsync[
    db.MlbCopyAnalysisParams,
    NoTeamFoundError | NoPlayerFoundError | HttpError | DecodeError | SkipBetError,
]:
    resolved = resolve_player_context(mlb_map, event, outcome.description)
    if not resolved:
        team_abv_home = mlb_map.team_full_name_to_abv(event.home_team)
        team_abv_away = mlb_map.team_full_name_to_abv(event.away_team)
        if not team_abv_home or not team_abv_away:
            return ErrAsync(
                NoTeamFoundError(
                    f"Not able to find team {event.home_team!r} {team_abv_home!r} or {event.away_team!r} {team_abv_away!r}"
                )
            )
        return ErrAsync(
            NoPlayerFoundError(
                f"No player found for {outcome.description} on team {team_abv_home} or {team_abv_away}"
            )
        )

    player_id, team_abv_player, team_abv_opponent, game_tag = resolved

    logger.info(
        f"    Handling outcome: {outcome.description} {stat} {outcome.point} {game_tag}"
    )

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
                analysis_url_for(stat),
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
        # The backend picks the side from the player's history; the row must
        # carry the price of the side the book actually offers for that pick.
        # If the book only offers the other side, there is nothing to sell.
        .try_catch(
            lambda text: (text, _pick_offered_side(text, outcome, sides)),
            lambda e: e if isinstance(e, SkipBetError) else DecodeError(e),
        )
        .map(
            lambda pair: db.MlbCopyAnalysisParams(
                analysis=pair[0],
                price=pair[1].price,
                game_time=parse_datetime(event.commence_time),
                game_tag=game_tag,
            )
        )
    )


async def filter_existing_analysis_params(
    pool: DBPool,
    mlb_map: MlbMap,
    params: list[tuple[SportEvent, Outcome, str]],
) -> list[tuple[SportEvent, Outcome, str]]:
    filtered: list[tuple[SportEvent, Outcome, str]] = []
    skipped_count = 0
    unresolved_count = 0
    async with pool.acquire() as conn:
        existing_keys = {
            make_existing_bet_key(
                row.game_time,
                row.game_tag,
                row.player_id,
                row.stat,
                row.line,
            )
            for row in await db.mlb_recent_analysis_keys(conn, days=1)
            if row.stat is not None
        }
        for event, outcome, stat in params:
            resolved = resolve_player_context(mlb_map, event, outcome.description)
            if not resolved:
                unresolved_count += 1
                continue

            player_id, _, _, game_tag = resolved
            key = make_existing_bet_key(
                parse_datetime(event.commence_time),
                game_tag,
                player_id,
                stat,
                outcome.point,
            )
            if key in existing_keys:
                skipped_count += 1
                logger.info(
                    f"    Skipping existing MLB bet: {outcome.description} {stat} {outcome.point} {game_tag}"
                )
                continue
            existing_keys.add(key)
            filtered.append((event, outcome, stat))

    if skipped_count:
        logger.info(f"Skipped {skipped_count} existing MLB bets before analysis")
    if unresolved_count:
        logger.info(f"Skipped {unresolved_count} unresolved MLB bets before analysis")
    return filtered


async def get_analysis_params(
    client: httpx.AsyncClient, tomorrow: date, mlb_map: MlbMap
) -> tuple[
    list[tuple[SportEvent, Outcome, str]], dict[tuple[str, str, str, float], Sides]
]:
    params: list[tuple[SportEvent, Outcome, str]] = []
    seen: set[tuple[str, str, str, float]] = set()
    sides_by_line: dict[tuple[str, str, str, float], Sides] = {}

    match await fetch_tomorrow_events(client, SPORT_KEY):
        case Ok(events):
            ...
        case Err() as e:
            logger.error(f"Error fetching tomorrow's MLB events: {e!r}")
            return [], {}

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
                return [], {}
        # fmt: on

        # Pitching markets are a second request, and a failing one is survivable.
        # The batter call above abandons the whole slate on error, which is the
        # right call when it is the slate; it is the wrong call for an extra
        # market a book may simply not offer. A miss here costs this event's
        # pitcher props and nothing else.
        market_to_stat = dict(MARKET_TO_STAT)
        bookmakers = list(game.bookmakers)
        match await fetch_game(
            client, SPORT_KEY, event.id, REGION, PITCHER_MARKET_TO_STAT.keys()
        ):
            case Ok(pitcher_game):
                logger.info(
                    f"  Fetched pitcher markets: bookmakers {len(pitcher_game.bookmakers)}"
                )
                bookmakers.extend(pitcher_game.bookmakers)
                market_to_stat.update(PITCHER_MARKET_TO_STAT)
            case Err() as e:
                logger.warning(
                    f"  No pitcher markets for {event.home_team} vs {event.away_team}: {e!r}"
                )

        for bookmaker in bookmakers:
            logger.info(
                f"    Bookmaker: {bookmaker.title} markets {len(bookmaker.markets)}"
            )
            for market in bookmaker.markets:
                logger.info(f"      Market: {market.key}")
                stat = market_to_stat.get(market.key)
                if not stat:
                    continue
                for outcome in market.outcomes:
                    key = line_key(event, outcome, stat)
                    side = outcome.name.strip().lower()
                    sides = sides_by_line.setdefault(key, {})
                    # Keep the first price seen per side (books are iterated in
                    # payload order, same as before).
                    _ = sides.setdefault(side, outcome)
                    if key in seen:
                        continue
                    seen.add(key)
                    params.append((event, outcome, stat))

    grouped_params: dict[tuple[str, str], list[tuple[SportEvent, Outcome, str]]] = (
        defaultdict(list)
    )
    unresolved_params: list[tuple[SportEvent, Outcome, str]] = []
    for param in params:
        event, outcome, _ = param
        resolved = resolve_player_context(mlb_map, event, outcome.description)
        if not resolved:
            unresolved_params.append(param)
            continue

        _, team_abv_player, _, game_tag = resolved
        grouped_params[(game_tag, team_abv_player)].append(param)

    limited_params: list[tuple[SportEvent, Outcome, str]] = []
    dropped_count = 0
    for grouped in grouped_params.values():
        # Counted apart so a team's nine hitters cannot fill the cap and leave
        # its starter off the board. Pitcher outcomes arrive after the batter
        # ones, so a single shared cap would always trim them first.
        hitters = [p for p in grouped if not is_pitcher_stat(p[2])]
        pitchers = [p for p in grouped if is_pitcher_stat(p[2])]
        limited_params.extend(hitters[:MAX_BETS_PER_TEAM_PER_GAME])
        limited_params.extend(pitchers[:MAX_PITCHER_BETS_PER_TEAM_PER_GAME])
        dropped_count += max(0, len(hitters) - MAX_BETS_PER_TEAM_PER_GAME)
        dropped_count += max(0, len(pitchers) - MAX_PITCHER_BETS_PER_TEAM_PER_GAME)

    if dropped_count:
        logger.info(
            "Trimmed %s MLB outcomes due to %s bets per team per game cap",
            dropped_count,
            MAX_BETS_PER_TEAM_PER_GAME,
        )

    return [*limited_params, *unresolved_params], sides_by_line


async def run(pool: DBPool):
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    copy_params: list[db.MlbCopyAnalysisParams] = []
    logger.info(f"Fetching tomorrow's MLB events: {tomorrow}")

    logger.info("Fetching MLB map from db")
    mlb_map = await MlbMap.from_db(pool)

    async with httpx.AsyncClient(timeout=30.0) as client:
        analysis_params, sides_by_line = await get_analysis_params(
            client, tomorrow, mlb_map
        )
        analysis_params = await filter_existing_analysis_params(
            pool, mlb_map, analysis_params
        )
        logger.info(f"Processing {len(analysis_params)} analysis params")
        analysis_jsons = await batch_calls_result_async(
            [
                (
                    mlb_map,
                    client,
                    event,
                    outcome,
                    stat,
                    sides_by_line.get(line_key(event, outcome, stat)),
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
                case Err(SkipBetError() as e): logger.info(f"Skipped outcome: {e}")  # noqa: E701
                case Err(e): logger.error(f"Error handling outcome: {e!r}")  # noqa: E701
            # fmt: on

    async with pool.acquire() as conn:
        dedupe_count = await db.mlb_dedupe_recent_analysis(conn, days=1)
        if dedupe_count:
            logger.info(f"Deleted {dedupe_count} recent duplicate MLB bets")
        upsert_count = 0
        translate_url = getattr(Env, "TRANSLATE_ES_URL", "") or ""
        if not translate_url:
            logger.warning(
                "TRANSLATE_ES_URL is not set — new bets will NOT get Spanish translations"
            )
        async with httpx.AsyncClient() as translate_client:
            for param in copy_params:
                inserted = (
                    await db.mlb_upsert_analysis(
                        conn,
                        analysis_json=param.analysis,
                        price=param.price,
                        game_time=param.game_time,
                        game_tag=param.game_tag,
                    )
                    or 0
                )
                upsert_count += inserted
                if inserted and translate_url:
                    try:
                        res = await translate_client.post(
                            translate_url,
                            json={"analysis": param.analysis},
                            timeout=30,
                        )
                        if res.is_success:
                            analysis_es = res.json()["analysis_es"]
                            await conn.execute(
                                "UPDATE v2_mlb_daily_bets SET analysis_es = $1 "
                                "WHERE game_time = $2 AND game_tag = $3 "
                                "AND analysis->>'short_answer' = $4",
                                analysis_es,
                                param.game_time,
                                param.game_tag,
                                json.loads(param.analysis)["short_answer"],
                            )
                        else:
                            logger.warning(
                                f"ES translation HTTP {res.status_code} for {param.game_tag}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"ES translation failed for {param.game_tag}: {e!r}"
                        )
        try:
            backup_inserted = await mlb_backup.run_backup_maintenance(conn, days=14)
        except Exception as e:
            logger.error(f"Backup maintenance failed: {e!r}")
            backup_inserted = 0
    print(f"Inserted {upsert_count} records; backup sync inserted {backup_inserted}")
