import asyncio
import os
import typing as t
from datetime import datetime, timedelta, timezone

from asyncpg import Pool
import httpx
from httpx._types import QueryParamTypes

from daily_bets.logger import logger
from daily_bets.models import (
    BetAnalysis,
    BetAnalysisInput,
    Game,
    NbaPlayer,
    NbaTeam,
    Outcome,
    SportEvent,
)
from daily_bets.utils import batch_calls, normalize_name

T = t.TypeVar("T")
R = t.TypeVar("R")


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


class NbaMap:
    players: dict[tuple[str, str], NbaPlayer]
    """('harry giles iii', 'CHA') -> NbaPlayer"""
    teams: dict[str, NbaTeam]
    """'1' -> NbaTeam"""
    team_name_to_abv: dict[str, str]
    """'charlotte hornets' -> 'CHA'"""

    def __init__(
        self,
        players: dict[tuple[str, str], NbaPlayer],
        teams: dict[str, NbaTeam],
        team_name_to_abv: dict[str, str],
    ):
        self.players = players
        self.teams = teams
        self.team_name_to_abv = team_name_to_abv

    def team_abv(self, name: str) -> str | None:
        return self.team_name_to_abv.get(normalize_name(name))

    def player(self, name: str, team_abv: str) -> NbaPlayer | None:
        return self.players.get((normalize_name(name), team_abv))

    def player_with_unknown_team(
        self, name: str, team_abvs: list[str]
    ) -> tuple[NbaPlayer, str] | None:
        for team_abv in team_abvs:
            player = self.player(name, team_abv)
            if player is not None and player.team_abv == team_abv:
                return player, team_abv

        return None

    @classmethod
    async def from_db(cls, pool: Pool) -> t.Self:
        [players, teams] = await asyncio.gather(
            NbaMap._load_nba_players_from_db(pool),
            NbaMap._load_nba_teams_from_db(pool),
        )
        team_name_to_abv = NbaMap._build_nba_team_fullname_map(teams)
        return cls(players, teams, team_name_to_abv)

    @staticmethod
    async def _load_nba_players_from_db(pool: Pool):
        query = """
            SELECT P.*, T.team_abv
            FROM nba_players as P
            LEFT JOIN nba_teams T on P.team_id = T.id"""
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)

            player_dict: dict[tuple[str, str], NbaPlayer] = {}
            for row in rows:
                row = dict(row)
                name = normalize_name(row["name"])
                abv = row["team_abv"]
                player_dict[(name, abv)] = NbaPlayer.model_validate(row)

        return player_dict

    @staticmethod
    async def _load_nba_teams_from_db(pool: Pool):
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

    @staticmethod
    def _build_nba_team_fullname_map(teams_dict: dict[str, NbaTeam]):
        full_map: dict[str, str] = {}
        for _, team in teams_dict.items():
            full_team_str = normalize_name(f"{team.team_city} {team.name}")
            full_map[full_team_str] = team.team_abv
        return full_map


async def analyze_bet(
    client: httpx.AsyncClient,
    outcome: Outcome,
    home_team_abv: str,
    away_team_abv: str,
    stat_type: str,
    nba_map: NbaMap,
    analysis_cache: dict[tuple[str, float, float], tuple[BetAnalysis, float]],
):
    # player_name_raw = outcome.description
    # name = normalize_name(player_name_raw)
    #
    # player = nba_player_dict.get(name)
    player_and_abv = nba_map.player_with_unknown_team(
        outcome.description, [home_team_abv, away_team_abv]
    )
    if not player_and_abv:
        logger.error(
            f"Player not found in DB: {outcome.description} {[home_team_abv, away_team_abv]}"
        )
        return None

    line = outcome.point
    price = outcome.price

    bet_key = (outcome.description, outcome.price, outcome.point)
    if bet_key in analysis_cache:
        logger.debug(f"Found existing bet with key {bet_key=}. Skipping analysis")
        return None

    player, team_abv = player_and_abv
    if team_abv == home_team_abv:
        opponent_abv = away_team_abv
    else:
        opponent_abv = home_team_abv

    req = BetAnalysisInput(
        player_id=player.player_id,
        team_code=team_abv,
        stat=stat_type,
        line=line,
        opponent_abv=opponent_abv,
    )

    logger.info(f"Calling backend for {outcome.description}: {req.model_dump()}")

    api_url = os.environ["NBA_ANALYSIS_API_URL"]
    headers = {"Content-Type": "application/json"}
    r = await client.post(api_url, json=req.model_dump(), headers=headers)
    if r.is_error:
        logger.error(f"analysis api error {r.status_code} {r.text}")
        return None

    response_data = r.json()

    bet_analysis = BetAnalysis.model_validate(response_data)

    # if the analysis doesn't match, then we don't want users to be able to see the bet
    ou_outcome_normalized = outcome.name.lower()
    ou_ours_normalized = (bet_analysis.over_under or "").lower()
    if ou_outcome_normalized != ou_ours_normalized:
        logger.debug(
            f"Analysis doesn't match expected: {ou_outcome_normalized}, ours: {ou_ours_normalized}"
        )
        return None

    bet_analysis = BetAnalysis.model_validate(response_data)

    analysis_cache[bet_key] = (bet_analysis, price)
    return bet_analysis, price


# TODO: fetch games in parallel
async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    nba_map: NbaMap,
    stats: list[str],
):
    logger.debug(
        f"{event.sport_key} | {stats} | {event.away_team} @ {event.home_team} | commence_time={event.commence_time} | event={event}"
    )
    if (home_team_abv := nba_map.team_abv(event.home_team)) is None:
        logger.error(f"Team not found {event.home_team} in db")
        return None
    if (away_team_abv := nba_map.team_abv(event.away_team)) is None:
        logger.error(f"Team not found {event.away_team} in db")
        return None

    single_odds_url = f"https://api.the-odds-api.com/v4/sports/{event.sport_key}/events/{event.id}/odds"
    odds_params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",  # or "us"
        "markets": ",".join(stats),
        "oddsFormat": "decimal",
    }

    resp_odds = await client.get(single_odds_url, params=odds_params)
    if resp_odds.is_error:
        return None

    odds_data = resp_odds.json()

    game = Game.model_validate(odds_data)
    logger.info(f"{game=}")

    backend_results: list[tuple[BetAnalysis, float] | None | Exception] = []

    # (desc, price, stat) -> (BetAnalysis, price)
    analysis_cache: dict[tuple[str, float, float], tuple[BetAnalysis, float]] = {}

    def analyze_bet_inner(outcome: Outcome, stat: str):
        try:
            return analyze_bet(
                client,
                outcome,
                home_team_abv,
                away_team_abv,
                stat,
                nba_map,
                analysis_cache,
            )
        except Exception as e:
            logger.error(f"Error running analysis: {e}", exc_info=True)
            raise e

    logger.info(
        f"Running analysis {sum(len(bookmaker.markets) for bookmaker in game.bookmakers)} times"
    )

    async with httpx.AsyncClient(timeout=60) as client:
        for bookmaker in game.bookmakers:
            for market in bookmaker.markets:
                stat_type = MARKET_TO_STAT.get(market.key)
                if stat_type is None:
                    logger.warning(f"Unknown market key {market.key=}")
                    continue
                outcomes = market.outcomes

                backend_results.extend(
                    await batch_calls(
                        map(lambda o: (o, stat_type), outcomes),
                        analyze_bet_inner,
                        10,
                    )
                )

    results_filtered = [
        res
        for res in backend_results
        if res is not None and not isinstance(res, Exception)
    ]

    logger.info(f"Filtered {len(backend_results)} -> {len(results_filtered)}")
    return results_filtered, game


async def fetch_sport(
    client: httpx.AsyncClient, sport: str, url: str, params: QueryParamTypes
):
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    events_list = [SportEvent(**data) for data in resp.json()]

    if not events_list:
        logger.warning(f"No {sport} events found in this date range.")
    else:
        logger.info(f"Found {len(events_list)} {sport} events.")
        # Tag each event with the sport

    return events_list


async def run(pool: Pool, stats: list[str]):
    logger.info("Starting NBA analysis")
    sport = "basketball_nba"

    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=2) - timedelta(seconds=1)

    day_start_iso = day_start.isoformat().replace("+00:00", "Z")
    day_end_iso = day_end.isoformat().replace("+00:00", "Z")
    async with httpx.AsyncClient(timeout=30) as client:
        get_events_url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
        params_events = {
            "apiKey": os.environ["API_KEY"],
            "commenceTimeFrom": day_start_iso,
            "commenceTimeTo": day_end_iso,
        }
        logger.info(f"Fetching events from {day_start_iso} to {day_end_iso}")
        all_events = await fetch_sport(client, sport, get_events_url, params_events)
        for event in all_events:
            logger.info(f"  {event}")

    if not all_events:
        logger.error(
            f"No events at all for NBA between {day_start_iso} and {day_end_iso}. Exiting."
        )
        return
    else:
        logger.info(f"Got {len(all_events)} events")

    logger.info("Building nba map")
    nba_map = await NbaMap.from_db(pool)

    all_games: list[Game] = []
    backend_results: list[tuple[BetAnalysis, float]] = []

    logger.info(f"Now fetching single-event odds for {len(all_events)} events...")
    async with httpx.AsyncClient(timeout=30) as client:
        for event in all_events:
            res = await fetch_game_bets(client, event, nba_map, stats)
            if res is None:
                continue
            backend_results_batch, game = res
            backend_results.extend(
                (bet, price, game.commence_time) for bet, price in backend_results_batch
            )
            all_games.append(game)

    logger.info(f"Got {len(all_games)} odds data events")

    logger.info(f"Got {len(backend_results)} analysis results")

    async with pool.acquire() as conn:
        res = await conn.copy_records_to_table(
            "v2_nba_daily_bets",
            columns=[
                "analysis",
                "price",
                "game_time",
            ],
            records=list(
                map(
                    lambda tup: (tup[0].model_dump_json(), tup[1]),
                    backend_results,
                )
            ),
        )
        logger.info(res)
