import asyncio
import os
from datetime import datetime, timedelta, timezone

from asyncpg import Pool
import httpx
from httpx._types import QueryParamTypes

from daily_bets.logger import logger
from daily_bets.utils import batch_calls
from daily_bets.models import (
    BetAnalysis,
    BetAnalysisInput,
    NflPlayer,
    NflTeam,
    SportEvent,
)

# For mapping keys -> "stat" strings
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


async def load_nfl_players_from_db(pool: Pool) -> dict[str, NflPlayer]:
    """
    Returns a dict mapping LOWERCASE player_name -> NflPlayer
    from your v3_nfl_players table.
    """
    query = """
        SELECT *
        FROM v3_nfl_players
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    players: dict[str, NflPlayer] = {}
    for row in rows:
        normalized = row["name"].strip().lower()
        players[normalized] = NflPlayer.model_validate(dict(row))
    return players


async def load_nfl_teams_from_db(pool: Pool) -> dict[str, NflTeam]:
    """
    Returns a dict mapping team_id (string) -> NflTeam,
    from v3_nfl_teams, where 'id' is numeric primary key.
    """
    query = """
        SELECT *
        FROM v3_nfl_teams
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    teams: dict[str, NflTeam] = {}
    for row in rows:
        t_id = row["id"]
        teams[t_id] = NflTeam.model_validate(dict(row))
    return teams


async def fetch_sport(
    client: httpx.AsyncClient, sport: str, url: str, params: QueryParamTypes
) -> list[SportEvent]:
    """
    Fetch events from The Odds API. Return a list of SportEvent objects.
    """
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    logger.info(f"Found {len(data)} {sport} events.")
    return [SportEvent(**ev) for ev in data]


async def analyze_bet(
    client: httpx.AsyncClient,
    player: NflPlayer,
    home_code: str,
    away_code: str,
    stat_type: str,
    line_val: float,
    nfl_teams_dict: dict[str, NflTeam],
) -> BetAnalysis:
    # 1) Lookup the player's actual team code
    if player.team_id not in nfl_teams_dict:
        msg = f"Team ID={player.team_id} for player {player.name} not found in DB!"
        logger.warning(msg)
        raise ValueError(msg)

    player_team = nfl_teams_dict[player.team_id]
    player_team_code = player_team.team_code  # e.g. "BUF" or "KC"

    # 2) Determine the opponent code:
    if player_team_code == home_code:
        opponent_code = away_code
    else:
        opponent_code = home_code

    req = BetAnalysisInput(
        player_id=player.id,
        team_code=player_team_code,
        stat=stat_type,
        line=line_val,
        opponent_abv=opponent_code,
    )

    api_url = os.environ["NFL_ANALYSIS_API_URL"]
    headers = {"Content-Type": "application/json"}

    logger.info(f"Calling NFL backend for {player.name}: {req.model_dump()}")
    r = await client.post(api_url, json=req.model_dump(), headers=headers)
    r.raise_for_status()

    raw_data = r.json()
    logger.info(f"Backend success for {player.name}: {raw_data}")

    bet_analysis = BetAnalysis.model_validate(raw_data)
    return bet_analysis


async def fetch_game_bets(
    client: httpx.AsyncClient,
    event: SportEvent,
    i: int,
    nfl_player_dict: dict[str, NflPlayer],
    nfl_teams_dict: dict[str, NflTeam],
) -> list[tuple[BetAnalysis, float] | Exception]:
    """
    Fetch single-game odds from The Odds API,
    then analyze each player's bet with your NFL backend.
    """
    logger.info(
        f"[{i}] {event.sport_key} | {event.away_team} @ {event.home_team} | {event.commence_time} | {event.id}"
    )

    # We find the home_code & away_code by matching the event's home_team/away_team
    # to v3_nfl_teams.name (ignoring case).
    home_code = None
    away_code = None

    # Walk through all known teams, see if we can match the event's home_team/away_team
    for team in nfl_teams_dict.values():
        if team.name.lower() == event.home_team.lower():
            home_code = team.team_code
        if team.name.lower() == event.away_team.lower():
            away_code = team.team_code

    if not home_code or not away_code:
        logger.warning(
            f"Could not find codes for home={event.home_team} or away={event.away_team}"
        )
        return []

    # Now fetch single-event odds
    single_odds_url = f"https://api.the-odds-api.com/v4/sports/{event.sport_key}/events/{event.id}/odds"
    odds_params = {
        "apiKey": os.environ["API_KEY"],
        "regions": "us_dfs",
        "markets": ",".join(MARKET_TO_STAT.keys()),
        "oddsFormat": "decimal",
    }

    odds_resp = await client.get(single_odds_url, params=odds_params)
    odds_resp.raise_for_status()
    game_data = odds_resp.json()

    if "bookmakers" not in game_data:
        logger.warning(f"No bookmakers found for event {event.id}")
        return []

    tasks_input: list[tuple[str, float, float, str, str]] = []

    # We'll define a closure that references `home_code`, `away_code`, `nfl_teams_dict`
    async def analyze_outcome(
        player_name_raw: str,
        line_val: float,
        price_val: float,
        stat_type_str: str,
    ) -> tuple[BetAnalysis, float]:
        """
        Called by batch_calls for each outcome
        """
        normalized_name = player_name_raw.lower()
        if normalized_name not in nfl_player_dict:
            msg = f"Player not found in DB: {player_name_raw}"
            logger.warning(msg)
            raise ValueError(msg)
        player = nfl_player_dict[normalized_name]

        # Now analyze the bet, with robust team/opponent logic

        bet_analysis = await analyze_bet(
            client,
            player,
            home_code,
            away_code,
            stat_type_str,
            line_val,
            nfl_teams_dict,
        )
        # bet_analysis.price_val = price_val
        return bet_analysis, price_val

    # Build tasks_input from the "bookmakers" data
    for bookmaker in game_data.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            stat_type_str = MARKET_TO_STAT.get(market["key"])
            if stat_type_str is None:
                logger.warning(f"Unknown market key: {market['key']}")
                continue

            for outcome in market.get("outcomes", []):
                player_name_raw = outcome["description"]
                line_val = outcome["point"]
                price_val = outcome["price"]
                over_under_str = outcome["name"]  # "Over" or "Under"

                tasks_input.append(
                    (
                        player_name_raw,
                        line_val,
                        price_val,
                        over_under_str,
                        stat_type_str,
                    )
                )

    if not tasks_input:
        logger.info(f"No outcomes to analyze for event {event.id}")
        return []

    # Now batch_calls with batch_size=10 or 200
    return await batch_calls(tasks_input, analyze_outcome, batch_size=50)


async def run(pool: Pool):
    logger.info("Starting NFL analysis")

    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=7) - timedelta(seconds=1)

    events_url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events"
    params_events = {
        "apiKey": os.environ["API_KEY"],
        "commenceTimeFrom": day_start.isoformat().replace("+00:00", "Z"),
        "commenceTimeTo": day_end.isoformat().replace("+00:00", "Z"),
    }

    async with httpx.AsyncClient(timeout=30) as client:
        events = await fetch_sport(
            client, "americanfootball_nfl", events_url, params_events
        )
        logger.debug(f"{events=}")
        if not events:
            logger.error(f"No events for NFL in {day_start} to {day_end}")
            return

        # Load players + teams
        nfl_player_dict, nfl_teams_dict = await asyncio.gather(
            load_nfl_players_from_db(pool),
            load_nfl_teams_from_db(pool),
        )

        all_analysis: list[tuple[BetAnalysis, float] | Exception] = []

        # For each event, fetch bets
        for i, event in enumerate(events, start=1):
            results = await fetch_game_bets(
                client, event, i, nfl_player_dict, nfl_teams_dict
            )
            if results:
                all_analysis.extend(results)

        successes = [r for r in all_analysis if not isinstance(r, Exception)]
        logger.info(
            f"Finished analyzing NFL. Good results = {len(successes)} / {len(all_analysis)} total"
        )

    # Insert them into `nfl_daily_bets`
    async with pool.acquire() as conn:
        records: list[tuple[BetAnalysis, float]] = []

        copy_res = await conn.copy_records_to_table(
            "v2_nfl_daily_bets",
            columns=[
                "analysis",
                "price",
            ],
            records=list(
                map(
                    lambda tup: (tup[0].model_dump_json(), tup[1]),
                    successes,
                )
            ),
        )
        logger.info(copy_res)
        logger.info(f"Inserted {len(records)} rows into v2_nfl_daily_bets.")
