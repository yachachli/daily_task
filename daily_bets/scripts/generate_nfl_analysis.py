"""Generate "deep analysis" for NFL game predictions and store it in Neon.

Reads upcoming rows from ``nfl_model_v2.game_predictions`` whose analysis is
missing or stale, calls the bestbet_backend ``nfl_analysis`` OpenAI endpoint
for each, and writes the result back to
``nfl_model_v2.game_predictions.explanation`` / ``explanation_es``.

The model repo (nfl-propgpt-model) owns the table and writes a prediction row
per (game, model_version) at card time; this job upgrades the explanation to
the LLM version. Staleness is tracked via ``analysis_generated_at`` vs the
prediction's ``made_at``, so re-predicted games get fresh analysis. Safe to run
on a cron several times a week.

The table may not exist yet on a fresh DB (it is created by the model repo's
Alembic migrations); in that case the job logs and exits 0.

Env:
  DB_NAME, DB_USER, DB_PASS, DB_HOST  -- Neon (bestbetdb)
  NFL_ANALYSIS_GAME_API_URL           -- optional; defaults to the deployed fn
                                         (NOT ``NFL_ANALYSIS_API_URL``, which is
                                         the player-prop function)
  NFL_ANALYSIS_DRY_RUN=1              -- print payloads; no POST, no UPDATE

Run from the project root:
  python -m daily_bets.scripts.generate_nfl_analysis [--dry-run]
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Any

try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    pass

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# `or` (not a default arg) so an empty env value still falls back to the URL.
NFL_ANALYSIS_GAME_API_URL = (
    os.environ.get("NFL_ANALYSIS_GAME_API_URL")
    or "https://us-central1-bestbet-d4d6b.cloudfunctions.net/nfl_analysis"
)

# NOTE: every table is schema-qualified. PgBouncer (Neon pooler) drops
# search_path between statements, so bare table names are not safe here.

# Columns are owned by the model repo's Alembic migration; recreated here
# defensively so this job never races the schema.
MIGRATE_SQL = """
ALTER TABLE nfl_model_v2.game_predictions ADD COLUMN IF NOT EXISTS explanation_es TEXT;
ALTER TABLE nfl_model_v2.game_predictions ADD COLUMN IF NOT EXISTS analysis_generated_at TIMESTAMPTZ;
"""

# Upcoming predicted games whose analysis is missing or older than the
# prediction itself. Latest prediction row per game wins. Weather is the
# latest non-synthetic forecast per game, falling back to a synthetic row
# (which still carries is_dome); NULL when absent.
SELECT_SQL = """
SELECT
    p.id AS prediction_id,
    g.game_id, g.season, g.week, g.kickoff_utc, g.game_date,
    g.home_team, g.away_team,
    ht.city AS home_city, ht.name AS home_nick,
    at.city AS away_city, at.name AS away_nick,
    p.p_home_win, p.ml_pick, p.ml_edge_pct, p.ml_grade,
    p.predicted_spread, p.vegas_spread, p.spread_pick, p.spread_grade, p.spread_edge_pts,
    p.predicted_total, p.vegas_total, p.total_pick, p.total_grade, p.total_edge_pts,
    p.vegas_home_moneyline, p.vegas_away_moneyline,
    p.is_bettable, p.sanity_flag,
    w.temp_f, w.wind_sustained_mph, w.wind_gust_mph,
    w.precip_prob, w.precip_type, w.is_dome
FROM (
    SELECT DISTINCT ON (game_id) *
    FROM nfl_model_v2.game_predictions
    ORDER BY game_id, made_at DESC
) p
JOIN nfl_model_v2.games g ON g.game_id = p.game_id
LEFT JOIN nfl_model_v2.teams ht ON ht.team_abv = g.home_team
LEFT JOIN nfl_model_v2.teams at ON at.team_abv = g.away_team
LEFT JOIN LATERAL (
    SELECT wf.temp_f, wf.wind_sustained_mph, wf.wind_gust_mph,
           wf.precip_prob, wf.precip_type, wf.is_dome
    FROM nfl_model_v2.weather_forecasts wf
    WHERE wf.game_id = g.game_id
    ORDER BY COALESCE(wf.synthetic, false) ASC, wf.pulled_at DESC NULLS LAST, wf.id DESC
    LIMIT 1
) w ON true
WHERE (
      p.p_home_win IS NOT NULL
      OR p.predicted_spread IS NOT NULL
      OR p.predicted_total IS NOT NULL
  )
  AND (
      (g.kickoff_utc IS NOT NULL AND g.kickoff_utc >= now())
      OR (
          g.kickoff_utc IS NULL
          AND g.game_date >= to_char((now() AT TIME ZONE 'America/New_York')::date, 'YYYYMMDD')
      )
  )
  AND (
      p.analysis_generated_at IS NULL
      OR p.analysis_generated_at < p.made_at
  )
ORDER BY g.kickoff_utc ASC NULLS LAST, g.game_date ASC, g.game_id ASC
"""

# Last-10 W-L record per team across seasons, e.g. "7-3" (ties not counted).
LAST_10_SQL = """
SELECT team_abv,
       count(*) FILTER (WHERE home_pts > away_pts AND is_home
                           OR away_pts > home_pts AND NOT is_home) AS wins,
       count(*) FILTER (WHERE home_pts < away_pts AND is_home
                           OR away_pts < home_pts AND NOT is_home) AS losses
FROM (
    SELECT t.team_abv,
           g.home_team = t.team_abv AS is_home,
           g.home_score AS home_pts,
           g.away_score AS away_pts,
           ROW_NUMBER() OVER (
               PARTITION BY t.team_abv
               ORDER BY g.game_date DESC, g.kickoff_utc DESC NULLS LAST, g.game_id DESC
           ) AS rn
    FROM unnest($1::text[]) AS t(team_abv)
    JOIN nfl_model_v2.games g
        ON (g.home_team = t.team_abv OR g.away_team = t.team_abv)
       AND g.home_score IS NOT NULL
       AND g.away_score IS NOT NULL
) x
WHERE rn <= 10
GROUP BY team_abv
"""

# Up to 3 highest-|impact| injuries per team for the selected games (best
# effort). direct_point_impact is the starter-vs-backup gap: positive means the
# team loses that many points with the player out. Negligible rows are dropped.
INJURIES_SQL = """
SELECT game_id, team_abv, long_name, position, direct_point_impact, alert_level
FROM (
    SELECT ii.game_id, ii.team_abv, pl.long_name, pl.position,
           ii.direct_point_impact, ii.alert_level,
           ROW_NUMBER() OVER (
               PARTITION BY ii.game_id, ii.team_abv
               ORDER BY abs(COALESCE(ii.direct_point_impact, 0)) DESC, ii.id DESC
           ) AS rn
    FROM nfl_model_v2.injury_impacts ii
    LEFT JOIN nfl_model_v2.players pl ON pl.player_id = ii.player_id
    WHERE ii.game_id = ANY($1::text[])
      AND abs(COALESCE(ii.direct_point_impact, 0)) >= 0.25
) x
WHERE rn <= 3
ORDER BY game_id, team_abv, rn
"""

UPDATE_SQL = """
UPDATE nfl_model_v2.game_predictions
SET explanation = $1, explanation_es = $2, analysis_generated_at = now()
WHERE id = $3
"""


def _float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# Fallback when nfl_model_v2.teams has no city/name populated.
TEAM_NAMES: dict[str, str] = {
    "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills", "CAR": "Carolina Panthers", "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns", "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers",
    "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs", "LAC": "Los Angeles Chargers", "LAR": "Los Angeles Rams",
    "LV": "Las Vegas Raiders", "MIA": "Miami Dolphins", "MIN": "Minnesota Vikings",
    "NE": "New England Patriots", "NO": "New Orleans Saints", "NYG": "New York Giants",
    "NYJ": "New York Jets", "PHI": "Philadelphia Eagles", "PIT": "Pittsburgh Steelers",
    "SEA": "Seattle Seahawks", "SF": "San Francisco 49ers", "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans", "WAS": "Washington Commanders",
}  # fmt: skip


def _team_name(city: object, nick: object, abbr: str) -> str:
    parts = [str(p) for p in (city, nick) if p]
    if parts:
        return " ".join(parts)
    return TEAM_NAMES.get(abbr.upper(), abbr)


def _injury_line(r: asyncpg.Record) -> str:
    """e.g. "IND: Alec Pierce (WR) red alert, -2.9 pts" (negative = hurts team)."""
    name = r["long_name"] or "Unknown player"
    pos = f" ({r['position']})" if r["position"] else ""
    level = str(r["alert_level"]).lower() if r["alert_level"] else "unknown"
    impact = _float(r["direct_point_impact"])
    impact_str = f", {-impact:+.1f} pts" if impact is not None else ""
    return f"{r['team_abv']}: {name}{pos} {level} alert{impact_str}"


def _build_payload(
    r: asyncpg.Record,
    form_by_team: dict[str, str],
    injuries_by_game: dict[str, list[str]],
) -> dict[str, Any]:
    home: str = r["home_team"]
    away: str = r["away_team"]
    predicted_winner: str | None = None
    if r["ml_pick"] == "home":
        predicted_winner = home
    elif r["ml_pick"] == "away":
        predicted_winner = away

    weather: dict[str, Any] | None = None
    if any(
        r[k] is not None
        for k in ("temp_f", "wind_sustained_mph", "precip_prob", "is_dome")
    ):
        weather = {
            "temp_f": _float(r["temp_f"]),
            "wind_mph": _float(r["wind_sustained_mph"]),
            "wind_gust_mph": _float(r["wind_gust_mph"]),
            "precip_pct": _float(r["precip_prob"]),
            "precip_type": r["precip_type"],
            "is_dome": r["is_dome"],
        }

    kickoff = r["kickoff_utc"]
    return {
        "home_team": home,
        "away_team": away,
        "home_name": _team_name(r["home_city"], r["home_nick"], home),
        "away_name": _team_name(r["away_city"], r["away_nick"], away),
        "season": _int(r["season"]),
        "week": _int(r["week"]),
        "kickoff_utc": kickoff.isoformat() if kickoff is not None else None,
        "p_home_win": _float(r["p_home_win"]),
        "predicted_winner": predicted_winner,
        "ml_pick": r["ml_pick"],
        "ml_edge_pct": _float(r["ml_edge_pct"]),
        "ml_grade": r["ml_grade"],
        "predicted_spread": _float(r["predicted_spread"]),
        "vegas_spread": _float(r["vegas_spread"]),
        "spread_pick": r["spread_pick"],
        "spread_grade": r["spread_grade"],
        "spread_edge_pts": _float(r["spread_edge_pts"]),
        "predicted_total": _float(r["predicted_total"]),
        "vegas_total": _float(r["vegas_total"]),
        "total_pick": r["total_pick"],
        "total_grade": r["total_grade"],
        "total_edge_pts": _float(r["total_edge_pts"]),
        "vegas_home_moneyline": _int(r["vegas_home_moneyline"]),
        "vegas_away_moneyline": _int(r["vegas_away_moneyline"]),
        "home_last_10": form_by_team.get(home),
        "away_last_10": form_by_team.get(away),
        "weather": weather,
        "is_bettable": bool(r["is_bettable"]),
        "sanity_flag": r["sanity_flag"],
        "key_injuries": injuries_by_game.get(r["game_id"], []),
    }


async def _fetch_injuries(
    conn: asyncpg.Connection[asyncpg.Record], game_ids: list[str]
) -> dict[str, list[str]]:
    """Best effort: an empty dict on any failure so analysis still runs."""
    out: dict[str, list[str]] = defaultdict(list)
    try:
        rows: list[asyncpg.Record] = await conn.fetch(INJURIES_SQL, game_ids)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Injury lookup failed (continuing without): %s", exc)
        return {}
    for ir in rows:
        out[ir["game_id"]].append(_injury_line(ir))
    return dict(out)


async def main() -> int:
    dry_run = "--dry-run" in sys.argv[1:] or os.environ.get(
        "NFL_ANALYSIS_DRY_RUN", ""
    ).strip().lower() in {"1", "true", "yes"}

    print("=" * 78)
    print("GENERATE NFL GAME DEEP ANALYSIS" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 78)
    verb = "Would generate" if dry_run else "Generated"

    conn = await asyncpg.connect(
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        host=os.environ["DB_HOST"],
    )

    generated = 0
    failed = 0
    try:
        try:
            if not dry_run:
                _ = await conn.execute(MIGRATE_SQL)
            rows = await conn.fetch(SELECT_SQL)
        except asyncpg.UndefinedTableError as exc:
            logger.warning(
                "nfl_model_v2.game_predictions is not available yet (%s); "
                "nothing to do.",
                exc,
            )
            print(f"\n{verb} 0 analyses, 0 failed.")
            return 0

        logger.info("Games needing analysis: %d", len(rows))
        if not rows:
            print(f"\n{verb} 0 analyses, 0 failed.")
            return 0

        game_ids: list[str] = [r["game_id"] for r in rows]
        team_abvs = sorted(
            {r["home_team"] for r in rows} | {r["away_team"] for r in rows}
        )
        form_rows = await conn.fetch(LAST_10_SQL, team_abvs)
        form_by_team: dict[str, str] = {
            fr["team_abv"]: f"{fr['wins']}-{fr['losses']}" for fr in form_rows
        }
        injuries_by_game = await _fetch_injuries(conn, game_ids)

        async with httpx.AsyncClient(timeout=60.0) as client:
            for r in rows:
                payload = _build_payload(r, form_by_team, injuries_by_game)
                label = f"{r['away_team']} @ {r['home_team']}"

                if dry_run:
                    print(
                        f"\n--- {r['game_id']} ({label}) prediction_id={r['prediction_id']}"
                    )
                    print(json.dumps(payload, indent=2, default=str))
                    generated += 1
                    continue

                try:
                    resp = await client.post(NFL_ANALYSIS_GAME_API_URL, json=payload)
                    _ = resp.raise_for_status()
                    body = resp.json()
                    analysis = body.get("analysis")
                    analysis_es = body.get("analysis_es")
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Analysis failed for game %s (%s): %s", r["game_id"], label, exc
                    )
                    failed += 1
                    continue

                if not analysis:
                    logger.error("Empty analysis for game %s (%s)", r["game_id"], label)
                    failed += 1
                    continue

                _ = await conn.execute(
                    UPDATE_SQL, analysis, analysis_es, r["prediction_id"]
                )
                generated += 1
                logger.info("Analyzed game %s: %s", r["game_id"], label)
    finally:
        await conn.close()

    print(f"\n{verb} {generated} analyses, {failed} failed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
