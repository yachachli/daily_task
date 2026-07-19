"""Generate "deep analysis" for MLB game predictions and store it in Neon.

Reads upcoming rows from ``propgpt_mlb.predictions`` whose analysis is missing
or stale, calls the bestbet_backend ``mlb_analysis`` OpenAI endpoint for each,
and writes the result back to ``propgpt_mlb.predictions.explanation`` /
``explanation_es``.

predict_slate (propgpt-mlb repo) writes a deterministic template into
``explanation`` at prediction time, so the app always has something to show;
this job upgrades it to the LLM version. Staleness is tracked via
``analysis_generated_at`` vs the prediction's ``made_at``, so re-predicted
games get fresh analysis. Safe to run on a daily cron.

Env:
  DB_NAME, DB_USER, DB_PASS, DB_HOST  -- Neon (bestbetdb)
  MLB_ANALYSIS_GAME_API_URL           -- optional; defaults to the deployed fn

Run from the project root:
  python -m daily_bets.scripts.generate_mlb_analysis
"""

from __future__ import annotations

import asyncio
import logging
import os

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
MLB_ANALYSIS_GAME_API_URL = (
    os.environ.get("MLB_ANALYSIS_GAME_API_URL")
    or "https://us-central1-bestbet-d4d6b.cloudfunctions.net/mlb_analysis"
)

# Columns are owned by propgpt-mlb's migrations (008_analysis_columns.sql);
# recreated here defensively so this job never races the schema.
MIGRATE_SQL = """
ALTER TABLE propgpt_mlb.predictions ADD COLUMN IF NOT EXISTS explanation_es TEXT;
ALTER TABLE propgpt_mlb.predictions ADD COLUMN IF NOT EXISTS analysis_generated_at TIMESTAMPTZ;
"""

# Upcoming predicted games whose analysis is missing or older than the
# prediction itself. Latest prediction row per game wins.
SELECT_SQL = """
SELECT
    p.id AS prediction_id,
    g.game_id,
    ht.abbr AS home_abbr, at.abbr AS away_abbr,
    ht.name AS home_name, at.name AS away_name,
    pk.name AS park_name,
    hp.full_name AS home_sp, ap.full_name AS away_sp,
    hp.throws AS home_sp_throws, ap.throws AS away_sp_throws,
    p.predicted_total, p.p_home_win,
    p.total_pick, p.total_grade, p.total_edge_runs, p.ml_pick,
    p.line_at_prediction_total AS vegas_total,
    p.line_at_prediction_ml_home AS vegas_home_moneyline,
    p.line_at_prediction_ml_away AS vegas_away_moneyline,
    w.temp_f, w.wind_mph, w.precip_pct, w.is_dome_game
FROM (
    SELECT DISTINCT ON (game_id) *
    FROM propgpt_mlb.predictions
    ORDER BY game_id, made_at DESC
) p
JOIN propgpt_mlb.games g ON g.game_id = p.game_id
JOIN propgpt_mlb.teams ht ON ht.team_id = g.home_team_id
JOIN propgpt_mlb.teams at ON at.team_id = g.away_team_id
LEFT JOIN propgpt_mlb.parks pk ON pk.park_id = g.park_id
LEFT JOIN propgpt_mlb.players hp ON hp.player_id = g.home_sp_id
LEFT JOIN propgpt_mlb.players ap ON ap.player_id = g.away_sp_id
LEFT JOIN propgpt_mlb.weather_observations w ON w.game_id = g.game_id
WHERE p.predicted_total IS NOT NULL
  AND (g.game_time_utc IS NULL OR g.game_time_utc >= now())
  AND (
      p.analysis_generated_at IS NULL
      OR p.analysis_generated_at < p.made_at
  )
ORDER BY g.game_time_utc ASC NULLS LAST
"""

# Last-10 W-L record per team, e.g. "7-3".
LAST_10_SQL = """
SELECT team_id,
       count(*) FILTER (WHERE won) AS wins,
       count(*) FILTER (WHERE NOT won) AS losses
FROM (
    SELECT t.team_id,
           CASE WHEN g.home_team_id = t.team_id
                THEN o.home_score > o.away_score
                ELSE o.away_score > o.home_score END AS won,
           ROW_NUMBER() OVER (
               PARTITION BY t.team_id
               ORDER BY g.game_date DESC, g.game_id DESC
           ) AS rn
    FROM unnest($1::bigint[]) AS t(team_id)
    JOIN propgpt_mlb.games g
        ON g.home_team_id = t.team_id OR g.away_team_id = t.team_id
    JOIN propgpt_mlb.outcomes o ON o.game_id = g.game_id
) x
WHERE rn <= 10
GROUP BY team_id
"""

UPDATE_SQL = """
UPDATE propgpt_mlb.predictions
SET explanation = $1, explanation_es = $2, analysis_generated_at = now()
WHERE id = $3
"""

TEAM_ID_SQL = """
SELECT game_id, home_team_id, away_team_id
FROM propgpt_mlb.games WHERE game_id = ANY($1::bigint[])
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


async def main() -> int:
    print("=" * 78)
    print("GENERATE MLB GAME DEEP ANALYSIS")
    print("=" * 78)

    conn = await asyncpg.connect(
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        host=os.environ["DB_HOST"],
    )

    generated = 0
    failed = 0
    try:
        await conn.execute(MIGRATE_SQL)
        rows = await conn.fetch(SELECT_SQL)
        logger.info("Games needing analysis: %d", len(rows))
        if not rows:
            return 0

        game_ids = [r["game_id"] for r in rows]
        team_rows = await conn.fetch(TEAM_ID_SQL, game_ids)
        teams_by_game = {
            tr["game_id"]: (tr["home_team_id"], tr["away_team_id"]) for tr in team_rows
        }
        team_ids = sorted({tid for pair in teams_by_game.values() for tid in pair})
        form_rows = await conn.fetch(LAST_10_SQL, team_ids)
        form_by_team = {
            fr["team_id"]: f"{fr['wins']}-{fr['losses']}" for fr in form_rows
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            for r in rows:
                home_id, away_id = teams_by_game.get(r["game_id"], (None, None))
                p_home = _float(r["p_home_win"])
                predicted_winner = None
                if r["ml_pick"] == "home":
                    predicted_winner = r["home_abbr"]
                elif r["ml_pick"] == "away":
                    predicted_winner = r["away_abbr"]
                payload = {
                    "home_team": r["home_name"] or r["home_abbr"],
                    "away_team": r["away_name"] or r["away_abbr"],
                    "park": r["park_name"],
                    "home_sp": r["home_sp"],
                    "away_sp": r["away_sp"],
                    "home_sp_throws": r["home_sp_throws"],
                    "away_sp_throws": r["away_sp_throws"],
                    "p_home_win": p_home,
                    "predicted_winner": predicted_winner,
                    "predicted_total": _float(r["predicted_total"]),
                    "vegas_total": _float(r["vegas_total"]),
                    "total_pick": r["total_pick"],
                    "total_grade": r["total_grade"],
                    "total_edge_runs": _float(r["total_edge_runs"]),
                    "vegas_home_moneyline": _int(r["vegas_home_moneyline"]),
                    "vegas_away_moneyline": _int(r["vegas_away_moneyline"]),
                    "home_last_10": form_by_team.get(home_id),
                    "away_last_10": form_by_team.get(away_id),
                    "weather": {
                        "temp_f": _float(r["temp_f"]),
                        "wind_mph": _float(r["wind_mph"]),
                        "precip_pct": _float(r["precip_pct"]),
                        "is_dome_game": r["is_dome_game"],
                    },
                }
                try:
                    resp = await client.post(MLB_ANALYSIS_GAME_API_URL, json=payload)
                    resp.raise_for_status()
                    analysis = resp.json().get("analysis")
                    analysis_es = resp.json().get("analysis_es")
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Analysis failed for game %s (%s @ %s): %s",
                        r["game_id"],
                        r["away_abbr"],
                        r["home_abbr"],
                        exc,
                    )
                    failed += 1
                    continue

                if not analysis:
                    failed += 1
                    continue

                await conn.execute(
                    UPDATE_SQL, analysis, analysis_es, r["prediction_id"]
                )
                generated += 1
                logger.info(
                    "Analyzed game %s: %s @ %s",
                    r["game_id"],
                    r["away_abbr"],
                    r["home_abbr"],
                )
    finally:
        await conn.close()

    print(f"\nGenerated {generated} analyses, {failed} failed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
