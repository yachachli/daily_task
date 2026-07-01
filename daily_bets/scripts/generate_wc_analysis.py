"""Generate "deep analysis" for World Cup predictions and store it in Neon.

Reads predictions that lack a current analysis, calls the bestbet_backend
``wc_analysis`` OpenAI endpoint for each, and writes the result back to
``predictions.analysis``.

Idempotent: it creates the ``analysis`` columns if missing and only
(re)generates rows whose analysis is absent or older than the prediction, so
it's safe to run on a daily cron alongside the prediction refresh.

Env:
  DB_NAME, DB_USER, DB_PASS, DB_HOST  -- Neon (bestbetdb)
  WC_ANALYSIS_API_URL                 -- optional; defaults to the deployed fn

Run from the project root:
  python -m daily_bets.scripts.generate_wc_analysis
"""

from __future__ import annotations

import asyncio
import json
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
WC_ANALYSIS_API_URL = (
    os.environ.get("WC_ANALYSIS_API_URL")
    or "https://us-central1-bestbet-d4d6b.cloudfunctions.net/wc_analysis"
)

MIGRATE_SQL = """
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS analysis TEXT;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS analysis_generated_at TIMESTAMPTZ;
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS analysis_es TEXT;
"""

# Fixtures with a real (non-degenerate) prediction whose analysis is missing
# or stale relative to when the prediction was (re)generated.
SELECT_SQL = """
SELECT p.fixture_id, f.team_a_name, f.team_b_name,
       p.prob_a_win, p.prob_draw, p.prob_b_win, p.xg_a, p.xg_b,
       p.most_likely_score, p.full_report
FROM predictions p
JOIN wc2026_fixtures f ON f.fixture_id = p.fixture_id
WHERE NOT (p.xg_a = 0 AND p.xg_b = 0)
  AND (
      p.analysis IS NULL
      OR p.analysis_generated_at IS NULL
      OR p.analysis_generated_at < p.predicted_at
  )
ORDER BY f.scheduled_at ASC
"""

UPDATE_SQL = """
UPDATE predictions SET analysis = $1, analysis_es = $2, analysis_generated_at = now()
WHERE fixture_id = $3
"""


def _float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _full_report(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


async def main() -> int:
    print("=" * 78)
    print("GENERATE WORLD CUP DEEP ANALYSIS")
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
        logger.info("Fixtures needing analysis: %d", len(rows))

        async with httpx.AsyncClient(timeout=60.0) as client:
            for r in rows:
                payload = {
                    "team_a": r["team_a_name"],
                    "team_b": r["team_b_name"],
                    "prob_a_win": _float(r["prob_a_win"]),
                    "prob_draw": _float(r["prob_draw"]),
                    "prob_b_win": _float(r["prob_b_win"]),
                    "xg_a": _float(r["xg_a"]),
                    "xg_b": _float(r["xg_b"]),
                    "most_likely_score": r["most_likely_score"],
                    "full_report": _full_report(r["full_report"]),
                }
                try:
                    resp = await client.post(WC_ANALYSIS_API_URL, json=payload)
                    resp.raise_for_status()
                    analysis = resp.json().get("analysis")
                    analysis_es = resp.json().get("analysis_es")
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Analysis failed for fixture %s (%s vs %s): %s",
                        r["fixture_id"], r["team_a_name"], r["team_b_name"], exc,
                    )
                    failed += 1
                    continue

                if not analysis:
                    failed += 1
                    continue

                await conn.execute(UPDATE_SQL, analysis, analysis_es, r["fixture_id"])
                generated += 1
                logger.info(
                    "Analyzed fixture %s: %s vs %s",
                    r["fixture_id"], r["team_a_name"], r["team_b_name"],
                )
    finally:
        await conn.close()

    print(f"\nGenerated {generated} analyses, {failed} failed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
