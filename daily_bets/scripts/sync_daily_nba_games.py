import asyncio
import datetime as dt
import json
import os
from collections.abc import Sequence
from typing import Any

import asyncpg
import httpx


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.nba_game_predictions (
    id BIGSERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    predicted_winner TEXT,
    predicted_spread NUMERIC(8, 3),
    home_win_prob NUMERIC(8, 6),
    away_win_prob NUMERIC(8, 6),
    confidence TEXT,
    home_adj_em NUMERIC(10, 4),
    away_adj_em NUMERIC(10, 4),
    matchup_pace NUMERIC(10, 4),
    short_answer TEXT,
    long_answer TEXT,
    head_to_head TEXT,
    factors JSONB NOT NULL DEFAULT '[]'::jsonb,
    home_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    away_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    game_payload JSONB NOT NULL,
    source_payload JSONB NOT NULL,
    source_endpoint TEXT NOT NULL,
    github_run_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_nba_game_predictions UNIQUE (prediction_date, home_team, away_team),
    CONSTRAINT ck_nba_game_predictions_teams_diff CHECK (home_team <> away_team)
);

CREATE INDEX IF NOT EXISTS idx_nba_game_predictions_date
ON public.nba_game_predictions (prediction_date DESC);
"""

UPSERT_SQL = """
INSERT INTO public.nba_game_predictions (
    prediction_date,
    home_team,
    away_team,
    predicted_winner,
    predicted_spread,
    home_win_prob,
    away_win_prob,
    confidence,
    home_adj_em,
    away_adj_em,
    matchup_pace,
    short_answer,
    long_answer,
    head_to_head,
    factors,
    home_context,
    away_context,
    game_payload,
    source_payload,
    source_endpoint,
    github_run_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15::jsonb, $16::jsonb, $17::jsonb, $18::jsonb,
    $19::jsonb, $20, $21
)
ON CONFLICT (prediction_date, home_team, away_team)
DO UPDATE SET
    predicted_winner = EXCLUDED.predicted_winner,
    predicted_spread = EXCLUDED.predicted_spread,
    home_win_prob = EXCLUDED.home_win_prob,
    away_win_prob = EXCLUDED.away_win_prob,
    confidence = EXCLUDED.confidence,
    home_adj_em = EXCLUDED.home_adj_em,
    away_adj_em = EXCLUDED.away_adj_em,
    matchup_pace = EXCLUDED.matchup_pace,
    short_answer = EXCLUDED.short_answer,
    long_answer = EXCLUDED.long_answer,
    head_to_head = EXCLUDED.head_to_head,
    factors = EXCLUDED.factors,
    home_context = EXCLUDED.home_context,
    away_context = EXCLUDED.away_context,
    game_payload = EXCLUDED.game_payload,
    source_payload = EXCLUDED.source_payload,
    source_endpoint = EXCLUDED.source_endpoint,
    github_run_id = EXCLUDED.github_run_id,
    updated_at = NOW();
"""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_prediction_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid response date format: {value!r}. Expected YYYYMMDD."
        ) from exc


async def fetch_predictions(
    endpoint: str, bearer_token: str | None = None
) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    timeout = httpx.Timeout(timeout=300.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(endpoint, headers=headers)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Predictor response was not a JSON object.")
    return payload


def row_values(
    prediction_date: dt.date,
    prediction: dict[str, Any],
    source_payload: dict[str, Any],
    source_endpoint: str,
    github_run_id: str | None,
) -> tuple[Any, ...]:
    return (
        prediction_date,
        prediction.get("home_team"),
        prediction.get("away_team"),
        prediction.get("predicted_winner"),
        prediction.get("predicted_spread"),
        prediction.get("home_win_prob"),
        prediction.get("away_win_prob"),
        prediction.get("confidence"),
        prediction.get("home_adj_em"),
        prediction.get("away_adj_em"),
        prediction.get("matchup_pace"),
        prediction.get("short_answer"),
        prediction.get("long_answer"),
        prediction.get("head_to_head"),
        json.dumps(prediction.get("factors", [])),
        json.dumps(prediction.get("home_context", {})),
        json.dumps(prediction.get("away_context", {})),
        json.dumps(prediction),
        json.dumps(source_payload),
        source_endpoint,
        github_run_id,
    )


async def get_connection() -> asyncpg.Connection:
    database_url = os.getenv("NEON_DATABASE_URL")
    if database_url:
        return await asyncpg.connect(dsn=database_url)

    db_name = require_env("DB_NAME")
    db_user = require_env("DB_USER")
    db_pass = require_env("DB_PASS")
    db_host = require_env("DB_HOST")
    return await asyncpg.connect(
        database=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        ssl="require",
    )


async def main() -> None:
    endpoint = require_env("NBA_GAME_PREDICTOR_URL")
    bearer_token = os.getenv("NBA_GAME_PREDICTOR_BEARER_TOKEN")
    github_run_id = os.getenv("GITHUB_RUN_ID")

    payload = await fetch_predictions(endpoint, bearer_token)
    date_raw = payload.get("date")
    if not isinstance(date_raw, str):
        raise RuntimeError("Predictor response is missing string field 'date'.")

    predictions_raw = payload.get("predictions")
    if not isinstance(predictions_raw, list):
        raise RuntimeError("Predictor response is missing list field 'predictions'.")

    prediction_date = parse_prediction_date(date_raw)
    predictions: list[dict[str, Any]] = [
        p for p in predictions_raw if isinstance(p, dict)
    ]

    print(
        f"Fetched {len(predictions)} predictions for {prediction_date.isoformat()} "
        f"from {endpoint}"
    )

    conn = await get_connection()
    try:
        async with conn.transaction():
            await conn.execute(CREATE_TABLE_SQL)
            if predictions:
                rows: Sequence[tuple[Any, ...]] = [
                    row_values(
                        prediction_date=prediction_date,
                        prediction=prediction,
                        source_payload=payload,
                        source_endpoint=endpoint,
                        github_run_id=github_run_id,
                    )
                    for prediction in predictions
                ]
                await conn.executemany(UPSERT_SQL, rows)
    finally:
        await conn.close()

    print(f"Upserted {len(predictions)} rows into public.nba_game_predictions.")


if __name__ == "__main__":
    asyncio.run(main())
