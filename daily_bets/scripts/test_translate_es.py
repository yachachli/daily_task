"""
DEV/TEST ONLY — tests the translate_analysis_es_test Firebase function end-to-end.

Grabs real analysis rows from Neon, posts them to the translate function,
prints the result, and optionally writes analysis_es back to verify the
full pipeline before wiring it into the daily_task cron.

Usage:
    uv run -m daily_bets.scripts.test_translate_es --url <FIREBASE_URL> [--write] [--table v2_mlb_daily_bets] [--limit 2]

Arguments:
    --url     URL of translate_analysis_es_test Firebase function (required)
    --table   Which table to pull a sample row from (default: v2_mlb_daily_bets)
    --limit   How many rows to translate (default: 2)
    --write   If set, write analysis_es back to Neon (default: dry-run only)
"""

import argparse
import asyncio
import json
import sys

import asyncpg
import httpx

from daily_bets.env import Env


async def run(url: str, table: str, limit: int, write: bool) -> None:
    conn = await asyncpg.connect(
        database=Env.DB_NAME,
        user=Env.DB_USER,
        password=Env.DB_PASS,
        host=Env.DB_HOST,
    )
    rows = await conn.fetch(
        f"SELECT id, game_tag, game_time, analysis FROM {table} "
        f"WHERE analysis IS NOT NULL "
        f"ORDER BY id DESC LIMIT $1",
        limit,
    )

    if not rows:
        print(f"No rows found in {table}")
        return

    async with httpx.AsyncClient() as client:
        for row in rows:
            analysis = row["analysis"]
            analysis_json = json.dumps(analysis) if isinstance(analysis, dict) else analysis
            short_answer = (analysis if isinstance(analysis, dict) else json.loads(analysis)).get("short_answer", "")

            print(f"\n--- id={row['id']} | {row['game_tag']} ---")
            print(f"English short_answer: {short_answer[:80]}...")

            res = await client.post(url, json={"analysis": analysis_json}, timeout=60)
            if not res.is_success:
                print(f"FAILED: {res.status_code} {res.text}")
                continue

            analysis_es = res.json().get("analysis_es")
            if not analysis_es:
                print(f"FAILED: no analysis_es in response: {res.text}")
                continue

            es = json.loads(analysis_es)
            print(f"Spanish short_answer: {es.get('short_answer', '')[:80]}...")
            print(f"Spanish long_answer:  {es.get('long_answer', '')[:80]}...")

            if write:
                await conn.execute(
                    f"UPDATE {table} SET analysis_es = $1 "
                    f"WHERE id = $2",
                    analysis_es,
                    row["id"],
                )
                print(f"  → wrote analysis_es to Neon (id={row['id']})")
            else:
                print("  → dry-run, not writing (pass --write to persist)")

    await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="URL of translate_analysis_es_test")
    parser.add_argument("--table", default="v2_mlb_daily_bets")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.table, args.limit, args.write))


if __name__ == "__main__":
    main()
