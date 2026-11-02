"""Bounded recovery for the NBA app-facing publisher; no scoring-policy changes."""
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOCK_NAMESPACE = 218960
LOCK_ID = 1


async def retry_publish(operation, *, attempts=2, timeout_seconds=3600, retry_delay=60, sleep=asyncio.sleep):
    for attempt in range(1, attempts + 1):
        print(f"NBA publication attempt {attempt}/{attempts}", flush=True)
        try:
            return await asyncio.wait_for(operation(), timeout=timeout_seconds)
        except Exception as exc:
            # Avoid printing exception text that may contain provider URLs/keys.
            print(f"NBA attempt {attempt} failed ({type(exc).__name__})", flush=True)
            if attempt == attempts:
                raise
            await sleep(retry_delay)


def write_summary(result):
    report = {"checked_at": datetime.now(timezone.utc).isoformat(), **result}
    print(json.dumps(report), flush=True)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with Path(path).open("a") as out:
            out.write("## NBA app props publication\n\n```json\n" + json.dumps(report, indent=2) + "\n```\n")


async def guarded_publish(pool, publisher):
    # Session-level DB lock also covers manual CLI runs and separate workflows.
    # Hold the same connection until publication/recovery has finished.
    async with pool.acquire() as conn:
        locked = await conn.fetchval("SELECT pg_try_advisory_lock($1, $2)", LOCK_NAMESPACE, LOCK_ID)
        if not locked:
            write_summary({"status": "skipped_active_publisher"})
            return
        try:
            result = await retry_publish(lambda: publisher(pool))
            write_summary(result)
        except BaseException:
            write_summary({"status": "failed"})
            raise
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1, $2)", LOCK_NAMESPACE, LOCK_ID)
