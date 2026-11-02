# NBA app publisher recovery

The app-facing publisher is `daily_nba_alt_bets.yml` / `nba_alt`, which writes
`v2_nba_daily_bets`. The similarly named `nba` job writes the other table.

## November 2 investigation

The configured primary cron was `15 8 * * *` (08:15 UTC). Scheduled run
41499994332 was created at 18:22:44 UTC; its runner started at 18:22:46.
The 10h 7m delay was before workflow creation, not a loop in the Python job
or a ten-hour runner provisioning delay. The previous eleven scheduled runs
had creation times between 12:40 and 15:13 UTC. GitHub's public API does not expose
why its scheduler delivered those individual events late, so the specific
internal cause cannot be established from these timestamps.

GitHub documents that schedule events can be delayed or dropped under load:
https://docs.github.com/en/actions/how-tos/troubleshoot-workflows

At 18:27 UTC the NBA scoring services had served 471 successful POSTs since
18:22:44, with no HTTP failures in that sample. Yesterday's app-facing job ran
15:10:47–15:52:31 UTC (41m44s). The implementation finishes scoring before
inserting rows and performs Spanish translation during insertion, so an
empty feed during the first few minutes was not evidence of a hung job.

## Active safeguards

- Primary remains 08:15 UTC. Four backup triggers: 11:37, 14:37, 17:37,
  and 20:37 UTC daily. In Pacific standard time these are 00:15, 03:37,
  06:37, 09:37, and 12:37. UTC schedules shift an hour locally during DST.
- Each backup reads current provider markets and skips already stored
  player/game/stat/line keys; new markets can still be published.
- Shared GitHub concurrency group with cancel-in-progress disabled.
- A session-level PostgreSQL advisory lock prevents concurrent guarded CLI
  publishers too. Jobs already running older code are not retroactively locked.
- Each scoring/publication attempt is bounded to 60 minutes. One automatic
  retry after 60 seconds retains completed inserts. Workflow timeout: 135 minutes.
- Event/market fetch failures, entirely unresolved available markets, failed
  scoring calls, and missing stored results now fail the job instead of
  silently reporting success. Legitimate no-market/offday results remain valid.
- Per-run GitHub step summary distinguishes publication, no new resolvable
  markets, active-publisher skip, and failure. Exhausted recovery emits an
  Actions error; subscriber notification preferences still control emails.

No model/scoring weights, confidence thresholds, odds inputs, or translations
were changed. The normal publisher still fetches and analyses current markets;
there are no synthetic props or copies of yesterday's picks.

## Limits and next step

These safeguards intentionally stay inside GitHub. All scheduled backups can
be delayed by the same scheduler. An external Cloud Scheduler watchdog remains
the stronger protection against that failure mode; it was not enabled by
request. New safeguards do not cancel old runs or promise a publication time.

A valid no-market result is not a promise that bookmakers will post no lines
later. Subsequent backups recheck. Partial unresolved player identities are
logged by the existing filter; they are not fabricated. API response/device
rendering is separate from verifying that analyzed rows reached the DB.

Tests: `uv run python -m unittest discover -s tests -v`.
Manual recovery: dispatch `daily_nba_alt_bets.yml` on main; do not run `nba`
expecting it to fill the app-facing table.
