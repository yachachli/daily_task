import os
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

for key in ('DB_NAME','DB_USER','DB_PASS','DB_HOST','API_KEY','NBA_ANALYSIS_API_URL','NBA_ALT_ANALYSIS_API_URL','NFL_ANALYSIS_API_URL','MLB_ANALYSIS_API_URL','WNBA_ANALYSIS_API_URL'):
    os.environ.setdefault(key, 'test-only')

from neverraise import Err, Ok  # noqa: E402
from daily_bets.analysis import nba_alt  # noqa: E402
from daily_bets.odds_api import SportEvent  # noqa: E402


class FetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_events_do_not_report_empty_success(self):
        with patch.object(nba_alt, 'fetch_tomorrow_events', AsyncMock(return_value=Err(RuntimeError()))):
            with self.assertRaisesRegex(RuntimeError, 'event fetch failed'):
                await nba_alt.get_analysis_params(None, date(2030,1,2))

    async def test_offday_is_valid_empty_result(self):
        with patch.object(nba_alt, 'fetch_tomorrow_events', AsyncMock(return_value=Ok([]))):
            self.assertEqual(await nba_alt.get_analysis_params(None, date(2030,1,2)), [])

    async def test_failed_market_fetch_is_failure(self):
        event = SportEvent('one','basketball_nba','NBA','2030-01-01T20:00:00Z','Boston Celtics','Dallas Mavericks')
        with patch.object(nba_alt, 'fetch_tomorrow_events', AsyncMock(return_value=Ok([event]))), patch.object(nba_alt, 'fetch_game', AsyncMock(return_value=Err(RuntimeError()))):
            with self.assertRaisesRegex(RuntimeError, 'market fetch failed'):
                await nba_alt.get_analysis_params(None, date(2030,1,2))

    async def test_started_events_are_not_republished(self):
        event = SportEvent('one','basketball_nba','NBA','2020-01-01T20:00:00Z','Boston Celtics','Dallas Mavericks')
        fetch = AsyncMock()
        with patch.object(nba_alt, 'fetch_tomorrow_events', AsyncMock(return_value=Ok([event]))), patch.object(nba_alt, 'fetch_game', fetch):
            self.assertEqual(await nba_alt.get_analysis_params(None, date(2030,1,2)), [])
        fetch.assert_not_awaited()

class PublicationTests(unittest.IsolatedAsyncioTestCase):
    async def run_case(self, responses, remaining):
        from contextlib import ExitStack
        from types import SimpleNamespace
        connection = AsyncMock()
        acquire = AsyncMock()
        acquire.__aenter__.return_value = connection
        pool = SimpleNamespace(acquire=lambda: acquire)
        event = SportEvent('one','basketball_nba','NBA','2030-01-01T20:00:00Z','Boston Celtics','Dallas Mavericks')
        outcome = SimpleNamespace(description='Player')
        params = [(event, outcome, 'points')]
        with ExitStack() as stack:
            for target, value in [
                ('NbaAltMap.from_db', AsyncMock(return_value=object())),
                ('get_analysis_params', AsyncMock(return_value=params)),
                ('resolve_player_context', lambda *args: (1,'BOS','DAL','DAL@BOS')),
                ('filter_existing_analysis_params', AsyncMock(side_effect=[params, remaining])),
                ('batch_calls_result_async', AsyncMock(return_value=responses)),
                ('db.nba_dedupe_recent_analysis', AsyncMock(return_value=0)),
                ('db.nba_upsert_analysis', AsyncMock(return_value=1)),
                ('nba_backup.run_backup_maintenance', AsyncMock(return_value=0)),
                ('Env.TRANSLATE_ES_URL', ''),
            ]:
                stack.enter_context(patch('daily_bets.analysis.nba_alt.'+target, value))
            return await nba_alt.run(pool)

    async def test_all_scoring_errors_fail_job(self):
        with self.assertRaisesRegex(RuntimeError, 'scoring incomplete'):
            await self.run_case([Err(RuntimeError())], [])

    async def test_missing_insert_fails_verification(self):
        from types import SimpleNamespace
        row = SimpleNamespace(analysis='{}',price=1.9,game_time=None,game_tag='DAL@BOS')
        with self.assertRaisesRegex(RuntimeError, 'publication incomplete'):
            await self.run_case([Ok(row)], [object()])

    async def test_persisted_results_report_success(self):
        from types import SimpleNamespace
        row = SimpleNamespace(analysis='{}',price=1.9,game_time=None,game_tag='DAL@BOS')
        result = await self.run_case([Ok(row)], [])
        self.assertEqual(result['status'], 'published')
        self.assertEqual(result['inserted'], 1)
