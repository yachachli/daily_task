import os
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

for key in ('DB_NAME','DB_USER','DB_PASS','DB_HOST','API_KEY','NBA_ANALYSIS_API_URL',
            'NBA_ALT_ANALYSIS_API_URL','NFL_ANALYSIS_API_URL','MLB_ANALYSIS_API_URL','WNBA_ANALYSIS_API_URL'):
    os.environ.setdefault(key, 'test')

from neverraise import Err, Ok
from daily_bets.analysis import nfl
from daily_bets.errors import SkipBetError


class RunFailureTests(unittest.IsolatedAsyncioTestCase):
    async def run_results(self, results):
        @asynccontextmanager
        async def acquire():
            yield object()
        pool = SimpleNamespace(acquire=acquire)
        self.upsert = AsyncMock(return_value=1)
        self.backup = AsyncMock(return_value=0)
        with patch.object(nfl.NflMap, 'from_db', AsyncMock(return_value=object())), \
             patch.object(nfl, 'get_analysis_params', AsyncMock(return_value=[])), \
             patch.object(nfl, 'filter_existing_analysis_params', AsyncMock(return_value=[])), \
             patch.object(nfl, 'batch_calls_result_async', AsyncMock(return_value=results)), \
             patch.object(nfl.db, 'nfl_dedupe_recent_analysis', AsyncMock(return_value=0)), \
             patch.object(nfl.db, 'nfl_upsert_analysis', self.upsert), \
             patch.object(nfl.nfl_backup, 'run_backup_maintenance', self.backup), \
             patch.object(nfl.Env, 'TRANSLATE_ES_URL', ''):
            await nfl.run(pool)

    async def test_total_service_failure_fails_run(self):
        with self.assertRaisesRegex(RuntimeError, 'failed for 2 outcomes'):
            await self.run_results([Err(RuntimeError('500')), Err(RuntimeError('500'))])
        self.upsert.assert_not_awaited()
        self.backup.assert_awaited_once()

    async def test_partial_failure_preserves_success_before_failing(self):
        pick = SimpleNamespace(analysis='{}',price=1.9,game_time='time',game_tag='DAL@PHI')
        with self.assertRaisesRegex(RuntimeError, 'published 1 records'):
            await self.run_results([Ok(pick), Err(RuntimeError('500'))])
        self.upsert.assert_awaited_once()
        self.backup.assert_awaited_once()

    async def test_intentional_skips_and_empty_runs_succeed(self):
        await self.run_results([Err(SkipBetError('rookie'))])
        await self.run_results([])
