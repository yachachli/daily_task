import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from daily_bets.publisher_guard import guarded_publish, retry_publish


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_does_not_retry(self):
        op = AsyncMock(return_value={"inserted": 10})
        delay = AsyncMock()
        self.assertEqual(await retry_publish(op, sleep=delay), {"inserted": 10})
        self.assertEqual(op.await_count, 1)
        delay.assert_not_awaited()

    async def test_transient_failure_recovers_once(self):
        op = AsyncMock(side_effect=[RuntimeError("upstream"), {"inserted": 10}])
        delay = AsyncMock()
        self.assertEqual(await retry_publish(op, sleep=delay), {"inserted": 10})
        self.assertEqual(op.await_count, 2)
        delay.assert_awaited_once_with(60)

    async def test_exhaustion_is_failure_not_green(self):
        op = AsyncMock(side_effect=RuntimeError("upstream"))
        with self.assertRaises(RuntimeError):
            await retry_publish(op, sleep=AsyncMock())
        self.assertEqual(op.await_count, 2)

    async def test_hung_attempt_is_cancelled_and_bounded(self):
        cancelled = []
        async def hang():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)
        with self.assertRaises(TimeoutError):
            await retry_publish(hang, timeout_seconds=.01, sleep=AsyncMock())
        self.assertEqual(len(cancelled), 2)

    async def test_external_cancel_does_not_restart_publisher(self):
        op = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await retry_publish(op, sleep=AsyncMock())
        self.assertEqual(op.await_count, 1)


class Connection:
    def __init__(self, locked):
        self.fetchval = AsyncMock(return_value=locked)
        self.execute = AsyncMock()
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass


class Pool:
    def __init__(self, locked): self.connection = Connection(locked)
    def acquire(self): return self.connection


class LockTests(unittest.IsolatedAsyncioTestCase):
    async def test_competing_publisher_is_skipped(self):
        pool, publisher = Pool(False), AsyncMock()
        with patch('daily_bets.publisher_guard.write_summary') as summary:
            await guarded_publish(pool, publisher)
        publisher.assert_not_awaited()
        pool.connection.execute.assert_not_awaited()
        summary.assert_called_once_with({"status": "skipped_active_publisher"})

    async def test_success_releases_lock(self):
        pool, publisher = Pool(True), AsyncMock(return_value={"status": "published"})
        with patch('daily_bets.publisher_guard.write_summary'):
            await guarded_publish(pool, publisher)
        publisher.assert_awaited_once_with(pool)
        pool.connection.execute.assert_awaited_once()

    async def test_failed_recovery_releases_lock(self):
        pool = Pool(True)
        with patch('daily_bets.publisher_guard.retry_publish', AsyncMock(side_effect=RuntimeError())), patch('daily_bets.publisher_guard.write_summary'):
            with self.assertRaises(RuntimeError):
                await guarded_publish(pool, AsyncMock())
        pool.connection.execute.assert_awaited_once()
