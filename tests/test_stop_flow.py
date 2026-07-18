import asyncio
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from stop_flow import (
    ACK_ACCEPTED,
    ACK_REJECTED,
    ACK_TIMEOUT,
    StopRegistry,
    execute_stop_request,
    sqlite_write_with_retry,
)


class Response:
    def __init__(self, status):
        self.status = status


class StopFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.registry = StopRegistry({})

    async def _context(self, tx_id=1):
        context, created = await self.registry.get_or_create(
            tx_id, "TW*MSI*E000100", "manual"
        )
        self.assertTrue(created)
        return context

    async def test_ack_accepted_and_stop_committed(self):
        context = await self._context()

        async def send():
            return Response("Accepted")

        async def complete():
            await asyncio.sleep(0.01)
            self.registry.complete_settlement(
                1,
                {"timestamp": "2026-07-18T00:00:00+00:00", "reason": "Remote"},
                committed=True,
            )

        asyncio.create_task(complete())
        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.1, stop_timeout=0.2
        )
        self.assertEqual(result["ack_status"], ACK_ACCEPTED)
        self.assertEqual(result["final_outcome"], "stopped")
        self.assertEqual(result["settlement_status"], "committed")

    async def test_ack_timeout_but_stop_committed_is_stopped(self):
        context = await self._context()

        async def send():
            await asyncio.sleep(1)

        async def complete():
            await asyncio.sleep(0.03)
            self.registry.complete_settlement(
                1, {"timestamp": "now", "reason": "Remote"}, committed=True
            )

        asyncio.create_task(complete())
        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.01, stop_timeout=0.2
        )
        self.assertEqual(result["ack_status"], ACK_TIMEOUT)
        self.assertEqual(result["final_outcome"], "stopped")

    async def test_ack_rejected_is_not_accepted(self):
        context = await self._context()

        async def send():
            return Response("Rejected")

        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.05, stop_timeout=0.03
        )
        self.assertEqual(result["ack_status"], ACK_REJECTED)
        self.assertEqual(result["final_outcome"], "stop_timeout")

    async def test_ack_rejected_but_stop_committed_is_stopped(self):
        context = await self._context()

        async def send():
            return Response("Rejected")

        async def complete():
            await asyncio.sleep(0.01)
            self.registry.complete_settlement(
                1, {"timestamp": "now", "reason": "Remote"}, committed=True
            )

        asyncio.create_task(complete())
        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.05, stop_timeout=0.1
        )
        self.assertEqual(result["ack_status"], ACK_REJECTED)
        self.assertEqual(result["final_outcome"], "stopped")

    async def test_ack_and_stop_timeout(self):
        context = await self._context()

        async def send():
            await asyncio.sleep(1)

        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.01, stop_timeout=0.03
        )
        self.assertEqual(result["ack_status"], ACK_TIMEOUT)
        self.assertEqual(result["stop_status"], "timeout")
        self.assertEqual(result["final_outcome"], "stop_timeout")

    async def test_stop_before_ack_is_success(self):
        context = await self._context()

        async def send():
            await asyncio.sleep(1)

        self.registry.complete_settlement(
            1, {"timestamp": "now", "reason": "Remote"}, committed=True
        )
        result = await execute_stop_request(
            context,
            send,
            lambda: False,
            ack_timeout=0.5,
            stop_timeout=0.1,
            ack_after_stop_grace=0.01,
        )
        self.assertEqual(result["final_outcome"], "stopped")
        self.assertEqual(result["ack_status"], ACK_TIMEOUT)

    async def test_settlement_failure_is_not_success(self):
        context = await self._context()

        async def send():
            return Response("Accepted")

        self.registry.complete_settlement(
            1,
            {"timestamp": "now", "reason": "Remote"},
            committed=False,
            error="commit failed",
        )
        result = await execute_stop_request(
            context, send, lambda: False, ack_timeout=0.1, stop_timeout=0.1
        )
        self.assertEqual(result["settlement_status"], "failed")
        self.assertEqual(result["final_outcome"], "settlement_failed")

    async def test_database_already_stopped_is_success_without_send(self):
        context = await self._context()
        calls = 0

        async def send():
            nonlocal calls
            calls += 1
            return Response("Accepted")

        result = await execute_stop_request(
            context, send, lambda: True, ack_timeout=0.1, stop_timeout=0.1
        )
        self.assertEqual(calls, 0)
        self.assertEqual(result["final_outcome"], "already_stopped")

    async def test_duplicate_requests_reuse_context(self):
        first, created_first = await self.registry.get_or_create(
            8, "CP", "manual"
        )
        second, created_second = await self.registry.get_or_create(
            8, "CP", "balance_estimate"
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertIs(first, second)
        self.assertEqual(list(self.registry.contexts), [8])

    async def test_context_cleanup_uses_identity_guard(self):
        context = await self._context(9)
        self.assertTrue(await self.registry.remove(9, context))
        self.assertEqual(self.registry.contexts, {})
        self.assertFalse(await self.registry.remove(9, context))


class SQLiteRetryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("CREATE TABLE values_table (value INTEGER)")
            conn.commit()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=0.01)

    def test_locked_retries_then_succeeds(self):
        attempts = 0
        retries = []

        def operation(conn):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise sqlite3.OperationalError("database is locked")
            conn.execute("INSERT INTO values_table VALUES (1)")

        sqlite_write_with_retry(
            self._connect,
            operation,
            attempts=4,
            backoff_seconds=(0, 0, 0),
            on_retry=lambda attempt, elapsed, exc: retries.append(attempt),
        )
        self.assertEqual(attempts, 3)
        self.assertEqual(retries, [1, 2])
        with closing(self._connect()) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM values_table").fetchone()[0], 1)

    def test_locked_final_failure_is_raised(self):
        def operation(conn):
            raise sqlite3.OperationalError("database is busy")

        with self.assertRaises(sqlite3.OperationalError):
            sqlite_write_with_retry(
                self._connect,
                operation,
                attempts=3,
                backoff_seconds=(0, 0),
            )

    def test_non_lock_error_is_not_retried(self):
        attempts = 0

        def operation(conn):
            nonlocal attempts
            attempts += 1
            raise sqlite3.OperationalError("no such table: missing")

        with self.assertRaises(sqlite3.OperationalError):
            sqlite_write_with_retry(self._connect, operation, attempts=5)
        self.assertEqual(attempts, 1)


if __name__ == "__main__":
    unittest.main()
