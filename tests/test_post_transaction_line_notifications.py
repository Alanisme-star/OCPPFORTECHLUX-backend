import asyncio
import unittest
from unittest.mock import patch

import main


class PostTransactionLineNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def _run_scheduler(
        self,
        *,
        send_low_balance,
        charge_error=False,
        low_balance_error=False,
    ):
        calls = []

        def send_charge_completed(transaction_id):
            calls.append("charge_completed")
            if charge_error:
                raise RuntimeError("charge completed test error")
            return {"ok": True, "status": "sent", "transactionId": transaction_id}

        def send_low_balance_mock(transaction_id):
            calls.append("low_balance")
            if low_balance_error:
                raise RuntimeError("low balance test error")
            return {"ok": True, "status": "sent", "transactionId": transaction_id}

        current_task = asyncio.current_task()
        tasks_before = asyncio.all_tasks()

        with (
            patch.object(
                main,
                "send_charge_completed_line_notification",
                side_effect=send_charge_completed,
            ),
            patch.object(
                main,
                "send_low_balance_line_notification",
                side_effect=send_low_balance_mock,
            ),
        ):
            main.schedule_post_transaction_line_notifications(
                123,
                send_low_balance=send_low_balance,
            )
            created_tasks = asyncio.all_tasks() - tasks_before
            created_tasks.discard(current_task)
            self.assertEqual(len(created_tasks), 1)

            task = next(iter(created_tasks))
            await task
            self.assertIsNone(task.exception())

        return calls

    async def test_low_balance_true_sends_in_strict_order(self):
        calls = await self._run_scheduler(send_low_balance=True)
        self.assertEqual(calls, ["charge_completed", "low_balance"])

    async def test_low_balance_false_only_sends_charge_completed(self):
        calls = await self._run_scheduler(send_low_balance=False)
        self.assertEqual(calls, ["charge_completed"])

    async def test_charge_completed_exception_still_sends_low_balance(self):
        calls = await self._run_scheduler(
            send_low_balance=True,
            charge_error=True,
        )
        self.assertEqual(calls, ["charge_completed", "low_balance"])

    async def test_low_balance_exception_does_not_escape_runner(self):
        calls = await self._run_scheduler(
            send_low_balance=True,
            low_balance_error=True,
        )
        self.assertEqual(calls, ["charge_completed", "low_balance"])


if __name__ == "__main__":
    unittest.main()
