import asyncio
import json
import os
import time
import unittest
from types import SimpleNamespace


if not os.environ.get("DATABASE_PATH"):
    raise RuntimeError("DATABASE_PATH must point to a temporary test database")

import main


CP_ID = "TW*TEST*STOP0001"
CARD_ID = "TEST-CARD"


class Response:
    def __init__(self, status):
        self.status = status


class FakeChargePoint:
    def __init__(self, *, status="Accepted", wait_event=None, instance_id="A", seq=1):
        self.status = status
        self.wait_event = wait_event
        self.connection_instance_id = instance_id
        self.connection_seq = seq
        self.calls = 0
        self.unique_ids = []

    async def call(self, payload, unique_id=None):
        self.calls += 1
        self.unique_ids.append(unique_id)
        if self.wait_event is not None:
            self.wait_event.set()
            await asyncio.Future()
        return Response(self.status)


class MainStopIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main.connected_charge_points.clear()
        main.pending_stop_transactions.clear()
        main.stop_registry.contexts.clear()
        main.cp_call_locks.clear()
        main.live_status_cache.clear()
        main.charging_point_status.clear()
        with main.get_conn() as conn:
            for table in (
                "line_message_logs",
                "payments",
                "stop_transactions",
                "meter_values",
                "status_logs",
                "transactions",
                "cards",
                "id_tags",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.commit()

    def _insert_transaction(self, tx_id, *, balance=100.0, auto_stop_reason=None):
        with main.get_conn() as conn:
            conn.execute(
                "INSERT INTO cards (card_id, balance) VALUES (?, ?)",
                (CARD_ID, balance),
            )
            conn.execute(
                "INSERT INTO id_tags (id_tag, status) VALUES (?, 'Accepted')",
                (CARD_ID,),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    transaction_id, charge_point_id, connector_id, id_tag,
                    meter_start, start_timestamp, auto_stop_reason
                ) VALUES (?, ?, 1, ?, 0, ?, ?)
                """,
                (tx_id, CP_ID, CARD_ID, "2026-07-18T00:00:00+00:00", auto_stop_reason),
            )
            conn.commit()

    async def test_only_one_stop_route_is_registered(self):
        routes = [
            route
            for route in main.app.routes
            if getattr(route, "path", None)
            == "/api/charge-points/{charge_point_id:path}/stop"
            and "POST" in (getattr(route, "methods", set()) or set())
        ]
        self.assertEqual(len(routes), 1)

    async def test_manual_and_auto_requests_send_only_one_remote_stop(self):
        tx_id = 201
        self._insert_transaction(tx_id)
        cp = FakeChargePoint()
        main.connected_charge_points[CP_ID] = cp

        first = asyncio.create_task(
            main.request_transaction_stop(
                CP_ID, tx_id, "manual", ack_timeout=0.1, stop_timeout=0.5
            )
        )
        second = asyncio.create_task(
            main.request_transaction_stop(
                CP_ID,
                tx_id,
                "balance_estimate",
                wait_for_stop=True,
                ack_timeout=0.1,
                stop_timeout=0.5,
            )
        )
        await asyncio.sleep(0.02)
        main.stop_registry.complete_settlement(
            tx_id,
            {"transaction_id": tx_id, "timestamp": "now", "reason": "Remote"},
            committed=True,
        )
        first_result, second_result = await asyncio.gather(first, second)

        self.assertEqual(cp.calls, 1)
        self.assertEqual(first_result["final_outcome"], "stopped")
        self.assertEqual(second_result["final_outcome"], "stopped")
        self.assertEqual(main.pending_stop_transactions, {})

    async def test_instance_a_send_and_instance_b_stop_completion(self):
        tx_id = 202
        self._insert_transaction(tx_id)
        sent = asyncio.Event()
        cp_a = FakeChargePoint(wait_event=sent, instance_id="A", seq=1)
        cp_b = FakeChargePoint(instance_id="B", seq=2)
        main.connected_charge_points[CP_ID] = cp_a

        request = asyncio.create_task(
            main.request_transaction_stop(
                CP_ID, tx_id, "manual", ack_timeout=1.0, stop_timeout=1.0
            )
        )
        await asyncio.wait_for(sent.wait(), timeout=0.2)
        main.connected_charge_points[CP_ID] = cp_b
        main.stop_registry.complete_settlement(
            tx_id,
            {"transaction_id": tx_id, "timestamp": "now", "reason": "Remote"},
            committed=True,
        )
        result = await request

        self.assertEqual(cp_a.calls, 1)
        self.assertEqual(cp_b.calls, 0)
        self.assertEqual(result["final_outcome"], "stopped")
        self.assertEqual(result["ack_status"], "timeout")
        self.assertTrue(result["websocket_replaced"])
        self.assertEqual(main.pending_stop_transactions, {})

    async def test_manual_stop_api_keeps_success_contract(self):
        tx_id = 205
        self._insert_transaction(tx_id)
        main.connected_charge_points[CP_ID] = FakeChargePoint()
        original_service = main.request_transaction_stop

        async def stopped(*args, **kwargs):
            return {
                "final_outcome": "stopped",
                "stop_result": {"reason": "Remote"},
            }

        main.request_transaction_stop = stopped
        try:
            response = await main.stop_transaction_by_charge_point(CP_ID)
            self.assertEqual(response["message"], "充電已停止")
            self.assertEqual(response["transaction_id"], tx_id)
            self.assertEqual(response["stop_result"], {"reason": "Remote"})
        finally:
            main.request_transaction_stop = original_service

    async def test_manual_stop_api_keeps_timeout_contract(self):
        tx_id = 206
        self._insert_transaction(tx_id)
        main.connected_charge_points[CP_ID] = FakeChargePoint()
        original_service = main.request_transaction_stop

        async def timed_out(*args, **kwargs):
            return {"final_outcome": "stop_timeout", "stop_result": None}

        main.request_transaction_stop = timed_out
        try:
            response = await main.stop_transaction_by_charge_point(CP_ID)
            self.assertEqual(response.status_code, 504)
            self.assertEqual(
                json.loads(response.body),
                {"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"},
            )
        finally:
            main.request_transaction_stop = original_service

    async def test_stop_timeout_cleans_pending_context(self):
        tx_id = 207
        self._insert_transaction(tx_id)
        main.connected_charge_points[CP_ID] = FakeChargePoint(
            wait_event=asyncio.Event()
        )
        result = await main.request_transaction_stop(
            CP_ID,
            tx_id,
            "manual",
            ack_timeout=0.01,
            stop_timeout=0.03,
        )
        self.assertEqual(result["final_outcome"], "stop_timeout")
        self.assertEqual(main.pending_stop_transactions, {})

    async def test_remote_stop_exception_cleans_pending_context(self):
        tx_id = 208
        self._insert_transaction(tx_id)

        class FailingChargePoint(FakeChargePoint):
            async def call(self, payload, unique_id=None):
                self.calls += 1
                raise RuntimeError("simulated send failure")

        main.connected_charge_points[CP_ID] = FailingChargePoint()
        result = await main.request_transaction_stop(
            CP_ID,
            tx_id,
            "manual",
            ack_timeout=0.02,
            stop_timeout=0.03,
        )
        self.assertEqual(result["ack_status"], "error")
        self.assertEqual(result["final_outcome"], "stop_timeout")
        self.assertEqual(main.pending_stop_transactions, {})

    async def test_duplicate_stop_transaction_settles_and_notifies_once(self):
        tx_id = 203
        self._insert_transaction(
            tx_id,
            balance=10.0,
            auto_stop_reason=main.AUTO_STOP_REASON_BALANCE_INSUFFICIENT,
        )
        with main.get_conn() as conn:
            conn.executemany(
                """
                INSERT INTO meter_values (
                    transaction_id, charge_point_id, connector_id,
                    timestamp, value, measurand, unit
                ) VALUES (?, ?, 1, ?, ?, 'Energy.Active.Import.Register', 'Wh')
                """,
                [
                    (tx_id, CP_ID, "2026-07-18T00:00:00+00:00", 0),
                    (tx_id, CP_ID, "2026-07-18T00:10:00+00:00", 1000),
                ],
            )
            conn.commit()

        counters = {"completed": 0, "low": 0, "auto": 0, "rebalance": 0}
        originals = (
            main.schedule_charge_completed_line_notification,
            main.schedule_low_balance_line_notification,
            main.schedule_auto_stop_balance_insufficient_line_notification,
            main.request_rebalance,
        )
        main.schedule_charge_completed_line_notification = lambda tx: counters.__setitem__(
            "completed", counters["completed"] + 1
        )
        main.schedule_low_balance_line_notification = lambda tx: counters.__setitem__(
            "low", counters["low"] + 1
        )
        main.schedule_auto_stop_balance_insufficient_line_notification = (
            lambda tx: counters.__setitem__("auto", counters["auto"] + 1)
        )
        main.request_rebalance = lambda reason: counters.__setitem__(
            "rebalance", counters["rebalance"] + 1
        )
        try:
            fake_self = SimpleNamespace(id=CP_ID)
            payload = {
                "transaction_id": tx_id,
                "meter_stop": 1000,
                "timestamp": "2026-07-18T00:20:00+00:00",
                "reason": "Remote",
            }
            await main.ChargePoint.on_stop_transaction(fake_self, **payload)
            with main.get_conn() as conn:
                balance_after_first = conn.execute(
                    "SELECT balance FROM cards WHERE card_id = ?", (CARD_ID,)
                ).fetchone()[0]
            await main.ChargePoint.on_stop_transaction(fake_self, **payload)

            with main.get_conn() as conn:
                balance_after_second = conn.execute(
                    "SELECT balance FROM cards WHERE card_id = ?", (CARD_ID,)
                ).fetchone()[0]
                payment_count = conn.execute(
                    "SELECT COUNT(*) FROM payments WHERE transaction_id = ?", (tx_id,)
                ).fetchone()[0]
                stop_count = conn.execute(
                    "SELECT COUNT(*) FROM stop_transactions WHERE transaction_id = ?",
                    (str(tx_id),),
                ).fetchone()[0]

            self.assertEqual(balance_after_first, balance_after_second)
            self.assertEqual(payment_count, 1)
            self.assertEqual(stop_count, 1)
            self.assertEqual(counters["completed"], 1)
            self.assertEqual(counters["low"], 1)
            self.assertEqual(counters["auto"], 1)
        finally:
            (
                main.schedule_charge_completed_line_notification,
                main.schedule_low_balance_line_notification,
                main.schedule_auto_stop_balance_insufficient_line_notification,
                main.request_rebalance,
            ) = originals

    async def test_settlement_failure_completes_context_as_failed(self):
        tx_id = 9999
        context, _ = await main.stop_registry.get_or_create(tx_id, CP_ID, "manual")
        fake_self = SimpleNamespace(id=CP_ID)
        await main.ChargePoint.on_stop_transaction(
            fake_self,
            transaction_id=tx_id,
            meter_stop=1,
            timestamp="2026-07-18T00:00:00+00:00",
            reason="Remote",
        )
        result = context.stop_future.result()
        self.assertFalse(result["settlement_committed"])
        self.assertEqual(result["settlement_status"], "failed")
        with main.get_conn() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM stop_transactions WHERE transaction_id = ?",
                    (str(tx_id),),
                ).fetchone()[0],
                0,
            )
        await main.stop_registry.remove(tx_id, context)

    async def test_meter_values_schedules_stop_after_commit_without_waiting(self):
        tx_id = 204
        self._insert_transaction(tx_id)
        with main.get_conn() as conn:
            conn.executemany(
                """
                INSERT INTO meter_values (
                    transaction_id, charge_point_id, connector_id,
                    timestamp, value, measurand, unit
                ) VALUES (?, ?, 1, ?, ?, 'Energy.Active.Import.Register', 'Wh')
                """,
                [
                    (tx_id, CP_ID, "2026-07-18T00:00:00+00:00", 0),
                    (tx_id, CP_ID, "2026-07-18T00:10:00+00:00", 1000),
                ],
            )
            conn.commit()

        scheduled = []
        original_schedule = main._schedule_balance_estimate_stop
        main._schedule_balance_estimate_stop = (
            lambda cp_id, transaction_id, estimated_amount: scheduled.append(
                (cp_id, transaction_id, estimated_amount)
            )
        )
        try:
            started = time.monotonic()
            await main.ChargePoint.on_meter_values(
                SimpleNamespace(id=CP_ID),
                connector_id=1,
                transaction_id=tx_id,
                meter_value=[
                    {
                        "timestamp": "2026-07-18T00:20:00+00:00",
                        "sampled_value": [
                            {
                                "value": "1100",
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            }
                        ],
                    }
                ],
            )
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 1.0)
            self.assertEqual(len(scheduled), 1)
            with main.get_conn() as conn:
                persisted = conn.execute(
                    "SELECT COUNT(*) FROM meter_values WHERE transaction_id = ?",
                    (tx_id,),
                ).fetchone()[0]
            self.assertEqual(persisted, 3)
        finally:
            main._schedule_balance_estimate_stop = original_schedule


if __name__ == "__main__":
    unittest.main()
