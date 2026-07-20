import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


# Importing main initializes its schema, so force that initialization onto a
# dedicated temporary database before main is imported.
_TEST_DIR = tempfile.TemporaryDirectory(
    prefix="ocpp_soc_behavior_", ignore_cleanup_errors=True
)
_TEST_DB = Path(_TEST_DIR.name) / "test_soc_behavior.sqlite3"
os.environ["DATABASE_PATH"] = str(_TEST_DB)

import main  # noqa: E402


CP_ID = "TW*TEST*SOC0001"
CARD_ID = "SOC-TEST-CARD"
INITIAL_BALANCE = 10_000.0


def _payload_value(payload, name):
    value = getattr(payload, name, None)
    if value is not None:
        return value
    if isinstance(payload, dict):
        return payload.get(name)
    return None


class VehicleSocStopBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.line_completed_calls = []
        self.low_balance_calls = []
        self.auto_stop_line_calls = []
        self.rebalance_calls = []
        self.balance_stop_candidates = []
        self.profile_calls = []
        self.remote_stop_calls = []

        self.originals = {
            "schedule_charge_completed_line_notification": main.schedule_charge_completed_line_notification,
            "schedule_low_balance_line_notification": main.schedule_low_balance_line_notification,
            "schedule_auto_stop_balance_insufficient_line_notification": main.schedule_auto_stop_balance_insufficient_line_notification,
            "request_rebalance": main.request_rebalance,
            "_schedule_balance_estimate_stop": main._schedule_balance_estimate_stop,
            "send_current_limit_profile": main.send_current_limit_profile,
            "_price_for_timestamp": main._price_for_timestamp,
            "request_transaction_stop": main.request_transaction_stop,
        }

        main.schedule_charge_completed_line_notification = (
            lambda tx_id: self.line_completed_calls.append(int(tx_id))
        )
        main.schedule_low_balance_line_notification = (
            lambda tx_id: self.low_balance_calls.append(int(tx_id))
        )
        main.schedule_auto_stop_balance_insufficient_line_notification = (
            lambda tx_id: self.auto_stop_line_calls.append(int(tx_id))
        )
        main.request_rebalance = (
            lambda reason: self.rebalance_calls.append(str(reason))
        )
        main._schedule_balance_estimate_stop = (
            lambda cp_id, transaction_id, estimated_amount: self.balance_stop_candidates.append(
                (cp_id, int(transaction_id), float(estimated_amount))
            )
        )

        async def fake_set_charging_profile(**kwargs):
            self.profile_calls.append(kwargs)
            return SimpleNamespace(status="Accepted")

        main.send_current_limit_profile = fake_set_charging_profile

        async def fake_remote_stop(*args, **kwargs):
            self.remote_stop_calls.append((args, kwargs))
            return {"final_outcome": "mocked"}

        main.request_transaction_stop = fake_remote_stop
        main._price_for_timestamp = lambda _timestamp: 10.0

        main.connected_charge_points.clear()
        main.connected_charge_points[CP_ID] = SimpleNamespace(id=CP_ID)
        main.pending_stop_transactions.clear()
        main.stop_registry.contexts.clear()
        main.cp_call_locks.clear()
        main.start_transaction_locks.clear()
        main.live_status_cache.clear()
        main.charging_point_status.clear()
        main.current_limit_state.clear()
        main.ws_disconnect_grace.clear()

        with main.get_conn() as conn:
            for table in (
                "line_message_logs",
                "line_bindings",
                "payments",
                "stop_transactions",
                "meter_values",
                "status_logs",
                "transactions",
                "card_whitelist",
                "cards",
                "id_tags",
                "charge_points",
            ):
                conn.execute(f"DELETE FROM {table}")

            conn.execute(
                """
                INSERT INTO charge_points
                    (charge_point_id, name, status, max_current_a)
                VALUES (?, 'SOC test charger', 'Available', 32)
                """,
                (CP_ID,),
            )
            conn.execute(
                "INSERT INTO id_tags (id_tag, status) VALUES (?, 'Accepted')",
                (CARD_ID,),
            )
            conn.execute(
                "INSERT INTO cards (card_id, balance) VALUES (?, ?)",
                (CARD_ID, INITIAL_BALANCE),
            )
            conn.execute(
                "INSERT INTO card_whitelist (card_id, charge_point_id) VALUES (?, ?)",
                (CARD_ID, CP_ID),
            )
            conn.execute(
                """
                UPDATE community_settings
                SET enabled=1, contract_kw=22, voltage_v=220, phases=1,
                    min_current_a=6, max_current_a=32, surcharge_per_kwh=0
                WHERE id=1
                """
            )
            conn.commit()

        self.cp = SimpleNamespace(
            id=CP_ID,
            supports_smart_charging=True,
        )

    async def asyncTearDown(self):
        for name, original in self.originals.items():
            setattr(main, name, original)

    async def _start(self, meter_start, timestamp):
        response = await main.ChargePoint.on_start_transaction(
            self.cp,
            connector_id=1,
            id_tag=CARD_ID,
            meter_start=meter_start,
            timestamp=timestamp,
        )
        tx_id = int(_payload_value(response, "transaction_id"))
        self.assertGreater(tx_id, 0)
        return response, tx_id

    async def _meter(self, tx_id, value, timestamp):
        return await main.ChargePoint.on_meter_values(
            self.cp,
            connector_id=1,
            transaction_id=tx_id,
            meter_value=[
                {
                    "timestamp": timestamp,
                    "sampled_value": [
                        {
                            "value": str(value),
                            "measurand": "Energy.Active.Import.Register",
                            "unit": "Wh",
                        }
                    ],
                }
            ],
        )

    async def _status(self, status, timestamp):
        return await main.ChargePoint.on_status_notification(
            self.cp,
            connector_id=1,
            status=status,
            error_code="NoError",
            timestamp=timestamp,
        )

    async def _stop(self, tx_id, meter_stop, timestamp, reason):
        return await main.ChargePoint.on_stop_transaction(
            self.cp,
            transaction_id=tx_id,
            meter_stop=meter_stop,
            timestamp=timestamp,
            reason=reason,
        )

    def _state(self):
        with main.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            transactions = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT transaction_id, meter_start, start_timestamp,
                           meter_stop, stop_timestamp, reason,
                           balance_before, balance_after
                    FROM transactions
                    ORDER BY transaction_id
                    """
                )
            ]
            payments = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT transaction_id, total_amount, paid_at
                    FROM payments ORDER BY id
                    """
                )
            ]
            balance = conn.execute(
                "SELECT balance FROM cards WHERE card_id=?", (CARD_ID,)
            ).fetchone()[0]
            meter_counts = {
                str(row[0]): row[1]
                for row in conn.execute(
                    """
                    SELECT CAST(transaction_id AS TEXT), COUNT(*)
                    FROM meter_values GROUP BY transaction_id
                    """
                )
            }
        return {
            "transactions": transactions,
            "payments": payments,
            "history": main.get_card_history(CARD_ID)["history"],
            "balance": balance,
            "meter_value_counts": meter_counts,
            "line_completed_calls": list(self.line_completed_calls),
            "line_completed_messages": [
                main.build_charge_completed_line_message(tx_id).get("message")
                for tx_id in self.line_completed_calls
            ],
            "charging_point_status": main.charging_point_status.get(CP_ID),
            "live_status_cache": main.live_status_cache.get(CP_ID),
            "smart_active_cp_ids": main.get_effective_active_cp_ids(),
        }

    def _print_snapshot(self, test_name, responses):
        snapshot = self._state()
        snapshot["handler_returns"] = [type(item).__name__ for item in responses]
        print(f"SOC_BEHAVIOR_RESULT {test_name} {json.dumps(snapshot, ensure_ascii=False, default=str)}")
        return snapshot

    async def test_1_start_meter_values_then_suspended_ev_keeps_transaction_active(self):
        start_result, tx_id = await self._start(1000, "2026-07-20T02:00:00+00:00")
        meter_result = await self._meter(tx_id, 1800, "2026-07-20T02:20:00+00:00")
        status_result = await self._status("SuspendedEV", "2026-07-20T02:21:00+00:00")

        state = self._print_snapshot("test_1_suspended_ev", [start_result, meter_result, status_result])
        self.assertEqual(len(state["transactions"]), 1)
        self.assertIsNone(state["transactions"][0]["stop_timestamp"])
        self.assertIsNone(state["transactions"][0]["meter_stop"])
        self.assertEqual(len(state["payments"]), 0)
        self.assertEqual(len(state["history"]), 0)
        self.assertEqual(state["balance"], INITIAL_BALANCE)
        self.assertEqual(state["line_completed_calls"], [])
        self.assertEqual(state["smart_active_cp_ids"], [CP_ID])
        self.assertEqual(state["charging_point_status"]["status"], "SuspendedEV")
        self.assertEqual(state["live_status_cache"]["status"], "SuspendedEV")

    async def test_2_stop_transaction_ends_and_settles_once(self):
        start_result, tx_id = await self._start(1000, "2026-07-20T03:00:00+00:00")
        meter_result = await self._meter(tx_id, 1800, "2026-07-20T03:20:00+00:00")
        stop_result = await self._stop(
            tx_id, 1800, "2026-07-20T03:21:00+00:00", "EVDisconnected"
        )

        state = self._print_snapshot("test_2_stop_transaction", [start_result, meter_result, stop_result])
        history = main.get_card_history(CARD_ID)
        self.assertEqual(len(state["transactions"]), 1)
        self.assertEqual(state["transactions"][0]["stop_timestamp"], "2026-07-20T03:21:00+00:00")
        self.assertEqual(state["transactions"][0]["meter_stop"], 1800)
        self.assertEqual(len(state["payments"]), 1)
        self.assertLess(state["balance"], INITIAL_BALANCE)
        self.assertEqual(state["line_completed_calls"], [tx_id])
        self.assertEqual(state["smart_active_cp_ids"], [])
        self.assertEqual(len(history["history"]), 1)
        self.assertEqual(history["history"][0]["stop_timestamp"], "2026-07-20T03:21:00+00:00")

    async def test_3_stop_a_then_start_and_stop_b_creates_two_sessions(self):
        start_a, tx_a = await self._start(1000, "2026-07-20T04:00:00+00:00")
        meter_a = await self._meter(tx_a, 1800, "2026-07-20T04:20:00+00:00")
        stop_a = await self._stop(tx_a, 1800, "2026-07-20T04:21:00+00:00", "Local")

        start_b, tx_b = await self._start(1800, "2026-07-20T04:30:00+00:00")
        meter_b = await self._meter(tx_b, 2300, "2026-07-20T04:45:00+00:00")
        stop_b = await self._stop(tx_b, 2300, "2026-07-20T04:46:00+00:00", "Local")

        state = self._print_snapshot(
            "test_3_two_start_stop_cycles",
            [start_a, meter_a, stop_a, start_b, meter_b, stop_b],
        )
        tx_rows = state["transactions"]
        self.assertNotEqual(tx_a, tx_b)
        self.assertEqual(len(tx_rows), 2)
        self.assertEqual(len(state["payments"]), 2)
        self.assertEqual(len(state["history"]), 2)
        self.assertEqual(state["line_completed_calls"], [tx_a, tx_b])
        self.assertEqual(tx_rows[0]["meter_start"], 1000)
        self.assertEqual(tx_rows[0]["meter_stop"], 1800)
        self.assertEqual(tx_rows[1]["meter_start"], 1800)
        self.assertEqual(tx_rows[1]["meter_stop"], 2300)
        self.assertEqual(tx_rows[1]["start_timestamp"], "2026-07-20T04:30:00+00:00")
        self.assertEqual(state["smart_active_cp_ids"], [])

    async def test_4_post_stop_meter_values_and_charging_do_not_reopen_transaction(self):
        start_result, tx_id = await self._start(1000, "2026-07-20T05:00:00+00:00")
        meter_before_stop = await self._meter(tx_id, 1800, "2026-07-20T05:20:00+00:00")
        stop_result = await self._stop(tx_id, 1800, "2026-07-20T05:21:00+00:00", "Local")
        meter_after_stop = await self._meter(tx_id, 2100, "2026-07-20T05:30:00+00:00")
        charging_after_stop = await self._status("Charging", "2026-07-20T05:30:01+00:00")

        state = self._print_snapshot(
            "test_4_events_without_new_start",
            [start_result, meter_before_stop, stop_result, meter_after_stop, charging_after_stop],
        )
        self.assertEqual(len(state["transactions"]), 1)
        self.assertEqual(state["transactions"][0]["stop_timestamp"], "2026-07-20T05:21:00+00:00")
        self.assertEqual(state["transactions"][0]["meter_stop"], 1800)
        self.assertEqual(len(state["payments"]), 1)
        self.assertEqual(len(state["history"]), 1)
        self.assertEqual(state["line_completed_calls"], [tx_id])
        self.assertEqual(state["smart_active_cp_ids"], [])
        self.assertEqual(state["meter_value_counts"][str(tx_id)], 2)
        self.assertEqual(state["charging_point_status"]["status"], "Available")
        self.assertEqual(state["charging_point_status"]["raw_status"], "Charging")
        self.assertEqual(
            state["charging_point_status"]["backend_state"],
            "ChargingWithoutTransaction",
        )
        self.assertEqual(state["live_status_cache"]["status"], "Available")
        self.assertEqual(state["live_status_cache"]["raw_status"], "Charging")

    async def test_5_suspend_resume_then_stop_keeps_one_transaction(self):
        start_result, tx_id = await self._start(1000, "2026-07-20T06:00:00+00:00")
        meter_1 = await self._meter(tx_id, 1600, "2026-07-20T06:15:00+00:00")
        suspended = await self._status("SuspendedEV", "2026-07-20T06:16:00+00:00")
        self.assertEqual(main.charging_point_status[CP_ID]["status"], "SuspendedEV")
        charging = await self._status("Charging", "2026-07-20T06:25:00+00:00")
        self.assertEqual(main.charging_point_status[CP_ID]["status"], "Charging")
        self.assertTrue(main.charging_point_status[CP_ID]["charging_authorized"])
        self.assertEqual(main.charging_point_status[CP_ID]["active_transaction_id"], tx_id)
        meter_2 = await self._meter(tx_id, 2300, "2026-07-20T06:40:00+00:00")
        stop_result = await self._stop(tx_id, 2300, "2026-07-20T06:41:00+00:00", "Local")

        state = self._print_snapshot(
            "test_5_suspend_resume_stop",
            [start_result, meter_1, suspended, charging, meter_2, stop_result],
        )
        self.assertEqual(len(state["transactions"]), 1)
        self.assertEqual(state["transactions"][0]["meter_start"], 1000)
        self.assertEqual(state["transactions"][0]["meter_stop"], 2300)
        self.assertEqual(state["transactions"][0]["stop_timestamp"], "2026-07-20T06:41:00+00:00")
        self.assertEqual(len(state["payments"]), 1)
        self.assertEqual(state["line_completed_calls"], [tx_id])
        self.assertEqual(state["smart_active_cp_ids"], [])


def tearDownModule():
    # main keeps one legacy module-level SQLite connection open. Close it only
    # after this isolated module has finished so Windows can remove the temp DB.
    try:
        try:
            main.cursor.close()
        except sqlite3.ProgrammingError:
            pass
        try:
            main.conn.close()
        except sqlite3.ProgrammingError:
            pass
    finally:
        _TEST_DIR.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
