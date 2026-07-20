import asyncio
import copy
import inspect
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


_TEST_DIR = tempfile.TemporaryDirectory(
    prefix="ocpp_soc_80_to_90_", ignore_cleanup_errors=True
)
_TEST_DB = Path(_TEST_DIR.name) / "test_soc_80_to_90.sqlite3"
os.environ["DATABASE_PATH"] = str(_TEST_DB)

import main  # noqa: E402


CP_ID = "TW*MSI*E000100"
CP_B_ID = "TW*TEST*SECOND0001"
CARD_ID = "SOC-80-90-CARD"
INITIAL_BALANCE = 10_000.0


def _payload_value(payload, name):
    value = getattr(payload, name, None)
    if value is not None:
        return value
    if isinstance(payload, dict):
        return payload.get(name)
    return None


class Soc80To90ProtocolPathTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.line_calls = []
        self.rebalance_calls = []
        self.remote_stop_calls = []
        self.profile_calls = []
        self.balance_stop_candidates = []

        self.originals = {
            "schedule_charge_completed_line_notification": main.schedule_charge_completed_line_notification,
            "schedule_low_balance_line_notification": main.schedule_low_balance_line_notification,
            "schedule_auto_stop_balance_insufficient_line_notification": main.schedule_auto_stop_balance_insufficient_line_notification,
            "request_rebalance": main.request_rebalance,
            "_schedule_balance_estimate_stop": main._schedule_balance_estimate_stop,
            "request_transaction_stop": main.request_transaction_stop,
            "send_current_limit_profile": main.send_current_limit_profile,
            "_price_for_timestamp": main._price_for_timestamp,
        }

        main.schedule_charge_completed_line_notification = (
            lambda tx_id: self.line_calls.append(int(tx_id))
        )
        main.schedule_low_balance_line_notification = lambda _tx_id: None
        main.schedule_auto_stop_balance_insufficient_line_notification = (
            lambda _tx_id: None
        )
        main.request_rebalance = (
            lambda reason: self.rebalance_calls.append(str(reason))
        )
        main._schedule_balance_estimate_stop = (
            lambda **kwargs: self.balance_stop_candidates.append(kwargs)
        )

        async def fake_remote_stop(*args, **kwargs):
            self.remote_stop_calls.append((args, kwargs))
            return {"final_outcome": "mocked"}

        async def fake_set_charging_profile(**kwargs):
            self.profile_calls.append(kwargs)
            return SimpleNamespace(status="Accepted")

        main.request_transaction_stop = fake_remote_stop
        main.send_current_limit_profile = fake_set_charging_profile
        main._price_for_timestamp = lambda _timestamp: 10.0

        main.connected_charge_points.clear()
        main.connected_charge_points[CP_ID] = SimpleNamespace(id=CP_ID)
        main.connected_charge_points[CP_B_ID] = SimpleNamespace(id=CP_B_ID)
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
                VALUES (?, 'MSI isolated replay', 'Available', 32)
                """,
                (CP_ID,),
            )
            conn.execute(
                """
                INSERT INTO charge_points
                    (charge_point_id, name, status, max_current_a)
                VALUES (?, 'Second isolated charger', 'Available', 32)
                """,
                (CP_B_ID,),
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
                "INSERT INTO card_whitelist (card_id, charge_point_id) VALUES (?, ?)",
                (CARD_ID, CP_B_ID),
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

        self.cp = SimpleNamespace(id=CP_ID, supports_smart_charging=True)
        self.cp_b = SimpleNamespace(id=CP_B_ID, supports_smart_charging=True)

    async def asyncTearDown(self):
        for name, original in self.originals.items():
            setattr(main, name, original)

    async def _start(self, meter_start, timestamp, cp=None):
        cp = cp or self.cp
        response = await main.ChargePoint.on_start_transaction(
            cp,
            connector_id=1,
            id_tag=CARD_ID,
            meter_start=meter_start,
            timestamp=timestamp,
        )
        tx_id = int(_payload_value(response, "transaction_id"))
        self.assertGreater(tx_id, 0)
        return response, tx_id

    async def _meter(self, tx_id, energy, timestamp):
        return await main.ChargePoint.on_meter_values(
            self.cp,
            connector_id=1,
            transaction_id=tx_id,
            meter_value=[
                {
                    "timestamp": timestamp,
                    "sampled_value": [
                        {
                            "value": str(energy),
                            "measurand": "Energy.Active.Import.Register",
                            "unit": "Wh",
                        }
                    ],
                }
            ],
        )

    async def _meter_without_transaction(
        self,
        *,
        connector_id=1,
        timestamp="2026-07-20T11:00:00Z",
        sampled_values=None,
    ):
        sampled_values = sampled_values or [
            {
                "value": "1800",
                "measurand": "Energy.Active.Import.Register",
                "unit": "Wh",
            }
        ]
        return await main.ChargePoint.on_meter_values(
            self.cp,
            connector_id=connector_id,
            meter_value=[
                {
                    "timestamp": timestamp,
                    "sampled_value": sampled_values,
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

    async def _stop(self, tx_id, meter_stop, timestamp):
        return await main.ChargePoint.on_stop_transaction(
            self.cp,
            transaction_id=tx_id,
            meter_stop=meter_stop,
            timestamp=timestamp,
            reason="Local",
        )

    def _snapshot(self):
        with main.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            transactions = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT transaction_id, meter_start, start_timestamp,
                           meter_stop, stop_timestamp, reason,
                           balance_before, balance_after
                    FROM transactions ORDER BY transaction_id
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
            stop_count = conn.execute(
                "SELECT COUNT(*) FROM stop_transactions"
            ).fetchone()[0]
            balance = conn.execute(
                "SELECT balance FROM cards WHERE card_id=?", (CARD_ID,)
            ).fetchone()[0]
            meter_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT transaction_id, timestamp, value, measurand, unit
                    FROM meter_values ORDER BY id
                    """
                )
            ]
            active_tx_ids = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT transaction_id FROM transactions
                    WHERE stop_timestamp IS NULL
                    ORDER BY transaction_id
                    """
                )
            ]

        return {
            "transactions": transactions,
            "payments": payments,
            "history": main.get_card_history(CARD_ID)["history"],
            "stop_transaction_rows": stop_count,
            "balance": balance,
            "meter_values": meter_rows,
            "line_calls": list(self.line_calls),
            "active_transaction_ids": active_tx_ids,
            "smart_active_cp_ids": main.get_effective_active_cp_ids(),
            "charging_point_status": copy.deepcopy(
                main.charging_point_status.get(CP_ID)
            ),
            "live_status_cache": copy.deepcopy(main.live_status_cache.get(CP_ID)),
            "current_limit_state": copy.deepcopy(
                main.current_limit_state.get(CP_ID)
            ),
        }

    def _print_result(self, path_name, result):
        print(
            f"SOC_80_TO_90_RESULT {path_name} "
            f"{json.dumps(result, ensure_ascii=False, default=str)}"
        )

    async def test_path_a_reuses_old_transaction_without_new_start(self):
        _, tx_a = await self._start(1000, "2026-07-20T02:00:00Z")
        await self._meter(tx_a, 1800, "2026-07-20T02:30:00Z")
        main.current_limit_state[CP_ID] = {
            "last_tx_id": tx_a,
            "requested_limit_a": 16.0,
            "applied": True,
        }
        await self._stop(tx_a, 1800, "2026-07-20T03:00:00Z")

        await self._status("Charging", "2026-07-20T03:10:00Z")
        await self._meter(tx_a, 2300, "2026-07-20T03:20:00Z")
        before_duplicate_stop = self._snapshot()

        context, _ = await main.stop_registry.get_or_create(
            tx_a, CP_ID, "duplicate_stop_probe"
        )
        await self._stop(tx_a, 2300, "2026-07-20T03:40:00Z")
        duplicate_completion = context.stop_future.result()
        await main.stop_registry.remove(tx_a, context)
        after_duplicate_stop = self._snapshot()

        result = {
            "transaction_id_a": tx_a,
            "before_duplicate_stop": before_duplicate_stop,
            "duplicate_stop_completion": duplicate_completion,
            "after_duplicate_stop": after_duplicate_stop,
        }
        self._print_result("A", result)

        self.assertEqual(len(after_duplicate_stop["transactions"]), 1)
        self.assertEqual(len(after_duplicate_stop["payments"]), 1)
        self.assertEqual(len(after_duplicate_stop["history"]), 1)
        self.assertEqual(after_duplicate_stop["line_calls"], [tx_a])
        self.assertEqual(after_duplicate_stop["balance"], 9992.0)
        self.assertEqual(after_duplicate_stop["stop_transaction_rows"], 1)
        self.assertEqual(
            after_duplicate_stop["transactions"][0]["meter_stop"], 1800
        )
        self.assertEqual(
            after_duplicate_stop["transactions"][0]["stop_timestamp"],
            "2026-07-20T03:00:00+00:00",
        )
        self.assertTrue(duplicate_completion["already_stopped"])
        self.assertEqual(before_duplicate_stop["active_transaction_ids"], [])
        self.assertEqual(before_duplicate_stop["smart_active_cp_ids"], [])
        self.assertIsNone(before_duplicate_stop["current_limit_state"])
        self.assertEqual(
            before_duplicate_stop["charging_point_status"]["status"], "Available"
        )
        self.assertEqual(
            before_duplicate_stop["charging_point_status"]["raw_status"], "Charging"
        )
        self.assertEqual(
            before_duplicate_stop["charging_point_status"]["backend_state"],
            "ChargingWithoutTransaction",
        )
        self.assertEqual(before_duplicate_stop["live_status_cache"]["status"], "Available")
        self.assertEqual(
            after_duplicate_stop["charging_point_status"]["status"], "Available"
        )
        self.assertFalse(
            any(row["value"] == 2300 for row in after_duplicate_stop["meter_values"])
        )

    async def test_path_b_creates_second_transaction(self):
        _, tx_a = await self._start(1000, "2026-07-20T02:00:00Z")
        await self._stop(tx_a, 1800, "2026-07-20T03:00:00Z")

        _, tx_b = await self._start(1800, "2026-07-20T03:10:00Z")
        after_start_b = self._snapshot()
        await self._meter(tx_b, 2300, "2026-07-20T03:30:00Z")
        await self._stop(tx_b, 2300, "2026-07-20T03:40:00Z")
        final_state = self._snapshot()

        result = {
            "transaction_id_a": tx_a,
            "transaction_id_b": tx_b,
            "after_start_b": after_start_b,
            "final": final_state,
        }
        self._print_result("B", result)

        self.assertNotEqual(tx_a, tx_b)
        self.assertEqual(after_start_b["active_transaction_ids"], [tx_b])
        self.assertEqual(after_start_b["smart_active_cp_ids"], [CP_ID])
        self.assertEqual(len(final_state["transactions"]), 2)
        self.assertEqual(len(final_state["payments"]), 2)
        self.assertEqual(len(final_state["history"]), 2)
        self.assertEqual(final_state["line_calls"], [tx_a, tx_b])
        self.assertEqual(final_state["balance"], 9987.0)
        tx_a_row, tx_b_row = final_state["transactions"]
        self.assertEqual(tx_a_row["meter_start"], 1000)
        self.assertEqual(tx_a_row["meter_stop"], 1800)
        self.assertEqual(tx_b_row["meter_start"], 1800)
        self.assertEqual(tx_b_row["meter_stop"], 2300)
        self.assertEqual(
            tx_b_row["start_timestamp"], "2026-07-20T03:10:00+00:00"
        )
        self.assertEqual(
            tx_b_row["stop_timestamp"], "2026-07-20T03:40:00+00:00"
        )
        self.assertEqual(tx_a_row["meter_stop"], tx_b_row["meter_start"])
        self.assertEqual(final_state["active_transaction_ids"], [])
        self.assertEqual(final_state["smart_active_cp_ids"], [])

    async def test_duplicate_start_on_same_active_cp_is_not_inserted(self):
        _, tx_a = await self._start(1000, "2026-07-20T07:00:00Z")
        balance_before = self._snapshot()["balance"]

        with self.assertLogs(level="WARNING") as captured:
            duplicate_response, duplicate_tx_id = await self._start(
                1000, "2026-07-20T07:00:00Z"
            )

        with main.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT transaction_id FROM transactions
                WHERE charge_point_id=? AND stop_timestamp IS NULL
                """,
                (CP_ID,),
            ).fetchall()

        self.assertEqual(duplicate_tx_id, tx_a)
        self.assertEqual(_payload_value(duplicate_response, "id_tag_info")["status"], "Accepted")
        self.assertEqual(rows, [(tx_a,)])
        self.assertTrue(
            any("[START_TX][DUPLICATE_ACTIVE_TX]" in line for line in captured.output)
        )
        self.assertEqual(self.line_calls, [])

        conflicting_response = await main.ChargePoint.on_start_transaction(
            self.cp,
            connector_id=1,
            id_tag=CARD_ID,
            meter_start=1001,
            timestamp="2026-07-20T07:01:00Z",
        )
        self.assertEqual(_payload_value(conflicting_response, "transaction_id"), 0)
        self.assertEqual(
            _payload_value(conflicting_response, "id_tag_info")["status"],
            "ConcurrentTx",
        )

        with main.get_conn() as conn:
            all_rows = conn.execute(
                """
                SELECT transaction_id, stop_timestamp
                FROM transactions
                WHERE charge_point_id=?
                ORDER BY transaction_id
                """,
                (CP_ID,),
            ).fetchall()
            payment_count = conn.execute(
                "SELECT COUNT(*) FROM payments"
            ).fetchone()[0]
            balance_after = conn.execute(
                "SELECT balance FROM cards WHERE card_id=?",
                (CARD_ID,),
            ).fetchone()[0]

        self.assertEqual(all_rows, [(tx_a, None)])
        self.assertEqual(payment_count, 0)
        self.assertEqual(balance_after, balance_before)
        self.assertEqual(self.line_calls, [])

    async def test_concurrent_exact_duplicate_start_creates_one_transaction(self):
        with self.assertLogs(level="WARNING") as captured:
            responses = await asyncio.gather(
                main.ChargePoint.on_start_transaction(
                    self.cp,
                    connector_id=1,
                    id_tag=CARD_ID,
                    meter_start=1000,
                    timestamp="2026-07-20T07:30:00Z",
                ),
                main.ChargePoint.on_start_transaction(
                    self.cp,
                    connector_id=1,
                    id_tag=CARD_ID,
                    meter_start=1000,
                    timestamp="2026-07-20T07:30:00Z",
                ),
            )

        response_ids = [
            int(_payload_value(response, "transaction_id"))
            for response in responses
        ]
        response_statuses = [
            _payload_value(response, "id_tag_info")["status"]
            for response in responses
        ]
        with main.get_conn() as conn:
            rows = conn.execute(
                "SELECT transaction_id FROM transactions ORDER BY transaction_id"
            ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(response_ids, [rows[0][0], rows[0][0]])
        self.assertEqual(response_statuses, ["Accepted", "Accepted"])
        self.assertIn(
            "[START_TX][DUPLICATE_ACTIVE_TX]", "\n".join(captured.output)
        )

    async def test_concurrent_conflicting_start_creates_one_transaction(self):
        responses = await asyncio.gather(
            main.ChargePoint.on_start_transaction(
                self.cp,
                connector_id=1,
                id_tag=CARD_ID,
                meter_start=1000,
                timestamp="2026-07-20T07:40:00Z",
            ),
            main.ChargePoint.on_start_transaction(
                self.cp,
                connector_id=1,
                id_tag=CARD_ID,
                meter_start=1001,
                timestamp="2026-07-20T07:40:01Z",
            ),
        )

        response_ids = [
            int(_payload_value(response, "transaction_id"))
            for response in responses
        ]
        response_statuses = [
            _payload_value(response, "id_tag_info")["status"]
            for response in responses
        ]
        with main.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT transaction_id, stop_timestamp
                FROM transactions
                ORDER BY transaction_id
                """
            ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(sorted(response_ids), [0, rows[0][0]])
        self.assertEqual(sorted(response_statuses), ["Accepted", "ConcurrentTx"])
        self.assertEqual(rows[0][1], None)

    async def test_different_charge_points_can_start_concurrently(self):
        _, tx_a = await self._start(1000, "2026-07-20T08:00:00Z", self.cp)
        _, tx_b = await self._start(5000, "2026-07-20T08:00:05Z", self.cp_b)

        with main.get_conn() as conn:
            active_rows = conn.execute(
                """
                SELECT transaction_id, charge_point_id
                FROM transactions
                WHERE start_timestamp IS NOT NULL AND stop_timestamp IS NULL
                ORDER BY transaction_id
                """
            ).fetchall()

        self.assertNotEqual(tx_a, tx_b)
        self.assertEqual(active_rows, [(tx_a, CP_ID), (tx_b, CP_B_ID)])

    async def test_meter_values_reject_invalid_transaction_relationships(self):
        with main.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO transactions (
                    transaction_id, charge_point_id, connector_id, id_tag,
                    meter_start, start_timestamp
                ) VALUES (9001, ?, 1, ?, 5000, '2026-07-20T09:00:00+00:00')
                """,
                (CP_B_ID, CARD_ID),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    transaction_id, charge_point_id, connector_id, id_tag,
                    meter_start, start_timestamp
                ) VALUES (9002, ?, 1, ?, 1000, NULL)
                """,
                (CP_ID, CARD_ID),
            )
            conn.commit()

        with self.assertLogs(level="WARNING") as captured:
            await self._meter(9999, 1100, "2026-07-20T09:10:00Z")
            await self._meter(9001, 5100, "2026-07-20T09:11:00Z")
            await self._meter(9002, 1200, "2026-07-20T09:12:00Z")

        with main.get_conn() as conn:
            meter_count = conn.execute("SELECT COUNT(*) FROM meter_values").fetchone()[0]

        self.assertEqual(meter_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        joined_logs = "\n".join(captured.output)
        self.assertIn("rejection_reason=transaction_not_found", joined_logs)
        self.assertIn("rejection_reason=charge_point_mismatch", joined_logs)
        self.assertIn("rejection_reason=transaction_not_started", joined_logs)

    async def test_mv_a_missing_transaction_id_resolves_unique_active_transaction(self):
        _, tx_a = await self._start(1000, "2026-07-20T11:00:00Z")

        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                timestamp="2026-07-20T11:10:00Z"
            )

        with main.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT transaction_id, value, measurand
                FROM meter_values
                ORDER BY id
                """
            ).fetchall()

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(
            rows,
            [(tx_a, 1800.0, "Energy.Active.Import.Register")],
        )
        self.assertEqual(main.live_status_cache[CP_ID]["status"], "Charging")
        self.assertIn(
            "[MV][RESOLVED_MISSING_TX_ID]", "\n".join(captured.output)
        )

    async def test_mv_b_missing_transaction_id_after_stop_is_rejected(self):
        _, tx_a = await self._start(1000, "2026-07-20T11:20:00Z")
        await self._stop(tx_a, 1800, "2026-07-20T11:30:00Z")
        before = self._snapshot()

        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                timestamp="2026-07-20T11:31:00Z"
            )
        after = self._snapshot()

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(after["meter_values"], before["meter_values"])
        self.assertEqual(after["payments"], before["payments"])
        self.assertEqual(after["balance"], before["balance"])
        self.assertEqual(after["line_calls"], before["line_calls"])
        self.assertEqual(after["live_status_cache"]["status"], "Available")
        self.assertIn(
            "[MV][REJECTED_MISSING_TX_ID_NO_ACTIVE_TX]",
            "\n".join(captured.output),
        )

    async def test_mv_c_missing_transaction_id_with_multiple_active_is_rejected(self):
        with main.get_conn() as conn:
            conn.executemany(
                """
                INSERT INTO transactions (
                    charge_point_id, connector_id, id_tag,
                    meter_start, start_timestamp
                ) VALUES (?, 1, ?, ?, ?)
                """,
                [
                    (CP_ID, CARD_ID, 1000, "2026-07-20T11:40:00+00:00"),
                    (CP_ID, CARD_ID, 2000, "2026-07-20T11:41:00+00:00"),
                ],
            )
            conn.commit()

        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                timestamp="2026-07-20T11:42:00Z"
            )

        with main.get_conn() as conn:
            meter_count = conn.execute(
                "SELECT COUNT(*) FROM meter_values"
            ).fetchone()[0]

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(meter_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        self.assertIn(
            "[MV][REJECTED_MISSING_TX_ID_MULTIPLE_ACTIVE_TX]",
            "\n".join(captured.output),
        )

    async def test_mv_d_connector_zero_telemetry_is_safely_ignored(self):
        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                connector_id=0,
                timestamp="2026-07-20T11:50:00Z",
                sampled_values=[
                    {"value": "220", "measurand": "Voltage", "unit": "V"}
                ],
            )

        with main.get_conn() as conn:
            meter_count = conn.execute(
                "SELECT COUNT(*) FROM meter_values"
            ).fetchone()[0]

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(meter_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        self.assertEqual(self.balance_stop_candidates, [])
        self.assertIn(
            "[MV][SKIPPED_NON_TRANSACTION_TELEMETRY]",
            "\n".join(captured.output),
        )

    async def test_mv_e_missing_transaction_id_lookup_error_fails_closed(self):
        _, _tx_a = await self._start(1000, "2026-07-20T12:00:00Z")

        with self.assertLogs(level="ERROR") as captured:
            with patch.object(
                main,
                "get_conn",
                side_effect=sqlite3.OperationalError("database is locked"),
            ):
                response = await self._meter_without_transaction(
                    timestamp="2026-07-20T12:01:00Z"
                )

        with main.get_conn() as conn:
            meter_count = conn.execute(
                "SELECT COUNT(*) FROM meter_values"
            ).fetchone()[0]

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(meter_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        self.assertEqual(self.balance_stop_candidates, [])
        self.assertIn(
            "[MV][MISSING_TX_ID_LOOKUP_FAILED]",
            "\n".join(captured.output),
        )

    async def test_mv_f_missing_measurand_defaults_to_energy_for_active_tx(self):
        _, tx_a = await self._start(1000, "2026-07-20T12:10:00Z")

        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                timestamp="2026-07-20T12:11:00Z",
                sampled_values=[{"value": "1600", "unit": "Wh"}],
            )

        with main.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT transaction_id, value, measurand, unit
                FROM meter_values
                ORDER BY id
                """
            ).fetchall()

        joined_logs = "\n".join(captured.output)
        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(
            rows,
            [(tx_a, 1600.0, "Energy.Active.Import.Register", "Wh")],
        )
        self.assertEqual(main.live_status_cache[CP_ID]["status"], "Charging")
        self.assertIn("[MV][RESOLVED_MISSING_TX_ID]", joined_logs)
        self.assertNotIn("[MV][SKIPPED_NON_TRANSACTION_TELEMETRY]", joined_logs)

    async def test_mv_g_missing_measurand_after_stop_is_rejected(self):
        _, tx_a = await self._start(1000, "2026-07-20T12:20:00Z")
        await self._stop(tx_a, 1600, "2026-07-20T12:30:00Z")
        before = self._snapshot()

        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                timestamp="2026-07-20T12:31:00Z",
                sampled_values=[{"value": "1700", "unit": "Wh"}],
            )
        after = self._snapshot()

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(after["meter_values"], before["meter_values"])
        self.assertEqual(after["payments"], before["payments"])
        self.assertEqual(after["balance"], before["balance"])
        self.assertEqual(after["line_calls"], before["line_calls"])
        self.assertEqual(after["live_status_cache"]["status"], "Available")
        self.assertIn(
            "[MV][REJECTED_MISSING_TX_ID_NO_ACTIVE_TX]",
            "\n".join(captured.output),
        )

    async def test_mv_h_connector_zero_missing_measurand_is_safely_ignored(self):
        with self.assertLogs(level="WARNING") as captured:
            response = await self._meter_without_transaction(
                connector_id=0,
                timestamp="2026-07-20T12:40:00Z",
                sampled_values=[{"value": "1600", "unit": "Wh"}],
            )

        with main.get_conn() as conn:
            meter_count = conn.execute(
                "SELECT COUNT(*) FROM meter_values"
            ).fetchone()[0]

        self.assertEqual(type(response).__name__, "MeterValuesPayload")
        self.assertEqual(meter_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        self.assertEqual(self.balance_stop_candidates, [])
        self.assertIn(
            "[MV][SKIPPED_NON_TRANSACTION_TELEMETRY]",
            "\n".join(captured.output),
        )

    async def test_mv_i_missing_measurand_without_valid_value_is_ignored(self):
        _, _tx_a = await self._start(1000, "2026-07-20T12:50:00Z")

        response_missing = await self._meter_without_transaction(
            timestamp="2026-07-20T12:51:00Z",
            sampled_values=[{"unit": "Wh"}],
        )
        response_invalid = await self._meter_without_transaction(
            timestamp="2026-07-20T12:52:00Z",
            sampled_values=[{"value": "not-a-number", "unit": "Wh"}],
        )

        with main.get_conn() as conn:
            meter_count = conn.execute(
                "SELECT COUNT(*) FROM meter_values"
            ).fetchone()[0]
            payment_count = conn.execute(
                "SELECT COUNT(*) FROM payments"
            ).fetchone()[0]

        self.assertEqual(type(response_missing).__name__, "MeterValuesPayload")
        self.assertEqual(type(response_invalid).__name__, "MeterValuesPayload")
        self.assertEqual(meter_count, 0)
        self.assertEqual(payment_count, 0)
        self.assertIsNone(main.live_status_cache.get(CP_ID))
        self.assertEqual(self.balance_stop_candidates, [])

    async def test_led_and_status_semantics_use_no_new_physical_control_command(self):
        _, tx_a = await self._start(1000, "2026-07-20T10:00:00Z")
        await self._status("SuspendedEV", "2026-07-20T10:10:00Z")
        self.assertEqual(main.charging_point_status[CP_ID]["status"], "SuspendedEV")

        await self._status("Charging", "2026-07-20T10:11:00Z")
        self.assertEqual(main.charging_point_status[CP_ID]["status"], "Charging")
        self.assertEqual(main.charging_point_status[CP_ID]["active_transaction_id"], tx_a)

        guarded_sources = "\n".join(
            [
                inspect.getsource(main.ChargePoint.on_status_notification),
                inspect.getsource(main.ChargePoint.on_start_transaction),
                inspect.getsource(main.ChargePoint.on_meter_values),
            ]
        )
        for forbidden in (
            "ResetPayload",
            "UnlockConnectorPayload",
            "ClearChargingProfilePayload",
            "RemoteStopTransactionPayload",
        ):
            self.assertNotIn(forbidden, guarded_sources)
        self.assertNotIn("LED_GREEN", guarded_sources)
        self.assertNotIn("LED_BLUE", guarded_sources)
        self.assertNotIn("await self.call", inspect.getsource(main.ChargePoint.on_status_notification))


def tearDownModule():
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
