import asyncio
import gc
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import main
from household_account_service import (
    bind_card_to_account,
    connect,
    create_household_account,
    get_account_by_id,
    update_account_card,
)
from tests.test_household_accounts import make_db


class HouseholdLineBroadcastTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_file = make_db(Path(self.tempdir.name))
        with patch.object(main, "DB_FILE", self.db_file):
            main.ensure_line_bindings_table()
            main.ensure_line_message_logs_table()

    def tearDown(self):
        gc.collect()
        self.tempdir.cleanup()

    def _create_account(self, floor, parking, cards, balance=1000):
        conn = connect(self.db_file)
        account = create_household_account(conn, floor, parking, balance)
        for card_id, _, _, _ in cards:
            bind_card_to_account(conn, account["account_id"], card_id)
        conn.commit()

        for card_id, status, line_user_id, enabled in cards:
            if status != "active":
                update_account_card(conn, card_id, status=status)

        for card_id, _, line_user_id, enabled in cards:
            if line_user_id is not None:
                conn.execute(
                    """
                    INSERT INTO line_bindings(
                        id_tag,line_user_id,display_name,enabled,
                        created_at,updated_at
                    ) VALUES (?,?,?,?, 'now','now')
                    """,
                    (card_id, line_user_id, line_user_id, int(enabled)),
                )
        conn.commit()
        conn.close()
        return account

    def _create_transaction(self, tx_id, account, card_id, *, auto_stop=False):
        conn = connect(self.db_file)
        conn.execute(
            """
            INSERT INTO transactions(
                transaction_id,id_tag,charge_point_id,connector_id,
                meter_start,start_timestamp,account_id,floor_no,
                parking_space_no,balance_after,auto_stop_reason
            ) VALUES (?,?,?,1,0,'2026-07-22T00:00:00+00:00',?,?,?,?,?)
            """,
            (
                tx_id,
                card_id,
                f"CP-{tx_id}",
                account["account_id"],
                account["floor_no"],
                account["parking_space_no"],
                10,
                (
                    main.AUTO_STOP_REASON_BALANCE_INSUFFICIENT
                    if auto_stop
                    else None
                ),
            ),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _completed_message(card_id):
        return {
            "idTag": card_id,
            "message": f"completed transaction card={card_id}",
            "data": {"idTag": card_id},
        }

    def test_unbound_transaction_card_notifies_bound_household_user_and_actual_card(self):
        account = self._create_account(
            "5F",
            "B12",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-B", "active", None, False),
            ],
        )
        self._create_transaction(1001, account, "CARD-B")

        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "build_charge_completed_line_message",
                return_value=self._completed_message("CARD-B"),
            ),
            patch.object(
                main, "send_line_message", return_value={"ok": True}
            ) as sender,
        ):
            result = main.send_charge_completed_line_notification(1001)

        self.assertEqual(result["summary"], {"recipients": 1, "sent": 1, "failed": 0, "skipped": 0})
        sender.assert_called_once()
        self.assertEqual(sender.call_args.kwargs["line_user_id"], "USER-A")
        self.assertIn("CARD-B", sender.call_args.kwargs["message"])
        self.assertNotIn("CARD-A", sender.call_args.kwargs["message"])

    def test_two_household_users_receive_each_transaction_once(self):
        account = self._create_account(
            "6F",
            "B13",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-B", "active", "USER-B", True),
            ],
        )
        self._create_transaction(1002, account, "CARD-A")
        self._create_transaction(1003, account, "CARD-B")

        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "build_charge_completed_line_message",
                side_effect=lambda tx_id: self._completed_message(
                    "CARD-A" if tx_id == 1002 else "CARD-B"
                ),
            ),
            patch.object(
                main, "send_line_message", return_value={"ok": True}
            ) as sender,
        ):
            first = main.send_charge_completed_line_notification(1002)
            second = main.send_charge_completed_line_notification(1003)

        self.assertEqual(first["summary"]["sent"], 2)
        self.assertEqual(second["summary"]["sent"], 2)
        self.assertEqual(sender.call_count, 4)
        self.assertEqual(
            sorted(call.kwargs["line_user_id"] for call in sender.call_args_list),
            ["USER-A", "USER-A", "USER-B", "USER-B"],
        )

    def test_resolver_deduplicates_duplicate_line_user_rows(self):
        transaction_cursor = MagicMock()
        transaction_cursor.fetchone.return_value = (1, "7F", "B14", "CARD-C")
        recipient_cursor = MagicMock()
        recipient_cursor.fetchall.return_value = [
            ("CARD-A", "USER-A", "Alpha"),
            ("CARD-B", " USER-A ", "Alpha old"),
            ("CARD-C", "USER-B", "Beta"),
        ]
        conn = MagicMock()
        conn.execute.side_effect = [transaction_cursor, recipient_cursor]

        recipients = main.resolve_household_line_recipients(conn, 1004)

        self.assertEqual([item["line_user_id"] for item in recipients], ["USER-A", "USER-B"])
        self.assertEqual(recipients[0]["source_card_ids"], ["CARD-A", "CARD-B"])

    def test_repeated_stop_debits_once_and_notifies_two_users_once(self):
        account = self._create_account(
            "8F",
            "B15",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-B", "active", "USER-B", True),
            ],
        )
        self._create_transaction(1005, account, "CARD-A")
        stop_args = {
            "transaction_id": 1005,
            "meter_stop": 1000,
            "timestamp": "2026-07-22T01:00:00+00:00",
            "reason": "Remote",
        }
        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "_calculate_multi_period_cost_detailed",
                return_value={"total": 100, "segments": []},
            ),
            patch.object(
                main, "get_community_settings", return_value={"surcharge_per_kwh": 0}
            ),
            patch.object(
                main,
                "build_charge_completed_line_message",
                return_value=self._completed_message("CARD-A"),
            ),
            patch.object(
                main,
                "schedule_charge_completed_line_notification",
                side_effect=main.send_charge_completed_line_notification,
            ),
            patch.object(main, "schedule_low_balance_line_notification"),
            patch.object(
                main, "schedule_auto_stop_balance_insufficient_line_notification"
            ),
            patch.object(main, "request_rebalance"),
            patch.object(
                main, "send_line_message", return_value={"ok": True}
            ) as sender,
        ):
            cp = SimpleNamespace(id="CP-1005")
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_args))
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_args))

        conn = connect(self.db_file)
        self.assertEqual(get_account_by_id(conn, account["account_id"])["balance"], 900)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM payments WHERE transaction_id=1005"
            ).fetchone()[0],
            1,
        )
        conn.close()
        self.assertEqual(sender.call_count, 2)

    def test_three_event_types_each_allow_two_recipients(self):
        account = self._create_account(
            "9F",
            "B16",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-B", "active", "USER-B", True),
            ],
        )
        self._create_transaction(1006, account, "CARD-A", auto_stop=True)
        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "build_charge_completed_line_message",
                return_value=self._completed_message("CARD-A"),
            ),
            patch.object(
                main,
                "build_low_balance_line_message",
                return_value={
                    "idTag": "CARD-A",
                    "message": "low CARD-A",
                    "data": {"idTag": "CARD-A", "balanceAfter": 10},
                },
            ),
            patch.object(
                main,
                "build_auto_stop_balance_insufficient_line_message",
                return_value={
                    "idTag": "CARD-A",
                    "message": "auto CARD-A",
                    "data": {
                        "idTag": "CARD-A",
                        "autoStopReason": main.AUTO_STOP_REASON_BALANCE_INSUFFICIENT,
                    },
                },
            ),
            patch.object(
                main, "send_line_message", return_value={"ok": True}
            ) as sender,
        ):
            results = [
                main.send_charge_completed_line_notification(1006),
                main.send_low_balance_line_notification(1006),
                main.send_auto_stop_balance_insufficient_line_notification(1006),
            ]
            duplicates = [
                main.send_charge_completed_line_notification(1006),
                main.send_low_balance_line_notification(1006),
                main.send_auto_stop_balance_insufficient_line_notification(1006),
            ]

        self.assertEqual(sender.call_count, 6)
        self.assertTrue(all(result["summary"]["sent"] == 2 for result in results))
        self.assertTrue(
            all(result["summary"]["skipped"] == 2 for result in duplicates)
        )

    def test_one_recipient_failure_does_not_block_other_and_is_not_retried(self):
        account = self._create_account(
            "10F",
            "B17",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-B", "active", "USER-B", True),
            ],
        )
        self._create_transaction(1007, account, "CARD-A")

        def send(*, line_user_id, message):
            if line_user_id == "USER-B":
                return {"ok": False, "error": "timeout"}
            return {"ok": True}

        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "build_charge_completed_line_message",
                return_value=self._completed_message("CARD-A"),
            ),
            patch.object(main, "send_line_message", side_effect=send) as sender,
        ):
            first = main.send_charge_completed_line_notification(1007)
            duplicate = main.send_charge_completed_line_notification(1007)

        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["summary"]["sent"], 1)
        self.assertEqual(first["summary"]["failed"], 1)
        self.assertEqual(duplicate["summary"]["skipped"], 2)
        self.assertEqual(sender.call_count, 2)

        conn = connect(self.db_file)
        statuses = dict(
            conn.execute(
                """
                SELECT line_user_id,status
                FROM line_recipient_notification_claims
                WHERE event_type='charge_completed' AND transaction_id=1007
                """
            ).fetchall()
        )
        logs = dict(
            conn.execute(
                """
                SELECT line_user_id,status
                FROM line_message_logs
                WHERE event_type='charge_completed' AND transaction_id=1007
                """
            ).fetchall()
        )
        conn.close()
        self.assertEqual(statuses, {"USER-A": "sent", "USER-B": "failed"})
        self.assertEqual(logs, {"USER-A": "sent", "USER-B": "failed"})

    def test_no_recipients_skips_without_affecting_stop_settlement(self):
        account = self._create_account(
            "11F", "B18", [("CARD-A", "active", None, False)]
        )
        self._create_transaction(1008, account, "CARD-A")
        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "_calculate_multi_period_cost_detailed",
                return_value={"total": 100, "segments": []},
            ),
            patch.object(
                main, "get_community_settings", return_value={"surcharge_per_kwh": 0}
            ),
            patch.object(
                main,
                "build_charge_completed_line_message",
                return_value=self._completed_message("CARD-A"),
            ),
            patch.object(
                main,
                "schedule_charge_completed_line_notification",
                side_effect=main.send_charge_completed_line_notification,
            ),
            patch.object(main, "schedule_low_balance_line_notification"),
            patch.object(
                main, "schedule_auto_stop_balance_insufficient_line_notification"
            ),
            patch.object(main, "request_rebalance"),
            patch.object(main, "send_line_message") as sender,
        ):
            asyncio.run(
                main.ChargePoint.on_stop_transaction(
                    SimpleNamespace(id="CP-1008"),
                    transaction_id=1008,
                    meter_stop=1000,
                    timestamp="2026-07-22T01:00:00+00:00",
                    reason="Local",
                )
            )

        conn = connect(self.db_file)
        self.assertEqual(get_account_by_id(conn, account["account_id"])["balance"], 900)
        self.assertEqual(
            conn.execute(
                """
                SELECT reason FROM line_message_logs
                WHERE event_type='charge_completed' AND transaction_id=1008
                """
            ).fetchone()[0],
            "no_household_recipients",
        )
        conn.close()
        sender.assert_not_called()

    def test_recipient_resolution_never_crosses_account_and_excludes_disabled_card(self):
        account_1 = self._create_account(
            "12F",
            "B19",
            [
                ("CARD-A", "active", "USER-A", True),
                ("CARD-DISABLED", "disabled", "USER-DISABLED", True),
            ],
        )
        self._create_account(
            "13F", "B20", [("CARD-B", "active", "USER-B", True)]
        )
        self._create_transaction(1009, account_1, "CARD-A")
        with patch.object(main, "DB_FILE", self.db_file):
            conn = connect(self.db_file)
            recipients = main.resolve_household_line_recipients(conn, 1009)
            conn.close()

        self.assertEqual(
            [recipient["line_user_id"] for recipient in recipients], ["USER-A"]
        )

    def test_recipient_claim_schema_is_idempotent_and_legacy_table_is_retained(self):
        with patch.object(main, "DB_FILE", self.db_file):
            main.ensure_line_message_logs_table()
            main.ensure_line_message_logs_table()
        conn = sqlite3.connect(self.db_file)
        legacy_pk = [
            (row[1], row[5])
            for row in conn.execute("PRAGMA table_info(line_notification_claims)")
            if row[5]
        ]
        recipient_pk = [
            (row[1], row[5])
            for row in conn.execute(
                "PRAGMA table_info(line_recipient_notification_claims)"
            )
            if row[5]
        ]
        conn.close()
        self.assertEqual(
            legacy_pk, [("event_type", 1), ("transaction_id", 2)]
        )
        self.assertEqual(
            recipient_pk,
            [
                ("event_type", 1),
                ("transaction_id", 2),
                ("line_user_id", 3),
            ],
        )


if __name__ == "__main__":
    unittest.main()
