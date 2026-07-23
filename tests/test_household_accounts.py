import asyncio
import gc
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import main
from household_account_service import (
    HouseholdAccountError,
    bind_card_to_account,
    cancel_enrollment,
    capture_unknown_card,
    confirm_enrollment,
    connect,
    create_enrollment_session,
    create_household_account,
    debit_household_account_atomic,
    disable_account_card,
    ensure_legacy_account_for_card,
    ensure_schema,
    get_account_by_id,
    get_account_balance_by_card,
    resolve_account_by_card,
    topup_household_account,
    update_account_card,
    update_household_account,
)
from migrate_household_accounts import migrate


def make_db(directory: Path) -> str:
    db_file = str(directory / "household.sqlite3")
    conn = connect(db_file)
    conn.executescript(
        """
        CREATE TABLE cards (id INTEGER PRIMARY KEY, card_id TEXT UNIQUE, balance REAL DEFAULT 0);
        CREATE TABLE id_tags (id_tag TEXT PRIMARY KEY, status TEXT, valid_until TEXT);
        CREATE TABLE card_owners (card_id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE users (
            id_tag TEXT PRIMARY KEY, name TEXT, department TEXT, card_number TEXT
        );
        CREATE TABLE card_whitelist (id INTEGER PRIMARY KEY, card_id TEXT, charge_point_id TEXT);
        CREATE TABLE charge_points (
            id INTEGER PRIMARY KEY, charge_point_id TEXT UNIQUE, name TEXT,
            status TEXT DEFAULT 'enabled', created_at TEXT, max_current_a REAL
        );
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY, id_tag TEXT, charge_point_id TEXT,
            connector_id INTEGER, meter_start INTEGER, start_timestamp TEXT,
            meter_stop INTEGER, stop_timestamp TEXT, reason TEXT,
            balance_before REAL, balance_after REAL, surplus_amount REAL,
            auto_stop_reason TEXT, auto_stop_triggered_at TEXT,
            auto_stop_balance REAL, auto_stop_estimated_amount REAL
        );
        CREATE TABLE stop_transactions (
            id INTEGER PRIMARY KEY, transaction_id INTEGER, meter_stop INTEGER,
            timestamp TEXT, reason TEXT
        );
        CREATE TABLE meter_values (
            id INTEGER PRIMARY KEY, charge_point_id TEXT, connector_id INTEGER,
            transaction_id INTEGER, value REAL, measurand TEXT, unit TEXT,
            timestamp TEXT, context TEXT, format TEXT
        );
        CREATE TABLE payments (
            id INTEGER PRIMARY KEY, transaction_id INTEGER, base_fee REAL,
            energy_fee REAL, overuse_fee REAL, total_amount REAL, paid_at TEXT
        );
        CREATE TABLE status_logs (
            id INTEGER PRIMARY KEY, charge_point_id TEXT, connector_id INTEGER,
            status TEXT, timestamp TEXT, error_code TEXT
        );
        """
    )
    conn.execute("INSERT INTO charge_points(charge_point_id,name) VALUES ('CP-1','One'),('CP-2','Two')")
    ensure_schema(conn)
    conn.close()
    return db_file


class HouseholdAccountTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_file = make_db(Path(self.tempdir.name))

    def tearDown(self):
        gc.collect()
        self.tempdir.cleanup()

    def test_shared_balance_cards_and_statuses(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "5F", "A-05-01", 3000)
        bind_card_to_account(conn, account["account_id"], "DAD")
        bind_card_to_account(conn, account["account_id"], "MOM")
        self.assertEqual(get_account_balance_by_card(conn, "DAD")["balance"], 3000)
        self.assertEqual(get_account_balance_by_card(conn, "MOM")["balance"], 3000)
        self.assertEqual(debit_household_account_atomic(conn, account["account_id"], 250), (3000.0, 2750.0))
        self.assertEqual(get_account_balance_by_card(conn, "MOM")["balance"], 2750)
        disable_account_card(conn, "DAD")
        self.assertEqual(resolve_account_by_card(conn, "DAD")["card_status"], "disabled")
        self.assertEqual(resolve_account_by_card(conn, "MOM")["card_status"], "active")
        update_household_account(conn, account["account_id"], status="disabled")
        self.assertEqual(resolve_account_by_card(conn, "MOM")["account_status"], "disabled")
        with self.assertRaises(HouseholdAccountError):
            bind_card_to_account(conn, account["account_id"], "MOM")
        conn.close()

    def test_floor_parking_validation_uniqueness_and_safe_updates(self):
        conn = connect(self.db_file)
        with self.assertRaisesRegex(HouseholdAccountError, "floor_no is required"):
            create_household_account(conn, "", "B12")
        with self.assertRaisesRegex(HouseholdAccountError, "parking_space_no is required"):
            create_household_account(conn, "5F", "")

        account = create_household_account(conn, "5F", "B12", 100)
        second = create_household_account(conn, "6F", "B13", 200)
        self.assertEqual(account["floor_no"], "5F")
        self.assertEqual(account["parking_space_no"], "B12")
        self.assertNotEqual(account["account_code"], second["account_code"])
        self.assertTrue(account["account_code"].startswith("ACCOUNT-"))
        self.assertEqual(account["account_name"], "")
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(conn, "5F", "B12")
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(conn, " 5f ", " b12 ")
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(conn, "５Ｆ", "Ｂ１２")
        with self.assertRaisesRegex(HouseholdAccountError, "floor_no is required"):
            create_household_account(conn, "   ", "B99")
        with self.assertRaisesRegex(HouseholdAccountError, "control characters"):
            create_household_account(conn, "5F\n6F", "B99")
        with self.assertRaisesRegex(HouseholdAccountError, "at most 64"):
            create_household_account(conn, "F" * 65, "B99")
        with self.assertRaisesRegex(HouseholdAccountError, "must be a string"):
            create_household_account(conn, 5, "B99")

        updated = update_household_account(
            conn,
            account["account_id"],
            floor_no="6F",
            parking_space_no="C01",
        )
        self.assertEqual((updated["floor_no"], updated["parking_space_no"]), ("6F", "C01"))
        self.assertEqual(updated["balance"], 100)
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            update_household_account(
                conn,
                account["account_id"],
                floor_no="6f",
                parking_space_no="b13",
            )
        conn.close()

    def test_household_api_conflicts_are_400_or_409(self):
        with patch.object(main, "DB_FILE", self.db_file):
            first = main.api_create_household_account(
                {"floorNo": "5F", "parkingSpaceNo": "B12"}
            )
            second = main.api_create_household_account(
                {"floorNo": "6F", "parkingSpaceNo": "B13"}
            )
            with self.assertRaises(main.HTTPException) as duplicate_post:
                main.api_create_household_account(
                    {"floorNo": "5f", "parkingSpaceNo": "b12"}
                )
            self.assertEqual(duplicate_post.exception.status_code, 409)

            with self.assertRaises(main.HTTPException) as duplicate_put:
                main.api_update_household_account(
                    second["account_id"],
                    {"floorNo": " 5F ", "parkingSpaceNo": " B12 "},
                )
            self.assertEqual(duplicate_put.exception.status_code, 409)

            with self.assertRaises(main.HTTPException) as alias_conflict:
                main.api_create_household_account(
                    {
                        "floor_no": "5F",
                        "floorNo": "6F",
                        "parking_space_no": "B12",
                        "parkingSpaceNo": "B13",
                    }
                )
            self.assertEqual(alias_conflict.exception.status_code, 400)

            partial = main.api_update_household_account(
                first["account_id"], {"floorNo": "7F"}
            )
            self.assertEqual(
                (partial["floorNo"], partial["parkingSpaceNo"]), ("7F", "B12")
            )
            with self.assertRaises(main.HTTPException) as empty_put:
                main.api_update_household_account(
                    first["account_id"], {"parkingSpaceNo": "   "}
                )
            self.assertEqual(empty_put.exception.status_code, 400)
            with self.assertRaises(main.HTTPException) as null_put:
                main.api_update_household_account(
                    first["account_id"], {"floor_no": None}
                )
            self.assertEqual(null_put.exception.status_code, 400)

    def test_household_api_uses_floor_parking_and_ignores_legacy_identity_inputs(self):
        with patch.object(main, "DB_FILE", self.db_file):
            created = main.api_create_household_account(
                {
                    "floorNo": "8F",
                    "parkingSpaceNo": "P08",
                    "balance": 200,
                    "account_name": "must-not-be-used",
                }
            )
            account_id = created["account_id"]
            self.assertEqual((created["floorNo"], created["parkingSpaceNo"]), ("8F", "P08"))
            self.assertNotIn("account_name", created)

            updated = main.api_update_household_account(
                account_id,
                {
                    "floor_no": "9F",
                    "parking_space_no": "P09",
                    "account_code": "must-not-be-used",
                    "status": "active",
                },
            )
            self.assertEqual((updated["floorNo"], updated["parkingSpaceNo"]), ("9F", "P09"))

            main.api_add_account_card(
                account_id,
                {
                    "card_id": "API-CARD",
                    "card_holder_name": "must-not-be-written",
                    "relationship": "must-not-be-written",
                    "charge_point_ids": ["CP-1"],
                },
            )
            accounts = main.api_list_household_accounts()
            card = accounts[0]["cards"][0]
            self.assertEqual(card["card_id"], "API-CARD")
            self.assertNotIn("card_holder_name", card)
            self.assertNotIn("relationship", card)
            self.assertEqual(
                main.api_get_household_account(account_id)["floorNo"], "9F"
            )
            self.assertEqual(
                main.api_topup_household_account(account_id, {"amount": 50})["balance"],
                250,
            )
            self.assertEqual(
                main.api_update_account_card("API-CARD", {"status": "disabled"})[
                    "card_status"
                ],
                "disabled",
            )
            self.assertEqual(
                main.api_disable_account_card("API-CARD")["card_status"], "disabled"
            )

            session = main.api_create_card_enrollment(
                {
                    "account_id": account_id,
                    "charge_point_id": "CP-2",
                    "card_holder_name": "must-not-be-written",
                    "relationship": "must-not-be-written",
                }
            )
            self.assertIsNone(session["card_holder_name"])
            self.assertIsNone(session["relationship"])
            with connect(self.db_file) as account_conn:
                self.assertTrue(
                    capture_unknown_card(account_conn, "ENROLL-CARD", "CP-2")[
                        "captured"
                    ]
                )
            fetched_session = main.api_get_card_enrollment(session["enrollment_id"])
            self.assertEqual(fetched_session["detected_id_tag"], "ENROLL-CARD")
            confirmed = main.api_confirm_card_enrollment(session["enrollment_id"])
            self.assertEqual(confirmed["card_id"], "ENROLL-CARD")

    def test_incremental_topup_and_concurrent_debit(self):
        conn = connect(self.db_file)
        account_id = create_household_account(conn, "1F", "B-01", 80)["account_id"]
        self.assertEqual(topup_household_account(conn, account_id, 20)["balance"], 100)
        conn.close()

        def debit():
            worker = connect(self.db_file)
            try:
                return debit_household_account_atomic(worker, account_id, 70)
            finally:
                worker.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: debit(), range(2)))
        verify = connect(self.db_file)
        self.assertEqual(get_account_by_id(verify, account_id)["balance"], 0)
        self.assertEqual(sorted(after for _, after in results), [0.0, 30.0])
        verify.close()

    def test_enrollment_is_cp_scoped_confirmed_and_audited(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "1F", "C-01", 500)
        self.assertFalse(capture_unknown_card(conn, "NEW-1", "CP-1")["captured"])
        session = create_enrollment_session(conn, account["account_id"], "CP-1")
        self.assertFalse(capture_unknown_card(conn, "NEW-1", "CP-2")["captured"])
        self.assertTrue(capture_unknown_card(conn, "NEW-1", "CP-1")["captured"])
        self.assertEqual(capture_unknown_card(conn, "NEW-2", "CP-1")["result"], "enrollment_already_has_card")
        self.assertIsNone(conn.execute("SELECT 1 FROM id_tags WHERE id_tag='NEW-1'").fetchone())
        confirmed = confirm_enrollment(conn, session["enrollment_id"])
        self.assertEqual(confirmed["account_id"], account["account_id"])
        self.assertEqual(conn.execute("SELECT status FROM id_tags WHERE id_tag='NEW-1'").fetchone()[0], "Accepted")
        self.assertIsNotNone(conn.execute(
            "SELECT 1 FROM card_whitelist WHERE card_id='NEW-1' AND charge_point_id='CP-1'"
        ).fetchone())
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM unknown_card_logs").fetchone()[0], 4)
        conn.close()

    def test_enrollment_confirm_rolls_back_every_write_on_whitelist_failure(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "4F", "ROLLBACK", 500)
        session = create_enrollment_session(conn, account["account_id"], "CP-1")
        self.assertTrue(capture_unknown_card(conn, "ROLLBACK-CARD", "CP-1")["captured"])
        conn.execute(
            """
            CREATE TRIGGER fail_test_whitelist
            BEFORE INSERT ON card_whitelist
            WHEN NEW.card_id='ROLLBACK-CARD'
            BEGIN
                SELECT RAISE(ABORT, 'forced whitelist failure');
            END
            """
        )
        conn.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            confirm_enrollment(conn, session["enrollment_id"])

        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM id_tags WHERE id_tag='ROLLBACK-CARD'"
            ).fetchone()
        )
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM cards WHERE card_id='ROLLBACK-CARD'"
            ).fetchone()
        )
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM account_cards WHERE card_id='ROLLBACK-CARD'"
            ).fetchone()
        )
        self.assertEqual(
            conn.execute(
                """
                SELECT status FROM card_enrollment_sessions
                WHERE enrollment_id=?
                """,
                (session["enrollment_id"],),
            ).fetchone()[0],
            "detected",
        )
        conn.close()

    def test_schema_preflight_reports_duplicate_account_ids(self):
        conn = connect(self.db_file)
        conn.execute("DROP INDEX idx_household_accounts_floor_parking")
        now = "2026-07-23T00:00:00+00:00"
        conn.executemany(
            """
            INSERT INTO household_accounts(
                account_code,account_name,floor_no,parking_space_no,
                balance,status,created_at,updated_at
            ) VALUES (?, '', ?, ?, 0, 'active', ?, ?)
            """,
            [
                ("DUP-A", " 5F ", "Ｂ１２", now, now),
                ("DUP-B", "5f", "b12", now, now),
            ],
        )
        conn.commit()
        ids = [
            row[0]
            for row in conn.execute(
                "SELECT account_id FROM household_accounts ORDER BY account_id"
            )
        ]
        with self.assertRaisesRegex(
            HouseholdAccountError,
            rf"account_ids=\[{ids[0]}, {ids[1]}\]",
        ):
            ensure_schema(conn)
        self.assertIsNone(
            conn.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='index'
                  AND name='idx_household_accounts_floor_parking'
                """
            ).fetchone()
        )
        conn.close()

    def test_cancelled_and_expired_enrollment_cannot_capture(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "1F", "D-01", 100)
        cancelled = create_enrollment_session(conn, account["account_id"], "CP-1")
        cancel_enrollment(conn, cancelled["enrollment_id"])
        self.assertFalse(capture_unknown_card(conn, "CANCELLED", "CP-1")["captured"])
        expired = create_enrollment_session(conn, account["account_id"], "CP-1", duration_seconds=1)
        conn.execute(
            "UPDATE card_enrollment_sessions SET expires_at='2000-01-01T00:00:00+00:00' WHERE enrollment_id=?",
            (expired["enrollment_id"],),
        )
        conn.commit()
        self.assertFalse(capture_unknown_card(conn, "EXPIRED", "CP-1")["captured"])
        conn.close()

    def test_authorize_first_enrollment_tap_stays_invalid(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "1F", "E-01", 100)
        session = create_enrollment_session(conn, account["account_id"], "CP-1")
        conn.close()
        with patch.object(main, "DB_FILE", self.db_file):
            first = asyncio.run(main.ChargePoint.on_authorize(SimpleNamespace(id="CP-1"), "TAP-1"))
            self.assertEqual(first.id_tag_info["status"], "Invalid")
            verify = connect(self.db_file)
            row = verify.execute(
                "SELECT status,detected_id_tag FROM card_enrollment_sessions WHERE enrollment_id=?",
                (session["enrollment_id"],),
            ).fetchone()
            self.assertEqual(tuple(row), ("detected", "TAP-1"))
            confirm_enrollment(verify, session["enrollment_id"])
            verify.close()
            second = asyncio.run(main.ChargePoint.on_authorize(SimpleNamespace(id="CP-1"), "TAP-1"))
            self.assertEqual(second.id_tag_info["status"], "Accepted")

    def test_card_and_account_status_authorization_stays_synchronized(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "3F", "STATUS", 500)
        bind_card_to_account(conn, account["account_id"], "STATUS-A")
        bind_card_to_account(conn, account["account_id"], "STATUS-B")
        conn.close()

        with patch.object(main, "DB_FILE", self.db_file):
            accepted_a = asyncio.run(
                main.ChargePoint.on_authorize(
                    SimpleNamespace(id="CP-1"), "STATUS-A"
                )
            )
            self.assertEqual(accepted_a.id_tag_info["status"], "Accepted")

            conn = connect(self.db_file)
            disable_account_card(conn, "STATUS-A")
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM id_tags WHERE id_tag='STATUS-A'"
                ).fetchone()[0],
                "Blocked",
            )
            conn.close()
            blocked_a = asyncio.run(
                main.ChargePoint.on_authorize(
                    SimpleNamespace(id="CP-1"), "STATUS-A"
                )
            )
            accepted_b = asyncio.run(
                main.ChargePoint.on_authorize(
                    SimpleNamespace(id="CP-1"), "STATUS-B"
                )
            )
            self.assertEqual(blocked_a.id_tag_info["status"], "Blocked")
            self.assertEqual(accepted_b.id_tag_info["status"], "Accepted")

            conn = connect(self.db_file)
            update_account_card(conn, "STATUS-A", status="active")
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM id_tags WHERE id_tag='STATUS-A'"
                ).fetchone()[0],
                "Accepted",
            )
            update_household_account(
                conn, account["account_id"], status="disabled"
            )
            conn.close()
            self.assertEqual(
                asyncio.run(
                    main.ChargePoint.on_authorize(
                        SimpleNamespace(id="CP-1"), "STATUS-A"
                    )
                ).id_tag_info["status"],
                "Blocked",
            )
            self.assertEqual(
                asyncio.run(
                    main.ChargePoint.on_authorize(
                        SimpleNamespace(id="CP-1"), "STATUS-B"
                    )
                ).id_tag_info["status"],
                "Blocked",
            )

            conn = connect(self.db_file)
            update_household_account(
                conn, account["account_id"], status="active"
            )
            conn.close()
            self.assertEqual(
                asyncio.run(
                    main.ChargePoint.on_authorize(
                        SimpleNamespace(id="CP-1"), "STATUS-A"
                    )
                ).id_tag_info["status"],
                "Accepted",
            )

    def test_legacy_migration_is_idempotent_and_keeps_cards(self):
        raw = sqlite3.connect(self.db_file)
        raw.execute("INSERT INTO cards(card_id,balance) VALUES ('OLD-A',125),('OLD-B',75)")
        raw.execute("INSERT INTO card_owners(card_id,name) VALUES ('OLD-A','甲'),('OLD-B','甲')")
        raw.execute(
            """
            INSERT INTO transactions(
                transaction_id,id_tag,balance_before,balance_after,surplus_amount
            ) VALUES (77,'OLD-A',125,100,3.5)
            """
        )
        raw.execute(
            "INSERT INTO payments(transaction_id,total_amount) VALUES (77,25)"
        )
        raw.commit()
        raw.close()
        first = migrate(self.db_file, create_backup=True)
        after_first = connect(self.db_file)
        balance_after_first = get_account_balance_by_card(after_first, "OLD-A")["balance"]
        after_first.close()
        second = migrate(self.db_file, create_backup=False)
        self.assertTrue(Path(first["backup"]).exists())
        self.assertEqual(first["accounts_created"], 2)
        self.assertEqual(second["accounts_created"], 0)
        self.assertEqual(second["cards_linked"], 0)
        conn = connect(self.db_file)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM cards").fetchone()[0], 2)
        self.assertEqual(get_account_balance_by_card(conn, "OLD-A")["balance"], 125)
        self.assertEqual(get_account_balance_by_card(conn, "OLD-A")["balance"], balance_after_first)
        self.assertEqual(get_account_balance_by_card(conn, "OLD-B")["balance"], 75)
        self.assertNotEqual(resolve_account_by_card(conn, "OLD-A")["account_id"], resolve_account_by_card(conn, "OLD-B")["account_id"])
        self.assertIsNone(resolve_account_by_card(conn, "OLD-A")["floor_no"])
        self.assertEqual(
            tuple(
                conn.execute(
                    """
                    SELECT balance_before,balance_after,surplus_amount
                    FROM transactions WHERE transaction_id=77
                    """
                ).fetchone()
            ),
            (125.0, 100.0, 3.5),
        )
        self.assertEqual(
            conn.execute(
                "SELECT total_amount FROM payments WHERE transaction_id=77"
            ).fetchone()[0],
            25.0,
        )
        tx_columns = {row[1] for row in conn.execute("PRAGMA table_info(transactions)")}
        self.assertIn("floor_no", tx_columns)
        self.assertIn("parking_space_no", tx_columns)
        conn.close()

    def test_legacy_account_topup_is_not_overwritten_by_cards_balance(self):
        conn = connect(self.db_file)
        conn.execute("INSERT INTO cards(card_id,balance) VALUES ('LEGACY-CARD',500)")
        conn.execute("INSERT INTO id_tags(id_tag,status) VALUES ('LEGACY-CARD','Accepted')")
        conn.commit()

        adopted = ensure_legacy_account_for_card(conn, "LEGACY-CARD")
        self.assertEqual(adopted["balance"], 500)
        topped_up = topup_household_account(conn, adopted["account_id"], 1000)
        self.assertEqual(topped_up["balance"], 1500)
        ensured_again = ensure_legacy_account_for_card(conn, "LEGACY-CARD")
        self.assertEqual(ensured_again["balance"], 1500)
        self.assertEqual(
            conn.execute("SELECT balance FROM cards WHERE card_id='LEGACY-CARD'").fetchone()[0],
            500,
        )
        conn.close()

        with patch.object(main, "DB_FILE", self.db_file):
            authorized = asyncio.run(
                main.ChargePoint.on_authorize(SimpleNamespace(id="CP-1"), "LEGACY-CARD")
            )
        self.assertEqual(authorized.id_tag_info["status"], "Accepted")
        verify = connect(self.db_file)
        self.assertEqual(get_account_balance_by_card(verify, "LEGACY-CARD")["balance"], 1500)
        verify.close()

    def test_duplicate_stop_transaction_debits_household_once(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "9F", "STOP-01", 1000)
        bind_card_to_account(conn, account["account_id"], "STOP-CARD")
        conn.execute(
            """
            INSERT INTO transactions (
                transaction_id,id_tag,charge_point_id,connector_id,meter_start,
                start_timestamp,account_id,floor_no,parking_space_no
            ) VALUES (901,'STOP-CARD','CP-1',1,0,'2026-07-22T00:00:00+00:00',?,?,?)
            """,
            (account["account_id"], account["floor_no"], account["parking_space_no"]),
        )
        conn.commit()
        conn.close()

        stop_kwargs = {
            "transaction_id": 901,
            "meter_stop": 1000,
            "timestamp": "2026-07-22T01:00:00+00:00",
            "reason": "Remote",
        }
        completed_notifications = []
        no_notification = patch.multiple(
            main,
            schedule_charge_completed_line_notification=(
                lambda transaction_id: completed_notifications.append(transaction_id)
            ),
            schedule_low_balance_line_notification=lambda *_: None,
            schedule_auto_stop_balance_insufficient_line_notification=lambda *_: None,
        )
        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(main, "_calculate_multi_period_cost_detailed", return_value={"total": 100, "segments": []}),
            patch.object(main, "get_community_settings", return_value={"surcharge_per_kwh": 0}),
            no_notification,
        ):
            cp = SimpleNamespace(id="CP-1")
            remote_result = asyncio.run(
                main.ChargePoint.on_remote_stop_transaction(
                    cp, transaction_id=901
                )
            )
            self.assertEqual(remote_result.status, "Accepted")
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_kwargs))
            first = connect(self.db_file)
            self.assertEqual(get_account_by_id(first, account["account_id"])["balance"], 900)
            first.close()
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_kwargs))

        verify = connect(self.db_file)
        self.assertEqual(completed_notifications, [901])
        self.assertEqual(get_account_by_id(verify, account["account_id"])["balance"], 900)
        self.assertEqual(verify.execute("SELECT COUNT(*) FROM payments WHERE transaction_id=901").fetchone()[0], 1)
        self.assertIsNotNone(verify.execute("SELECT stop_timestamp FROM transactions WHERE transaction_id=901").fetchone()[0])
        snapshot = verify.execute(
            "SELECT floor_no,parking_space_no FROM transactions WHERE transaction_id=901"
        ).fetchone()
        self.assertEqual(tuple(snapshot), ("9F", "STOP-01"))
        verify.close()

        with patch.object(main, "DB_FILE", self.db_file):
            transaction_response = asyncio.run(
                main.get_transactions(
                    idTag=None,
                    chargePointId=None,
                    start=None,
                    end=None,
                    startDate=None,
                    endDate=None,
                    includeSummary=False,
                )
            )
            items = json.loads(transaction_response.body)
        self.assertEqual((items[0]["floorNo"], items[0]["parkingSpaceNo"]), ("9F", "STOP-01"))

        detail_conn = sqlite3.connect(self.db_file)
        try:
            with patch.object(main, "cursor", detail_conn.cursor()):
                detail_response = asyncio.run(main.get_transaction_detail(901))
            detail = json.loads(detail_response.body)
        finally:
            detail_conn.close()
        self.assertEqual((detail["floorNo"], detail["parkingSpaceNo"]), ("9F", "STOP-01"))

        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "compute_transaction_cost",
                return_value={"cost": 100, "details": [], "balanceAfter": 900},
            ),
        ):
            completed_message = main.build_charge_completed_line_message(901)
            low_balance_message = main.build_low_balance_line_message(901)
        self.assertIn("樓號／車位：9F／STOP-01", completed_message["message"])
        self.assertNotIn("持卡人", completed_message["message"])
        self.assertIn("9F／STOP-01 餘額偏低", low_balance_message["message"])

    def test_concurrent_stop_transactions_share_balance_without_lost_update(self):
        for iteration in range(5):
            conn = connect(self.db_file)
            account = create_household_account(
                conn, f"{20 + iteration}F", f"CONCURRENT-{iteration}", 1000
            )
            account_id = account["account_id"]
            tx_a = 2000 + iteration * 2
            tx_b = tx_a + 1
            transaction_specs = (
                (f"CONCURRENT-A-{iteration}", tx_a, "CP-1"),
                (f"CONCURRENT-B-{iteration}", tx_b, "CP-2"),
            )
            for card_id, _, _ in transaction_specs:
                bind_card_to_account(conn, account_id, card_id)
            for card_id, tx_id, cp_id in transaction_specs:
                conn.execute(
                    """
                    INSERT INTO transactions(
                        transaction_id,id_tag,charge_point_id,connector_id,
                        meter_start,start_timestamp,account_id,floor_no,
                        parking_space_no
                    ) VALUES (?,?,?,?,0,'2026-07-22T00:00:00+00:00',?,?,?)
                    """,
                    (
                        tx_id,
                        card_id,
                        cp_id,
                        1,
                        account_id,
                        account["floor_no"],
                        account["parking_space_no"],
                    ),
                )
            conn.commit()
            conn.close()

            def stop(tx_id, cp_id):
                return asyncio.run(
                    main.ChargePoint.on_stop_transaction(
                        SimpleNamespace(id=cp_id),
                        transaction_id=tx_id,
                        meter_stop=1000,
                        timestamp="2026-07-22T01:00:00+00:00",
                        reason="Local",
                    )
                )

            with (
                patch.object(main, "DB_FILE", self.db_file),
                patch.object(
                    main,
                    "_calculate_multi_period_cost_detailed",
                    side_effect=lambda transaction_id: {
                        "total": 100 if transaction_id == tx_a else 200,
                        "segments": [],
                    },
                ),
                patch.object(
                    main,
                    "get_community_settings",
                    return_value={"surcharge_per_kwh": 0},
                ),
                patch.multiple(
                    main,
                    schedule_charge_completed_line_notification=lambda *_: None,
                    schedule_low_balance_line_notification=lambda *_: None,
                    schedule_auto_stop_balance_insufficient_line_notification=lambda *_: None,
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                futures = [
                    pool.submit(stop, tx_a, "CP-1"),
                    pool.submit(stop, tx_b, "CP-2"),
                ]
                for future in futures:
                    future.result(timeout=30)

            verify = connect(self.db_file)
            self.assertEqual(get_account_by_id(verify, account_id)["balance"], 700)
            self.assertEqual(
                verify.execute(
                    "SELECT COUNT(*) FROM payments WHERE transaction_id IN (?,?)",
                    (tx_a, tx_b),
                ).fetchone()[0],
                2,
            )
            balances_after = sorted(
                row[0]
                for row in verify.execute(
                    """
                    SELECT balance_after FROM transactions
                    WHERE transaction_id IN (?,?)
                    """,
                    (tx_a, tx_b),
                )
            )
            self.assertEqual(balances_after[0], 700.0)
            self.assertIn(balances_after[1], (800.0, 900.0))
            verify.close()

    def test_line_notifications_use_persistent_transaction_type_dedup(self):
        with patch.object(main, "DB_FILE", self.db_file):
            main.ensure_line_bindings_table()
            main.ensure_line_message_logs_table()
            conn = connect(self.db_file)
            account = create_household_account(conn, "30F", "LINE", 100)
            bind_card_to_account(conn, account["account_id"], "LINE-CARD")
            conn.executemany(
                """
                INSERT INTO transactions(
                    transaction_id,id_tag,charge_point_id,account_id,
                    floor_no,parking_space_no
                ) VALUES (?,'LINE-CARD','CP-1',?,?,?)
                """,
                [
                    (777, account["account_id"], "30F", "LINE"),
                    (778, account["account_id"], "30F", "LINE"),
                ],
            )
            conn.execute(
                """
                INSERT INTO line_bindings(
                    id_tag,line_user_id,display_name,enabled,created_at,updated_at
                ) VALUES ('LINE-CARD','U-LINE','',1,'now','now')
                """
            )
            conn.commit()
            conn.close()

            builders = {
                "charge_completed": patch.object(
                    main,
                    "build_charge_completed_line_message",
                    return_value={
                        "idTag": "LINE-CARD",
                        "message": "completed",
                        "data": {"idTag": "LINE-CARD"},
                    },
                ),
                "low_balance": patch.object(
                    main,
                    "build_low_balance_line_message",
                    return_value={
                        "idTag": "LINE-CARD",
                        "message": "low",
                        "data": {"idTag": "LINE-CARD", "balanceAfter": 10},
                    },
                ),
                "auto_stop": patch.object(
                    main,
                    "build_auto_stop_balance_insufficient_line_message",
                    return_value={
                        "idTag": "LINE-CARD",
                        "message": "auto",
                        "data": {
                            "idTag": "LINE-CARD",
                            "autoStopReason": main.AUTO_STOP_REASON_BALANCE_INSUFFICIENT,
                        },
                    },
                ),
            }
            with (
                builders["charge_completed"],
                builders["low_balance"],
                builders["auto_stop"],
                patch.object(
                    main,
                    "send_line_message",
                    return_value={"ok": True, "status_code": 200},
                ) as sender,
            ):
                functions = (
                    main.send_charge_completed_line_notification,
                    main.send_low_balance_line_notification,
                    main.send_auto_stop_balance_insufficient_line_notification,
                )
                for function in functions:
                    first = function(777)
                    duplicate = function(777)
                    self.assertEqual(first["status"], "sent")
                    self.assertEqual(
                        duplicate["reason"], "duplicate_notification"
                    )
                self.assertEqual(sender.call_count, 3)

            verify = connect(self.db_file)
            self.assertEqual(
                verify.execute(
                    """
                    SELECT COUNT(*) FROM line_recipient_notification_claims
                    WHERE transaction_id=777
                    """
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                verify.execute(
                    """
                    SELECT COUNT(*) FROM line_message_logs
                    WHERE transaction_id=777 AND status='sent'
                    """
                ).fetchone()[0],
                3,
            )
            verify.close()

            with (
                patch.object(
                    main,
                    "build_charge_completed_line_message",
                    return_value={
                        "idTag": "LINE-CARD",
                        "message": "timeout",
                        "data": {"idTag": "LINE-CARD"},
                    },
                ),
                patch.object(
                    main,
                    "send_line_message",
                    return_value={"ok": False, "error": "timeout"},
                ) as timeout_sender,
            ):
                failed = main.send_charge_completed_line_notification(778)
                duplicate = main.send_charge_completed_line_notification(778)
                self.assertEqual(failed["status"], "failed")
                self.assertEqual(duplicate["reason"], "duplicate_notification")
                self.assertEqual(timeout_sender.call_count, 1)


if __name__ == "__main__":
    unittest.main()
