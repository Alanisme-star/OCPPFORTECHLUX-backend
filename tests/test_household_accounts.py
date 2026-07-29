import asyncio
import gc
import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# Importing main performs additive schema initialization. Keep that import
# isolated even when this module is run directly without a suite-level
# DATABASE_PATH.
_IMPORT_TEST_DIR = tempfile.TemporaryDirectory(
    prefix="ocpp_household_import_", ignore_cleanup_errors=True
)
os.environ.setdefault(
    "DATABASE_PATH", str(Path(_IMPORT_TEST_DIR.name) / "import.sqlite3")
)

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

        account = create_household_account(
            conn, "5F", "B12", 100, door_no="No. 8"
        )
        second = create_household_account(
            conn, "6F", "B13", 200, door_no="No. 9"
        )
        without_door = create_household_account(conn, "5F", "B12")
        self.assertEqual(account["floor_no"], "5F")
        self.assertEqual(account["parking_space_no"], "B12")
        self.assertEqual(account["door_no"], "No. 8")
        self.assertIsNone(without_door["door_no"])
        self.assertNotEqual(account["account_code"], second["account_code"])
        self.assertTrue(account["account_code"].startswith("ACCOUNT-"))
        self.assertEqual(account["account_name"], "")
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(conn, "5F", "B12", door_no="No. 8")
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(
                conn, " 5f ", " b12 ", door_no=" no. 8 "
            )
        with self.assertRaisesRegex(HouseholdAccountError, "already exist"):
            create_household_account(
                conn, "５Ｆ", "Ｂ１２", door_no="Ｎｏ． ８"
            )
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
                door_no="No. 9",
                floor_no="6f",
                parking_space_no="b13",
            )
        conn.close()

    def test_household_api_conflicts_are_400_or_409(self):
        with patch.object(main, "DB_FILE", self.db_file):
            first = main.api_create_household_account(
                {"doorNo": "No. 5", "floorNo": "5F", "parkingSpaceNo": "B12"}
            )
            second = main.api_create_household_account(
                {"doorNo": "No. 6", "floorNo": "6F", "parkingSpaceNo": "B13"}
            )
            with self.assertRaises(main.HTTPException) as duplicate_post:
                main.api_create_household_account(
                    {
                        "doorNo": "no. 5",
                        "floorNo": "5f",
                        "parkingSpaceNo": "b12",
                    }
                )
            self.assertEqual(duplicate_post.exception.status_code, 409)

            with self.assertRaises(main.HTTPException) as duplicate_put:
                main.api_update_household_account(
                    second["account_id"],
                    {
                        "doorNo": " NO. 5 ",
                        "floorNo": " 5F ",
                        "parkingSpaceNo": " B12 ",
                    },
                )
            self.assertEqual(duplicate_put.exception.status_code, 409)

            with self.assertRaises(main.HTTPException) as alias_conflict:
                main.api_create_household_account(
                    {
                        "floor_no": "5F",
                        "floorNo": "6F",
                        "parking_space_no": "B12",
                        "parkingSpaceNo": "B13",
                        "door_no": "No. 5",
                        "doorNo": "No. 6",
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
                    "doorNo": "No. 8",
                    "floorNo": "8F",
                    "parkingSpaceNo": "P08",
                    "balance": 200,
                    "account_name": "must-not-be-used",
                }
            )
            account_id = created["account_id"]
            self.assertEqual((created["floorNo"], created["parkingSpaceNo"]), ("8F", "P08"))
            self.assertEqual((created["door_no"], created["doorNo"]), ("No. 8", "No. 8"))
            self.assertIsNone(created["firstCardHolderName"])
            self.assertNotIn("account_name", created)

            updated = main.api_update_household_account(
                account_id,
                {
                    "door_no": "No. 9",
                    "floor_no": "9F",
                    "parking_space_no": "P09",
                    "account_code": "must-not-be-used",
                    "status": "active",
                },
            )
            self.assertEqual((updated["floorNo"], updated["parkingSpaceNo"]), ("9F", "P09"))
            self.assertEqual(updated["doorNo"], "No. 9")

            main.api_add_account_card(
                account_id,
                {
                    "card_id": "API-CARD",
                    "cardHolderName": "王小明",
                    "relationship": "must-not-be-written",
                    "charge_point_ids": ["CP-1"],
                },
            )
            accounts = main.api_list_household_accounts()
            card = accounts[0]["cards"][0]
            self.assertEqual(card["card_id"], "API-CARD")
            self.assertEqual(card["card_holder_name"], "王小明")
            self.assertNotIn("relationship", card)
            self.assertEqual(accounts[0]["firstCardHolderName"], "王小明")
            self.assertEqual(accounts[0]["first_card_holder_name"], "王小明")
            self.assertEqual(
                main.api_get_household_account(account_id)["floorNo"], "9F"
            )
            self.assertEqual(
                main.api_topup_household_account(account_id, {"amount": 50})["balance"],
                250,
            )
            self.assertEqual(
                main.api_update_account_card(
                    "API-CARD", {"status": "disabled"}
                )["card_status"],
                "disabled",
            )
            self.assertEqual(
                main.api_update_account_card("API-CARD", {})[
                    "card_holder_name"
                ],
                "王小明",
            )
            self.assertEqual(
                main.api_update_account_card(
                    "API-CARD", {"card_holder_name": ""}
                )["card_holder_name"],
                "",
            )
            self.assertIsNone(
                main.api_get_household_account(account_id)[
                    "firstCardHolderName"
                ]
            )
            nameless = main.api_add_account_card(
                account_id, {"cardId": "API-CARD-NAMELESS"}
            )
            self.assertIsNone(nameless["card_holder_name"])
            self.assertEqual(
                main.api_disable_account_card("API-CARD")["card_status"], "disabled"
            )

            session = main.api_create_card_enrollment(
                {
                    "account_id": account_id,
                    "charge_point_id": "CP-2",
                    "cardHolderName": "陳小美",
                    "relationship": "must-not-be-written",
                }
            )
            self.assertEqual(session["card_holder_name"], "陳小美")
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
            self.assertEqual(confirmed["card_holder_name"], "陳小美")

            main.api_add_account_card(
                account_id,
                {"cardId": "API-CARD-2", "card_holder_name": "林大華"},
            )
            cards = main.api_list_account_cards(account_id)
            self.assertEqual(
                {card["card_id"]: card["card_holder_name"] for card in cards},
                {
                    "API-CARD": "",
                    "API-CARD-NAMELESS": None,
                    "API-CARD-2": "林大華",
                    "ENROLL-CARD": "陳小美",
                },
            )
            self.assertEqual(
                main.api_get_household_account(account_id)[
                    "firstCardHolderName"
                ],
                "陳小美",
            )

    def test_household_api_optional_door_and_alias_conflict(self):
        with patch.object(main, "DB_FILE", self.db_file):
            old_contract = main.api_create_household_account(
                {"floorNo": "1F", "parkingSpaceNo": "OLD"}
            )
            snake_contract = main.api_create_household_account(
                {
                    "door_no": "No. 1",
                    "floor_no": "2F",
                    "parking_space_no": "SNAKE",
                }
            )
            self.assertEqual(old_contract["doorNo"], "")
            self.assertEqual(snake_contract["doorNo"], "No. 1")
            cleared = main.api_update_household_account(
                snake_contract["account_id"], {"doorNo": ""}
            )
            self.assertEqual(cleared["door_no"], "")
            with self.assertRaises(main.HTTPException) as conflict:
                main.api_create_household_account(
                    {
                        "door_no": "No. 2",
                        "doorNo": "No. 3",
                        "floorNo": "3F",
                        "parkingSpaceNo": "CONFLICT",
                    }
                )
            self.assertEqual(conflict.exception.status_code, 400)

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
        conn.execute("DROP INDEX idx_household_accounts_door_floor_parking")
        now = "2026-07-23T00:00:00+00:00"
        conn.executemany(
            """
            INSERT INTO household_accounts(
                account_code,account_name,door_no,floor_no,parking_space_no,
                balance,status,created_at,updated_at
            ) VALUES (?, '', ?, ?, ?, 0, 'active', ?, ?)
            """,
            [
                ("DUP-A", " No. 5 ", " 5F ", "Ｂ１２", now, now),
                ("DUP-B", "no. 5", "5f", "b12", now, now),
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
                  AND name='idx_household_accounts_door_floor_parking'
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
        account_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(household_accounts)")
        }
        card_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(account_cards)")
        }
        enrollment_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(card_enrollment_sessions)"
            )
        }
        self.assertIn("door_no", account_columns)
        self.assertIn("card_holder_name", card_columns)
        self.assertIn("card_holder_name", enrollment_columns)
        self.assertIn("door_no", tx_columns)
        self.assertIn("card_holder_name", tx_columns)
        self.assertIn("floor_no", tx_columns)
        self.assertIn("parking_space_no", tx_columns)
        self.assertEqual(
            conn.execute(
                "SELECT card_holder_name FROM account_cards WHERE card_id='OLD-A'"
            ).fetchone()[0],
            "甲",
        )
        conn.close()

    def test_additive_schema_upgrade_is_repeatable_and_preserves_old_rows(self):
        legacy_file = str(Path(self.tempdir.name) / "legacy-formal.sqlite3")
        conn = connect(legacy_file)
        conn.executescript(
            """
            CREATE TABLE household_accounts (
                account_id INTEGER PRIMARY KEY,
                account_code TEXT UNIQUE,
                account_name TEXT,
                floor_no TEXT,
                parking_space_no TEXT,
                balance REAL,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE account_cards (
                card_id TEXT PRIMARY KEY,
                account_id INTEGER,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE card_enrollment_sessions (
                enrollment_id TEXT PRIMARY KEY,
                account_id INTEGER,
                charge_point_id TEXT,
                status TEXT,
                created_at TEXT,
                expires_at TEXT
            );
            CREATE TABLE transactions (
                transaction_id INTEGER PRIMARY KEY,
                id_tag TEXT
            );
            CREATE UNIQUE INDEX idx_household_accounts_floor_parking
                ON household_accounts(floor_no, parking_space_no);
            INSERT INTO household_accounts VALUES
                (1, 'OLD', '舊住戶', '5F', 'B12', 88, 'active', 'old', 'old');
            INSERT INTO account_cards VALUES
                ('OLD-CARD', 1, 'active', 'old', 'old');
            INSERT INTO card_enrollment_sessions VALUES
                ('OLD-SESSION', 1, 'CP-1', 'waiting', 'old', 'future');
            INSERT INTO transactions VALUES (77, 'OLD-CARD');
            """
        )
        conn.commit()

        first_changes = ensure_schema(conn)
        second_changes = ensure_schema(conn)

        self.assertIn("household_accounts.door_no", first_changes)
        self.assertIn("account_cards.card_holder_name", first_changes)
        self.assertIn("card_enrollment_sessions.card_holder_name", first_changes)
        self.assertIn("transactions.door_no", first_changes)
        self.assertIn("transactions.card_holder_name", first_changes)
        self.assertEqual(second_changes, [])
        self.assertEqual(
            tuple(
                conn.execute(
                    "SELECT account_code,account_name,balance FROM household_accounts"
                ).fetchone()
            ),
            ("OLD", "舊住戶", 88.0),
        )
        self.assertEqual(
            conn.execute("SELECT card_id FROM account_cards").fetchone()[0],
            "OLD-CARD",
        )
        indexes = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='index'"
            )
        }
        self.assertNotIn("idx_household_accounts_floor_parking", indexes)
        self.assertIn("idx_household_accounts_door_floor_parking", indexes)
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

    def test_transaction_list_detail_and_history_use_persisted_snapshots(self):
        conn = connect(self.db_file)
        account = create_household_account(
            conn, "1樓", "B12", 1000, door_no="10號"
        )
        bind_card_to_account(
            conn,
            account["account_id"],
            "6678B3EB",
            card_holder_name="王小明",
        )
        conn.execute(
            """
            INSERT INTO transactions(
                transaction_id,charge_point_id,connector_id,id_tag,
                meter_start,start_timestamp,meter_stop,stop_timestamp,reason,
                balance_before,balance_after,surplus_amount,account_id,
                account_code,door_no,floor_no,parking_space_no,card_holder_name
            ) VALUES (
                4101,'CP-1',1,'6678B3EB',1000,'2026-07-20T01:00:00+00:00',
                2500,'2026-07-20T02:00:00+00:00','Local',1000,980,3.5,?,
                ?,'10號','1樓','B12','王小明'
            )
            """,
            (account["account_id"], account["account_code"]),
        )
        conn.execute(
            """
            INSERT INTO payments(
                transaction_id,base_fee,energy_fee,overuse_fee,total_amount,paid_at
            ) VALUES (4101,0,20,0,20,'2026-07-20T02:00:00+00:00')
            """
        )
        conn.execute(
            "UPDATE household_accounts SET door_no='12號' WHERE account_id=?",
            (account["account_id"],),
        )
        conn.execute(
            "UPDATE account_cards SET card_holder_name='王大明' WHERE card_id='6678B3EB'"
        )
        conn.commit()
        conn.close()

        with patch.object(main, "DB_FILE", self.db_file):
            list_response = asyncio.run(
                main.get_transactions(
                    idTag=None,
                    chargePointId=None,
                    start=None,
                    end=None,
                    startDate=None,
                    endDate=None,
                    includeSummary=True,
                )
            )
            items = json.loads(list_response.body)["items"]
            history = main.get_card_history("6678B3EB")["history"]

        item = next(row for row in items if row["transaction_id"] == 4101)
        for key in ("door_no", "doorNo"):
            self.assertEqual(item[key], "10號")
        for key in ("floor_no", "floorNo"):
            self.assertEqual(item[key], "1樓")
        for key in ("parking_space_no", "parkingSpaceNo"):
            self.assertEqual(item[key], "B12")
        for key in ("card_holder_name", "cardHolderName"):
            self.assertEqual(item[key], "王小明")
        self.assertEqual((item["id_tag"], item["idTag"]), ("6678B3EB", "6678B3EB"))
        self.assertEqual((item["cost"], item["amount"]), (20.0, 20.0))
        self.assertEqual((item["balance_before"], item["balance_after"]), (1000.0, 980.0))
        self.assertEqual(item["energy_kwh"], 1.5)
        self.assertEqual(item["surplus_amount"], 3.5)

        detail_conn = sqlite3.connect(self.db_file)
        try:
            with patch.object(main, "cursor", detail_conn.cursor()):
                detail_response = asyncio.run(main.get_transaction_detail(4101))
            detail = json.loads(detail_response.body)
        finally:
            detail_conn.close()
        self.assertEqual((detail["door_no"], detail["doorNo"]), ("10號", "10號"))
        self.assertEqual(
            (detail["card_holder_name"], detail["cardHolderName"]),
            ("王小明", "王小明"),
        )
        self.assertEqual((detail["account_id"], detail["accountId"]), (account["account_id"], account["account_id"]))
        self.assertEqual(detail["energy_kwh"], 1.5)
        self.assertEqual(detail["surplus_amount"], 3.5)

        self.assertEqual(history[0]["door_no"], "10號")
        self.assertEqual(history[0]["card_holder_name"], "王小明")
        self.assertEqual(history[0]["account_id"], account["account_id"])
        self.assertEqual(history[0]["id_tag"], "6678B3EB")

    def test_transaction_apis_keep_null_snapshot_fields_and_detail_404(self):
        conn = connect(self.db_file)
        conn.execute(
            """
            INSERT INTO transactions(
                transaction_id,charge_point_id,connector_id,id_tag,
                start_timestamp,door_no,floor_no,parking_space_no,
                card_holder_name
            ) VALUES (
                4102,'CP-1',1,'OLD-CARD','2026-07-20T03:00:00+00:00',
                NULL,'1樓','B12',NULL
            )
            """
        )
        conn.commit()
        conn.close()

        with patch.object(main, "DB_FILE", self.db_file):
            list_response = asyncio.run(
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
        item = next(
            row for row in json.loads(list_response.body)
            if row["transaction_id"] == 4102
        )
        self.assertIsNone(item["door_no"])
        self.assertIsNone(item["card_holder_name"])
        self.assertEqual((item["floor_no"], item["parking_space_no"]), ("1樓", "B12"))

        detail_conn = sqlite3.connect(self.db_file)
        try:
            with patch.object(main, "cursor", detail_conn.cursor()):
                detail_response = asyncio.run(main.get_transaction_detail(4102))
                detail = json.loads(detail_response.body)
                with self.assertRaises(main.HTTPException) as missing:
                    asyncio.run(main.get_transaction_detail(999999))
        finally:
            detail_conn.close()
        self.assertIsNone(detail["door_no"])
        self.assertIsNone(detail["card_holder_name"])
        self.assertEqual((detail["floor_no"], detail["parking_space_no"]), ("1樓", "B12"))
        self.assertEqual(missing.exception.status_code, 404)

    def test_current_transaction_summary_uses_active_card_snapshot(self):
        conn = connect(self.db_file)
        account = create_household_account(
            conn, "1樓", "B12", 1000, door_no="10號"
        )
        bind_card_to_account(
            conn,
            account["account_id"],
            "6678B3EB",
            card_holder_name="王小明",
        )
        bind_card_to_account(
            conn,
            account["account_id"],
            "96133DEB",
            card_holder_name="王小美",
        )
        conn.execute(
            """
            INSERT INTO transactions(
                transaction_id,charge_point_id,connector_id,id_tag,meter_start,
                start_timestamp,account_id,account_code,door_no,floor_no,
                parking_space_no,card_holder_name
            ) VALUES (
                4103,'CP-1',1,'96133DEB',1000,
                '2026-07-20T04:00:00+00:00',?,?,'10號','1樓','B12','王小美'
            )
            """,
            (account["account_id"], account["account_code"]),
        )
        conn.execute(
            "UPDATE household_accounts SET door_no='12號' WHERE account_id=?",
            (account["account_id"],),
        )
        conn.execute(
            "UPDATE account_cards SET card_holder_name='王大明' WHERE card_id='96133DEB'"
        )
        conn.commit()
        conn.close()

        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "_calculate_multi_period_cost_detailed",
                return_value={"total": 0, "segments": []},
            ),
        ):
            summary = main.get_current_tx_summary_by_cp("CP-1")

        self.assertTrue(summary["found"])
        self.assertEqual(summary["transaction_id"], 4103)
        self.assertEqual(summary["account_id"], account["account_id"])
        self.assertEqual(summary["id_tag"], "96133DEB")
        self.assertEqual(summary["door_no"], "10號")
        self.assertEqual(summary["floor_no"], "1樓")
        self.assertEqual(summary["parking_space_no"], "B12")
        self.assertEqual(summary["card_holder_name"], "王小美")
        self.assertNotEqual(summary["card_holder_name"], "王小明")

        legacy_active = connect(self.db_file)
        legacy_active.execute(
            """
            UPDATE transactions
            SET door_no=NULL, floor_no=NULL, parking_space_no=NULL,
                card_holder_name=NULL
            WHERE transaction_id=4103
            """
        )
        legacy_active.commit()
        legacy_active.close()
        with (
            patch.object(main, "DB_FILE", self.db_file),
            patch.object(
                main,
                "_calculate_multi_period_cost_detailed",
                return_value={"total": 0, "segments": []},
            ),
        ):
            fallback_summary = main.get_current_tx_summary_by_cp("CP-1")
        self.assertEqual(fallback_summary["door_no"], "12號")
        self.assertEqual(fallback_summary["card_holder_name"], "王大明")
        self.assertNotEqual(fallback_summary["card_holder_name"], "王小明")

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
        post_transaction_notifications = []
        no_notification = patch.multiple(
            main,
            schedule_post_transaction_line_notifications=(
                lambda transaction_id, send_low_balance=False:
                post_transaction_notifications.append(
                    (transaction_id, bool(send_low_balance))
                )
            ),
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
        self.assertEqual(post_transaction_notifications, [(901, True)])
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
                    schedule_post_transaction_line_notifications=lambda *_args, **_kwargs: None,
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
