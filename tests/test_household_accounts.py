import asyncio
import gc
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
            timestamp TEXT
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
        account = create_household_account(conn, "A-05-01", "A棟5樓01室", 3000)
        bind_card_to_account(conn, account["account_id"], "DAD", "爸爸", "父")
        bind_card_to_account(conn, account["account_id"], "MOM", "媽媽", "母")
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

    def test_incremental_topup_and_concurrent_debit(self):
        conn = connect(self.db_file)
        account_id = create_household_account(conn, "B-01", "B棟1樓", 80)["account_id"]
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
        account = create_household_account(conn, "C-01", "C棟1樓", 500)
        self.assertFalse(capture_unknown_card(conn, "NEW-1", "CP-1")["captured"])
        session = create_enrollment_session(
            conn, account["account_id"], "CP-1", card_holder_name="小明", relationship="子"
        )
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

    def test_cancelled_and_expired_enrollment_cannot_capture(self):
        conn = connect(self.db_file)
        account = create_household_account(conn, "D-01", "D棟1樓", 100)
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
        account = create_household_account(conn, "E-01", "E棟1樓", 100)
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

    def test_legacy_migration_is_idempotent_and_keeps_cards(self):
        raw = sqlite3.connect(self.db_file)
        raw.execute("INSERT INTO cards(card_id,balance) VALUES ('OLD-A',125),('OLD-B',75)")
        raw.execute("INSERT INTO card_owners(card_id,name) VALUES ('OLD-A','甲'),('OLD-B','甲')")
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
        account = create_household_account(conn, "STOP-01", "Stop account", 1000)
        bind_card_to_account(conn, account["account_id"], "STOP-CARD", "Resident")
        conn.execute(
            """
            INSERT INTO transactions (
                transaction_id,id_tag,charge_point_id,connector_id,meter_start,
                start_timestamp,account_id,account_code,card_holder_name
            ) VALUES (901,'STOP-CARD','CP-1',1,0,'2026-07-22T00:00:00+00:00',?,?,?)
            """,
            (account["account_id"], account["account_code"], "Resident"),
        )
        conn.commit()
        conn.close()

        stop_kwargs = {
            "transaction_id": 901,
            "meter_stop": 1000,
            "timestamp": "2026-07-22T01:00:00+00:00",
            "reason": "Remote",
        }
        no_notification = patch.multiple(
            main,
            schedule_charge_completed_line_notification=lambda *_: None,
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
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_kwargs))
            first = connect(self.db_file)
            self.assertEqual(get_account_by_id(first, account["account_id"])["balance"], 900)
            first.close()
            asyncio.run(main.ChargePoint.on_stop_transaction(cp, **stop_kwargs))

        verify = connect(self.db_file)
        self.assertEqual(get_account_by_id(verify, account["account_id"])["balance"], 900)
        self.assertEqual(verify.execute("SELECT COUNT(*) FROM payments WHERE transaction_id=901").fetchone()[0], 1)
        self.assertIsNotNone(verify.execute("SELECT stop_timestamp FROM transactions WHERE transaction_id=901").fetchone()[0])
        verify.close()


if __name__ == "__main__":
    unittest.main()
