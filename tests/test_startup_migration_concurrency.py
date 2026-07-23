import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StartupMigrationConcurrencyTests(unittest.TestCase):
    def test_two_startup_migration_processes_are_serialized(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = Path(directory) / "concurrent.sqlite3"
            conn = sqlite3.connect(db_file)
            conn.executescript(
                """
                CREATE TABLE cards(
                    id INTEGER PRIMARY KEY,
                    card_id TEXT UNIQUE,
                    balance REAL DEFAULT 0
                );
                INSERT INTO cards(card_id,balance)
                VALUES ('CONCURRENT-A',100),('CONCURRENT-B',200);
                """
            )
            conn.commit()
            conn.close()

            env = os.environ.copy()
            env["DATABASE_PATH"] = str(db_file)
            processes = [
                subprocess.Popen(
                    [sys.executable, "run_startup_migrations.py"],
                    cwd=ROOT,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                for _ in range(2)
            ]
            results = [process.communicate(timeout=60) for process in processes]

            for process, (output, _) in zip(processes, results):
                self.assertEqual(process.returncode, 0, output)
                self.assertIn("[STARTUP][MIGRATION_LOCK] acquired=", output)

            conn = sqlite3.connect(db_file)
            try:
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM household_accounts").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM account_cards").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(DISTINCT card_id) FROM account_cards"
                    ).fetchone()[0],
                    2,
                )
            finally:
                conn.close()

            self.assertGreaterEqual(
                len(list(Path(directory).glob("concurrent.sqlite3.backup.*"))),
                4,
            )
