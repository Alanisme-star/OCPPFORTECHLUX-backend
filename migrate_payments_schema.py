"""Idempotently migrate the payments table used by transaction settlement."""

from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path

from db_config import get_database_path


DB_FILE = get_database_path()


def ensure_db_backup() -> Path | None:
    db_path = Path(DB_FILE)
    if not db_path.exists():
        print(f"[MIGRATION][PAYMENTS][BACKUP] skipped_missing_database={db_path}")
        return None
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    backup_path = db_path.parent / f"{db_path.name}.backup.{stamp}"
    source_conn = sqlite3.connect(str(db_path), timeout=30)
    backup_conn = sqlite3.connect(str(backup_path))
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()
    print(f"[MIGRATION][PAYMENTS][BACKUP] path={backup_path}")
    return backup_path


def table_exists(cursor: sqlite3.Cursor, name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    )
    return cursor.fetchone() is not None


def colset(cursor: sqlite3.Cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def add_col(cursor: sqlite3.Cursor, table: str, definition: str) -> bool:
    column_name = definition.split()[0]
    if column_name in colset(cursor, table):
        return False
    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
    print(f"[MIGRATION][PAYMENTS][COLUMN_ADDED] {table}.{column_name}")
    return True


def migrate() -> None:
    ensure_db_backup()
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        with conn:
            cursor = conn.cursor()
            if not table_exists(cursor, "payments"):
                cursor.execute(
                    """
                    CREATE TABLE payments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        transaction_id INTEGER,
                        base_fee REAL,
                        energy_fee REAL,
                        overuse_fee REAL,
                        total_amount REAL,
                        paid_at TEXT
                    )
                    """
                )
                print("[MIGRATION][PAYMENTS] table_created")
                return

            for definition in (
                "base_fee REAL DEFAULT 0",
                "energy_fee REAL DEFAULT 0",
                "overuse_fee REAL DEFAULT 0",
                "total_amount REAL DEFAULT 0",
            ):
                add_col(cursor, "payments", definition)

            columns = colset(cursor, "payments")
            if "paid_at" not in columns:
                add_col(cursor, "payments", "paid_at TEXT")
                if "timestamp" in columns:
                    cursor.execute(
                        "UPDATE payments SET paid_at=timestamp WHERE paid_at IS NULL"
                    )
                    print("[MIGRATION][PAYMENTS] copied timestamp to paid_at")
    finally:
        conn.close()
    print("[MIGRATION][PAYMENTS] complete")


if __name__ == "__main__":
    migrate()
