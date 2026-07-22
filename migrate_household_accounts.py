"""Idempotent migration from per-card balances to household accounts."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from db_config import get_database_path
from household_account_service import connect, ensure_schema, utc_iso


def _safe_legacy_code(card_id: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in card_id.strip())
    return f"LEGACY-{safe or 'CARD'}"


def backup_database(db_file: str) -> Path | None:
    source = Path(db_file)
    if not source.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    target = source.with_name(f"{source.name}.backup.household.{stamp}")
    # SQLite's backup API includes committed WAL pages and is safe while
    # another process has the database open.
    source_conn = sqlite3.connect(str(source), timeout=30)
    target_conn = sqlite3.connect(str(target))
    try:
        source_conn.backup(target_conn)
    finally:
        target_conn.close()
        source_conn.close()
    return target


def migrate(db_file: str | None = None, create_backup: bool = True) -> dict[str, object]:
    db_file = db_file or get_database_path()
    backup = backup_database(db_file) if create_backup else None
    report: dict[str, object] = {
        "database": db_file,
        "backup": str(backup) if backup else None,
        "accounts_created": 0,
        "cards_linked": 0,
        "columns_added": [],
    }
    conn = connect(db_file)
    try:
        report["columns_added"] = ensure_schema(conn)
        table_names = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "cards" not in table_names:
            return report
        owner_exists = "card_owners" in table_names
        owner_join = "LEFT JOIN card_owners o ON o.card_id=c.card_id" if owner_exists else ""
        owner_select = "o.name" if owner_exists else "NULL"
        conn.execute("BEGIN IMMEDIATE")
        try:
            rows = conn.execute(
                f"SELECT c.card_id,c.balance,{owner_select} AS owner_name FROM cards c {owner_join} ORDER BY c.card_id"
            ).fetchall()
            for row in rows:
                card_id = str(row["card_id"] or "").strip()
                if not card_id:
                    continue
                if conn.execute("SELECT 1 FROM account_cards WHERE card_id=?", (card_id,)).fetchone():
                    continue
                base_code = _safe_legacy_code(card_id)
                code, suffix = base_code, 1
                while conn.execute("SELECT 1 FROM household_accounts WHERE account_code=?", (code,)).fetchone():
                    suffix += 1
                    code = f"{base_code}-{suffix}"
                now = utc_iso()
                cur = conn.execute(
                    """
                    INSERT INTO household_accounts
                        (account_code,account_name,balance,status,created_at,updated_at)
                    VALUES (?,?,MAX(?,0),'active',?,?)
                    """,
                    (code, (row["owner_name"] or card_id), float(row["balance"] or 0), now, now),
                )
                conn.execute(
                    """
                    INSERT INTO account_cards
                        (card_id,account_id,card_holder_name,relationship,status,created_at,updated_at)
                    VALUES (?,?,?,NULL,'active',?,?)
                    """,
                    (card_id, cur.lastrowid, row["owner_name"], now, now),
                )
                report["accounts_created"] = int(report["accounts_created"]) + 1
                report["cards_linked"] = int(report["cards_linked"]) + 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
    return report


if __name__ == "__main__":
    result = migrate()
    print("Household account migration complete")
    for key, value in result.items():
        print(f"- {key}: {value}")
