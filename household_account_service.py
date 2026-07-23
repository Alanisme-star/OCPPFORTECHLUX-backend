"""Household account and RFID enrollment data access.

This module owns the new shared-balance rules.  It intentionally has no
FastAPI or OCPP dependency so the accounting and enrollment state machine can
be tested independently.
"""

from __future__ import annotations

import sqlite3
import time
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


MONEY_QUANT = Decimal("0.01")
ACTIVE_ENROLLMENT_STATUSES = ("waiting", "detected")
HOUSEHOLD_IDENTITY_MAX_LENGTH = 64


class HouseholdAccountError(ValueError):
    """A safe, user-facing household account validation error."""


class HouseholdAccountConflictError(HouseholdAccountError):
    """A household identity or card binding conflicts with existing data."""


def normalize_household_identity(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise HouseholdAccountError(f"{field} must be a string")
    normalized = unicodedata.normalize("NFKC", value).strip()
    if not normalized:
        raise HouseholdAccountError(f"{field} is required")
    if len(normalized) > HOUSEHOLD_IDENTITY_MAX_LENGTH:
        raise HouseholdAccountError(
            f"{field} must be at most {HOUSEHOLD_IDENTITY_MAX_LENGTH} characters"
        )
    if any(unicodedata.category(char).startswith("C") for char in normalized):
        raise HouseholdAccountError(f"{field} contains invalid control characters")
    return normalized


def _find_household_identity_conflicts(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    if not {"floor_no", "parking_space_no"} <= _columns(conn, "household_accounts"):
        return []
    groups: dict[tuple[str, str], list[int]] = {}
    rows = conn.execute(
        """
        SELECT account_id, floor_no, parking_space_no
        FROM household_accounts
        WHERE NULLIF(TRIM(floor_no), '') IS NOT NULL
          AND NULLIF(TRIM(parking_space_no), '') IS NOT NULL
        """
    ).fetchall()
    for row in rows:
        floor = unicodedata.normalize("NFKC", str(row[1])).strip().upper()
        parking = unicodedata.normalize("NFKC", str(row[2])).strip().upper()
        groups.setdefault((floor, parking), []).append(int(row[0]))
    return [
        {
            "floor_no": key[0],
            "parking_space_no": key[1],
            "account_ids": account_ids,
        }
        for key, account_ids in groups.items()
        if len(account_ids) > 1
    ]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime | None = None) -> str:
    value = value or utc_now()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def money(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    except Exception as exc:
        raise HouseholdAccountError("invalid amount") from exc


def connect(db_file: str, timeout: float = 15.0) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file, timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def ensure_schema(conn: sqlite3.Connection) -> list[str]:
    """Idempotently create the new tables, columns, and indexes."""
    changes: list[str] = []
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS household_accounts (
            account_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_code TEXT UNIQUE,
            account_name TEXT,
            floor_no TEXT,
            parking_space_no TEXT,
            balance REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK (balance >= 0),
            CHECK (status IN ('active', 'disabled'))
        );

        CREATE TABLE IF NOT EXISTS account_cards (
            card_id TEXT PRIMARY KEY,
            account_id INTEGER NOT NULL,
            card_holder_name TEXT,
            relationship TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES household_accounts(account_id),
            CHECK (status IN ('active', 'disabled'))
        );

        CREATE TABLE IF NOT EXISTS card_enrollment_sessions (
            enrollment_id TEXT PRIMARY KEY,
            account_id INTEGER NOT NULL,
            charge_point_id TEXT NOT NULL,
            requested_by TEXT,
            card_holder_name TEXT,
            relationship TEXT,
            status TEXT NOT NULL DEFAULT 'waiting',
            detected_id_tag TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            confirmed_at TEXT,
            cancelled_at TEXT,
            FOREIGN KEY (account_id) REFERENCES household_accounts(account_id),
            CHECK (status IN ('waiting','detected','confirmed','expired','cancelled'))
        );

        CREATE TABLE IF NOT EXISTS unknown_card_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tag TEXT NOT NULL,
            charge_point_id TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            enrollment_id TEXT,
            result TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_account_cards_account
            ON account_cards(account_id, status);
        CREATE INDEX IF NOT EXISTS idx_enrollment_cp_status_expiry
            ON card_enrollment_sessions(charge_point_id, status, expires_at);
        CREATE INDEX IF NOT EXISTS idx_unknown_card_detected
            ON unknown_card_logs(detected_at);
        """
    )
    table_names = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    conn.execute("BEGIN IMMEDIATE")
    account_columns = _columns(conn, "household_accounts")
    for definition in ("floor_no TEXT", "parking_space_no TEXT"):
        name = definition.split()[0]
        if name not in account_columns:
            conn.execute(f"ALTER TABLE household_accounts ADD COLUMN {definition}")
            changes.append(f"household_accounts.{name}")
    conflicts = _find_household_identity_conflicts(conn)
    if conflicts:
        details = "; ".join(
            f"{item['floor_no']}/{item['parking_space_no']}: "
            f"account_ids={item['account_ids']}"
            for item in conflicts
        )
        conn.rollback()
        raise HouseholdAccountConflictError(
            f"duplicate floor/parking data must be resolved before migration: {details}"
        )
    expected_index_sql = (
        "CREATE UNIQUE INDEX idx_household_accounts_floor_parking "
        "ON household_accounts(UPPER(TRIM(floor_no)), "
        "UPPER(TRIM(parking_space_no))) "
        "WHERE NULLIF(TRIM(floor_no), '') IS NOT NULL "
        "AND NULLIF(TRIM(parking_space_no), '') IS NOT NULL"
    )
    existing_index = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' "
        "AND name='idx_household_accounts_floor_parking'"
    ).fetchone()
    if existing_index and "UPPER(TRIM(floor_no))" not in (existing_index[0] or ""):
        conn.execute("DROP INDEX idx_household_accounts_floor_parking")
        existing_index = None
    if not existing_index:
        conn.execute(expected_index_sql)
    if "cards" in table_names:
        # A direct deletion from the legacy cards table represents removal of
        # that legacy card.  Drop only its LEGACY mapping so a later re-created
        # card is a fresh adoption; ordinary/shared accounts are untouched.
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS cleanup_deleted_legacy_card_mapping
            AFTER DELETE ON cards
            BEGIN
                DELETE FROM account_cards
                WHERE card_id = OLD.card_id
                  AND account_id IN (
                      SELECT account_id FROM household_accounts
                      WHERE account_code LIKE 'LEGACY-%'
                  );
            END
            """
        )
    if "transactions" in table_names:
        tx_columns = _columns(conn, "transactions")
        for definition in (
            "account_id INTEGER",
            "account_code TEXT",
            "card_holder_name TEXT",
            "floor_no TEXT",
            "parking_space_no TEXT",
        ):
            name = definition.split()[0]
            if name not in tx_columns:
                conn.execute(f"ALTER TABLE transactions ADD COLUMN {definition}")
                changes.append(f"transactions.{name}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transactions_account_id "
            "ON transactions(account_id)"
        )
    conn.commit()
    return changes


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def resolve_account_by_card(conn: sqlite3.Connection, card_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT ac.card_id, ac.account_id, ac.status AS card_status,
               ha.floor_no, ha.parking_space_no,
               ha.balance, ha.status AS account_status
        FROM account_cards ac
        JOIN household_accounts ha ON ha.account_id = ac.account_id
        WHERE ac.card_id = ?
        """,
        (card_id.strip(),),
    ).fetchone()
    return _row_dict(row)


def get_account_by_id(conn: sqlite3.Connection, account_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM household_accounts WHERE account_id = ?", (account_id,)
    ).fetchone()
    return _row_dict(row)


def list_household_accounts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ha.*,
               COUNT(ac.card_id) AS card_count
        FROM household_accounts ha
        LEFT JOIN account_cards ac ON ac.account_id = ha.account_id
        GROUP BY ha.account_id
        ORDER BY
            CASE WHEN NULLIF(TRIM(ha.floor_no), '') IS NULL THEN 1 ELSE 0 END,
            UPPER(TRIM(ha.floor_no)),
            CASE WHEN NULLIF(TRIM(ha.parking_space_no), '') IS NULL THEN 1 ELSE 0 END,
            UPPER(TRIM(ha.parking_space_no)),
            ha.account_id
        """
    ).fetchall()
    return [dict(row) for row in rows]


def list_account_cards(conn: sqlite3.Connection, account_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ac.card_id, ac.account_id, ac.status, ac.created_at, ac.updated_at,
               it.status AS id_tag_status, it.valid_until
        FROM account_cards ac
        LEFT JOIN id_tags it ON it.id_tag = ac.card_id
        WHERE ac.account_id = ?
        ORDER BY ac.created_at, ac.card_id
        """,
        (account_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_account_balance_by_card(conn: sqlite3.Connection, card_id: str) -> dict[str, Any] | None:
    return resolve_account_by_card(conn, card_id)


def ensure_legacy_account_for_card(
    conn: sqlite3.Connection, card_id: str, *, commit: bool = True
) -> dict[str, Any] | None:
    """Adopt one unmigrated legacy card without ever writing cards.balance.

    The explicit migration remains the deployment path.  This narrow bridge
    keeps an old card usable if it is presented before operators run that
    migration; from that point onward its formal balance lives only in the
    household account.
    """
    existing = resolve_account_by_card(conn, card_id)
    if existing:
        return existing
    legacy = conn.execute(
        """
        SELECT c.balance, o.name
        FROM cards c
        LEFT JOIN card_owners o ON o.card_id = c.card_id
        WHERE c.card_id = ?
        """,
        (card_id,),
    ).fetchone()
    if not legacy:
        return None
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in card_id.strip())
    base_code = f"LEGACY-{safe or 'CARD'}"
    code, suffix = base_code, 1
    while conn.execute(
        "SELECT 1 FROM household_accounts WHERE account_code = ?", (code,)
    ).fetchone():
        suffix += 1
        code = f"{base_code}-{suffix}"
    now = utc_iso()
    try:
        cur = conn.execute(
            """
            INSERT INTO household_accounts
                (account_code,account_name,balance,status,created_at,updated_at)
            VALUES (?,?,MAX(?,0),'active',?,?)
            """,
            (code, legacy["name"] or card_id, float(legacy["balance"] or 0), now, now),
        )
        conn.execute(
            """
            INSERT INTO account_cards
                (card_id,account_id,card_holder_name,relationship,status,created_at,updated_at)
            VALUES (?,?,?,NULL,'active',?,?)
            """,
            (card_id, cur.lastrowid, legacy["name"], now, now),
        )
        if commit:
            conn.commit()
    except sqlite3.IntegrityError:
        if commit:
            conn.rollback()
    return resolve_account_by_card(conn, card_id)


def create_household_account(
    conn: sqlite3.Connection,
    floor_no: str,
    parking_space_no: str,
    balance: Any = 0,
    status: str = "active",
) -> dict[str, Any]:
    floor = normalize_household_identity(floor_no, "floor_no")
    parking = normalize_household_identity(parking_space_no, "parking_space_no")
    if status not in ("active", "disabled"):
        raise HouseholdAccountError("invalid account status")
    opening = money(balance)
    if opening < 0:
        raise HouseholdAccountError("balance cannot be negative")
    now = utc_iso()
    # Existing production databases still enforce NOT NULL/UNIQUE on these
    # deprecated columns. Keep opaque compatibility values without deriving
    # either value from the new floor/parking identity.
    internal_code = f"ACCOUNT-{uuid.uuid4().hex}"
    try:
        cur = conn.execute(
            """
            INSERT INTO household_accounts
                (account_code, account_name, floor_no, parking_space_no,
                 balance, status, created_at, updated_at)
            VALUES (?, '', ?, ?, ?, ?, ?, ?)
            """,
            (internal_code, floor, parking, float(opening), status, now, now),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HouseholdAccountConflictError(
            "floor_no and parking_space_no already exist"
        ) from exc
    return get_account_by_id(conn, int(cur.lastrowid)) or {}


def update_household_account(
    conn: sqlite3.Connection, account_id: int, **fields: Any
) -> dict[str, Any]:
    allowed = {"floor_no", "parking_space_no", "status"}
    values = {key: value for key, value in fields.items() if key in allowed}
    if "status" in values and values["status"] not in ("active", "disabled"):
        raise HouseholdAccountError("invalid account status")
    for key in ("floor_no", "parking_space_no"):
        if key in values:
            values[key] = normalize_household_identity(values[key], key)
    if "floor_no" in values or "parking_space_no" in values:
        current = get_account_by_id(conn, account_id)
        if current is None:
            raise HouseholdAccountError("account not found")
        if not values.get("floor_no", current.get("floor_no")):
            raise HouseholdAccountError("floor_no is required")
        if not values.get("parking_space_no", current.get("parking_space_no")):
            raise HouseholdAccountError("parking_space_no is required")
    if not values:
        account = get_account_by_id(conn, account_id)
        if account is None:
            raise HouseholdAccountError("account not found")
        return account
    values["updated_at"] = utc_iso()
    assignments = ", ".join(f"{key} = ?" for key in values)
    try:
        cur = conn.execute(
            f"UPDATE household_accounts SET {assignments} WHERE account_id = ?",
            (*values.values(), account_id),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HouseholdAccountConflictError(
            "floor_no and parking_space_no already exist"
        ) from exc
    if cur.rowcount != 1:
        raise HouseholdAccountError("account not found")
    return get_account_by_id(conn, account_id) or {}


def topup_household_account(conn: sqlite3.Connection, account_id: int, amount: Any) -> dict[str, Any]:
    increment = money(amount)
    if increment <= 0:
        raise HouseholdAccountError("amount must be greater than zero")
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = utc_iso()
        cur = conn.execute(
            """
            UPDATE household_accounts
            SET balance = ROUND(balance + ?, 2), updated_at = ?
            WHERE account_id = ?
            """,
            (float(increment), now, account_id),
        )
        if cur.rowcount != 1:
            raise HouseholdAccountError("account not found")
        row = conn.execute(
            "SELECT * FROM household_accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise


def bind_card_to_account(
    conn: sqlite3.Connection,
    account_id: int,
    card_id: str,
    status: str = "active",
    valid_until: str | None = None,
) -> dict[str, Any]:
    card_id = card_id.strip()
    if not card_id:
        raise HouseholdAccountError("card_id is required")
    if status not in ("active", "disabled"):
        raise HouseholdAccountError("invalid card status")
    if get_account_by_id(conn, account_id) is None:
        raise HouseholdAccountError("account not found")
    now = utc_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if conn.execute("SELECT 1 FROM account_cards WHERE card_id = ?", (card_id,)).fetchone():
            raise HouseholdAccountError("card is already bound to an account")
        conn.execute(
            "INSERT OR IGNORE INTO cards(card_id, balance) VALUES (?, 0)", (card_id,)
        )
        conn.execute(
            "INSERT OR REPLACE INTO id_tags(id_tag, status, valid_until) VALUES (?, ?, ?)",
            (card_id, "Accepted" if status == "active" else "Blocked", valid_until),
        )
        conn.execute(
            """
            INSERT INTO account_cards
                (card_id, account_id, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (card_id, account_id, status, now, now),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise HouseholdAccountError("card is already bound to an account") from exc
    except Exception:
        conn.rollback()
        raise
    return resolve_account_by_card(conn, card_id) or {}


def update_account_card(conn: sqlite3.Connection, card_id: str, **fields: Any) -> dict[str, Any]:
    allowed = {"status"}
    values = {key: value for key, value in fields.items() if key in allowed}
    if "status" in values and values["status"] not in ("active", "disabled"):
        raise HouseholdAccountError("invalid card status")
    if not values:
        existing = resolve_account_by_card(conn, card_id)
        if existing is None:
            raise HouseholdAccountError("card not found")
        return existing
    values["updated_at"] = utc_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        assignments = ", ".join(f"{key} = ?" for key in values)
        cur = conn.execute(
            f"UPDATE account_cards SET {assignments} WHERE card_id = ?",
            (*values.values(), card_id),
        )
        if cur.rowcount != 1:
            raise HouseholdAccountError("card not found")
        if "status" in values:
            conn.execute(
                "UPDATE id_tags SET status = ? WHERE id_tag = ?",
                ("Accepted" if values["status"] == "active" else "Blocked", card_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return resolve_account_by_card(conn, card_id) or {}


def disable_account_card(conn: sqlite3.Connection, card_id: str) -> dict[str, Any]:
    return update_account_card(conn, card_id, status="disabled")


def expire_enrollments(conn: sqlite3.Connection, now: datetime | None = None) -> int:
    cur = conn.execute(
        """
        UPDATE card_enrollment_sessions
        SET status = 'expired'
        WHERE status IN ('waiting','detected') AND expires_at <= ?
        """,
        (utc_iso(now),),
    )
    conn.commit()
    return int(cur.rowcount or 0)


def create_enrollment_session(
    conn: sqlite3.Connection,
    account_id: int,
    charge_point_id: str,
    requested_by: str | None = None,
    duration_seconds: int = 120,
) -> dict[str, Any]:
    duration_seconds = max(1, min(int(duration_seconds or 120), 600))
    account = get_account_by_id(conn, account_id)
    if not account or account["status"] != "active":
        raise HouseholdAccountError("active account not found")
    charge_point_id = charge_point_id.strip()
    cp = conn.execute(
        "SELECT 1 FROM charge_points WHERE charge_point_id = ?", (charge_point_id,)
    ).fetchone()
    if not cp:
        raise HouseholdAccountError("charge point not found")
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = utc_now()
        conn.execute(
            "UPDATE card_enrollment_sessions SET status='expired' "
            "WHERE status IN ('waiting','detected') AND expires_at <= ?",
            (utc_iso(now),),
        )
        active = conn.execute(
            """
            SELECT enrollment_id FROM card_enrollment_sessions
            WHERE charge_point_id = ? AND status IN ('waiting','detected')
              AND expires_at > ? LIMIT 1
            """,
            (charge_point_id, utc_iso(now)),
        ).fetchone()
        if active:
            raise HouseholdAccountError("charge point already has an active enrollment")
        enrollment_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO card_enrollment_sessions
                (enrollment_id, account_id, charge_point_id, requested_by,
                 status, created_at, expires_at)
            VALUES (?, ?, ?, ?, 'waiting', ?, ?)
            """,
            (
                enrollment_id, account_id, charge_point_id, requested_by,
                utc_iso(now), utc_iso(now + timedelta(seconds=duration_seconds)),
            ),
        )
        row = conn.execute(
            "SELECT * FROM card_enrollment_sessions WHERE enrollment_id = ?",
            (enrollment_id,),
        ).fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise


def get_enrollment_session(conn: sqlite3.Connection, enrollment_id: str) -> dict[str, Any] | None:
    expire_enrollments(conn)
    row = conn.execute(
        "SELECT * FROM card_enrollment_sessions WHERE enrollment_id = ?",
        (enrollment_id,),
    ).fetchone()
    return _row_dict(row)


def capture_unknown_card(conn: sqlite3.Connection, card_id: str, charge_point_id: str) -> dict[str, Any]:
    """Audit an unknown Authorize and capture it only for the matching CP."""
    card_id, charge_point_id = card_id.strip(), charge_point_id.strip()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = utc_iso()
        conn.execute(
            "UPDATE card_enrollment_sessions SET status='expired' "
            "WHERE status IN ('waiting','detected') AND expires_at <= ?",
            (now,),
        )
        known = conn.execute(
            "SELECT 1 FROM id_tags WHERE id_tag = ? UNION SELECT 1 FROM account_cards WHERE card_id = ?",
            (card_id, card_id),
        ).fetchone()
        enrollment = conn.execute(
            """
            SELECT * FROM card_enrollment_sessions
            WHERE charge_point_id = ? AND status IN ('waiting','detected')
              AND expires_at > ? ORDER BY created_at DESC LIMIT 1
            """,
            (charge_point_id, now),
        ).fetchone()
        result = "known_card" if known else "no_active_enrollment"
        enrollment_id = enrollment["enrollment_id"] if enrollment else None
        captured = False
        if not known and enrollment:
            detected = enrollment["detected_id_tag"]
            if enrollment["status"] == "waiting" and not detected:
                conn.execute(
                    "UPDATE card_enrollment_sessions SET status='detected', detected_id_tag=? "
                    "WHERE enrollment_id=? AND status='waiting'",
                    (card_id, enrollment_id),
                )
                result, captured = "captured", True
            elif detected == card_id:
                result = "already_captured"
            else:
                result = "enrollment_already_has_card"
        conn.execute(
            """
            INSERT INTO unknown_card_logs
                (id_tag, charge_point_id, detected_at, enrollment_id, result)
            VALUES (?, ?, ?, ?, ?)
            """,
            (card_id, charge_point_id, now, enrollment_id, result),
        )
        conn.commit()
        return {"captured": captured, "result": result, "enrollment_id": enrollment_id}
    except Exception:
        conn.rollback()
        raise


def confirm_enrollment(conn: sqlite3.Connection, enrollment_id: str) -> dict[str, Any]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = utc_iso()
        row = conn.execute(
            "SELECT * FROM card_enrollment_sessions WHERE enrollment_id = ?",
            (enrollment_id,),
        ).fetchone()
        if not row:
            raise HouseholdAccountError("enrollment not found")
        if row["expires_at"] <= now:
            conn.execute(
                "UPDATE card_enrollment_sessions SET status='expired' WHERE enrollment_id=?",
                (enrollment_id,),
            )
            conn.commit()
            raise HouseholdAccountError("enrollment expired")
        if row["status"] != "detected" or not row["detected_id_tag"]:
            raise HouseholdAccountError("enrollment has no detected card")
        card_id = row["detected_id_tag"]
        exists = conn.execute(
            "SELECT 1 FROM id_tags WHERE id_tag=? UNION SELECT 1 FROM account_cards WHERE card_id=?",
            (card_id, card_id),
        ).fetchone()
        if exists:
            raise HouseholdAccountError("card already exists")
        account = conn.execute(
            "SELECT status FROM household_accounts WHERE account_id=?", (row["account_id"],)
        ).fetchone()
        if not account or account["status"] != "active":
            raise HouseholdAccountError("account is not active")
        created = utc_iso()
        conn.execute("INSERT INTO id_tags(id_tag,status,valid_until) VALUES (?, 'Accepted', NULL)", (card_id,))
        conn.execute("INSERT OR IGNORE INTO cards(card_id,balance) VALUES (?,0)", (card_id,))
        conn.execute(
            """
            INSERT INTO account_cards
                (card_id,account_id,status,created_at,updated_at)
            VALUES (?,?,'active',?,?)
            """,
            (card_id, row["account_id"], created, created),
        )
        if not conn.execute(
            "SELECT 1 FROM card_whitelist WHERE card_id=? AND charge_point_id=?",
            (card_id, row["charge_point_id"]),
        ).fetchone():
            conn.execute(
                "INSERT INTO card_whitelist(card_id,charge_point_id) VALUES (?,?)",
                (card_id, row["charge_point_id"]),
            )
        conn.execute(
            "UPDATE card_enrollment_sessions SET status='confirmed', confirmed_at=? WHERE enrollment_id=?",
            (created, enrollment_id),
        )
        conn.commit()
        return resolve_account_by_card(conn, card_id) or {}
    except Exception:
        conn.rollback()
        raise


def cancel_enrollment(conn: sqlite3.Connection, enrollment_id: str) -> dict[str, Any]:
    now = utc_iso()
    cur = conn.execute(
        """
        UPDATE card_enrollment_sessions
        SET status='cancelled', cancelled_at=?
        WHERE enrollment_id=? AND status IN ('waiting','detected')
        """,
        (now, enrollment_id),
    )
    conn.commit()
    if cur.rowcount != 1:
        raise HouseholdAccountError("active enrollment not found")
    return get_enrollment_session(conn, enrollment_id) or {}


def debit_household_account_atomic(
    conn: sqlite3.Connection, account_id: int, amount: Any
) -> tuple[float, float]:
    charge = money(amount)
    if charge < 0:
        raise HouseholdAccountError("amount cannot be negative")
    for attempt in range(3):
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT balance FROM household_accounts WHERE account_id=?", (account_id,)
            ).fetchone()
            if not row:
                raise HouseholdAccountError("account not found")
            before = money(row["balance"])
            after = max(Decimal("0.00"), before - charge).quantize(
                MONEY_QUANT, rounding=ROUND_HALF_UP
            )
            conn.execute(
                "UPDATE household_accounts SET balance=?,updated_at=? WHERE account_id=?",
                (float(after), utc_iso(), account_id),
            )
            conn.commit()
            return float(before), float(after)
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "locked" not in str(exc).lower() or attempt == 2:
                raise
            time.sleep(0.05 * (attempt + 1))
        except Exception:
            conn.rollback()
            raise
    raise RuntimeError("unreachable")
