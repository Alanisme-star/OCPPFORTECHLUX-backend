# migrate_payments_schema.py
import datetime
import shutil
import sqlite3
from pathlib import Path

from db_config import get_database_path


DB_FILE = get_database_path()

def ensure_db_backup():
    db_path = Path(DB_FILE)
    if db_path.exists():
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = db_path.parent / f"{db_path.name}.backup.{ts}"
        shutil.copy2(db_path, bak)
        print(f"✓ 備份資料庫 -> {bak}")

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def colset(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def add_col(cur, table, col_def):
    col_name = col_def.split()[0]
    cols = colset(cur, table)
    if col_name not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
        print(f"  + {table}.{col_name}")
        return True
    return False

def migrate():
    ensure_db_backup()
    conn = sqlite3.connect(DB_FILE)
    try:
        with conn:
            cur = conn.cursor()

            # 若沒有 payments 表，直接用最新版 schema 建立
            if not table_exists(cur, "payments"):
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        transaction_id INTEGER,
                        base_fee REAL,
                        energy_fee REAL,
                        overuse_fee REAL,
                        total_amount REAL,
                        paid_at TEXT
                    )
                """)
                print("✓ 建立 payments（最新版 schema）")
            else:
                print("✓ 檢查/補齊 payments 欄位…")
                # 逐欄位補齊
                add_col(cur, "payments", "base_fee REAL DEFAULT 0")
                add_col(cur, "payments", "energy_fee REAL DEFAULT 0")
                add_col(cur, "payments", "overuse_fee REAL DEFAULT 0")
                add_col(cur, "payments", "total_amount REAL DEFAULT 0")
                # 舊版可能有 timestamp，沒有 paid_at：補欄位並把舊值搬過去
                cols = colset(cur, "payments")
                if "paid_at" not in cols:
                    add_col(cur, "payments", "paid_at TEXT")
                    if "timestamp" in cols:
                        cur.execute("UPDATE payments SET paid_at = timestamp WHERE paid_at IS NULL")
                        print("  ↳ 將舊欄位 timestamp 的值搬到 paid_at")
    finally:
        conn.close()

    print("✓ 完成")

if __name__ == "__main__":
    migrate()
