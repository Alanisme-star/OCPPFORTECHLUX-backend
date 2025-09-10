import sqlite3
from datetime import datetime, timezone

DB_FILE = "ocpp_data.db"   # 依你的實際資料庫路徑調整

def fix_transactions():
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()

        # 檢查有問題的交易
        cur.execute("""
            SELECT transaction_id, start_timestamp, stop_timestamp
            FROM transactions
            ORDER BY transaction_id DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print("🔎 最近 20 筆交易：")
        for r in rows:
            print(r)

        # 修補 start_timestamp = NULL 的
        cur.execute("""
            SELECT transaction_id FROM transactions WHERE start_timestamp IS NULL
        """)
        missing_start = cur.fetchall()
        for (tx_id,) in missing_start:
            fallback = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            print(f"⚠️ 修補 start_timestamp | tx={tx_id} | set={fallback}")
            cur.execute("""
                UPDATE transactions
                SET start_timestamp = ?
                WHERE transaction_id = ?
            """, (fallback, tx_id))

        # 修補 stop_timestamp = NULL 但交易已存在的
        cur.execute("""
            SELECT transaction_id FROM transactions
            WHERE stop_timestamp IS NULL
              AND transaction_id IN (SELECT transaction_id FROM stop_transactions)
        """)
        missing_stop = cur.fetchall()
        for (tx_id,) in missing_stop:
            fallback = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            print(f"⚠️ 修補 stop_timestamp | tx={tx_id} | set={fallback}")
            cur.execute("""
                UPDATE transactions
                SET stop_timestamp = ?
                WHERE transaction_id = ?
            """, (fallback, tx_id))

        conn.commit()
        print("✅ 修補完成！")

if __name__ == "__main__":
    fix_transactions()
