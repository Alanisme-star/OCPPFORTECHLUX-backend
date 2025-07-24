import sqlite3

db_path = "ocpp_data.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 查出所有沒有 charge_point_id 的付款紀錄
cursor.execute("""
    SELECT id, transaction_id FROM payments
    WHERE charge_point_id IS NULL OR charge_point_id = ''
""")
rows = cursor.fetchall()
count = 0

for payment_id, transaction_id in rows:
    # 用 transaction_id 去 transactions 查對應的 charge_point_id
    cursor.execute("""
        SELECT charge_point_id FROM transactions
        WHERE transaction_id = ?
    """, (transaction_id,))
    result = cursor.fetchone()
    if result:
        cp_id = result[0]
        cursor.execute("""
            UPDATE payments SET charge_point_id = ?
            WHERE id = ?
        """, (cp_id, payment_id))
        count += 1

conn.commit()
conn.close()
print(f"✅ 已補齊 {count} 筆 payments 的 charge_point_id 欄位")
