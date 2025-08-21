import sqlite3
from tabulate import tabulate

# 資料庫檔名
DB_FILE = "ocpp_data"

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# 查詢最新 5 筆交易
cursor.execute("""
    SELECT transaction_id, charge_point_id, id_tag, start_timestamp, meter_start,
           stop_timestamp, meter_stop, reason
    FROM transactions
    ORDER BY start_timestamp DESC
    LIMIT 5
""")
rows = cursor.fetchall()

headers = ["transaction_id", "charge_point_id", "id_tag", "start_time", "meter_start",
           "stop_time", "meter_stop", "reason"]

print("\n=== 最新 5 筆交易狀態 ===\n")
print(tabulate(rows, headers=headers, tablefmt="grid"))
conn.close()
