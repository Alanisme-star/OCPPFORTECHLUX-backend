import sqlite3
conn = sqlite3.connect("ocpp_data.db")
cursor = conn.cursor()
cursor.execute("DELETE FROM transactions WHERE stop_timestamp IS NULL")
conn.commit()
conn.close()
print("已清除所有未結束交易！")
