import sqlite3

db_path = "ocpp_data.db"  # 若你的資料庫路徑不同請自行修改

# 要新增的欄位與型別
columns_to_add = {
    "measurand": "TEXT",
    "unit": "TEXT",
    "context": "TEXT",
    "format": "TEXT"
}

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 查詢目前已存在的欄位
cursor.execute("PRAGMA table_info(meter_values)")
existing_columns = [row[1] for row in cursor.fetchall()]

for column, col_type in columns_to_add.items():
    if column not in existing_columns:
        print(f"➕ 新增欄位: {column} ({col_type})")
        cursor.execute(f"ALTER TABLE meter_values ADD COLUMN {column} {col_type}")
    else:
        print(f"✅ 欄位已存在: {column}")

conn.commit()
conn.close()

print("✅ 修復完成！")
