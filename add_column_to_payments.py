import sqlite3

db_path = "ocpp_data.db"  # 請確認檔名正確，與你主程式一致

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    # 嘗試新增欄位（若已存在會報錯但不影響）
    cursor.execute("ALTER TABLE payments ADD COLUMN charge_point_id TEXT;")
    print("✅ 已新增欄位 charge_point_id 到 payments")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("⚠️ 欄位 charge_point_id 已經存在，無需新增")
    else:
        print(f"❌ 其他錯誤: {e}")

conn.commit()
conn.close()
