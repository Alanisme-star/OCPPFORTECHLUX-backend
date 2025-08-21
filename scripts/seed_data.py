import sqlite3
import os
from datetime import datetime

# 🚨 安全保護機制：避免在 production 環境誤用
if os.environ.get("ENV") == "production":
    raise RuntimeError("⚠️ 不可在 production 環境執行 seed_data.py！")

# 連線到本地資料庫
conn = sqlite3.connect("energy.db")
cursor = conn.cursor()

# 設定卡片與電價資料
card_id = "6678B3EB"
initial_balance = 300.0
today = datetime.now().strftime("%Y-%m-%d")
price_per_kwh = 4.5

# 插入卡片資料（如果尚未存在）
cursor.execute("SELECT * FROM cards WHERE card_id = ?", (card_id,))
if cursor.fetchone() is None:
    cursor.execute("INSERT INTO cards (card_id, balance) VALUES (?, ?)", (card_id, initial_balance))
    print(f"✅ 已新增卡片 {card_id}，餘額 {initial_balance} 元")
else:
    print(f"✔️ 卡片 {card_id} 已存在，略過新增")

# 插入每日電價資料（如果尚未存在）
cursor.execute("SELECT * FROM daily_pricing WHERE date = ?", (today,))
if cursor.fetchone() is None:
    cursor.execute("INSERT INTO daily_pricing (date, price_per_kwh) VALUES (?, ?)", (today, price_per_kwh))
    print(f"✅ 已新增 {today} 每度電價：{price_per_kwh} 元/kWh")
else:
    print(f"✔️ 今日 {today} 每度電價已存在，略過新增")

# 儲存與關閉連線
conn.commit()
conn.close()
