import sqlite3

conn = sqlite3.connect("energy.db")
cursor = conn.cursor()

# 建立 cards 表
cursor.execute("""
CREATE TABLE IF NOT EXISTS cards (
    card_id TEXT PRIMARY KEY,
    balance REAL NOT NULL
)
""")

# 建立 daily_pricing 表
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_pricing (
    date TEXT PRIMARY KEY,
    price_per_kwh REAL NOT NULL
)
""")

conn.commit()
conn.close()

print("✅ 資料表建立完成")
