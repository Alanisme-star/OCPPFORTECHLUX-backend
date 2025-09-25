import asyncio
import websockets
import json
import datetime
import uuid
import requests
import sqlite3
import os

# ===== 固定參數 =====
DB_FILE = "ocpp_data.db"  # 資料庫檔名
CHARGE_POINT_ID = "TW*MSI*E000100"
ID_TAG = "6678B3EB"  # 測試卡片 ID
METER_INTERVAL_MINUTES = 5
TOTAL_METER_VALUES = 6

# 測試不同時段（凌晨、白天、晚上）
TIME_SLOTS = [
    datetime.datetime(2025, 9, 22, 2, 0, 0),   # 凌晨
    datetime.datetime(2025, 9, 22, 10, 0, 0),  # 白天
    datetime.datetime(2025, 9, 22, 20, 0, 0),  # 晚上
]

# OCPP WebSocket
OCPP_BACKEND_URL = f"wss://ocppfortechlux-backend.onrender.com/{CHARGE_POINT_ID}"
# REST API
REST_API_BASE = "https://ocppfortechlux-backend.onrender.com/api"


# === 初始化資料庫（id_tags + cards + 分時段電價） ===
def init_db():
    if not os.path.exists(DB_FILE):
        print(f"⚠️ 找不到 {DB_FILE}，請確認路徑是否正確")
        return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # 建立 id_tags 表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS id_tags (
        id_tag TEXT PRIMARY KEY,
        status TEXT DEFAULT 'Accepted',
        valid_until TEXT
    )
    """)
    cur.execute("DELETE FROM id_tags WHERE id_tag = ?", (ID_TAG,))
    cur.execute(
        "INSERT INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)",
        (ID_TAG, "Accepted", "2030-12-31T23:59:59Z")
    )

    # 建立 cards 表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cards (
        card_id TEXT PRIMARY KEY,
        balance REAL DEFAULT 1000
    )
    """)
    cur.execute("DELETE FROM cards WHERE card_id = ?", (ID_TAG,))
    cur.execute(
        "INSERT INTO cards (card_id, balance) VALUES (?, ?)",
        (ID_TAG, 1000)
    )

    # 建立分時段電價表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_pricing_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        price REAL NOT NULL
    )
    """)

    # 插入 2025-09-22 的三段電價
    cur.execute("DELETE FROM daily_pricing_segments WHERE date = '2025-09-22'")
    cur.executemany("""
        INSERT INTO daily_pricing_segments (date, start_time, end_time, price)
        VALUES (?, ?, ?, ?)
    """, [
        ('2025-09-22', '00:00', '06:00', 2.0),   # 凌晨
        ('2025-09-22', '06:00', '18:00', 3.2),   # 白天
        ('2025-09-22', '18:00', '23:59', 2.5),   # 晚上
    ])

    conn.commit()
    conn.close()
    print("✅ 已初始化 id_tags、cards 與分時段電價")


# === 執行單筆模擬交易 ===
async def run_transaction(start_time: datetime.datetime):
    async with websockets.connect(
        OCPP_BACKEND_URL, subprotocols=["ocpp1.6"]
    ) as ws:

        async def send_msg(action, payload):
            msg_id = str(uuid.uuid4())
            message = [2, msg_id, action, payload]
            await ws.send(json.dumps(message))
            response = await ws.recv()
            print(f"➡️ {action} sent, ⬅️ response: {response}")
            return json.loads(response)

        # BootNotification
        await send_msg("BootNotification", {
            "chargePointVendor": "SimVendor",
            "chargePointModel": "SimModel"
        })

        # StartTransaction
        resp = await send_msg("StartTransaction", {
            "connectorId": 1,
            "idTag": ID_TAG,
            "timestamp": start_time.isoformat(),
            "meterStart": 0
        })

        # 取得 transactionId
        try:
            transaction_id = resp[2]["transactionId"]
            print(f"✅ 取得 transactionId: {transaction_id}")
        except Exception:
            print("❌ 無法取得 transactionId，預設為 1")
            transaction_id = 1

        # MeterValues (Wh)
        for i in range(TOTAL_METER_VALUES):
            meter_time = (start_time + datetime.timedelta(
                minutes=METER_INTERVAL_MINUTES * (i + 1)
            )).isoformat()

            await send_msg("MeterValues", {
                "connectorId": 1,
                "transactionId": transaction_id,
                "meterValue": [{
                    "timestamp": meter_time,
                    "sampledValue": [{
                        "value": str((i + 1) * 1000),  # Wh
                        "unit": "Wh"
                    }]
                }]
            })

        # StopTransaction
        stop_time = (start_time + datetime.timedelta(
            minutes=METER_INTERVAL_MINUTES * TOTAL_METER_VALUES
        )).isoformat()

        await send_msg("StopTransaction", {
            "transactionId": transaction_id,
            "idTag": ID_TAG,
            "timestamp": stop_time,
            "meterStop": TOTAL_METER_VALUES * 1000  # Wh
        })

        # === 查詢 REST API 的交易結果 ===
        try:
            url = f"{REST_API_BASE}/charge-points/{CHARGE_POINT_ID}/last-finished-transaction/summary"
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()

            print("\n🧾 電價驗證結果 ==================")
            print(f"交易ID: {data.get('transaction_id')}")
            print(f"起始時間: {data.get('start_timestamp')}")
            print(f"結束時間: {data.get('stop_timestamp')}")
            print(f"耗電量: {data.get('final_energy_kwh')} kWh")
            print(f"計算金額: {data.get('total_amount')} 元")
            print("=================================\n")
        except Exception as e:
            print(f"⚠️ 查詢 REST API 失敗: {e}")


# === 主程式：跑全部時段 ===
async def simulate_all():
    init_db()  # 初始化 DB
    for t in TIME_SLOTS:
        print(f"\n🚀 模擬交易開始，時間 = {t.strftime('%Y-%m-%d %H:%M:%S')}")
        await run_transaction(t)


if __name__ == "__main__":
    asyncio.run(simulate_all())
