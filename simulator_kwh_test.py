import requests
import time
from datetime import datetime, timedelta

# ✅ 設定雲端 API Base
API_BASE = "https://ocppfortechlux-backend.onrender.com"
CHARGE_POINT_ID = "TW*MSI*E000100"  # 請確認這個 ID 存在於你的 charge_points 資料表中
ID_TAG = "ABC123"                   # 請確認這張卡片存在且為 Accepted

# Step 1: 建立模擬交易（取代 StartTransaction）
now = datetime.utcnow()
start_time = now.isoformat()
meter_start = 10000  # ➜ 初始電表讀數（Wh）

transaction_payload = {
    "chargePointId": CHARGE_POINT_ID,
    "idTag": ID_TAG,
    "connector_id": 1,
    "meter_start": meter_start,
    "start_timestamp": start_time,
    "meter_stop": None,
    "stop_timestamp": None
}
resp = requests.post(f"{API_BASE}/api/transactions", json=transaction_payload)
resp.raise_for_status()
txn_id = resp.json()["transaction_id"]
print(f"✅ 建立模擬交易成功 transaction_id = {txn_id}")

# Step 2: 每次 +150 Wh，模擬即時電錶數據（共送 5 筆）
for i in range(5):
    value = meter_start + (i + 1) * 150  # 累加 150Wh
    timestamp = (now + timedelta(seconds=(i + 1) * 10)).isoformat()

    meter_payload = {
        "transaction_id": txn_id,
        "charge_point_id": CHARGE_POINT_ID,
        "connector_id": 1,
        "timestamp": timestamp,
        "value": value,
        "measurand": "Energy.Active.Import.Register",
        "unit": "Wh"
    }

    resp = requests.post(f"{API_BASE}/api/internal/meter_values", json=meter_payload)
    resp.raise_for_status()
    print(f"📤 上傳 meter value：{value} Wh @ {timestamp}")

    time.sleep(1)  # 實際模擬每 10 秒，這裡用 1 秒方便測試

print("✅ 模擬完成，請到前端查看「本次累積度數」是否正確變化！")
