import requests
import time
from datetime import datetime, timedelta
import random
import string

# ✅ 固定使用雲端 API，不再依照參數切換
BASE_URL = "https://ocppfortechlux-backend.onrender.com/api"
print(f"🌐 使用雲端後端：{BASE_URL}")
print("⏳ 等待 Render 伺服器啟動中 (sleep 10 秒)...")
time.sleep(10)

CARD_ID = "TEST123"
CP_ID = "CP001"

def create_card():
    print(f"🔄 嘗試儲值卡片 {CARD_ID} ...")
    try:
        res = requests.post(f"{BASE_URL}/cards/{CARD_ID}/topup", json={"amount": 500})
        print(f"✅ 儲值結果：{res.status_code} | {res.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ 儲值請求失敗：{e}")

def create_reservation(start_ts, end_ts):
    payload = {
        "chargePointId": CP_ID,
        "idTag": CARD_ID,
        "startTime": start_ts,
        "endTime": end_ts
    }
    try:
        res = requests.post(f"{BASE_URL}/reservations", json=payload)
        return res.ok
    except:
        return False

def create_transaction(start_ts, stop_ts, meter_start, meter_stop):
    payload = {
        "chargePointId": CP_ID,
        "idTag": CARD_ID,
        "start_timestamp": start_ts,
        "stop_timestamp": stop_ts,
        "meter_start": meter_start,
        "meter_stop": meter_stop
    }
    try:
        res = requests.post(f"{BASE_URL}/transactions", json=payload)
        if res.ok:
            return True
        else:
            print(f"⛔ 建立失敗（status {res.status_code}）：{res.text}")
            return False
    except Exception as e:
        print(f"❌ 建立交易失敗：{e}")
        return False

def create_payment(txn_id, energy_wh):
    kWh = energy_wh / 1000
    base_fee = 20.0
    energy_fee = round(kWh * 10, 2)
    overuse_fee = round(kWh * 2 if kWh > 5 else 0, 2)
    total_amount = round(base_fee + energy_fee + overuse_fee, 2)

    payload = {
        "transaction_id": txn_id,
        "base_fee": base_fee,
        "energy_fee": energy_fee,
        "overuse_fee": overuse_fee,
        "total_amount": total_amount
    }

    try:
        res = requests.post(f"{BASE_URL}/payments", json=payload)
        if res.ok:
            print(f"💰 已寫入費用紀錄：總金額 ${total_amount}")
        else:
            print(f"⚠️ 費用寫入失敗：{res.status_code} {res.text}")
    except:
        print("❌ 費用請求失敗")

def add_meter_value(txn_id, ts, val):
    payload = {
        "transaction_id": txn_id,
        "charge_point_id": CP_ID,
        "connector_id": 1,
        "timestamp": ts,
        "value": val,
        "measurand": "Energy.Active.Import.Register",
        "unit": "Wh",
        "context": "Sample.Periodic",
        "format": "Raw"
    }
    try:
        res = requests.post(f"{BASE_URL}/internal/meter_values", json=payload)
        if res.status_code != 200:
            print(f"⚠️ meter_values 寫入失敗：{res.text}")
    except:
        print("❌ meter_values 請求失敗")

def get_transaction_id(start_ts):
    try:
        res = requests.get(f"{BASE_URL}/transactions")
        data = res.json()
        for txn_id, record in data.items():
            if record["startTimestamp"].startswith(start_ts[:10]) and record["idTag"] == CARD_ID:
                return txn_id
    except:
        pass
    return None

def run_seed():
    create_card()
    print("🔁 開始建立 7 筆模擬資料...\n")
    for i in range(7):
        base = datetime(2025, 6, 20) + timedelta(days=i)
        start_ts = base.replace(hour=9, minute=0, second=0, microsecond=0).isoformat()
        stop_ts = base.replace(hour=10, minute=0, second=0, microsecond=0).isoformat()
        meter_start = 10000 + i * 500
        meter_stop = meter_start + random.randint(150, 600)

        create_reservation(start_ts, stop_ts)
        if create_transaction(start_ts, stop_ts, meter_start, meter_stop):
            txn_id = get_transaction_id(start_ts)
            add_meter_value(txn_id, stop_ts, meter_stop)
            create_payment(txn_id, meter_stop - meter_start)
            print(f"✅ [{start_ts[:10]}] 用電 {meter_stop - meter_start} Wh 已建立")
        else:
            print(f"❌ [{start_ts[:10]}] 建立交易失敗")

    print("\n⚡ 建立今日交易資料用來測試 Dashboard...\n")
    today = datetime.now()
    start_ts = today.replace(hour=9, minute=0, second=0, microsecond=0).isoformat()
    stop_ts = today.replace(hour=10, minute=0, second=0, microsecond=0).isoformat()
    meter_start = 20000
    meter_stop = meter_start + random.randint(200, 400)

    create_reservation(start_ts, stop_ts)
    if create_transaction(start_ts, stop_ts, meter_start, meter_stop):
        txn_id = get_transaction_id(start_ts)
        add_meter_value(txn_id, stop_ts, meter_stop)
        create_payment(txn_id, meter_stop - meter_start)
        print(f"✅ 今日用電 {meter_stop - meter_start} Wh 已建立")
    else:
        print("❌ 建立今日交易失敗")

    # ✅ 自動建立隨機使用者
    def random_string(prefix, length=6):
        return prefix + ''.join(random.choices(string.digits, k=length))

    new_id_tag = random_string("USER_")
    new_card_number = random_string("CARD_")
    new_user_payload = {
        "idTag": new_id_tag,
        "name": "測試使用者",
        "department": "測試部門",
        "cardNumber": new_card_number
    }

    print(f"\n🧪 嘗試建立使用者 ID: {new_id_tag}")
    try:
        res = requests.post(f"{BASE_URL}/users", json=new_user_payload)
        if res.ok:
            print(f"✅ 成功建立使用者：{new_id_tag}")
        else:
            print(f"❌ 建立使用者失敗：{res.status_code} {res.text}")
    except:
        print("❌ 使用者請求失敗")

if __name__ == "__main__":
    run_seed()

import asyncio
import websockets
import uuid
import json
from datetime import datetime

WS_URL = "ws://localhost:9000/CP001"  # ⚠️ 若是 Render 上部署，請改成你的雲端 WebSocket URL
CHARGE_POINT_ID = "CP001"

# OCPP 子協議
HEADERS = {
    "Sec-WebSocket-Protocol": "ocpp1.6"
}

def make_ocpp_message(message_id, action, payload):
    return json.dumps([2, message_id, action, payload])

async def send_status_notification():
    async with websockets.connect(WS_URL, subprotocols=["ocpp1.6"]) as ws:
        print("🔌 已連線 WebSocket")

        # Step 1: BootNotification
        boot_payload = {
            "chargePointModel": "TEST_MODEL",
            "chargePointVendor": "TEST_VENDOR"
        }
        await ws.send(make_ocpp_message(str(uuid.uuid4()), "BootNotification", boot_payload))
        print("🚀 發送 BootNotification")
        await ws.recv()

        # Step 2: StatusNotification
        status_payload = {
            "connectorId": 1,
            "status": "Available",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await ws.send(make_ocpp_message(str(uuid.uuid4()), "StatusNotification", status_payload))
        print("📡 已發送 StatusNotification：Available")

        await ws.recv()  # 接收回應

asyncio.run(send_status_notification())

