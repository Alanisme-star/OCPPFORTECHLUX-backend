import requests
import time
from datetime import datetime, timedelta
import random

BASE_URL = "https://ocppfortechlux-backend.onrender.com/api"
CARD_ID = "TEST123"
CP_ID = "CP001"

def create_card():
    print(f"🔄 儲值卡片 {CARD_ID} ...")
    try:
        res = requests.post(f"{BASE_URL}/cards/{CARD_ID}/topup", json={"amount": 10000})
        print(f"✅ 儲值結果：{res.status_code}")
    except Exception as e:
        print(f"❌ 儲值錯誤：{e}")

def create_reservation(start_ts, end_ts):
    try:
        res = requests.post(f"{BASE_URL}/reservations", json={
            "chargePointId": CP_ID,
            "idTag": CARD_ID,
            "startTime": start_ts,
            "endTime": end_ts
        })
        return res.ok
    except:
        return False

def create_transaction(start_ts, stop_ts, meter_start, meter_stop):
    try:
        res = requests.post(f"{BASE_URL}/transactions", json={
            "chargePointId": CP_ID,
            "idTag": CARD_ID,
            "start_timestamp": start_ts,
            "stop_timestamp": stop_ts,
            "meter_start": meter_start,
            "meter_stop": meter_stop
        })
        return res.ok
    except:
        return False

def get_transaction_id(start_ts):
    try:
        res = requests.get(f"{BASE_URL}/transactions")
        data = res.json()
        for txn_id, tx in data.items():
            if tx["startTimestamp"].startswith(start_ts[:10]) and tx["idTag"] == CARD_ID:
                return txn_id
    except:
        return None

def add_meter_value(txn_id, ts, val):
    try:
        res = requests.post(f"{BASE_URL}/internal/meter_values", json={
            "transaction_id": txn_id,
            "charge_point_id": CP_ID,
            "connector_id": 1,
            "timestamp": ts,
            "value": val,
            "measurand": "Energy.Active.Import.Register",
            "unit": "Wh",
            "context": "Sample.Periodic",
            "format": "Raw"
        })
    except:
        pass

def create_payment(txn_id, energy_wh):
    kWh = energy_wh / 1000
    base_fee = 20.0
    energy_fee = round(kWh * 10, 2)
    overuse_fee = round(kWh * 2 if kWh > 5 else 0, 2)
    total_amount = round(base_fee + energy_fee + overuse_fee, 2)
    try:
        res = requests.post(f"{BASE_URL}/payments", json={
            "transaction_id": txn_id,
            "base_fee": base_fee,
            "energy_fee": energy_fee,
            "overuse_fee": overuse_fee,
            "total_amount": total_amount
        })
    except:
        pass

def run_seed(days=30, start_date_str="2025-06-01"):
    print(f"🌐 使用後端：{BASE_URL}")
    time.sleep(2)
    # 1. 自動補 daily pricing
    try:
        print(f"🔄 補齊每日單價: {start_date_str} + {days}天")
        res = requests.post(
            f"{BASE_URL}/internal/mock-daily-pricing?start={start_date_str}&days={days}"
        )
        print(f"✅ 日電價補齊，狀態：{res.status_code}")
    except Exception as e:
        print(f"❌ 補日電價錯誤：{e}")

    # 2. 建立卡片
    create_card()

    start_date = datetime.fromisoformat(start_date_str)
    meter_base = 10000

    print(f"\n📆 建立連續 {days} 天模擬資料...\n")
    for i in range(days):
        base = start_date + timedelta(days=i)
        start_ts = base.replace(hour=9, minute=0, second=0).isoformat()
        stop_ts = base.replace(hour=10, minute=0, second=0).isoformat()
        meter_start = meter_base + i * 400
        meter_stop = meter_start + random.randint(100, 600)

        create_reservation(start_ts, stop_ts)
        if create_transaction(start_ts, stop_ts, meter_start, meter_stop):
            txn_id = get_transaction_id(start_ts)
            if txn_id:
                add_meter_value(txn_id, stop_ts, meter_stop)
                create_payment(txn_id, meter_stop - meter_start)
                print(f"✅ {base.date()} 用電 {meter_stop - meter_start} Wh")
            else:
                print(f"⚠️ 找不到交易 ID {base.date()}")
        else:
            print(f"❌ {base.date()} 建立失敗")

    # 3. 最後自動重算所有成本（強制正確）
    try:
        res = requests.post(f"{BASE_URL}/internal/recalculate-all-payments")
        print(f"🔄 已自動重算所有交易成本：{res.json()}")
    except Exception as e:
        print(f"❌ 自動重算成本失敗：{e}")

if __name__ == "__main__":
    run_seed()




def mock_cp_status(cp_id, status="Available"):
    try:
        res = requests.post(
            f"{BASE_URL}/internal/mock-status",
            json={
                "cp_id": cp_id,
                "connector_id": 1,
                "status": status
            }
        )
        if res.ok:
            print(f"🔌 模擬狀態成功：{cp_id} ➜ {status}")
        else:
            print(f"⚠️ 模擬狀態失敗：HTTP {res.status_code}")
    except Exception as e:
        print(f"❌ 發送模擬狀態錯誤：{e}")



if __name__ == "__main__":
    run_seed()
    mock_cp_status("CP001", "Charging")
    mock_cp_status("CP002", "Available")
