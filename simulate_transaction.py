import requests
import time

# ✅ 設定後端 API 網址（請確認已部署）
BASE_URL = "https://ocppfortechlux-backend.onrender.com"

# ✅ 測試參數
card_id = "6678B3EB"
charging_kwh = 2.5
price_per_kwh = 10.0
cost = charging_kwh * price_per_kwh

# ✅ 步驟 1：查詢原始餘額
print("取得原始餘額...")
r1 = requests.get(f"{BASE_URL}/api/card_balance/{card_id}")
if r1.status_code == 200:
    old_balance = r1.json()["balance"]
    print(f"🔸 原始餘額：{old_balance} 元")
else:
    print("❌ 查詢餘額失敗")
    print(f"伺服器回應碼：{r1.status_code}")
    print(f"伺服器回應內容：{r1.text}")
    exit()

# ✅ 步驟 2：送出模擬交易
print("模擬充電交易中...")
tx_payload = {
    "card_id": card_id,
    "energy_kwh": charging_kwh,
    "cost": cost
}
res = requests.post(f"{BASE_URL}/api/simulate_transaction", json=tx_payload)

if res.status_code == 200:
    print("✅ 交易完成，等待餘額更新...")
    time.sleep(2)

    # ✅ 步驟 3：查詢新餘額
    r2 = requests.get(f"{BASE_URL}/api/card_balance/{card_id}")
    if r2.status_code == 200:
        new_balance = r2.json()["balance"]
        print(f"🔹 新餘額：{new_balance} 元")
        print(f"🧮 差額：{old_balance - new_balance:.2f} 元")
    else:
        print("❌ 無法取得新餘額")
        print(f"伺服器回應碼：{r2.status_code}")
        print(f"伺服器回應內容：{r2.text}")
else:
    print("❌ 模擬交易失敗")
    print(f"伺服器回應碼：{res.status_code}")
    print(f"伺服器回應內容：{res.text}")
