# sim_charge.py
# 雲端 API 模擬充電：每秒上報功率/電壓/電流、更新樁態、扣卡片餘額（PUT 覆寫）
# 以及：當餘額歸零時，自動呼叫後端 stop API（RemoteStopTransaction）

import time, random, requests
from datetime import datetime, timezone, timedelta

# === 後端 API 網址（注意：必須用 backend 網域） ===
BACKEND = "https://ocppfortechlux-backend.onrender.com"

# === 模擬參數 ===
CP_ID = "TW*MSI*E000100"
CARD_ID = "6678B3EB"
CONNECTOR_ID = 1
BASE_KW = 7.20
VOLTAGE = 230.0
FLUCT = 0.20
INTERVAL = 1.0
PRINT_EVERY = 5

# 餘額判定閾值（<= EPS 視為歸零）
NEAR_ZERO_EPS = 0.001

TZ_TAIPEI = timezone(timedelta(hours=8))
def now_iso():
    return datetime.now(TZ_TAIPEI).isoformat(timespec="seconds")

# 取得當前電價
def get_price_now():
    try:
        r = requests.get(f"{BACKEND}/api/pricing/price-now", timeout=5)
        r.raise_for_status()
        return float(r.json().get("price", 6.0))
    except Exception as e:
        print("⚠️ 取得電價失敗:", e)
        return 6.0

# 更新樁態（mock-status，寫入記憶體快取）
def set_status(cp_id, status):
    payload = {
        "cp_id": cp_id,
        "connector_id": CONNECTOR_ID,
        "status": status,
        "timestamp": now_iso()
    }
    try:
        res = requests.post(f"{BACKEND}/api/internal/mock-status", json=payload, timeout=5)
        if not res.ok:
            print("❌ mock-status 失敗:", res.status_code, res.text)
    except Exception as e:
        print("❌ mock-status 發送錯誤:", e)

# 上報量測值（internal，寫入 DB）
def insert_meter_value(cp_id, connector_id, measurand, value, unit):
    payload = {
        "charge_point_id": cp_id,
        "connector_id": connector_id,
        "transaction_id": 0,
        "value": value,
        "measurand": measurand,
        "unit": unit,
        "timestamp": now_iso()
    }
    try:
        res = requests.post(f"{BACKEND}/api/internal/meter_values", json=payload, timeout=5)
        if not res.ok:
            print(f"❌ 上報 {measurand} 失敗:", res.status_code, res.text)
    except Exception as e:
        print(f"❌ 上報 {measurand} 發送錯誤:", e)

# 取得卡片餘額
def get_balance(card_id):
    try:
        r = requests.get(f"{BACKEND}/api/cards/{card_id}/balance", timeout=5)
        if r.ok:
            return float(r.json().get("balance", 0.0))
        else:
            print("❌ 取得餘額失敗:", r.status_code, r.text)
    except Exception as e:
        print("❌ 取得餘額錯誤:", e)
    return 0.0

# 寫入卡片餘額（覆寫）
def set_balance(card_id, new_balance):
    try:
        res = requests.put(
            f"{BACKEND}/api/cards/{card_id}",
            json={"balance": round(new_balance, 3)},
            timeout=5
        )
        if not res.ok:
            print("❌ 寫入餘額失敗:", res.status_code, res.text)
    except Exception as e:
        print("❌ 寫入餘額錯誤:", e)

# 遠端停止充電（RemoteStopTransaction）
def remote_stop(cp_id):
    try:
        res = requests.post(f"{BACKEND}/api/charge-points/{cp_id}/stop", timeout=10)
        if res.ok:
            print("🛑 已送出停樁指令（RemoteStopTransaction）：", res.json())
            return True
        else:
            print("❌ 停樁失敗:", res.status_code, res.text)
            return False
    except Exception as e:
        print("❌ 停樁呼叫錯誤:", e)
        return False

# 主程式
def main():
    print("🚀 雲端 API 模擬器啟動：上報功率/電壓/電流 + 扣卡片餘額 + 餘額歸零自動停樁 + 更新樁態")
    set_status(CP_ID, "Charging")

    bal = get_balance(CARD_ID)
    print(f"💳 {CARD_ID} 初始餘額：{bal:.3f} 元")

    sent_auto_stop = False
    tick = 0
    try:
        while True:
            # 模擬功率波動
            kw = max(0.0, BASE_KW + random.uniform(-FLUCT, FLUCT))
            a = (kw * 1000.0) / VOLTAGE

            # 上報功率、電壓、電流
            insert_meter_value(CP_ID, CONNECTOR_ID, "Power.Active.Import", kw, "kW")
            insert_meter_value(CP_ID, CONNECTOR_ID, "Voltage", VOLTAGE, "V")
            insert_meter_value(CP_ID, CONNECTOR_ID, "Current.Import", a, "A")

            # 扣除餘額
            price = get_price_now()
            delta_cost = (kw * price) / 3600.0  # 元/秒
            bal = max(0.0, bal - delta_cost)
            set_balance(CARD_ID, bal)

            # 餘額歸零 → 自動停樁（只送一次）
            if (not sent_auto_stop) and (bal <= NEAR_ZERO_EPS):
                print(f"⚠️ 餘額已達 0（{bal:.3f}），嘗試遠端停止充電…")
                ok = remote_stop(CP_ID)
                sent_auto_stop = True
                if ok:
                    # 停止上報，交給後端處理 StopTransaction；我們只做立即視覺回饋
                    set_status(CP_ID, "Finishing")
                    time.sleep(2)
                    set_status(CP_ID, "Available")
                    print("✅ 模擬器已停止上報功率/電流（結束充電）。")
                    break  # ← 結束 while True 迴圈
                else:
                    # 失敗就別狂送，維持鎖定避免重試風暴
                    print("⚠️ RemoteStopTransaction 回應失敗，已不再重試。")


            tick += 1
            if tick % PRINT_EVERY == 0:
                print(f"[{now_iso()}] kw={kw:.3f}  V={VOLTAGE:.1f}  A={a:.2f}  price={price:.2f}  扣={delta_cost:.5f}/s  餘={bal:.3f}")

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 中斷，準備結束模擬…")
    finally:
        # 若尚未停樁，離開前仍回復成 Available（快取）
        set_status(CP_ID, "Available")
        print(f"🏁 {CP_ID} 狀態 -> Available，模擬結束。")

if __name__ == "__main__":
    main()
