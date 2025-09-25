import asyncio
import websockets
import json
import requests
from datetime import datetime, timezone

# === 後端設定 ===
BACKEND_URL = "https://ocppfortechlux-backend.onrender.com"
CP_ID = "TW*STRESS*0001"
ID_TAG = "6678B3EB"
INIT_BALANCE = 150   # 10 筆交易，每筆 15 元 → 共 150 元
PRICE = 100          # 測試電價 (元/kWh)
TRANSACTIONS = 10    # 測試交易數量

OCPP_URL = f"wss://ocppfortechlux-backend.onrender.com/{CP_ID}"

results = []  # 儲存每筆交易的結果

async def run_transaction(tx_index):
    global results

    # 建立 WebSocket 連線
    async with websockets.connect(OCPP_URL, subprotocols=["ocpp1.6"]) as ws:
        # BootNotification (只在第一次需要，但為安全這裡每次都送)
        boot = [2, "boot", "BootNotification", {"chargePointVendor": "Test", "chargePointModel": "Sim"}]
        await ws.send(json.dumps(boot))
        await ws.recv()

        # StartTransaction
        start = [2, f"start-{tx_index}", "StartTransaction", {
            "connectorId": 1,
            "idTag": ID_TAG,
            "meterStart": 0,
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }]
        await ws.send(json.dumps(start))
        start_resp = json.loads(await ws.recv())
        txn_id = start_resp[2]["transactionId"]

        last_estimated_amount = None

        # 模擬送 3 次 MeterValues，每次 50Wh (=0.05kWh)
        for i in range(1, 4):
            mv = [2, f"mv-{tx_index}-{i}", "MeterValues", {
                "connectorId": 1,
                "transactionId": txn_id,
                "meterValue": [{
                    "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                    "sampledValue": [{
                        "value": str(i*50),
                        "measurand": "Energy.Active.Import.Register",
                        "unit": "Wh"
                    }]
                }]
            }]
            await ws.send(json.dumps(mv))
            await ws.recv()
            await asyncio.sleep(0.5)

            # 查詢 /live-status，更新最後一次預估金額
            try:
                live_res = requests.get(f"{BACKEND_URL}/api/charge-points/{CP_ID}/live-status")
                if live_res.status_code == 200:
                    live_data = live_res.json()
                    last_estimated_amount = live_data.get("estimated_amount")
            except:
                pass

        # StopTransaction at 150Wh
        stop = [2, f"stop-{tx_index}", "StopTransaction", {
            "transactionId": txn_id,
            "meterStop": 150,
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
            "idTag": ID_TAG,
            "reason": "Local"
        }]
        await ws.send(json.dumps(stop))
        await ws.recv()

    # === 查詢最終金額 ===
    summary_url = f"{BACKEND_URL}/api/charge-points/{CP_ID}/last-transaction/summary"
    res = requests.get(summary_url)
    total_amount = None
    if res.status_code == 200:
        data = res.json()
        if data.get("found"):
            total_amount = data.get("total_amount")

    diff = None
    if last_estimated_amount is not None and total_amount is not None:
        diff = abs(total_amount - last_estimated_amount)

    results.append({
        "tx_id": txn_id,
        "estimated": last_estimated_amount,
        "final": total_amount,
        "diff": diff
    })

async def main():
    # === Step 1: 準備白名單與卡片 ===
    try:
        resp = requests.post(
            f"{BACKEND_URL}/api/debug/force-add-charge-point",
            params={"charge_point_id": CP_ID,
                    "card_id": ID_TAG,
                    "initial_balance": INIT_BALANCE}
        )
        print("→ force-add-charge-point:", resp.status_code, resp.text)
    except Exception as e:
        print("⚠️ 無法呼叫 force-add-charge-point API:", e)
        return

    # === Step 2: 逐筆執行交易 ===
    for i in range(1, TRANSACTIONS + 1):
        print(f"\n=== 開始第 {i} 筆交易 ===")
        await run_transaction(i)

    # === Step 3: 輸出統計表格 ===
    print("\n📊 測試結果統計：")
    print(f"{'TxID':<18} {'預估金額':<12} {'最終金額':<12} {'差異'}")
    for r in results:
        diff_str = f"{r['diff']:.2f}" if r['diff'] is not None else "N/A"
        print(f"{r['tx_id']:<18} {r['estimated']:<12} {r['final']:<12} {diff_str}")

if __name__ == "__main__":
    asyncio.run(main())
