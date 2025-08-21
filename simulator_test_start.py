import asyncio
import websockets
import json
from datetime import datetime, timezone
import uuid

OCPP_URL = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
CHARGE_POINT_ID = "TW*MSI*E000100"
ID_TAG = "6678B3EB"

def now():
    return datetime.now(timezone.utc).isoformat()

def new_uid():
    return str(uuid.uuid4())

async def main():
    async with websockets.connect(OCPP_URL, subprotocols=["ocpp1.6"]) as ws:
        print("✅ 已連線 WebSocket")

        # 1. BootNotification
        boot_payload = {
            "chargePointVendor": "TEST",
            "chargePointModel": "SIMULATOR"
        }
        await ws.send(json.dumps([2, new_uid(), "BootNotification", boot_payload]))
        print("📤 已送出 BootNotification")
        msg = await ws.recv()
        print(f"📥 BootNotification 回應：{msg}")

        # 2. Authorize
        authorize_payload = {
            "idTag": ID_TAG
        }
        await ws.send(json.dumps([2, new_uid(), "Authorize", authorize_payload]))
        print("📤 已送出 Authorize")
        msg = await ws.recv()
        print(f"📥 Authorize 回應：{msg}")

        print("⌛ 等待 RemoteStartTransaction 指令（最多 60 秒）...")
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=60)
                print(f"📥 接收到訊息：{msg}")
                arr = json.loads(msg)

                if arr[2] == "RemoteStartTransaction":
                    payload = arr[3]
                    print(f"⚡ 收到 RemoteStartTransaction：{payload}")

                    # 3. 主動送出 StartTransaction
                    transaction_id = None
                    start_payload = {
                        "connectorId": payload.get("connectorId", 1),
                        "idTag": payload.get("idTag", ID_TAG),
                        "meterStart": 0,
                        "timestamp": now()
                    }
                    await ws.send(json.dumps([2, new_uid(), "StartTransaction", start_payload]))
                    print("📤 已送出 StartTransaction")
                    msg = await ws.recv()
                    print(f"📥 StartTransaction 回應：{msg}")
                    arr = json.loads(msg)
                    transaction_id = arr[2].get("transactionId") or arr[2].get("transaction_id")
                    print(f"✅ 取得 transaction_id: {transaction_id}")

                    # 4. 傳送 MeterValues 5 次
                    for i in range(1, 6):
                        payload = {
                            "connectorId": 1,
                            "transactionId": transaction_id,
                            "meterValue": [{
                                "timestamp": now(),
                                "sampledValue": [{
                                    "value": str(i * 100),
                                    "measurand": "Energy.Active.Import.Register",
                                    "unit": "Wh"
                                }]
                            }]
                        }
                        await ws.send(json.dumps([2, new_uid(), "MeterValues", payload]))
                        print(f"📤 傳送第 {i} 筆 MeterValues")
                        msg = await ws.recv()
                        print(f"📥 MeterValues 回應：{msg}")
                        await asyncio.sleep(1)

                    print("🟢 測試結束，模擬器即將離線")
                    await asyncio.sleep(2)
                    return

        except asyncio.TimeoutError:
            print("⛔ 等待 RemoteStartTransaction 超時，模擬器結束")

if __name__ == "__main__":
    asyncio.run(main())
