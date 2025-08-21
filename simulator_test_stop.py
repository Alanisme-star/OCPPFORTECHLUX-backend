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
    # ⬇️ 關鍵！補上 subprotocols
    async with websockets.connect(OCPP_URL, subprotocols=["ocpp1.6"]) as ws:
        print("已連線 WebSocket")
        
        # 1. BootNotification
        boot_payload = {
            "chargePointVendor": "TEST",
            "chargePointModel": "SIMULATOR"
        }
        await ws.send(json.dumps([2, new_uid(), "BootNotification", boot_payload]))
        print("已送出 BootNotification")
        msg = await ws.recv()
        print(f"BootNotification 回應：{msg}")

        # 2. Authorize
        authorize_payload = {
            "idTag": ID_TAG
        }
        await ws.send(json.dumps([2, new_uid(), "Authorize", authorize_payload]))
        print("已送出 Authorize")
        msg = await ws.recv()
        print(f"Authorize 回應：{msg}")

        # 3. StartTransaction
        transaction_id = None
        start_payload = {
            "connectorId": 1,
            "idTag": ID_TAG,
            "meterStart": 0,
            "timestamp": now()
        }
        uid = new_uid()
        await ws.send(json.dumps([2, uid, "StartTransaction", start_payload]))
        print("已送出 StartTransaction")
        msg = await ws.recv()
        print(f"StartTransaction 回應：{msg}")
        try:
            arr = json.loads(msg)
            transaction_id = arr[2].get("transactionId") or arr[2].get("transaction_id")
            print(f"StartTransaction 取得 transaction_id: {transaction_id}")
        except Exception as e:
            print(f"無法解析 StartTransaction 回應: {e}")
            return

        # 4. 定期送 MeterValues
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
            print(f"已傳送第 {i} 筆 MeterValues")
            msg = await ws.recv()
            print(f"MeterValues 回應：{msg}")
            await asyncio.sleep(1)

        print("等待 RemoteStopTransaction 指令（最多 60 秒）...")
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=60)
                print(f"接收到訊息：{msg}")
                arr = json.loads(msg)
                if arr[2] == "RemoteStopTransaction":
                    remote_payload = arr[3]
                    print(f"收到 RemoteStopTransaction，內容：{remote_payload}")
                    if str(remote_payload.get("transactionId")) == str(transaction_id):
                        # 立即主動發 StopTransaction
                        stop_payload = {
                            "transactionId": transaction_id,
                            "idTag": ID_TAG,
                            "meterStop": 500,
                            "timestamp": now(),
                            "reason": "Remote"
                        }
                        print(f"→ 主動發送 StopTransaction：{stop_payload}")
                        await ws.send(json.dumps([2, new_uid(), "StopTransaction", stop_payload]))
                        msg2 = await ws.recv()
                        print(f"StopTransaction 回應：{msg2}")
                        print("StopTransaction 回應完成，模擬器結束")
                        await asyncio.sleep(2)
                        return
        except asyncio.TimeoutError:
            print("等待 RemoteStopTransaction 超時，模擬器結束")

if __name__ == "__main__":
    asyncio.run(main())
