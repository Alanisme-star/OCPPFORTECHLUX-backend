import asyncio
import websockets
from datetime import datetime, timezone
import json
import random
import time

CHARGE_POINT_ID = "TW*MSI*E000100"   # 換成你自己的
ID_TAG = "ABC123"                    # 換成你自己的卡號
URI = f"wss://ocppfortechlux-backend.onrender.com/{CHARGE_POINT_ID}"

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

async def simulate():
    async with websockets.connect(URI, subprotocols=["ocpp1.6"]) as ws:
        # 1. BootNotification
        await ws.send(json.dumps([
            2, "msg-1", "BootNotification", {
                "chargePointModel": "JimmyTest",
                "chargePointVendor": "JimmyCorp"
            }
        ]))
        await ws.recv()

        # 2. Authorize
        await ws.send(json.dumps([
            2, "msg-2", "Authorize", {
                "idTag": ID_TAG
            }
        ]))
        await ws.recv()

        # 3. StartTransaction
        meter_start = 10000
        start_time = now_iso()
        await ws.send(json.dumps([
            2, "msg-3", "StartTransaction", {
                "connectorId": 1,
                "idTag": ID_TAG,
                "meterStart": meter_start,
                "timestamp": start_time
            }
        ]))
        start_resp = json.loads(await ws.recv())
        transaction_id = start_resp[2]['transactionId']

        print(f"啟動充電，transactionId={transaction_id}，開始時間：{start_time}")

        # 4. 持續送 MeterValues，模擬充電過程
        meter = meter_start
        for i in range(6):   # 模擬充電 30 秒
            await asyncio.sleep(5)   # 每 5 秒送一次
            meter += random.randint(30, 50)
            await ws.send(json.dumps([
                2, f"msg-meter-{i}", "MeterValues", {
                    "connectorId": 1,
                    "transactionId": transaction_id,
                    "meterValue": [{
                        "timestamp": now_iso(),
                        "sampledValue": [{
                            "value": str(meter),
                            "measurand": "Energy.Active.Import.Register",
                            "unit": "Wh"
                        }]
                    }]
                }
            ]))
            await ws.recv()

        # 5. StopTransaction
        stop_time = now_iso()
        await ws.send(json.dumps([
            2, "msg-stop", "StopTransaction", {
                "transactionId": transaction_id,
                "idTag": ID_TAG,
                "meterStop": meter,
                "timestamp": stop_time,
                "reason": "Local"
            }
        ]))
        await ws.recv()
        print(f"結束充電，stop_time={stop_time}，最終電表={meter}")

if __name__ == "__main__":
    asyncio.run(simulate())
