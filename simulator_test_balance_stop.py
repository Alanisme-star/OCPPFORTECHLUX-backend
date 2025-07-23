import asyncio
import websockets
import json
import uuid
from datetime import datetime, timezone

URI = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
ID_TAG = "6678B3EB"
CONNECTOR_ID = 1

# 模擬 7kW，每秒約增加 1.94 Wh
POWER_WATT = 7000
ENERGY_PER_SECOND_WH = POWER_WATT / 3600

# 發送封包用
async def send(ws, action, payload):
    message_id = str(uuid.uuid4())
    msg = [2, message_id, action, payload]
    await ws.send(json.dumps(msg))
    print(f"➡️ 發送 {action}：{payload}")
    while True:
        response = await ws.recv()
        parsed = json.loads(response)
        if parsed[0] == 3 and parsed[1] == message_id:
            print(f"⬅️ 收到 {action} 回應：{parsed[2]}")
            return parsed[2]
        elif parsed[0] == 4:
            print(f"❌ 錯誤：{parsed}")
            return None

async def simulate():
    async with websockets.connect(URI, subprotocols=["ocpp1.6"]) as ws:
        # Boot
        await send(ws, "BootNotification", {
            "chargePointModel": "TestModel",
            "chargePointVendor": "TestVendor"
        })

        # Authorize
        await send(ws, "Authorize", {"idTag": ID_TAG})

        # StartTransaction
        meter_start = 10000
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        start_response = await send(ws, "StartTransaction", {
            "connectorId": CONNECTOR_ID,
            "idTag": ID_TAG,
            "meterStart": meter_start,
            "timestamp": now
        })
        transaction_id = start_response.get("transactionId", 1)

        # 每秒發送一次 MeterValues
        meter_value = meter_start
        while True:
            await asyncio.sleep(1)
            meter_value += ENERGY_PER_SECOND_WH

            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            payload = {
                "connectorId": CONNECTOR_ID,
                "transactionId": transaction_id,
                "meterValue": [{
                    "timestamp": timestamp,
                    "sampledValue": [{
                        "measurand": "Energy.Active.Import.Register",
                        "value": f"{meter_value:.2f}",
                        "unit": "Wh"
                    }]
                }]
            }
            await send(ws, "MeterValues", payload)

asyncio.run(simulate())
