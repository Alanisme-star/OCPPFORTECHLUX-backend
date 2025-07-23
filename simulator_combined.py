# simulator_combined.py
import asyncio
import websockets
import json
import uuid
from datetime import datetime, timezone

URI = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
ID_TAG = "6678B3EB"
CONNECTOR_ID = 1
METER_START = 1000  # 初始電錶值（Wh）

async def send_message(ws, action, payload):
    msg = [2, str(uuid.uuid4()), action, payload]
    await ws.send(json.dumps(msg))
    response = await ws.recv()
    print(f"⬅️ [{action}] 回應：{response}")
    return response

async def run_simulation():
    async with websockets.connect(URI, subprotocols=["ocpp1.6"]) as ws:
        print("✅ WebSocket 已連線")

        # BootNotification
        await send_message(ws, "BootNotification", {
            "chargePointModel": "SimCP",
            "chargePointVendor": "TestVendor"
        })

        # Authorize
        await send_message(ws, "Authorize", {
            "idTag": ID_TAG
        })

        # StartTransaction
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        start_response = await send_message(ws, "StartTransaction", {
            "connectorId": CONNECTOR_ID,
            "idTag": ID_TAG,
            "meterStart": METER_START,
            "timestamp": now
        })

        try:
            resp_data = json.loads(start_response)
            transaction_id = resp_data[2].get("transactionId", 123456)
            print(f"✅ 取得 transactionId：{transaction_id}")
        except Exception as e:
            print(f"❌ 無法解析 transactionId，使用預設值 123456：{e}")
            transaction_id = 123456

        # 傳送 10 筆 MeterValues，每次增加 50 Wh
        for i in range(10):
            simulated_power = round(min(5.0 + i * 0.2, 7.0), 2)
            current_wh = METER_START + i * 50

            meter_value = [{
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                "sampledValue": [
                    {
                        "value": current_wh,
                        "measurand": "Energy.Active.Import.Register",
                        "unit": "Wh"
                    },
                    {
                        "value": simulated_power,
                        "measurand": "Power.Active.Import",
                        "unit": "kW"
                    },
                    {
                        "value": 12.5,  # 固定電流
                        "measurand": "Current.Import",
                        "unit": "A"
                    }
                ]
            }]

            await send_message(ws, "MeterValues", {
                "connectorId": CONNECTOR_ID,
                "meterValue": meter_value
            })
            await asyncio.sleep(2)

        # StopTransaction
        meter_stop = METER_START + (10 - 1) * 50
        await send_message(ws, "StopTransaction", {
            "transactionId": transaction_id,
            "meterStop": meter_stop,
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
            "idTag": ID_TAG,
            "reason": "Local"
        })
        print("🛑 StopTransaction 回應完成")

# 執行模擬器
asyncio.run(run_simulation())
