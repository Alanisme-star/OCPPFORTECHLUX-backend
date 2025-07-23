# simulator.py
import asyncio
import websockets
import json
import uuid
from datetime import datetime

URI = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"

async def send_message(ws, action, payload):
    msg = [
        2,  # CALL
        str(uuid.uuid4()),  # unique message ID
        action,
        payload
    ]
    await ws.send(json.dumps(msg))
    response = await ws.recv()
    print("⬅️ 收到回應：", response)

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
            "idTag": "6678B3EB"
        })

        now = datetime.utcnow().isoformat() + "Z"
        meter_start = 1000  # 初始電錶值（Wh）
        await send_message(ws, "StartTransaction", {
            "connectorId": 1,
            "idTag": "6678B3EB",
            "meterStart": meter_start,
            "timestamp": now
        })

        # 模擬每 3 秒送出 1 筆 MeterValues，共 5 筆
        for i in range(5):
            simulated_power = min(5 + i, 7.0)  # 不超過 7.0 kW
            current_wh = meter_start + i * 50  # 每次增加 50 Wh

            meter_value = [{
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "sampledValue": [
                    {
                        "value": str(current_wh),
                        "measurand": "Energy.Active.Import.Register",
                        "unit": "Wh"
                    },
                    {
                        "value": str(simulated_power),
                        "measurand": "Power.Active.Import",
                        "unit": "kW"
                    }
                ]
            }]

            # 安全檢查每筆 sampledValue 都包含 measurand
            for entry in meter_value:
                for sv in entry["sampledValue"]:
                    if "measurand" not in sv:
                        sv["measurand"] = "Energy.Active.Import.Register"

            await send_message(ws, "MeterValues", {
                "connectorId": 1,
                "meterValue": meter_value
            })
            await asyncio.sleep(3)

        # StopTransaction，meterStop 對應最後一筆電表值
        meter_stop = meter_start + (5 - 1) * 50
        await send_message(ws, "StopTransaction", {
            "transactionId": 123456,
            "meterStop": meter_stop,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "idTag": "6678B3EB",
            "reason": "Local"
        })

asyncio.run(run_simulation())
