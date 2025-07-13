import asyncio
import websockets
import json
import time

async def simulate_charge():
    uri = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"  # 換成你的雲端 ws 路徑
    async with websockets.connect(uri, subprotocols=["ocpp1.6"]) as ws:
        # 1. BootNotification
        boot = [2, "msgid-boot", "BootNotification", {"chargePointModel": "Sim", "chargePointVendor": "Sim"}]
        await ws.send(json.dumps(boot))
        print(await ws.recv())

        # 2. Authorize
        auth = [2, "msgid-auth", "Authorize", {"idTag": "6678B3EB"}]
        await ws.send(json.dumps(auth))
        print(await ws.recv())

        # 3. StartTransaction
        start = [2, "msgid-start", "StartTransaction", {
            "connectorId": 1,
            "idTag": "6678B3EB",
            "meterStart": 10000,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }]
        await ws.send(json.dumps(start))
        res_start = await ws.recv()
        print(res_start)
        # 取得回應內的 transactionId
        transaction_id = None
        try:
            obj = json.loads(res_start)
            if isinstance(obj, list) and len(obj) >= 3 and isinstance(obj[2], dict):
                transaction_id = obj[2].get("transactionId") or obj[2].get("transaction_id") or obj[2].get("transaction_id")
            if not transaction_id:
                transaction_id = 12345678  # fallback
        except Exception as e:
            transaction_id = 12345678

        # 4. MeterValues
        mv = [2, "msgid-mv", "MeterValues", {
            "connectorId": 1,
            "transactionId": transaction_id,
            "meterValue": [{
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sampledValue": [{
                    "value": "10800",
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh"
                }]
            }]
        }]
        await ws.send(json.dumps(mv))
        print(await ws.recv())

        # 5. StopTransaction
        stop = [2, "msgid-stop", "StopTransaction", {
            "transactionId": transaction_id,
            "meterStop": 10800,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "idTag": "6678B3EB",
            "reason": "Local"
        }]
        await ws.send(json.dumps(stop))
        print(await ws.recv())

asyncio.run(simulate_charge())
