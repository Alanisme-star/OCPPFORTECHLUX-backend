import asyncio
import websockets
import json
import uuid
from datetime import datetime, timezone

URI = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
ID_TAG = "6678B3EB"
CONNECTOR_ID = 1
METER_START = 10000

def build_call_msg(action, payload):
    return json.dumps([
        2,
        str(uuid.uuid4()),
        action,
        payload
    ])

async def simulate():
    async with websockets.connect(URI, subprotocols=["ocpp1.6"]) as ws:
        print("已連線 WebSocket")

        # 發送 BootNotification
        await ws.send(build_call_msg("BootNotification", {
            "chargePointModel": "SimCP",
            "chargePointVendor": "TechLux"
        }))
        response = await ws.recv()
        print("BootNotification 回應：", response)

        # 發送 Authorize
        await ws.send(build_call_msg("Authorize", {
            "idTag": ID_TAG
        }))
        response = await ws.recv()
        print("Authorize 回應：", response)

        # 發送 StartTransaction
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        await ws.send(build_call_msg("StartTransaction", {
            "connectorId": CONNECTOR_ID,
            "idTag": ID_TAG,
            "meterStart": METER_START,
            "timestamp": now
        }))
        print("已送出 StartTransaction")
        response = await ws.recv()
        print("StartTransaction 回應：", response)

        # 傳送 5 筆 MeterValues，每隔 10 秒
        for i in range(5):
            await asyncio.sleep(10)
            meter_value = [{
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                "sampledValue": [{
                    "value": str(METER_START + i * 100),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh"
                }]
            }]
            await ws.send(build_call_msg("MeterValues", {
                "connectorId": CONNECTOR_ID,
                "meterValue": meter_value
            }))
            print(f"已傳送第 {i+1} 筆 MeterValues")
            await ws.recv()  # 接收伺服器回應

        # 等待後端主動送出 StopTransaction 指令（最多 60 秒）
        print("等待 StopTransaction 指令（最多 60 秒）...")
        try:
            while True:
                message = await asyncio.wait_for(ws.recv(), timeout=60)
                print("接收到訊息：", message)
                # 處理 StopTransaction，主動回應 CallResult
                try:
                    msg_obj = json.loads(message)
                except Exception:
                    msg_obj = None

                # OCPP message structure: [2,call_id,action,payload] or [3,call_id,payload]
                # 我們只要處理 Call (type==2) 的 StopTransaction
                if (
                    isinstance(msg_obj, list) and
                    len(msg_obj) >= 3 and
                    msg_obj[0] == 2 and
                    msg_obj[2] == "StopTransaction"
                ):
                    call_id = msg_obj[1]
                    print("成功接收到停止命令，回覆 CallResult")
                    # 標準 StopTransaction CallResult payload
                    reply = [
                        3,
                        call_id,
                        {"idTagInfo": {"status": "Accepted"}}
                    ]
                    await ws.send(json.dumps(reply))
                    print("已送出 StopTransaction 回覆")
                    break
        except asyncio.TimeoutError:
            print("未收到停止命令，模擬器自動結束")

if __name__ == "__main__":
    asyncio.run(simulate())
