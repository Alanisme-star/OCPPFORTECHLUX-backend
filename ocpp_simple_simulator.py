import asyncio
import websockets
import json
import time

# ===== 參數區 =====
ws_url = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"   # 請換成你的後端 WebSocket 地址與 charge_point_id
charge_point_id = "TW*MSI*E000100"                    # 同時作為 ws_url 路徑用
id_tag = "6678B3EB"                            # 請確保這個 tag 已註冊且啟用
connector_id = 1

# ===== OCPP 必要欄位 =====
call_id = 1

def next_call_id():
    global call_id
    call_id += 1
    return str(call_id)

async def main():
    async with websockets.connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        print("WebSocket Connected!")

        # BootNotification (可選，讓你後端 log 出現初始化)
        boot_req = [
            2, next_call_id(), "BootNotification", {
                "chargePointModel": "TestModel",
                "chargePointVendor": "TestVendor"
            }
        ]
        await ws.send(json.dumps(boot_req))
        resp = await ws.recv()
        print("BootNotification resp:", resp)

        # StartTransaction
        tx_id = int(time.time())
        start_req = [
            2, next_call_id(), "StartTransaction", {
                "connectorId": connector_id,
                "idTag": id_tag,
                "meterStart": 1000,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")  # ISO8601
            }
        ]
        await ws.send(json.dumps(start_req))
        resp = await ws.recv()
        print("StartTransaction resp:", resp)
        tx_resp = json.loads(resp)
        transactionId = tx_resp[2].get("transactionId", tx_id)

        # 模擬充電數秒
        await asyncio.sleep(3)

        # StopTransaction
        stop_req = [
            2, next_call_id(), "StopTransaction", {
                "transactionId": transactionId,
                "meterStop": 1500,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "reason": "Remote"
            }
        ]
        await ws.send(json.dumps(stop_req))
        resp = await ws.recv()
        print("StopTransaction resp:", resp)

        print("測試結束！你後端如果能收到並處理 StopTransaction，就代表 handler 正常。")

if __name__ == "__main__":
    asyncio.run(main())
