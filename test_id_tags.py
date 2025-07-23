import asyncio
import websockets
import uuid
import json
from datetime import datetime, timezone

# 請依你的充電樁與雲端主機正確填寫
OCPP_URL = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
ID_TAG = "6678B3EB"  # 測試卡片 idTag

async def test_ocpp_flow():
    async with websockets.connect(OCPP_URL, subprotocols=['ocpp1.6']) as ws:
        # 1. 送出 Authorize 訊息
        call_id = str(uuid.uuid4())
        authorize_msg = [
            2, call_id, "Authorize", {"idTag": ID_TAG}
        ]
        await ws.send(json.dumps(authorize_msg))
        print(f"➡️ 已發送 Authorize: {authorize_msg}")
        authorize_resp = await ws.recv()
        print(f"⬅️ 收到 Authorize 回應: {authorize_resp}")
        auth_data = json.loads(authorize_resp)
        # 檢查授權結果
        status = (
            auth_data[2].get("idTagInfo", {}).get("status")
            if isinstance(auth_data, list) and len(auth_data) > 2 else None
        )
        if status != "Accepted":
            print(f"❌ 卡片授權未通過，status: {status}")
            return

        # 2. 送出 StartTransaction
        txn_id = str(uuid.uuid4())
        meter_start = 10000  # 模擬電表初始值
        timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        start_txn_msg = [
            2, txn_id, "StartTransaction", {
                "connectorId": 1,
                "idTag": ID_TAG,
                "meterStart": meter_start,
                "timestamp": timestamp
            }
        ]
        await ws.send(json.dumps(start_txn_msg))
        print(f"➡️ 已發送 StartTransaction: {start_txn_msg}")
        start_txn_resp = await ws.recv()
        print(f"⬅️ 收到 StartTransaction 回應: {start_txn_resp}")

        # 檢查回應內容（依據後端回傳格式）
        start_data = json.loads(start_txn_resp)
        if isinstance(start_data, list) and len(start_data) > 2:
            print("✅ StartTransaction 成功，回傳內容：", start_data[2])
        else:
            print("⚠️ StartTransaction 回應格式異常！")

if __name__ == "__main__":
    asyncio.run(test_ocpp_flow())
