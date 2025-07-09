import asyncio
import websockets
import json
import uuid

# === 請根據你的實際狀況修改以下 ===
CHARGE_POINT_ID = "TW*MSI*AE000100"   # 模擬的充電樁 ID（應與白名單相符）
ENDPOINT_URL = f"wss://ocppfortechlux-backend.onrender.com/{CHARGE_POINT_ID}"
OCPP_SUBPROTOCOL = "ocpp1.6"

# === 建立 BootNotification 封包 ===
def make_boot_notification():
    unique_id = str(uuid.uuid4())
    message = [
        2,  # MessageTypeId: CALL
        unique_id,
        "BootNotification",
        {
            "chargePointModel": "SimulatedCP",
            "chargePointVendor": "SimulatedVendor"
        }
    ]
    return json.dumps(message)

# === 模擬程式主流程 ===
async def simulate_ocpp_connection():
    try:
        print(f"🔌 Connecting to {ENDPOINT_URL} ...")
        async with websockets.connect(ENDPOINT_URL, subprotocols=[OCPP_SUBPROTOCOL]) as ws:
            print("✅ WebSocket connected")

            msg = make_boot_notification()
            print(f"📤 Sending BootNotification: {msg}")
            await ws.send(msg)

            reply = await ws.recv()
            print(f"📥 Received response: {reply}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")

# === 執行 ===
if __name__ == "__main__":
    asyncio.run(simulate_ocpp_connection())
