import asyncio
import websockets

async def test():
    uri = "ws://ocppfortechlux-backend.onrender.com:10000/TW*MSI*E000100"
    async with websockets.connect(uri, subprotocols=["ocpp1.6"]) as websocket:
        print("✅ 成功連線到後端！")
        await websocket.send("Hello from test client")
        while True:
            msg = await websocket.recv()
            print("收到伺服器訊息:", msg)

asyncio.run(test())
