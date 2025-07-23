import asyncio
import logging
from datetime import datetime
from ocpp.v16 import ChargePoint as CP
from ocpp.v16 import call
from ocpp.v16.enums import Reason, Measurand
from ocpp.v16.datatypes import MeterValue, SampledValue
import websockets

# 啟用基本 log
logging.basicConfig(level=logging.INFO)

class ChargePoint(CP):
    def __init__(self, id, connection):
        super().__init__(id, connection)
        self.transaction_id = None

    async def send_boot_notification(self):
        request = call.BootNotification(
            charge_point_model="Model-X",
            charge_point_vendor="TestVendor"
        )
        response = await self.call(request)
        if response:
            print(f"✅ BootNotification 回應: status={response.status}, interval={response.interval}")
        else:
            print("❌ BootNotification 回傳 None")
        return response

    async def send_heartbeat(self):
        request = call.Heartbeat()
        response = await self.call(request)
        if response:
            print(f"❤️ Heartbeat 回應時間: {response.current_time}")
        else:
            print("❌ Heartbeat 回傳 None")
        return response

    async def send_meter_values(self, value_wh):
        meter_value = [MeterValue(
            timestamp=datetime.utcnow().isoformat(),
            sampled_value=[SampledValue(
                value=str(value_wh),
                measurand=Measurand.energy_active_import_register,
                unit="Wh",
                context="Sample.Periodic",
                format="Raw"
            )]
        )]
        request = call.MeterValues(
            connector_id=1,
            meter_value=meter_value
        )
        return await self.call(request)


    async def send_start_transaction(self, id_tag):
        request = call.StartTransaction(
            connector_id=1,
            id_tag=id_tag,
            meter_start=0,
            timestamp=datetime.utcnow().isoformat()
        )
        response = await self.call(request)
        if response:
            self.transaction_id = response.transaction_id
            print(f"🚗 StartTransaction 回應：transactionId={response.transaction_id}, idTagInfo.status={response.id_tag_info['status']}")
        else:
            print("❌ StartTransaction 回傳 None")
        return response

    async def send_stop_transaction(self):
        request = call.StopTransaction(
            transaction_id=self.transaction_id,
            meter_stop=100,
            timestamp=datetime.utcnow().isoformat(),
            id_tag="ABC123",
            reason=Reason.local
        )
        response = await self.call(request)
        if response:
            print("🛑 StopTransaction 回應成功")
        else:
            print("❌ StopTransaction 回傳 None")
        return response

    async def send_authorize(self, id_tag):
        request = call.Authorize(id_tag=id_tag)
        response = await self.call(request)
        if response:
            print(f"🆔 Authorize 回應：idTag={id_tag} | status={response.id_tag_info['status']}")
        else:
            print(f"❌ Authorize 回傳 None | idTag={id_tag}")
        return response


async def main():
    async with websockets.connect("ws://localhost:9000/CP_TEST", subprotocols=["ocpp1.6"]) as ws:
        cp = ChargePoint("CP_TEST", ws)
        receiver_task = asyncio.create_task(cp.start())
        await asyncio.sleep(0.5)

        # 初始化流程
        await cp.send_boot_notification()
        await asyncio.sleep(1)
        await cp.send_heartbeat()
        await asyncio.sleep(1)

        # 測試不同授權情境
        await cp.send_authorize("ABC123")
        await asyncio.sleep(1)
        await cp.send_authorize("TAG001")
        await asyncio.sleep(1)
        await cp.send_authorize("RANDOM")
        await asyncio.sleep(1)

        # 測試交易流程（成功案例）
        print("\n✅ 測試成功授權交易流程")
        start_res = await cp.send_start_transaction("ABC123")
        await asyncio.sleep(1)

        if start_res and start_res.id_tag_info["status"] == "Accepted":
            # 上傳多筆 meter value
            for i in range(4):
                energy = 1000 * i  # 0, 1000, 2000, 3000 Wh（模擬 0→3 度）
                print(f"📤 上傳第 {i+1} 筆 MeterValues（{energy} Wh）...")
                await cp.send_meter_values(energy)
                print(f"🕒 上傳時間: {datetime.utcnow().isoformat()}")
                await asyncio.sleep(70)  # 間隔超過 1 小時以跨午夜（可手動調整時間測試）


            await cp.send_stop_transaction()
            await asyncio.sleep(1)

            # 取得 /cost 計算結果
            import httpx
            txn_id = cp.transaction_id
            print(f"🔍 查詢交易費用：transactionId = {txn_id}")

            import json  # ← 若上方尚未 import

            async with httpx.AsyncClient() as client:
                resp = await client.get(f"http://localhost:8000/api/transactions/{txn_id}/cost")
                if resp.status_code == 200:
                    cost_info = resp.json()
                    print("📊 成本計算結果：")
                    print(json.dumps(cost_info, indent=2, ensure_ascii=False))
                else:
                    print("❌ 查詢費用失敗")



        # 測試交易流程（授權拒絕）
        print("\n🚫 測試失敗授權的 StartTransaction（RANDOM）")
        fail_res = await cp.send_start_transaction("RANDOM")
        await asyncio.sleep(1)

        if fail_res and fail_res.id_tag_info["status"] != "Accepted":
            print(f"❌ StartTransaction 被拒絕：status = {fail_res.id_tag_info['status']}")

        # 測試結束
        receiver_task.cancel()
        try:
            await receiver_task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
