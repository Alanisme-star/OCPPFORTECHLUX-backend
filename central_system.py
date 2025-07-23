import uuid  # 在檔案最上方加入
import asyncio
import logging
from datetime import datetime

from websockets.server import serve
from ocpp.routing import on
from ocpp.v16 import call
from ocpp.v16.enums import RegistrationStatus, Action
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16.call_result import BootNotification, Heartbeat, MeterValues, StartTransaction, StopTransaction
from ocpp.v16.call_result import StopTransaction as StopTransactionPayload

logging.basicConfig(level=logging.DEBUG)

class ChargePoint(BaseChargePoint):
    @on(Action.boot_notification)
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        print(f"🔌 BootNotification 來自: {self.id} / {charge_point_vendor}")
        return BootNotification(
            current_time=datetime.utcnow().isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )


    @on(Action.start_transaction)
    async def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        print(f"🔋 StartTransaction from {self.id} | connector: {connector_id}, idTag: {id_tag}, meterStart: {meter_start}, time: {timestamp}")
        transaction_id = 1  # 這裡先用固定整數測試，之後可改成交易編號遞增系統
        return StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={
                "status": "Accepted"
            }
        )


    @on(Action.stop_transaction)
    async def on_stop_transaction(self, transaction_id, meter_stop, timestamp, id_tag, reason, **kwargs):
        print(f"🛑 StopTransaction from {self.id} | transactionId: {transaction_id}, meterStop: {meter_stop}, time: {timestamp}, idTag: {id_tag}, reason: {reason}")
        return StopTransactionPayload(
            id_tag_info={
                "status": "Accepted"
            }
        )





    @on(Action.heartbeat)
    async def on_heartbeat(self):
        now = datetime.utcnow().isoformat()
        print(f"❤️ Heartbeat received from CP={self.id} at {now}")
        return Heartbeat(current_time=now)

    @on(Action.meter_values)
    async def on_meter_values(self, connector_id, meter_value, **kwargs):
        print(f"⚡ MeterValues 來自 {self.id} / connector {connector_id}")
        for entry in meter_value:
            timestamp = entry.get("timestamp")
            for sampled_value in entry.get("sampledValue", []):
                measurand = sampled_value.get("measurand", "Energy.Active.Import.Register")
                value = sampled_value.get("value")
                unit = sampled_value.get("unit", "Wh")
                print(f"   - {timestamp} | {measurand}: {value} {unit}")
        return MeterValues()

async def on_connect(websocket, path):
    from websockets.exceptions import ConnectionClosedOK
    try:
        cp_id = path.strip("/")
        cp = ChargePoint(cp_id, websocket)
        logging.info(f"🪻 有充電樁嘗試連線：ID={cp_id}，IP={websocket.remote_address[0]} Port={websocket.remote_address[1]}")
        await cp.start()
    except ConnectionClosedOK:
        logging.info("🔌 連線正常結束。")

async def main():
    server = await serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6"]
    )
    print("🚀 OCPP Central System 啟動中：ws://localhost:9000")
    await server.wait_closed()

if __name__ == '__main__':
    asyncio.run(main())
