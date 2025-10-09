import asyncio
import logging
import random
import signal
import sys
from datetime import datetime, timezone
from urllib.parse import quote

from websockets import connect
from ocpp.v16 import ChargePoint as BaseChargePoint, call
from ocpp.v16.enums import Action
from ocpp.routing import on

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# ====================== 使用者需確認的設定 ======================
CHARGE_POINT_ID = "TW*MSI*E000100"   # ⚡ 與後端 charge_points 表一致
ID_TAG = "6678B3EB"            # ⚡ 與後端 cards / id_tags 表中存在的卡號一致
BACKEND_URL = "wss://ocppfortechlux-backend.onrender.com"
WS_URL = BACKEND_URL.rstrip("/") + "/" + quote(CHARGE_POINT_ID, safe="")
# ===============================================================

def iso_utc():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


class SimChargePoint(BaseChargePoint):
    def __init__(self, charge_point_id, websocket):
        super().__init__(charge_point_id, websocket)
        self.running = True
        self.tx_id = None
        self.energy_wh = 0

    # ---- 後端主動事件 ----
    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id=None, **kwargs):
        logging.info(f"[SIM] 🛑 收到遠端停充指令: tx={transaction_id}")
        self.running = False
        await self._send_stop_tx(transaction_id)
        return {"status": "Accepted"}

    # ---- 模擬器主動送出 ----
    async def send_boot(self):
        logging.info("[SIM] → BootNotification")
        res = await self.call(
            call.BootNotification(charge_point_model="SimCP", charge_point_vendor="DemoVendor")
        )
        logging.info(f"[SIM] BootNotification 回應: {res}")

    async def send_status(self, status="Available"):
        logging.info(f"[SIM] → StatusNotification: {status}")
        await self.call(
            call.StatusNotification(connector_id=1, status=status, error_code="NoError")
        )

    async def send_authorize(self, id_tag=ID_TAG):
        logging.info(f"[SIM] → Authorize {id_tag}")
        res = await self.call(call.Authorize(id_tag=id_tag))
        logging.info(f"[SIM] Authorize 回應: {res}")

    async def start_tx(self, id_tag=ID_TAG):
        logging.info("[SIM] → StartTransaction")
        res = await self.call(
            call.StartTransaction(
                connector_id=1,
                id_tag=id_tag,
                meter_start=self.energy_wh,
                timestamp=iso_utc()
            )
        )
        logging.info(f"[SIM] StartTransaction 原始回應: {res.__dict__ if hasattr(res, '__dict__') else res}")
        self.tx_id = getattr(res, "transaction_id", None) or getattr(res, "transactionId", None)
        logging.info(f"[SIM] >>> 取得 transaction_id = {self.tx_id}")
        if not self.tx_id:
            logging.warning("⚠️ StartTransaction 沒有回傳有效的 transaction_id，後續 MeterValues 可能被後端拒絕")

    async def send_meter_values(self):
        power = random.randint(3000, 3500)
        voltage = random.uniform(220, 230)
        current = power / voltage
        self.energy_wh += power / 3600

        mv = {
            "timestamp": iso_utc(),
            "sampledValue": [
                {"value": str(power), "measurand": "Power.Active.Import", "unit": "W"},
                {"value": f"{voltage:.1f}", "measurand": "Voltage", "unit": "V"},
                {"value": f"{current:.1f}", "measurand": "Current.Import", "unit": "A"},
                {
                    "value": str(int(self.energy_wh)),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh",
                },
            ],
        }

        logging.info(
            f"[SIM] → MeterValues (tx={self.tx_id}) | {power}W {voltage:.1f}V {current:.1f}A total={self.energy_wh:.1f}Wh"
        )
        try:
            res = await self.call(
                call.MeterValues(connector_id=1, transaction_id=self.tx_id, meter_value=[mv])
            )
            logging.info(f"[SIM] MeterValues 回應: {res}")
        except Exception as e:
            logging.error(f"[SIM] 發送 MeterValues 失敗: {e}")

    async def _send_stop_tx(self, transaction_id):
        logging.info(f"[SIM] → StopTransaction: {transaction_id}, meter_stop={self.energy_wh:.1f}Wh")
        await self.call(
            call.StopTransaction(
                transaction_id=transaction_id,
                meter_stop=int(self.energy_wh),
                timestamp=iso_utc()
            )
        )

    async def send_heartbeat(self):
        logging.info("[SIM] → Heartbeat")
        await self.call(call.Heartbeat())


async def main():
    logging.info(f"[SIM] Connecting to {WS_URL}")
    async with connect(WS_URL, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(CHARGE_POINT_ID, ws)
        asyncio.create_task(cp.start())

        await cp.send_boot()
        await cp.send_status("Available")
        await cp.send_authorize()
        await cp.send_status("Preparing")
        await cp.start_tx()
        await cp.send_status("Charging")

        async def heartbeat_task():
            while cp.running:
                await asyncio.sleep(30)
                await cp.send_heartbeat()
                await cp.send_status("Charging")

        asyncio.create_task(heartbeat_task())

        while cp.running:
            await asyncio.sleep(1)
            await cp.send_meter_values()

        await cp.send_status("Finishing")
        await cp.send_status("Available")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    def stop_loop(sig, frame):
        logging.info("[SIM] 收到中斷訊號，結束模擬")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_loop)
    signal.signal(signal.SIGTERM, stop_loop)

    asyncio.run(main())
