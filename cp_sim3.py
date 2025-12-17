import asyncio
import logging
import random
from datetime import datetime, timezone
from urllib.parse import quote

from websockets import connect
from ocpp.v16 import ChargePoint as BaseChargePoint, call
from ocpp.v16.enums import Action
from ocpp.routing import on

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ====================== 白名單設定（務必與後端一致） ======================
CHARGE_POINT_ID = "TW*MSI*E000100"
ID_TAG = "6678B3EB"
WS_BASE_URL = "wss://ocppfortechlux-backend.onrender.com"
# =======================================================================


def iso_utc():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def build_ws_url(base: str, cp_id: str) -> str:
    # Render / Proxy 環境下，* 必須 encode
    return f"{base.rstrip('/')}/{quote(cp_id, safe='')}"


class SimChargePoint(BaseChargePoint):
    def __init__(self, charge_point_id, websocket):
        super().__init__(charge_point_id, websocket)
        self.running = True
        self.tx_id = None

        # 統一用 Wh（整數）避免浮點誤差
        self.energy_Wh = 0

        self.power_min = 3000
        self.power_max = 3500

    # ============================================================
    # 🔴 關鍵：接收中控 RemoteStopTransaction（修正點）
    # ============================================================
    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id: int, **kwargs):
        logging.warning(f"[SIM] ← RemoteStopTransaction tx_id={transaction_id}")

        await self.send_stop_transaction(reason="Remote")
        self.running = False

        return {"status": "Accepted"}

    # ================= 主動送出的 OCPP 行為 =================
    async def send_boot(self):
        logging.info("[SIM] → BootNotification")
        res = await self.call(
            call.BootNotification(
                charge_point_model="SimCP",
                charge_point_vendor="TechLux-Demo",
            )
        )
        logging.info(f"[SIM] BootNotification 回應: {res}")

    async def send_status(self, status="Available"):
        logging.info(f"[SIM] → StatusNotification: {status}")
        await self.call(
            call.StatusNotification(
                connector_id=1,
                status=status,
                error_code="NoError",
                timestamp=iso_utc(),
            )
        )

    async def send_authorize(self):
        logging.info(f"[SIM] → Authorize {ID_TAG}")
        res = await self.call(call.Authorize(id_tag=ID_TAG))
        logging.info(f"[SIM] Authorize 回應: {res}")

    async def start_transaction(self):
        logging.info("[SIM] → StartTransaction")
        res = await self.call(
            call.StartTransaction(
                connector_id=1,
                id_tag=ID_TAG,
                meter_start=int(self.energy_Wh),
                timestamp=iso_utc(),
            )
        )
        self.tx_id = getattr(res, "transaction_id", None) or getattr(res, "transactionId", None)
        logging.info(f"[SIM] ✅ transaction_id = {self.tx_id}")

    async def send_meter_values(self):
        if not self.tx_id:
            return

        power = random.randint(self.power_min, self.power_max)
        voltage = random.uniform(220, 230)
        current = power / voltage

        self.energy_Wh += int(power / 3600)

        mv = {
            "timestamp": iso_utc(),
            "sampledValue": [
                {"value": str(power), "measurand": "Power.Active.Import", "unit": "W"},
                {"value": f"{voltage:.1f}", "measurand": "Voltage", "unit": "V"},
                {"value": f"{current:.1f}", "measurand": "Current.Import", "unit": "A"},
                {
                    "value": str(self.energy_Wh),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh",
                },
            ],
        }

        logging.info(
            f"[SIM] → MeterValues | {power}W {voltage:.1f}V {current:.1f}A total={self.energy_Wh}Wh"
        )

        await self.call(
            call.MeterValues(
                connector_id=1,
                transaction_id=int(self.tx_id),
                meter_value=[mv],
            )
        )

    async def send_heartbeat(self):
        logging.info("[SIM] → Heartbeat")
        await self.call(call.Heartbeat())

    async def send_stop_transaction(self, reason="Local"):
        if not self.tx_id:
            logging.warning("[SIM] ⚠️ 無 transaction_id，略過 StopTransaction")
            return

        logging.info(f"[SIM] → StopTransaction tx={self.tx_id} reason={reason}")
        await self.call(
            call.StopTransaction(
                transaction_id=int(self.tx_id),
                meter_stop=int(self.energy_Wh),
                timestamp=iso_utc(),
                id_tag=ID_TAG,
                reason=reason,
            )
        )
        logging.info(f"[SIM] ✅ StopTransaction 完成（{self.energy_Wh}Wh）")


async def main():
    ws_url = build_ws_url(WS_BASE_URL, CHARGE_POINT_ID)
    logging.info(f"[SIM] Connecting to {ws_url}")

    async with connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(CHARGE_POINT_ID, ws)
        cp_task = asyncio.create_task(cp.start())

        # ===== 啟動流程 =====
        await cp.send_boot()
        await cp.send_status("Available")
        await cp.send_authorize()
        await cp.send_status("Preparing")
        await asyncio.sleep(1)

        await cp.start_transaction()
        await cp.send_status("Charging")

        # ===== Heartbeat =====
        async def heartbeat_loop():
            while cp.running:
                await asyncio.sleep(30)
                await cp.send_heartbeat()

        hb_task = asyncio.create_task(heartbeat_loop())

        # ===== 主模擬迴圈 =====
        try:
            while cp.running:
                await asyncio.sleep(1)
                await cp.send_meter_values()
        except KeyboardInterrupt:
            logging.info("[SIM] 🛑 使用者中斷")
            await cp.send_stop_transaction(reason="Local")
        finally:
            cp.running = False
            try:
                await cp.send_status("Finishing")
                await asyncio.sleep(1)
                await cp.send_status("Available")
            except Exception:
                pass

            hb_task.cancel()
            cp_task.cancel()
            await ws.close()
            logging.info("[SIM] ✅ 模擬器結束")


if __name__ == "__main__":
    asyncio.run(main())
