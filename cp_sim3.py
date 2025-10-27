import asyncio
import logging
import random
import sys
from datetime import datetime, timezone
from websockets import connect
from ocpp.v16 import ChargePoint as BaseChargePoint, call

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# ====================== 使用者設定 ======================
CHARGE_POINT_ID = "TW*MSI*E000100"
ID_TAG = "6678B3EB"
WS_URL = "wss://ocppfortechlux-backend.onrender.com/TW*MSI*E000100"
# =======================================================


def iso_utc():
    """取得目前 UTC ISO 時間字串"""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


class SimChargePoint(BaseChargePoint):
    def __init__(self, charge_point_id, websocket):
        super().__init__(charge_point_id, websocket)
        self.running = True
        self.tx_id = None
        self.energy_wh = 0

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
        self.tx_id = getattr(res, "transaction_id", None) or getattr(res, "transactionId", None)
        logging.info(f"[SIM] >>> 取得 transaction_id = {self.tx_id}")

    async def send_meter_values(self):
        """每秒送出一次即時量測"""
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
            await self.call(
                call.MeterValues(connector_id=1, transaction_id=self.tx_id, meter_value=[mv])
            )
        except Exception as e:
            logging.error(f"[SIM] 發送 MeterValues 失敗: {e}")

    async def send_heartbeat(self):
        logging.info("[SIM] → Heartbeat")
        await self.call(call.Heartbeat())

    async def send_stop_transaction(self, reason="Local"):
        """模擬結束時送出 StopTransaction"""
        if not self.tx_id:
            logging.warning("[SIM] ⚠️ 無有效 transaction_id，略過 StopTransaction")
            return
        logging.info(f"[SIM] → StopTransaction tx_id={self.tx_id}")
        try:
            await self.call(
                call.StopTransaction(
                    transaction_id=int(self.tx_id),
                    meter_stop=int(self.energy_wh),
                    timestamp=iso_utc(),
                    id_tag=ID_TAG,
                    reason=reason
                )
            )
            logging.info(f"[SIM] ✅ StopTransaction 已送出（tx={self.tx_id}, energy={self.energy_wh:.1f}Wh）")
        except Exception as e:
            logging.error(f"[SIM] StopTransaction 發送失敗: {e}")


async def main():
    logging.info(f"[SIM] Connecting to {WS_URL}")
    async with connect(WS_URL, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(CHARGE_POINT_ID, ws)
        asyncio.create_task(cp.start())

        # ====== 啟動階段 ======
        await cp.send_boot()
        await cp.send_status("Available")
        await cp.send_authorize()
        await cp.send_status("Preparing")
        await asyncio.sleep(3)
        await cp.start_tx()
        await cp.send_status("Charging")

        # ====== 維持心跳任務 ======
        async def heartbeat_task():
            while cp.running:
                await asyncio.sleep(30)
                await cp.send_heartbeat()
                await cp.send_status("Charging")

        asyncio.create_task(heartbeat_task())

        # ====== 主模擬迴圈 ======
        try:
            while cp.running:
                await asyncio.sleep(1)
                await cp.send_meter_values()
        except KeyboardInterrupt:
            logging.info("[SIM] 🛑 使用者中斷，準備送出 StopTransaction")
            await cp.send_stop_transaction(reason="Local")
            await cp.send_status("Finishing")
            await asyncio.sleep(1)
            await cp.send_status("Available")
        except Exception as e:
            logging.error(f"[SIM] 發生例外：{e}")
            await cp.send_stop_transaction(reason="Error")
        finally:
            logging.info("[SIM] ✅ 模擬器結束，關閉連線")
            cp.running = False
            try:
                # 保險：結束前再送一次 StopTransaction（避免錯過）
                await cp.send_stop_transaction(reason="PowerLoss")
            except Exception:
                pass
            try:
                await cp.send_status("Available")
            except Exception:
                pass
            try:
                await ws.close()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
