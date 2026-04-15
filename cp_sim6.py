import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import quote

from websockets import connect
from websockets.exceptions import ConnectionClosed
from ocpp.v16 import ChargePoint as BaseChargePoint, call, call_result
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

        # ✅ 精準能量累積（Wh）
        self.energy_Wh_f = 0.0
        self.energy_Wh_int = 0
        self._wh_residual = 0.0

        # ✅ 固定基準：220V / 32A
        self.fixed_voltage_v = 220.0
        self.fixed_raw_current_a = 32.0

        # ✅ 可選：確保每次送 MeterValues 時，Energy 至少 +1 Wh
        self.force_monotonic_step = True

        # ✅ 避免 finally 重複送 StopTransaction
        self.stopped_by_remote = False
        self.stop_sent = False

        # ✅ 確保 StopTransaction 完成後才關 ws
        self._stop_done_evt = asyncio.Event()
        self._stop_task = None

        # ===============================
        # ✅ SmartCharging / 限流模擬
        # ===============================
        self.current_limit_a = None
        self.smart_charging_enabled = True
        self.allowed_rate_unit = "A"
        self.max_profiles_installed = 20

    # ============================================================
    # ✅ OCPP 1.6 GetConfiguration：回覆 SmartCharging 能力
    # ============================================================
    @on(Action.get_configuration)
    async def on_get_configuration(self, key=None, **kwargs):
        keys = key or []
        if isinstance(keys, str):
            keys = [keys]

        def _kv(k, v, readonly=False):
            return {"key": k, "value": str(v), "readonly": bool(readonly)}

        supported = {
            "ChargingScheduleAllowedChargingRateUnit": self.allowed_rate_unit,
            "SmartChargingEnabled": "true" if self.smart_charging_enabled else "false",
            "MaxChargingProfilesInstalled": str(self.max_profiles_installed),
        }

        configuration_key = []
        unknown_key = []

        for k in keys:
            if k in supported:
                configuration_key.append(_kv(k, supported[k], readonly=True))
            else:
                unknown_key.append(k)

        logging.warning(
            f"[SIM][CONF] → GetConfiguration reply | keys={keys} | "
            f"SmartChargingEnabled={supported['SmartChargingEnabled']} | "
            f"AllowedUnit={supported['ChargingScheduleAllowedChargingRateUnit']}"
        )

        return call_result.GetConfigurationPayload(
            configuration_key=configuration_key,
            unknown_key=unknown_key,
        )

    # ============================================================
    # ✅ 接收後端下發的 SetChargingProfile
    # ============================================================
    @on(Action.set_charging_profile)
    async def on_set_charging_profile(
        self, connector_id=None, cs_charging_profiles=None, **kwargs
    ):
        try:
            prof = cs_charging_profiles or {}

            # ✅ 同時支援 camelCase / snake_case
            sched = (
                prof.get("chargingSchedule")
                or prof.get("charging_schedule")
                or {}
            )

            unit = (
                sched.get("chargingRateUnit")
                or sched.get("charging_rate_unit")
                or ""
            ).upper()

            periods = (
                sched.get("chargingSchedulePeriod")
                or sched.get("charging_schedule_period")
                or []
            )

            limit = None
            if periods and isinstance(periods, list):
                limit = periods[0].get("limit")

            tx_id = prof.get("transactionId") or prof.get("transaction_id")
            purpose = (
                prof.get("chargingProfilePurpose")
                or prof.get("charging_profile_purpose")
                or "Unknown"
            )

            if unit == "A" and limit is not None:
                old_limit = self.current_limit_a
                self.current_limit_a = float(limit)
                logging.warning(
                    f"[SIM][LIMIT][SET] ← SetChargingProfile APPLIED | "
                    f"cp_id={self.id} | connector_id={connector_id} | tx_id={tx_id} | "
                    f"purpose={purpose} | old_limit={old_limit} | "
                    f"new_limit={self.current_limit_a:.2f}A | prof={prof}"
                )
            else:
                logging.warning(
                    f"[SIM][LIMIT][SET] ← SetChargingProfile NOT_APPLIED | "
                    f"cp_id={self.id} | connector_id={connector_id} | tx_id={tx_id} | "
                    f"purpose={purpose} | unit={unit} | limit={limit} | prof={prof}"
                )

        except Exception as e:
            logging.exception(f"[SIM][LIMIT][SET] parse error | cp_id={self.id} | err={e}")

        # ✅ 修正：你目前的 ocpp 版本要用 SetChargingProfile，不是 SetChargingProfilePayload
        return call_result.SetChargingProfile(status="Accepted")

    # ============================================================
    # 🔴 接收中控 RemoteStopTransaction
    # ============================================================
    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id: int, **kwargs):
        logging.error("[SIM][HIT] RemoteStopTransaction handler ENTERED")
        logging.warning(f"[SIM] ← RemoteStopTransaction tx_id={transaction_id}")

        self.stopped_by_remote = True
        payload = call_result.RemoteStopTransactionPayload(status="Accepted")

        if self._stop_task and not self._stop_task.done():
            logging.warning("[SIM] ⚠️ Stop task already running, skip re-create.")
            return payload

        async def _do_stop():
            try:
                logging.error("[SIM][SEND] StopTransaction about to send (Remote)")
                await self.send_stop_transaction(reason="Remote")
                logging.error("[SIM][DONE] StopTransaction sent OK (Remote)")
            except Exception as e:
                logging.exception(f"[SIM] ❌ RemoteStop 後送 StopTransaction 失敗: {e}")
            finally:
                self._stop_done_evt.set()
                await asyncio.sleep(1.0)
                self.running = False

        self._stop_done_evt.clear()
        self._stop_task = asyncio.create_task(_do_stop())

        return payload

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

        self.energy_Wh_int = int(self.energy_Wh_f)

        res = await self.call(
            call.StartTransaction(
                connector_id=1,
                id_tag=ID_TAG,
                meter_start=int(self.energy_Wh_int),
                timestamp=iso_utc(),
            )
        )
        self.tx_id = getattr(res, "transaction_id", None) or getattr(
            res, "transactionId", None
        )
        logging.info(f"[SIM] ✅ transaction_id = {self.tx_id}")

    def _update_energy_wh(self, power_w: float, dt_s: float = 1.0):
        inc_wh = (float(power_w) * float(dt_s)) / 3600.0

        self.energy_Wh_f += inc_wh
        new_int = int(self.energy_Wh_f)

        if self.force_monotonic_step:
            self._wh_residual += inc_wh
            if new_int <= self.energy_Wh_int and self._wh_residual >= 1.0:
                new_int = self.energy_Wh_int + 1
                self._wh_residual -= 1.0

        self.energy_Wh_int = max(self.energy_Wh_int, new_int)

    async def send_meter_values(self):
        if not self.tx_id or not self.running:
            return

        # ✅ 固定基準：220V / 32A
        voltage = self.fixed_voltage_v
        raw_current = self.fixed_raw_current_a

        # 原始功率固定 = 220 * 32 = 7040W
        raw_power = int(voltage * raw_current)

        # ✅ 若已收到限流，套用限流
        if self.current_limit_a is not None and self.current_limit_a > 0:
            current = min(raw_current, float(self.current_limit_a))
            limited = True
        else:
            current = raw_current
            limited = False

        # ✅ 功率永遠由固定電壓 * 實際電流得出
        power = int(voltage * current)

        self._update_energy_wh(power_w=power, dt_s=1.0)

        mv = {
            "timestamp": iso_utc(),
            "sampledValue": [
                {"value": str(power), "measurand": "Power.Active.Import", "unit": "W"},
                {"value": f"{voltage:.1f}", "measurand": "Voltage", "unit": "V"},
                {"value": f"{current:.1f}", "measurand": "Current.Import", "unit": "A"},
                {
                    "value": str(self.energy_Wh_int),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh",
                },
            ],
        }

        logging.warning(
            f"[SIM][MV] cp_id={self.id} | tx_id={self.tx_id} | "
            f"LIMITED={'YES' if limited else 'NO'} | "
            f"limit_a={self.current_limit_a} | "
            f"fixed_voltage_v={voltage:.1f} | raw_current_a={raw_current:.1f} | "
            f"effective_current_a={current:.1f} | raw_power_w={raw_power} | "
            f"power_w={power} | energy_wh={self.energy_Wh_int}"
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
        if self.stop_sent:
            logging.warning("[SIM] ⚠️ StopTransaction 已送過，略過")
            return

        if not self.tx_id:
            logging.warning("[SIM] ⚠️ 無 transaction_id，略過 StopTransaction")
            return

        self.stop_sent = True
        logging.info(f"[SIM] → StopTransaction tx={self.tx_id} reason={reason}")

        await self.call(
            call.StopTransaction(
                transaction_id=int(self.tx_id),
                meter_stop=int(self.energy_Wh_int),
                timestamp=iso_utc(),
                id_tag=ID_TAG,
                reason=reason,
            )
        )

        logging.info(f"[SIM] ✅ StopTransaction 完成（{self.energy_Wh_int}Wh）")


async def main():
    ws_url = build_ws_url(WS_BASE_URL, CHARGE_POINT_ID)
    logging.info(f"[SIM] Connecting to {ws_url}")

    async with connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(CHARGE_POINT_ID, ws)
        logging.warning(
            f"[SIM][BOOTSTRAP] STARTED | cp_id={CHARGE_POINT_ID} | "
            f"fixed_voltage_v={cp.fixed_voltage_v} | fixed_raw_current_a={cp.fixed_raw_current_a}"
        )
        cp_task = asyncio.create_task(cp.start())

        await asyncio.sleep(0.8)

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
                try:
                    await cp.send_heartbeat()
                except Exception as e:
                    logging.warning(f"[SIM] ⚠️ Heartbeat 失敗: {e}")
                    return

        hb_task = asyncio.create_task(heartbeat_loop())

        # ===== 主模擬迴圈 =====
        try:
            while cp.running:
                await asyncio.sleep(1)
                await cp.send_meter_values()

        except KeyboardInterrupt:
            logging.info("[SIM] 🛑 使用者中斷")
            try:
                await cp.send_stop_transaction(reason="Local")
                cp._stop_done_evt.set()
            except Exception as e:
                logging.error(f"[SIM] ❌ StopTransaction 失敗: {e}")

        except ConnectionClosed:
            logging.warning("[SIM] ⚠️ WebSocket 已關閉（ConnectionClosed）")

        finally:
            if cp.stopped_by_remote:
                logging.warning("[SIM] 🧩 RemoteStop 流程：等待 StopTransaction 完成再關閉 ws...")
                try:
                    await asyncio.wait_for(cp._stop_done_evt.wait(), timeout=15)
                    logging.warning("[SIM] ✅ 已確認 StopTransaction 流程結束")
                except asyncio.TimeoutError:
                    logging.error("[SIM] ❌ 等待 StopTransaction 完成超時（仍可能導致後端 504）")

            if cp.tx_id and (not cp.stopped_by_remote) and (not cp.stop_sent):
                try:
                    logging.warning("[SIM] ⚠️ finally 區段補送 StopTransaction")
                    await cp.send_stop_transaction(reason="Local")
                    cp._stop_done_evt.set()
                except Exception as e:
                    logging.error(f"[SIM] ❌ 補送 StopTransaction 失敗: {e}")

            cp.running = False

            try:
                await cp.send_status("Finishing")
                await asyncio.sleep(0.5)
                await cp.send_status("Available")
            except Exception:
                pass

            hb_task.cancel()
            cp_task.cancel()

            try:
                await ws.close()
            except Exception:
                pass

            logging.info("[SIM] ✅ 模擬器結束")


if __name__ == "__main__":
    asyncio.run(main())