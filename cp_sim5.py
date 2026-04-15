# cp_sim5.py
# ============================================================
# Techlux OCPP 1.6 Simulator
#
# Fixes included:
#   - Receiver loop started before any call()
#   - Old python-ocpp API compatible (ocpp.v16.call, call_result)
#   - Set/ClearChargingProfile responses
#   - MeterValues includes V/A/W + Energy.Active.Import.Register
#   - Energy register starts from StartTransaction.meter_start (diff-based session kWh OK)
#   - Graceful Ctrl+C shutdown
#   - StatusNotification (Available/Charging) so UI status updates
#   - Heartbeat loop using BootNotification interval
#   - ✅ IMPORTANT: ISO time uses "+00:00" (NOT "Z") to avoid backend parse issues
#   - ✅ Status keepalive loop (every 30s) to avoid status "stale" fallback
# ============================================================

import argparse
import asyncio
import logging
import random
import signal
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import websockets
from ocpp.v16 import ChargePoint as CP16
from ocpp.v16 import call, call_result
from ocpp.v16.enums import (
    RegistrationStatus,
    AuthorizationStatus,
    ChargingProfileStatus,
    RemoteStartStopStatus,
    ChargePointStatus,
)
from ocpp.routing import on


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("cp_sim5")


# -----------------------------
# Helpers
# -----------------------------
def now_iso() -> str:
    # ✅ Use "+00:00" format (NOT "Z") to maximize backend compatibility
    # Example: "2026-03-04T11:16:29.771691+00:00"
    return datetime.now(timezone.utc).isoformat()


def rand_between(a: float, b: float) -> float:
    return random.uniform(float(a), float(b))


def quantize_0p1(x: float) -> float:
    return round(float(x) * 10.0) / 10.0


def extract_status(maybe_obj: Any) -> Optional[str]:
    """
    Robustly extract 'status' from ocpp response structures that might be:
      - object with .status
      - dict with ["status"]
      - nested dict-like
    """
    if maybe_obj is None:
        return None
    if isinstance(maybe_obj, dict):
        v = maybe_obj.get("status")
        return str(v) if v is not None else None
    v = getattr(maybe_obj, "status", None)
    return str(v) if v is not None else None


# -----------------------------
# Config
# -----------------------------
@dataclass
class SimConfig:
    ws_base: str = "wss://ocppfortechlux-backend.onrender.com"
    meter_interval_s: int = 2

    # Electrical model
    voltage_v: float = 220
    default_a: float = 32.0
    hw_max_a: float = 32.0

    # Current draw randomness (before clamp)
    base_current_min_a: float = 25.0
    base_current_max_a: float = 32.0

    # Auto-run behaviors
    do_authorize: bool = True
    do_start_tx: bool = True
    start_stagger_s: float = 0.0

    # Heartbeat fallback (if server doesn't provide interval)
    heartbeat_fallback_s: int = 30

    # Status keepalive interval
    status_keepalive_s: int = 30


# -----------------------------
# Simulator ChargePoint
# -----------------------------
class SimChargePoint(CP16):
    def __init__(self, cp_id: str, ws, config: SimConfig):
        super().__init__(cp_id, ws)
        self.config = config

        # Applied current limit (A). None = unlimited (use default_a)
        self.current_limit_a: Optional[float] = None

        self.connector_id = 1
        self.transaction_id: Optional[int] = None
        self.is_charging: bool = False

        self.last_profile: Dict[str, Any] = {}
        self.last_profile_purpose: Optional[str] = None

        # 第一階段：追蹤後端要求值（方便觀察功率分配 -> 電流下發）
        self.last_requested_current_a: Optional[float] = None
        self.last_requested_power_kw: Optional[float] = None
        self._stop_event = asyncio.Event()

        # Energy register (Wh) & meter_start (Wh)
        self.energy_wh: float = 0.0
        self.meter_start_wh: int = 0

        # Heartbeat interval from BootNotification response
        self.heartbeat_interval_s: int = int(self.config.heartbeat_fallback_s)

    def stop(self):
        self._stop_event.set()

    # ---------- status notification ----------
    async def send_status(self, status: ChargePointStatus, error_code: str = "NoError"):
        try:
            req = call.StatusNotification(
                connector_id=self.connector_id,
                error_code=error_code,
                status=status,
                timestamp=now_iso(),
            )
            await self.call(req)
            log.info(f"[{self.id}] => StatusNotification status={status}")
        except Exception as e:
            log.warning(f"[{self.id}] StatusNotification failed: {e}")

    async def status_keepalive_loop(self):
        """
        ✅ Some backends/UI consider status stale if not refreshed.
        We periodically re-send Available/Charging to avoid fallback.
        """
        while not self._stop_event.is_set():
            try:
                if self.is_charging and self.transaction_id is not None:
                    await self.send_status(ChargePointStatus.charging)
                else:
                    await self.send_status(ChargePointStatus.available)
            except Exception:
                pass
            await asyncio.sleep(max(10, int(self.config.status_keepalive_s)))

    # ---------- heartbeat loop ----------
    async def heartbeat_loop(self):
        """
        ✅ Keep connection alive & keep backend from marking CP as Available/Offline due to inactivity.
        """
        while not self._stop_event.is_set():
            try:
                req = call.Heartbeat()
                resp = await self.call(req)
                ct = getattr(resp, "current_time", None)
                log.info(f"[{self.id}] => Heartbeat ok current_time={ct}")
            except Exception as e:
                log.warning(f"[{self.id}] Heartbeat failed: {e}")

            await asyncio.sleep(max(5, int(self.heartbeat_interval_s)))

    # ---------- core flows ----------
    async def boot(self):
        req = call.BootNotification(
            charge_point_model="Techlux-Sim",
            charge_point_vendor="Techlux",
        )
        log.info(f"[{self.id}] => BootNotification")
        resp = await self.call(req)

        status = getattr(resp, "status", None)
        interval = getattr(resp, "interval", None)

        ok = (status == RegistrationStatus.accepted) or str(status).lower().endswith("accepted")

        # Save heartbeat interval if provided
        try:
            if interval is not None:
                self.heartbeat_interval_s = int(interval)
            else:
                self.heartbeat_interval_s = int(self.config.heartbeat_fallback_s)
        except Exception:
            self.heartbeat_interval_s = int(self.config.heartbeat_fallback_s)

        log.info(f"[{self.id}] Boot status={status} ok={ok} interval={self.heartbeat_interval_s}s")

        # After boot, announce Available once
        await self.send_status(ChargePointStatus.available)

    async def authorize(self, id_tag: str = "6678B3EB") -> bool:
        req = call.Authorize(id_tag=id_tag)
        log.info(f"[{self.id}] => Authorize id_tag={id_tag}")
        resp = await self.call(req)

        # id_tag_info can be obj or dict depending on versions
        id_info = getattr(resp, "id_tag_info", None)
        if id_info is None and isinstance(resp, dict):
            id_info = resp.get("idTagInfo") or resp.get("id_tag_info")

        s = extract_status(id_info)
        ok = (s is not None and s.lower().endswith("accepted"))
        log.info(f"[{self.id}] Authorize status={s} ok={ok}")
        return ok

    async def start_transaction(self, id_tag: str = "6678B3EB") -> Optional[int]:
        meter_start = int(rand_between(0, 1000))
        self.meter_start_wh = meter_start

        req = call.StartTransaction(
            connector_id=self.connector_id,
            id_tag=id_tag,
            meter_start=meter_start,
            timestamp=now_iso(),
        )
        log.info(f"[{self.id}] => StartTransaction")
        resp = await self.call(req)

        tx_id = getattr(resp, "transaction_id", None)
        if tx_id is None and isinstance(resp, dict):
            tx_id = resp.get("transactionId") or resp.get("transaction_id")

        self.transaction_id = tx_id
        self.is_charging = True if tx_id is not None else False

        if self.is_charging:
            # ✅ energy register must be on same scale as meter_start
            self.energy_wh = float(self.meter_start_wh)
            await self.send_status(ChargePointStatus.charging)

        log.info(f"[{self.id}] StartTransaction tx_id={tx_id} meter_start={meter_start}Wh")
        log.info(
            f"[{self.id}] 🚗 CHARGE START | "
            f"voltage_v={self.config.voltage_v} | "
            f"base_min_a={self.config.base_current_min_a} | "
            f"base_max_a={self.config.base_current_max_a} | "
            f"base_min_kw={(self.config.base_current_min_a * self.config.voltage_v)/1000.0:.2f} | "
            f"base_max_kw={(self.config.base_current_max_a * self.config.voltage_v)/1000.0:.2f} | "
            f"hw_max_a={self.config.hw_max_a}"
        )
        return tx_id

    async def stop_transaction(self, reason: str = "Local") -> bool:
        if not self.transaction_id:
            return True

        meter_stop = int(self.energy_wh)
        req = call.StopTransaction(
            transaction_id=self.transaction_id,
            meter_stop=meter_stop,
            timestamp=now_iso(),
            reason=reason,
        )
        log.info(f"[{self.id}] => StopTransaction tx_id={self.transaction_id}")
        await self.call(req)

        self.transaction_id = None
        self.is_charging = False
        self.meter_start_wh = 0

        await self.send_status(ChargePointStatus.available)
        log.info(f"[{self.id}] StopTransaction done")
        return True

    def _power_w_to_current_a(self, power_w: float) -> float:
        """
        將功率(W)換算成電流(A)
        單相簡化：A = W / V
        """
        v = max(1.0, float(self.config.voltage_v))
        return quantize_0p1(max(0.0, float(power_w) / v))

    # ---------- simulation: meter loop ----------
    def _get_effective_current_a(self) -> float:
        base = rand_between(self.config.base_current_min_a, self.config.base_current_max_a)

        # ✅ 尚未收到後端限流 → 先用「保守電流」
        if self.current_limit_a is None:
            safe_a = 16.0
            eff = min(base, safe_a)

            log.warning(
                f"[{self.id}] [SAFE-START] no_limit_yet | "
                f"base={base:.1f}A | safe_a={safe_a:.1f}A | eff={eff:.1f}A"
            )

        # ✅ 已收到後端限流 → 直接貼近限制值，不再大幅亂跳
        else:
            eff = float(self.current_limit_a)

            log.warning(
                f"[{self.id}] [FOLLOW-LIMIT] limit_a={self.current_limit_a:.1f}A | eff={eff:.1f}A"
            )

        eff = min(eff, float(self.config.hw_max_a))
        return quantize_0p1(max(0.0, eff))


    async def meter_values_loop(self):
        while not self._stop_event.is_set():
            if self.is_charging and self.transaction_id is not None:
                i_a = self._get_effective_current_a()
                v = float(self.config.voltage_v)
                p_w = max(0.0, v * i_a)

                # Wh += W * (sec/3600)
                self.energy_wh += p_w * (float(self.config.meter_interval_s) / 3600.0)

                req = call.MeterValues(
                    connector_id=self.connector_id,
                    transaction_id=self.transaction_id,
                    meter_value=[
                        {
                            "timestamp": now_iso(),
                            "sampledValue": [
                                {"measurand": "Current.Import", "unit": "A", "value": str(i_a)},
                                {"measurand": "Voltage", "unit": "V", "value": str(quantize_0p1(v))},
                                {"measurand": "Power.Active.Import", "unit": "W", "value": str(int(p_w))},
                                # ✅ total register, must be >= meter_start
                                {"measurand": "Energy.Active.Import.Register", "unit": "Wh", "value": str(int(self.energy_wh))},
                            ],
                        }
                    ],
                )

                try:
                    await self.call(req)

                    base_max_a = float(self.config.base_current_max_a)

                    is_limited = (
                        self.current_limit_a is not None
                        and float(i_a) <= float(self.current_limit_a) + 0.1
                        and float(self.current_limit_a) < base_max_a
                    )


                    log.info(
                        f"[{self.id}] => MeterValues | "
                        f"I={i_a:.1f}A V={v:.1f}V P={p_w/1000.0:.2f}kW "
                        f"E={self.energy_wh/1000.0:.4f}kWh | "
                        f"limited={'YES' if is_limited else 'NO'} | "
                        f"{self._limit_debug_summary()}"
                    )
                except Exception as e:
                    log.warning(f"[{self.id}] MeterValues failed: {e}")

            await asyncio.sleep(self.config.meter_interval_s)


    def _limit_debug_summary(self) -> str:
        """
        統一輸出目前限流狀態，方便觀察：
        - 後端最近要求功率
        - 後端最近要求電流
        - 模擬器目前實際套用上限
        - 模擬器原始電流範圍
        """
        return (
            f"requested_power_kw={self.last_requested_power_kw} | "
            f"requested_current_a={self.last_requested_current_a} | "
            f"effective_limit_a={self.current_limit_a if self.current_limit_a is not None else 'None'} | "
            f"base_min_a={self.config.base_current_min_a} | "
            f"base_max_a={self.config.base_current_max_a} | "
            f"hw_max_a={self.config.hw_max_a}"
        )

    # ---------- OCPP handlers ----------
    @on("RemoteStartTransaction")
    async def on_remote_start_transaction(self, id_tag: str, connector_id: Optional[int] = None, **kwargs):
        if connector_id is not None:
            self.connector_id = int(connector_id)

        log.warning(f"[{self.id}] <= RemoteStartTransaction id_tag={id_tag} connector_id={self.connector_id}")
        asyncio.create_task(self.authorize(id_tag))
        asyncio.create_task(self.start_transaction(id_tag))
        return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

    @on("RemoteStopTransaction")
    async def on_remote_stop_transaction(self, transaction_id: int, **kwargs):
        log.warning(f"[{self.id}] <= RemoteStopTransaction tx_id={transaction_id}")
        asyncio.create_task(self.stop_transaction(reason="Remote"))
        return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

    @on("SetChargingProfile")
    async def on_set_charging_profile(self, connector_id: int, cs_charging_profiles: dict, **kwargs):
        applied = False
        unit = "A"

        try:
            schedule = cs_charging_profiles.get("chargingSchedule") \
                or cs_charging_profiles.get("charging_schedule") \
                or {}

            periods = schedule.get("chargingSchedulePeriod") \
                or schedule.get("charging_schedule_period") \
                or []

            unit = schedule.get("chargingRateUnit") \
                or schedule.get("charging_rate_unit") \
                or "A"

            purpose = cs_charging_profiles.get("chargingProfilePurpose") \
                or cs_charging_profiles.get("charging_profile_purpose") \
                or "Unknown"

            self.last_profile = cs_charging_profiles
            self.last_profile_purpose = purpose

            if periods:
                raw_limit = periods[0].get("limit")
                if raw_limit is not None:
                    limit = float(raw_limit)

                    self.last_requested_current_a = None
                    self.last_requested_power_kw = None

                    if unit.upper() == "A":
                        self.last_requested_current_a = quantize_0p1(max(0.0, limit))
                        self.last_requested_power_kw = round(
                            (self.last_requested_current_a * float(self.config.voltage_v)) / 1000.0,
                            3,
                        )
                        self.current_limit_a = self.last_requested_current_a

                    elif unit.upper() == "W":
                        self.last_requested_power_kw = round(limit / 1000.0, 3)
                        self.last_requested_current_a = self._power_w_to_current_a(limit)
                        self.current_limit_a = self.last_requested_current_a

                    else:
                        self.current_limit_a = None
                        self.last_requested_current_a = None
                        self.last_requested_power_kw = None

                    if self.current_limit_a is not None:
                        self.current_limit_a = min(self.current_limit_a, float(self.config.hw_max_a))
                        applied = True

            log.warning(
                f"[{self.id}] 🔒 LIMIT APPLIED | purpose={purpose} | unit={unit} "
                f"| requested_raw={periods[0].get('limit') if periods else None} "
                f"| requested_power_kw={self.last_requested_power_kw} "
                f"| requested_current_a={self.last_requested_current_a} "
                f"| effective_current_a={self.current_limit_a} "
                f"| hw_max_a={self.config.hw_max_a} | applied={applied}"
            )
            log.warning(f"[{self.id}] 🔍 LIMIT STATE | {self._limit_debug_summary()}")

        except Exception as e:
            log.warning(f"[{self.id}] SetChargingProfile error: {e}")
            applied = False

        return call_result.SetChargingProfile(
            status=ChargingProfileStatus.accepted if applied else ChargingProfileStatus.rejected
        )

    @on("ClearChargingProfile")
    async def on_clear_charging_profile(self, **kwargs):
        self.current_limit_a = None
        self.last_profile = {}
        self.last_profile_purpose = None
        log.warning(f"[{self.id}] 🧹 ClearChargingProfile => limit cleared")
        return call_result.ClearChargingProfile(status=ChargingProfileStatus.accepted)

    @on("Heartbeat")
    async def on_heartbeat(self, **kwargs):
        # If server ever sends Heartbeat to CP (rare), reply.
        return call_result.Heartbeat(current_time=now_iso())


# -----------------------------
# Connection / Runner
# -----------------------------
async def run_one_cp(cp_id: str, config: SimConfig, stop_all: asyncio.Event):
    if config.start_stagger_s > 0:
        await asyncio.sleep(config.start_stagger_s)

    ws_url = f"{config.ws_base.rstrip('/')}/{cp_id}"
    log.info(f"[{cp_id}] Connecting to {ws_url}")

    try:
        async with websockets.connect(
            ws_url,
            subprotocols=["ocpp1.6"],
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=None,
        ) as ws:
            cp = SimChargePoint(cp_id, ws, config)

            # MUST start receiver loop first
            receiver_task = asyncio.create_task(cp.start())

            # boot first (get heartbeat interval)
            await cp.boot()

            # background loops
            meter_task = asyncio.create_task(cp.meter_values_loop())
            heartbeat_task = asyncio.create_task(cp.heartbeat_loop())
            status_task = asyncio.create_task(cp.status_keepalive_loop())

            # optional authorize/start
            if config.do_authorize:
                await cp.authorize()

            if config.do_start_tx:
                await cp.start_transaction()

            # wait stop_all or receiver ends
            stop_task = asyncio.create_task(stop_all.wait())

            done, pending = await asyncio.wait(
                {stop_task, receiver_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not stop_task.done():
                stop_task.cancel()
                try:
                    await stop_task
                except Exception:
                    pass

            # stop loops
            cp.stop()

            for t in (status_task, heartbeat_task, meter_task):
                t.cancel()
                try:
                    await t
                except Exception:
                    pass

            # graceful stop tx
            try:
                await cp.stop_transaction(reason="Local")
            except Exception:
                pass

            receiver_task.cancel()
            try:
                await receiver_task
            except Exception:
                pass

    except Exception as e:
        log.error(f"[{cp_id}] Connection failed: {e}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cp_ids", nargs="*", default=["SIM_CP_001"], help="Charge point IDs")
    parser.add_argument("--ws", dest="ws_base", default="wss://ocppfortechlux-backend.onrender.com", help="WS base")
    parser.add_argument("--meter", dest="meter_interval_s", type=int, default=2, help="MeterValues interval (sec)")
    parser.add_argument("--voltage", dest="voltage_v", type=float, default=220, help="Voltage V")
    parser.add_argument("--default_a", dest="default_a", type=float, default=32.0, help="Default current A")
    parser.add_argument("--hw_max_a", dest="hw_max_a", type=float, default=32.0, help="HW max current A")
    parser.add_argument("--base_min_a", dest="base_current_min_a", type=float, default=25.0, help="Base min current A")
    parser.add_argument("--base_max_a", dest="base_current_max_a", type=float, default=32.0, help="Base max current A")
    parser.add_argument("--no-auth", dest="do_authorize", action="store_false", help="Disable Authorize")
    parser.add_argument("--no-start", dest="do_start_tx", action="store_false", help="Disable auto StartTransaction")
    parser.add_argument("--stagger", dest="start_stagger_s", type=float, default=0.0, help="Stagger start seconds")
    parser.add_argument("--hb", dest="heartbeat_fallback_s", type=int, default=30, help="Heartbeat fallback seconds")
    parser.add_argument("--status-keepalive", dest="status_keepalive_s", type=int, default=30, help="Status keepalive seconds")

    args = parser.parse_args()

    config = SimConfig(
        ws_base=args.ws_base,
        meter_interval_s=args.meter_interval_s,
        voltage_v=args.voltage_v,
        default_a=args.default_a,
        hw_max_a=args.hw_max_a,
        base_current_min_a=args.base_current_min_a,
        base_current_max_a=args.base_current_max_a,
        do_authorize=args.do_authorize,
        do_start_tx=args.do_start_tx,
        start_stagger_s=args.start_stagger_s,
        heartbeat_fallback_s=args.heartbeat_fallback_s,
        status_keepalive_s=args.status_keepalive_s,
    )

    cp_ids: List[str] = list(args.cp_ids) if args.cp_ids else ["SIM_CP_001"]
    log.info(f"Sim starting cp_ids={cp_ids}, ws_base={config.ws_base}")
    log.info(
        "Sim config | "
        f"meter_interval_s={config.meter_interval_s} | "
        f"voltage_v={config.voltage_v} | "
        f"default_a={config.default_a} | "
        f"hw_max_a={config.hw_max_a} | "
        f"base_min_a={config.base_current_min_a} | "
        f"base_max_a={config.base_current_max_a} | "
        f"base_min_kw={(config.base_current_min_a * config.voltage_v)/1000.0:.2f} | "
        f"base_max_kw={(config.base_current_max_a * config.voltage_v)/1000.0:.2f}"
    )

    stop_all = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _sig_handler():
        log.warning("SIGINT/SIGTERM received -> stopping...")
        stop_all.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _sig_handler)
        loop.add_signal_handler(signal.SIGTERM, _sig_handler)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda s, f: _sig_handler())
        signal.signal(signal.SIGTERM, lambda s, f: _sig_handler())

    tasks = [asyncio.create_task(run_one_cp(cp_id, config, stop_all)) for cp_id in cp_ids]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"Main error: {e}")
    finally:
        stop_all.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Sim stopped.")


if __name__ == "__main__":
    asyncio.run(main())