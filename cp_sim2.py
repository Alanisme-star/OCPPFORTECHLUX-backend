import asyncio
import math
import random
import signal
import sys
import time
from datetime import datetime, UTC
from urllib.parse import quote

import websockets
from ocpp.routing import on
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action
from ocpp.v16 import ChargePoint as OcppChargePoint


def iso_now() -> str:
    return datetime.now(UTC).isoformat()


class SimChargePoint(OcppChargePoint):
    def __init__(self, charge_point_id, connection):
        super().__init__(charge_point_id, connection)
        self._running = True
        self.current_txn_id: int | None = None
        self.latest_energy_kwh = 0.0  # <== 儲存最新 kWh 供 StopTransaction 使用

    # ⚡⚡ 修正：使用套件實際定義的小寫 Action 名稱 + 正確的回傳 Payload
    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        print(f"[SIM] 🛑 收到遠端停充指令 | transaction_id={transaction_id}")

        # 1️⃣ 停止內部的充電模擬迴圈
        self._running = False

        # 2️⃣ 主動送出 StopTransaction
        await self._send_stop_transaction(reason="Remote")

        # 3️⃣ 回覆後端「已接受」停充指令
        return call_result.RemoteStopTransactionPayload(status="Accepted")


    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        return call_result.Heartbeat(current_time=iso_now())

    async def _send_boot(self):
        print("[SIM] → BootNotification")
        res = await self.call(call.BootNotification(
            charge_point_model="MSI-Sim-Model",
            charge_point_vendor="MSI"
        ))
        print("[SIM] BootNotification result:", res)

    async def _send_status(self, status="Available", connector_id=1):
        print(f"[SIM] → StatusNotification: {status}")
        await self.call(call.StatusNotification(
            connector_id=connector_id,
            error_code="NoError",
            status=status,
            timestamp=iso_now()
        ))

    async def _send_authorize(self, id_tag: str):
        print(f"[SIM] → Authorize: {id_tag}")
        res = await self.call(call.Authorize(id_tag=id_tag))
        print("[SIM] Authorize result:", res)

    async def _send_start_transaction(self, id_tag: str, connector_id=1, meter_start_wh=0):
        local_txn_id = int(time.time() * 1000)
        print(f"[SIM] → StartTransaction (local txn={local_txn_id})")
        res = await self.call(call.StartTransaction(
            connector_id=connector_id,
            id_tag=id_tag,
            meter_start=meter_start_wh,
            timestamp=iso_now()
        ))
        self.current_txn_id = int(res.transaction_id) if getattr(res, "transaction_id", 0) else local_txn_id
        print("[SIM] StartTransaction result:", res)

    async def _send_meter_values(self, connector_id=1, power_w=0.0, voltage_v=230.0, current_a=None, energy_kwh=0.0):
        self.latest_energy_kwh = energy_kwh  # <== 儲存最新的 kWh

        if current_a is None:
            current_a = (power_w / voltage_v) if voltage_v > 0 else 0.0

        mv = {
            "timestamp": iso_now(),
            "sampledValue": [
                {"value": f"{power_w:.2f}", "measurand": "Power.Active.Import", "unit": "W"},
                {"value": f"{voltage_v:.1f}", "measurand": "Voltage", "unit": "V"},
                {"value": f"{current_a:.2f}", "measurand": "Current.Import", "unit": "A"},
                {"value": f"{energy_kwh:.4f}", "measurand": "Energy.Active.Import.Register", "unit": "kWh"},
                {"value": f"{(power_w / 3600000.0):.6f}", "measurand": "Energy.Active.Import.Interval", "unit": "kWh"},
            ],
        }

        kwargs = dict(connector_id=connector_id, meter_value=[mv])
        if isinstance(self.current_txn_id, int):
            kwargs["transaction_id"] = self.current_txn_id

        await self.call(call.MeterValues(**kwargs))

        # Debug log
        print(f"[SIM] MeterValues Sent | txn={self.current_txn_id}")
        for sv in mv["sampledValue"]:
            print(f"  - {sv['measurand']}: {sv['value']} {sv.get('unit', '')}")

    async def _send_stop_transaction(self, reason="Local"):
        if not isinstance(self.current_txn_id, int):
            return
        print(f"[SIM] → StopTransaction (txn={self.current_txn_id})")
        await self.call(call.StopTransaction(
            transaction_id=self.current_txn_id,
            meter_stop=int(self.latest_energy_kwh * 1000),  # 回到 Wh 單位
            timestamp=iso_now(),
            reason=reason
        ))
        self.current_txn_id = None

    async def scenario_charge(self, id_tag: str, seconds: int = 60):  # ⭐ 預設改為 60 秒（1 分鐘）
        await self._send_boot()
        await self._send_status("Available")
        await self._send_authorize(id_tag)
        await self._send_status("Preparing")
        await self._send_start_transaction(id_tag=id_tag, meter_start_wh=0)
        await self._send_status("Charging")

        energy_kwh = 0.0
        base_power_w = 3200.0
        voltage_v = 230.0
        t0 = time.time()

        while self._running and (time.time() - t0) < seconds:
            t = time.time() - t0
            power_w = base_power_w * (0.85 + 0.15 * (1 + math.sin(2 * math.pi * t / 12.0)) / 2.0)
            power_w += random.uniform(-50, 50)
            power_w = max(0.0, power_w)
            energy_kwh += power_w / 3600000.0  # W → kWh（注意換算）

            await self._send_meter_values(
                power_w=power_w,
                voltage_v=voltage_v,
                energy_kwh=max(0.0, energy_kwh)
            )
            await asyncio.sleep(1.0)

        await self._send_status("Finishing")
        await self._send_stop_transaction(reason="Local")
        await self._send_status("Available")


async def run_simulator(backend_base_url: str, charge_point_id: str, id_tag: str, duration_sec: int = 60):  # ⭐ 統一改為 60
    ws_url = backend_base_url.rstrip("/") + "/" + quote(charge_point_id, safe="")
    print(f"[SIM] Connecting to {ws_url}")

    async with websockets.connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(charge_point_id, ws)
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: setattr(cp, "_running", False))
            except NotImplementedError:
                pass
        await asyncio.gather(
            cp.start(),
            cp.scenario_charge(id_tag=id_tag, seconds=duration_sec)
        )


def main():
    backend = "wss://ocppfortechlux-backend.onrender.com"
    cpid = "TW*MSI*E000100"
    idtag = "6678B3EB"
    duration = 900  # ⭐ 預設改為 900 秒（15 分鐘）

    if len(sys.argv) >= 2:
        backend = sys.argv[1]
    if len(sys.argv) >= 3:
        cpid = sys.argv[2]
    if len(sys.argv) >= 4:
        idtag = sys.argv[3]
    if len(sys.argv) >= 5:
        duration = int(sys.argv[4])

    if backend.startswith("https://"):
        backend = "wss://" + backend[len("https://"):]
    elif backend.startswith("http://"):
        backend = "ws://" + backend[len("http://"):]

    asyncio.run(run_simulator(backend, cpid, idtag, duration))


if __name__ == "__main__":
    main()
