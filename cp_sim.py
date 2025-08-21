#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCPP 1.6 Charge Point Simulator (Power/Voltage/Current/Energy)
--------------------------------------------------------------
用來模擬充電樁，會送 BootNotification → StartTransaction →
持續推送 MeterValues → StopTransaction
"""

import asyncio
import math
import random
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
import argparse
import logging

import websockets
from ocpp.routing import on
from ocpp.v16 import ChargePoint as _ChargePoint
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action, RegistrationStatus


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class SimConfig:
    url: str
    cp_id: str
    period: float = 2.0
    duration: float = 0.0
    phases: int = 1
    base_power_kw: float = 3.0
    voltage_v: float = 230.0
    power_variation: float = 0.15
    noise: float = 0.03
    id_tag: str = "TEST1234"


class SimChargePoint(_ChargePoint):
    def __init__(self, cp_id: str, websocket, config: SimConfig):
        super().__init__(cp_id, websocket)
        self.cfg = config
        self._tx_id = None
        self._running = True
        self._energy_kwh = 0.0

    # --- handlers ---
    @on(Action.reset)
    async def on_reset(self, **kwargs):
        logging.info(f"[CP] Received Reset: {kwargs}")
        return call_result.ResetPayload(status="Accepted")

    @on(Action.remote_start_transaction)
    async def on_remote_start(self, **kwargs):
        logging.info(f"[CP] Received RemoteStartTransaction: {kwargs}")
        return call_result.RemoteStartTransactionPayload(status="Accepted")

    @on(Action.remote_stop_transaction)
    async def on_remote_stop(self, **kwargs):
        logging.info(f"[CP] Received RemoteStopTransaction: {kwargs}")
        return call_result.RemoteStopTransactionPayload(status="Accepted")

    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        return call_result.HeartbeatPayload(current_time=utc_now_iso())

    # --- outgoing messages ---
    async def send_boot(self):
        req = call.BootNotificationPayload(
            charge_point_model="SIM-CP",
            charge_point_vendor="ChatGPT-Lab"
        )
        res = await self.call(req)
        logging.info(f"[CP] BootNotification result: {res}")
        return res

    async def start_transaction(self):
        req = call.StartTransactionPayload(
            connector_id=1,
            id_tag=self.cfg.id_tag,
            meter_start=int(self._energy_kwh * 1000),
            timestamp=utc_now_iso()
        )
        res = await self.call(req)
        self._tx_id = getattr(res, "transaction_id", None)
        logging.info(f"[CP] StartTransaction -> tx_id={self._tx_id}")
        return res

    async def stop_transaction(self, reason: str = "Local"):
        if self._tx_id is None:
            return
        req = call.StopTransactionPayload(
            transaction_id=self._tx_id,
            meter_stop=int(self._energy_kwh * 1000),
            timestamp=utc_now_iso(),
            reason=reason
        )
        res = await self.call(req)
        logging.info(f"[CP] StopTransaction result: {res}")
        self._tx_id = None
        return res

    async def meter_values_loop(self):
        i = 0
        while self._running:
            i += 1
            ts = utc_now_iso()

            # 模擬功率波動
            base = self.cfg.base_power_kw
            span = base * self.cfg.power_variation
            wave = math.sin(i * self.cfg.period / 6.0)
            power_kw = max(0.0, base + wave * span + random.uniform(-span * self.cfg.noise, span * self.cfg.noise))

            voltage = self.cfg.voltage_v
            if self.cfg.phases == 1:
                current_a = (power_kw * 1000.0) / max(1.0, voltage)
            else:
                current_a = (power_kw * 1000.0) / max(1.0, (3.0 * voltage))

            self._energy_kwh += power_kw * (self.cfg.period / 3600.0)

            sampled = [
                {"measurand": "Power.Active.Import", "unit": "kW", "value": f"{power_kw:.3f}"},
                {"measurand": "Voltage", "unit": "V", "value": f"{voltage:.0f}"},
                {"measurand": "Current.Import", "unit": "A", "value": f"{current_a:.2f}"},
                {"measurand": "Energy.Active.Import.Register", "unit": "kWh", "value": f"{self._energy_kwh:.3f}"},
            ]

            mv = call.MeterValuesPayload(
                connector_id=1,
                transaction_id=self._tx_id,
                meter_value=[{"timestamp": ts, "sampledValue": sampled}]
            )
            await self.call(mv)
            logging.debug(f"[CP] MeterValues: {sampled}")

            await asyncio.sleep(self.cfg.period)

    def stop(self):
        self._running = False


async def run_sim(cfg: SimConfig):
    logging.info(f"[SIM] Connecting to {cfg.url} as {cfg.cp_id} ...")
    async with websockets.connect(cfg.url, subprotocols=["ocpp1.6"]) as ws:
        cp = SimChargePoint(cfg.cp_id, ws, cfg)

        handler_task = asyncio.create_task(cp.start())
        await cp.send_boot()
        await cp.start_transaction()

        meter_task = asyncio.create_task(cp.meter_values_loop())

        if cfg.duration > 0:
            await asyncio.sleep(cfg.duration)
            cp.stop()

        while cp._running:
            await asyncio.sleep(0.2)

        await cp.stop_transaction()
        meter_task.cancel()
        handler_task.cancel()

    logging.info("[SIM] Disconnected.")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", type=str, required=True)
    p.add_argument("--cp-id", type=str, required=True)
    p.add_argument("--period", type=float, default=2.0)
    p.add_argument("--duration", type=float, default=0.0)
    p.add_argument("--phases", type=int, choices=[1, 3], default=1)
    p.add_argument("--base-power-kw", type=float, default=3.0)
    p.add_argument("--voltage-v", type=float, default=230.0)
    p.add_argument("--power-variation", type=float, default=0.15)
    p.add_argument("--noise", type=float, default=0.03)
    p.add_argument("--id-tag", type=str, default="TEST1234")
    p.add_argument("--log-level", type=str, default="INFO")
    args = p.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    return SimConfig(
        url=args.url,
        cp_id=args.cp_id,
        period=args.period,
        duration=args.duration,
        phases=args.phases,
        base_power_kw=args.base_power_kw,
        voltage_v=args.voltage_v,
        power_variation=args.power_variation,
        noise=args.noise,
        id_tag=args.id_tag,
    )


if __name__ == "__main__":
    cfg = parse_args()
    try:
        asyncio.run(run_sim(cfg))
    except KeyboardInterrupt:
        pass
