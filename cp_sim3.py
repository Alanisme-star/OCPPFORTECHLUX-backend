import asyncio
import argparse
import json
import logging
from datetime import datetime, timezone
from urllib import request, error

import websockets
from ocpp.v16 import ChargePoint as CP
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action
from ocpp.v16.call import MeterValues
from ocpp.routing import on


# =====================
# 基本設定
# =====================
DEFAULT_VOLTAGE = 220.0
DEFAULT_PHASES = 1
DEFAULT_ID_TAG = "6678B3EB"
DEFAULT_TEST_BALANCE = 1000.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def ws_to_http_base(ws_url: str) -> str:
    if ws_url.startswith("wss://"):
        scheme = "https://"
        rest = ws_url[len("wss://"):]
    elif ws_url.startswith("ws://"):
        scheme = "http://"
        rest = ws_url[len("ws://"):]
    else:
        scheme = "https://"
        rest = ws_url

    host = rest.split("/", 1)[0]
    return scheme + host


def http_get_json(url: str, timeout: int = 10):
    req = request.Request(url, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="ignore")
            return json.loads(data) if data else None
    except Exception as e:
        logging.warning(f"[HTTP][GET][FAIL] url={url} err={e}")
        return None


def http_post_json(url: str, payload: dict, timeout: int = 10):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="ignore")
            return json.loads(data) if data else {}
    except error.HTTPError as e:
        try:
            msg = e.read().decode("utf-8", errors="ignore")
        except Exception:
            msg = str(e)
        logging.warning(f"[HTTP][POST][HTTPError] url={url} code={e.code} body={msg}")
        return None
    except Exception as e:
        logging.warning(f"[HTTP][POST][FAIL] url={url} err={e}")
        return None


# =====================
# 🧪 自動建卡＋補餘額（測試用）
# =====================
def ensure_test_card(http_base: str, *, id_tag: str, min_balance: float):
    """
    測試模式：
    - 確保 cards 表中有此 id_tag
    - 若餘額不足，自動補到 min_balance
    """

    # 1️⃣ 嘗試查卡片（你系統目前沒有 GET API，直接用 INSERT/UPDATE）
    logging.warning(
        f"[SIM][CARD][AUTO] ensure card idTag={id_tag} balance>={min_balance}"
    )

    # 👉 使用 SQLite INSERT OR IGNORE 對應的 API（你後端已存在）
    # 這個 endpoint 你已經有：
    #   cards(card_id, balance)

    # 2️⃣ 建卡（若不存在）
    payload = {
        "card_id": id_tag,
        "balance": float(min_balance),
    }

    # ⚠️ 這裡假設你有一個 debug / cards API
    # 若你沒有，我下面有「備用方案」
    res = http_post_json(f"{http_base}/api/debug/force-add-card", payload)

    if res:
        logging.info(f"[SIM][CARD] ensured card {id_tag} balance={min_balance}")
    else:
        logging.warning(
            "[SIM][CARD] force-add-card API not available, fallback to update"
        )

        # 3️⃣ 補餘額（即使存在也安全）
        http_post_json(
            f"{http_base}/api/debug/topup-card",
            {"card_id": id_tag, "balance": float(min_balance)},
        )


# =====================
# ChargePoint 模擬器
# =====================
class SimChargePoint(CP):
    def __init__(self, cp_id, ws):
        super().__init__(cp_id, ws)

        self.tx_map = {}
        self.tx_tasks = {}
        self.current_limit_a = {}
        self.energy_wh_float = {}
        self.energy_wh_int = {}

    async def send_boot(self):
        logging.info(f"[SIM][{self.id}] → BootNotification")
        await self.call(
            call.BootNotification(
                charge_point_model="SimCP",
                charge_point_vendor="TechLux-SC-Test",
            )
        )

    async def send_status(self, connector_id, status):
        await self.call(
            call.StatusNotification(
                connector_id=int(connector_id),
                error_code="NoError",
                status=status,
                timestamp=utc_now(),
            )
        )

    async def send_authorize(self, id_tag: str):
        logging.info(f"[SIM][{self.id}] → Authorize idTag={id_tag}")
        res = await self.call(call.Authorize(id_tag=id_tag))

        status = (
            res.id_tag_info.get("status")
            if res and getattr(res, "id_tag_info", None)
            else None
        )

        logging.info(f"[SIM][{self.id}] ← Authorize result={status}")

        if status != "Accepted":
            raise RuntimeError(f"Authorize rejected: {status}")

    @on(Action.set_charging_profile)
    async def on_set_charging_profile(self, connectorId, csChargingProfiles, **kwargs):
        limit = (
            csChargingProfiles
            .get("chargingSchedule", {})
            .get("chargingSchedulePeriod", [{}])[0]
            .get("limit", 0)
        )

        try:
            limit = float(limit)
        except Exception:
            limit = 0.0

        self.current_limit_a[int(connectorId)] = limit

        logging.info(
            f"[SIM][{self.id}] ⚡ SetChargingProfile connector={connectorId} limit={limit}A"
        )

        return call_result.SetChargingProfile(status="Accepted")

    async def start_tx(self, connector_id: int, id_tag: str):
        logging.info(f"[SIM][{self.id}] → StartTransaction connector={connector_id}")

        res = await self.call(
            call.StartTransaction(
                connector_id=int(connector_id),
                id_tag=str(id_tag),
                meter_start=0,
                timestamp=utc_now(),
            )
        )

        tx_id = getattr(res, "transaction_id", 0) or 0
        if not tx_id:
            st = res.id_tag_info.get("status") if res and res.id_tag_info else None
            logging.warning(
                f"[SIM][{self.id}] ❌ StartTransaction rejected connector={connector_id} status={st}"
            )
            return False

        self.tx_map[connector_id] = tx_id
        self.current_limit_a.setdefault(connector_id, 0.0)
        self.energy_wh_float.setdefault(connector_id, 0.0)
        self.energy_wh_int.setdefault(connector_id, 0)

        await self.send_status(connector_id, "Charging")

        task = asyncio.create_task(self.meter_loop(connector_id, tx_id))
        self.tx_tasks[connector_id] = task

        logging.info(f"[SIM][{self.id}] ✅ STARTED connector={connector_id} tx_id={tx_id}")
        return True

    async def meter_loop(self, connector_id: int, tx_id: int):
        while connector_id in self.tx_map:
            limit_a = float(self.current_limit_a.get(connector_id, 0.0))
            if limit_a <= 0:
                await asyncio.sleep(1)
                continue

            voltage = DEFAULT_VOLTAGE
            current = limit_a
            power_w = voltage * current * DEFAULT_PHASES

            self.energy_wh_float[connector_id] += power_w / 3600.0
            now = int(self.energy_wh_float[connector_id])
            self.energy_wh_int[connector_id] = max(
                self.energy_wh_int[connector_id], now
            )

            await self.call(
                MeterValues(
                    connector_id=connector_id,
                    transaction_id=tx_id,
                    meter_value=[{
                        "timestamp": utc_now(),
                        "sampledValue": [
                            {"measurand": "Voltage", "value": str(voltage), "unit": "V"},
                            {"measurand": "Current.Import", "value": str(current), "unit": "A"},
                            {"measurand": "Power.Active.Import", "value": str(power_w), "unit": "W"},
                            {"measurand": "Energy.Active.Import.Register", "value": str(now), "unit": "Wh"},
                        ],
                    }],
                )
            )

            await asyncio.sleep(1)


# =====================
# Main
# =====================
async def main(ws_url, cars, interval, id_tag, auto_card, card_balance):
    cp_id = ws_url.rstrip("/").split("/")[-1]
    http_base = ws_to_http_base(ws_url)

    logging.info(f"[SIM] Connecting: {ws_url}")
    logging.info(f"[SIM] HTTP base: {http_base}")
    logging.info(f"[SIM] idTag={id_tag} cars={cars} interval={interval}s")

    if auto_card:
        await asyncio.to_thread(
            ensure_test_card,
            http_base,
            id_tag=id_tag,
            min_balance=card_balance,
        )

    async with websockets.connect(
        ws_url,
        subprotocols=["ocpp1.6"],
        ping_interval=20,
        ping_timeout=20,
    ) as ws:
        cp = SimChargePoint(cp_id, ws)
        asyncio.create_task(cp.start())

        await cp.send_boot()
        await cp.send_status(0, "Available")
        await cp.send_authorize(id_tag)

        for i in range(1, cars + 1):
            await cp.send_status(i, "Available")
            await asyncio.sleep(0.3)
            await cp.start_tx(i, id_tag)
            await asyncio.sleep(interval)

        while True:
            await asyncio.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ws", required=True)
    parser.add_argument("--cars", type=int, default=4)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--idtag", type=str, default=DEFAULT_ID_TAG)

    # 🧪 測試模式
    parser.add_argument("--auto-card", action="store_true")
    parser.add_argument("--card-balance", type=float, default=DEFAULT_TEST_BALANCE)

    args = parser.parse_args()

    asyncio.run(
        main(
            args.ws,
            args.cars,
            args.interval,
            args.idtag,
            args.auto_card,
            args.card_balance,
        )
    )
