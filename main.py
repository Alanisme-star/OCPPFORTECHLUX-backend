import os
import logging
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from websockets.exceptions import ConnectionClosedOK

from ocpp.routing import on
from ocpp.v16 import ChargePoint as OcppChargePoint
from ocpp.v16 import call_result
from ocpp.v16.enums import Action, RegistrationStatus


# =========================================================
# 基本設定
# =========================================================
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("minimal_ocpp")

app = FastAPI(title="Minimal OCPP 1.6 Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# =========================================================
# 你的專案資訊（註解用 / debug 用）
# =========================================================
PROJECT_NAME = "MSI充電樁"
DEFAULT_CP_ID = "TW*MSI*E000100"
DEFAULT_TEST_ID_TAG = "6678B3EB"
BACKEND_URL = "https://ocppfortechlux-backend.onrender.com"

# =========================================================
# 卡片設定
# 需求：卡片全部接受
# =========================================================
ALLOW_ALL_ID_TAGS = True

# 若未來改成只接受指定卡號，再把上面改成 False
ALLOWED_ID_TAGS = {
    "6678B3EB",
}

# =========================================================
# 充電樁白名單
# 需求：不限制白名單充電樁
# 空集合 = 全部允許
# =========================================================
ALLOWED_CP_IDS = set()

# =========================================================
# 記憶體狀態
# =========================================================
connected_charge_points = {}
charging_point_status = {}
transaction_seq = 1000
active_transactions = {}


# =========================================================
# 工具函式
# =========================================================
def utc_now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def normalize_cp_id(cp_id: str) -> str:
    return (cp_id or "").strip().lstrip("/")


def next_transaction_id() -> int:
    global transaction_seq
    transaction_seq += 1
    return transaction_seq


def is_cp_allowed(cp_id: str) -> bool:
    if not ALLOWED_CP_IDS:
        return True
    return cp_id in ALLOWED_CP_IDS


def is_id_tag_accepted(id_tag: str) -> bool:
    if ALLOW_ALL_ID_TAGS:
        return True
    return id_tag in ALLOWED_ID_TAGS


# =========================================================
# FastAPI WebSocket Adapter
# =========================================================
class FastAPIWebSocketAdapter:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    async def recv(self):
        return await self.websocket.receive_text()

    async def send(self, data):
        try:
            await self.websocket.send_text(data)
        except (WebSocketDisconnect, ConnectionClosedOK):
            raise
        except RuntimeError as e:
            if "websocket.send" in str(e) or "websocket.close" in str(e):
                raise WebSocketDisconnect(code=1006)
            raise

    @property
    def subprotocol(self):
        return self.websocket.headers.get("sec-websocket-protocol") or "ocpp1.6"


# =========================================================
# OCPP ChargePoint
# =========================================================
class ChargePoint(OcppChargePoint):
    def __init__(self, cp_id: str, connection):
        super().__init__(cp_id, connection)
        self.id = cp_id

    @on(Action.boot_notification)
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        logger.warning(
            f"[BOOT] cp_id={self.id} | vendor={charge_point_vendor} | model={charge_point_model}"
        )

        charging_point_status[self.id] = {
            "status": "Booted",
            "timestamp": utc_now_iso(),
            "vendor": charge_point_vendor,
            "model": charge_point_model,
            "project_name": PROJECT_NAME,
        }

        return call_result.BootNotification(
            current_time=utc_now_iso(),
            interval=30,
            status=RegistrationStatus.accepted,
        )

    @on(Action.authorize)
    async def on_authorize(self, id_tag, **kwargs):
        accepted = is_id_tag_accepted(id_tag)
        status = "Accepted" if accepted else "Invalid"

        logger.warning(
            f"[AUTHORIZE] cp_id={self.id} | id_tag={id_tag} | result={status}"
        )

        charging_point_status[self.id] = {
            "status": "Authorized" if accepted else "AuthRejected",
            "timestamp": utc_now_iso(),
            "id_tag": id_tag,
            "authorize_result": status,
            "project_name": PROJECT_NAME,
        }

        return call_result.Authorize(
            id_tag_info={
                "status": status
            }
        )

    @on(Action.status_notification)
    async def on_status_notification(
        self,
        connector_id,
        error_code,
        status,
        timestamp=None,
        info=None,
        vendor_id=None,
        vendor_error_code=None,
        **kwargs,
    ):
        logger.warning(
            f"[STATUS] cp_id={self.id} | connector_id={connector_id} | status={status} | error_code={error_code}"
        )

        charging_point_status[self.id] = {
            "status": status,
            "connector_id": connector_id,
            "error_code": error_code,
            "timestamp": timestamp or utc_now_iso(),
            "info": info,
            "vendor_id": vendor_id,
            "vendor_error_code": vendor_error_code,
            "project_name": PROJECT_NAME,
        }

        return call_result.StatusNotification()

    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        logger.warning(f"[HEARTBEAT] cp_id={self.id}")

        return call_result.Heartbeat(
            current_time=utc_now_iso()
        )

    @on(Action.start_transaction)
    async def on_start_transaction(
        self,
        connector_id,
        id_tag,
        meter_start,
        timestamp,
        reservation_id=None,
        **kwargs,
    ):
        accepted = is_id_tag_accepted(id_tag)
        tx_id = next_transaction_id()

        if accepted:
            active_transactions[self.id] = tx_id
            status = "Accepted"
        else:
            status = "Invalid"

        logger.warning(
            f"[START_TX] cp_id={self.id} | connector_id={connector_id} | id_tag={id_tag} | tx_id={tx_id} | result={status}"
        )

        charging_point_status[self.id] = {
            "status": "Charging" if accepted else "StartTxRejected",
            "connector_id": connector_id,
            "id_tag": id_tag,
            "transaction_id": tx_id,
            "meter_start": meter_start,
            "timestamp": timestamp or utc_now_iso(),
            "start_tx_result": status,
            "project_name": PROJECT_NAME,
        }

        return call_result.StartTransaction(
            transaction_id=tx_id,
            id_tag_info={
                "status": status
            }
        )

    @on(Action.stop_transaction)
    async def on_stop_transaction(
        self,
        meter_stop,
        timestamp,
        transaction_id,
        reason=None,
        id_tag=None,
        **kwargs,
    ):
        logger.warning(
            f"[STOP_TX] cp_id={self.id} | tx_id={transaction_id} | meter_stop={meter_stop} | reason={reason}"
        )

        if self.id in active_transactions:
            active_transactions.pop(self.id, None)

        charging_point_status[self.id] = {
            "status": "Available",
            "transaction_id": transaction_id,
            "meter_stop": meter_stop,
            "timestamp": timestamp or utc_now_iso(),
            "reason": reason,
            "id_tag": id_tag,
            "project_name": PROJECT_NAME,
        }

        return call_result.StopTransaction(
            id_tag_info={
                "status": "Accepted"
            }
        )

    @on(Action.meter_values)
    async def on_meter_values(self, connector_id, meter_value, transaction_id=None, **kwargs):
        logger.warning(
            f"[METER] cp_id={self.id} | connector_id={connector_id} | tx_id={transaction_id}"
        )
        return call_result.MeterValues()

    @on(Action.data_transfer)
    async def on_data_transfer(self, vendor_id, message_id=None, data=None, **kwargs):
        logger.warning(
            f"[DATA_TRANSFER] cp_id={self.id} | vendor_id={vendor_id} | message_id={message_id}"
        )
        return call_result.DataTransfer(
            status="Accepted"
        )


# =========================================================
# WebSocket 入口
# =========================================================
@app.websocket("/{charge_point_id:path}")
async def websocket_endpoint(websocket: WebSocket, charge_point_id: str):
    cp_id = normalize_cp_id(charge_point_id)

    if not is_cp_allowed(cp_id):
        logger.warning(f"[WS][REJECT] cp_id={cp_id} | reason=not_in_whitelist")
        await websocket.close(code=1008)
        return

    await websocket.accept(subprotocol="ocpp1.6")
    logger.warning(f"[WS][ACCEPT] cp_id={cp_id}")

    cp = ChargePoint(cp_id, FastAPIWebSocketAdapter(websocket))
    connected_charge_points[cp_id] = cp

    charging_point_status[cp_id] = {
        "status": "Connected",
        "timestamp": utc_now_iso(),
        "project_name": PROJECT_NAME,
    }

    try:
        await cp.start()

    except WebSocketDisconnect:
        logger.warning(f"[WS][DISCONNECTED] cp_id={cp_id}")

    except Exception as e:
        logger.exception(f"[WS][ERROR] cp_id={cp_id} | err={e}")

    finally:
        connected_charge_points.pop(cp_id, None)

        last_status = charging_point_status.get(cp_id, {}) or {}
        charging_point_status[cp_id] = {
            **last_status,
            "status": "Disconnected",
            "timestamp": utc_now_iso(),
            "project_name": PROJECT_NAME,
        }

        logger.warning(f"[WS][FINALLY] cp_id={cp_id}")


# =========================================================
# 簡易 HTTP API（debug 用）
# =========================================================
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "minimal_ocpp_backend",
        "project_name": PROJECT_NAME,
        "backend_url": BACKEND_URL,
        "default_cp_id": DEFAULT_CP_ID,
        "default_test_id_tag": DEFAULT_TEST_ID_TAG,
        "message": "OCPP 1.6 minimal backend is running",
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": utc_now_iso(),
        "connected_count": len(connected_charge_points),
        "project_name": PROJECT_NAME,
    }


@app.get("/api/connections")
def get_connections():
    return {
        "count": len(connected_charge_points),
        "charge_points": list(connected_charge_points.keys()),
        "project_name": PROJECT_NAME,
    }


@app.get("/api/status")
def get_all_status():
    return charging_point_status


@app.get("/api/status/{cp_id}")
def get_status(cp_id: str):
    cp_norm = normalize_cp_id(cp_id)
    return charging_point_status.get(cp_norm, {})


@app.get("/api/config")
def get_config():
    return {
        "project_name": PROJECT_NAME,
        "backend_url": BACKEND_URL,
        "allow_all_id_tags": ALLOW_ALL_ID_TAGS,
        "allowed_id_tags": sorted(ALLOWED_ID_TAGS),
        "allowed_cp_ids": sorted(ALLOWED_CP_IDS),
        "default_cp_id": DEFAULT_CP_ID,
        "default_test_id_tag": DEFAULT_TEST_ID_TAG,
    }


# =========================================================
# 啟動
# =========================================================
if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)