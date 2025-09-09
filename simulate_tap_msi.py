import asyncio
import json
import uuid
import argparse
from datetime import datetime, timezone

import websockets

# OCPP 1.6 message type ids
CALL = 2
CALL_RESULT = 3

def ocpp_now():
    return datetime.now(timezone.utc).isoformat()

async def send(ws, action, payload):
    """Send an OCPP CALL frame and wait for the matching CALLRESULT."""
    msg_id = str(uuid.uuid4())
    frame = [CALL, msg_id, action, payload]
    await ws.send(json.dumps(frame))
    reply_raw = await ws.recv()
    try:
        reply = json.loads(reply_raw)
    except Exception as e:
        raise RuntimeError(f"Non-JSON response for {action}: {reply_raw}") from e

    if not (isinstance(reply, list) and len(reply) >= 3 and reply[0] == CALL_RESULT and reply[1] == msg_id):
        raise RuntimeError(f"Unexpected OCPP response for {action}: {reply}")

    return reply[2]  # payload

async def boot_sequence(ws, cp_id: str, vendor: str, model: str):
    # BootNotification
    boot_conf = await send(ws, "BootNotification", {
        "chargePointVendor": vendor,
        "chargePointModel": model,
        "chargePointSerialNumber": cp_id,
        "firmwareVersion": "sim-0.1"
    })
    print("[BootNotification.conf]", boot_conf)

    # 樁狀態：Available
    stat_conf = await send(ws, "StatusNotification", {
        "connectorId": 0,
        "errorCode": "NoError",
        "status": "Available",
        "timestamp": ocpp_now()
    })
    print("[StatusNotification.conf]", stat_conf)

async def heartbeat_task(ws, interval_sec: int = 30):
    try:
        while True:
            await asyncio.sleep(interval_sec)
            hb_conf = await send(ws, "Heartbeat", {})
            print("[Heartbeat.conf]", hb_conf)
    except asyncio.CancelledError:
        return

async def simulate_tap(ws, connector_id: int, id_tag: str, kwh_total: float, only_auth: bool):
    # 1) Authorize
    auth_conf = await send(ws, "Authorize", {"idTag": id_tag})
    print(f"[Authorize.conf] idTag={id_tag} ->", auth_conf)
    status = (auth_conf.get("idTagInfo") or {}).get("status")

    if status != "Accepted":
        print(f"Authorize not accepted (status={status}). Stop here.")
        return

    if only_auth:
        print("Only Authorize was requested; skipping StartTransaction.")
        return

    # 2) StartTransaction
    start_conf = await send(ws, "StartTransaction", {
        "connectorId": connector_id,
        "idTag": id_tag,
        "meterStart": 0,                 # Wh 起始值
        "timestamp": ocpp_now()
    })
    print("[StartTransaction.conf]", start_conf)
    tx_id = start_conf.get("transactionId", 1)

    # 3) 模擬送 4 筆 MeterValues（累加 Wh）
    steps = 4
    for i in range(1, steps + 1):
        await asyncio.sleep(2)
        wh = int((kwh_total * 1000.0) * i / steps)
        mv_conf = await send(ws, "MeterValues", {
            "connectorId": connector_id,
            "transactionId": tx_id,
            "meterValue": [{
                "timestamp": ocpp_now(),
                "sampledValue": [{
                    "value": str(wh),
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh"
                }]
            }]
        })
        print(f"[MeterValues.conf] step {i}/{steps}", mv_conf)

    # 4) StopTransaction
    stop_conf = await send(ws, "StopTransaction", {
        "transactionId": tx_id,
        "idTag": id_tag,
        "meterStop": int(kwh_total * 1000.0),  # Wh 終值
        "timestamp": ocpp_now(),
        "reason": "Local"
    })
    print("[StopTransaction.conf]", stop_conf)

async def main():
    parser = argparse.ArgumentParser(description="OCPP 1.6 CP simulator for MSI charger & card tap.")
    parser.add_argument("--backend", default="ocppfortechlux-backend.onrender.com", help="Backend host (no scheme).")
    parser.add_argument("--cp-id", default="TW*MSI*E000100", help="Charge point ID (path segment).")
    parser.add_argument("--connector-id", type=int, default=1, help="Connector to use for transaction.")
    parser.add_argument("--id-tag", default="6678B3EB", help="RFID/Card idTag to authorize.")
    parser.add_argument("--kwh", type=float, default=0.20, help="Total kWh to simulate before StopTransaction.")
    parser.add_argument("--only-auth", action="store_true", help="Only do Authorize; skip Start/Stop/MeterValues.")
    parser.add_argument("--no-heartbeat", action="store_true", help="Disable periodic Heartbeat.")
    parser.add_argument("--vendor", default="TechLux-Sim", help="chargePointVendor in BootNotification.")
    parser.add_argument("--model", default="MSI-Sim-1", help="chargePointModel in BootNotification.")
    args = parser.parse_args()

    ws_url = f"wss://{args.backend}/ocpp/{args.cp-id if False else ''}"
    # 上面一行只為了避免靜態掃描誤報，實際使用下行：
    ws_url = f"wss://{args.backend}/ocpp/{args.cp_id}"

    print("=== OCPP SIM START ===")
    print(f"WS URL      : {ws_url}")
    print(f"CP ID       : {args.cp_id}")
    print(f"Connector   : {args.connector_id}")
    print(f"idTag       : {args.id_tag}")
    print(f"Total kWh   : {args.kwh} (only_auth={args.only_auth})")
    print("======================")

    # 必須協商子協議 ocpp1.6
    async with websockets.connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        # Boot & Available
        await boot_sequence(ws, cp_id=args.cp_id, vendor=args.vendor, model=args.model)

        hb_task = None
        if not args.no_heartbeat:
            hb_task = asyncio.create_task(heartbeat_task(ws))

        try:
            await simulate_tap(
                ws,
                connector_id=args.connector_id,
                id_tag=args.id_tag,
                kwh_total=args.kwh,
                only_auth=args.only_auth
            )
        finally:
            if hb_task:
                hb_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
