
import asyncio
import argparse
import json
import uuid
import urllib.parse
from datetime import datetime, timezone

import websockets

def build_ws_url(base: str, cp_id: str, token: str | None):
    # normalize base http(s) -> ws(s)
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    cp_enc = urllib.parse.quote(cp_id, safe="")
    url = f"{base.rstrip('/')}/ocpp/{cp_enc}"
    if token:
        url += f"?token={urllib.parse.quote(token)}"
    return url

def ocpp_frame(action: str, payload: dict) -> str:
    uid = str(uuid.uuid4())
    frame = [2, uid, action, payload]
    return uid, json.dumps(frame)

async def send_and_wait(ws, action: str, payload: dict, verbose=True):
    uid, frame = ocpp_frame(action, payload)
    if verbose:
        print(f"-> {action}:", frame)
    await ws.send(frame)
    # wait for either CallResult [3, uid, payload] or CallError [4, uid, code, desc, details]
    while True:
        msg = await ws.recv()
        if verbose:
            print("<- raw:", msg)
        try:
            data = json.loads(msg)
        except Exception:
            print("(!) non-JSON frame:", msg)
            continue
        if isinstance(data, list) and len(data) >= 3 and data[1] == uid:
            if data[0] == 3:
                print(f"<- {action}.conf:", json.dumps(data[2]))
                return data[2]
            elif data[0] == 4:
                code = data[2] if len(data) > 2 else "Unknown"
                desc = data[3] if len(data) > 3 else ""
                details = data[4] if len(data) > 4 else {}
                raise RuntimeError(f"{action} CallError: {code} - {desc} - {details}")
        # otherwise keep listening (server may send other messages with different uid)

async def run_once(url: str, status: str, vendor: str, model: str, verbose=True):
    print("Connecting to:", url)
    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        # 1) BootNotification
        boot_payload = {"chargePointVendor": vendor, "chargePointModel": model}
        await send_and_wait(ws, "BootNotification", boot_payload, verbose)

        # 2) StatusNotification
        ts = datetime.now(timezone.utc).isoformat()
        status_payload = {
            "connectorId": 1,
            "errorCode": "NoError",
            "status": status,
            "timestamp": ts,
        }
        await send_and_wait(ws, "StatusNotification", status_payload, verbose)

        # 3) Heartbeat
        await send_and_wait(ws, "Heartbeat", {}, verbose)

def main():
    p = argparse.ArgumentParser(description="Tiny OCPP 1.6 client simulator (Boot + Status + Heartbeat)")
    p.add_argument("--backend", required=True, help="Backend base URL, e.g. https://ocppfortechlux-backend.onrender.com")
    p.add_argument("--cp", required=True, help='Charge point ID, e.g. "TW*MSI*E000100"')
    p.add_argument("--token", default="", help="OCPP_WS_TOKEN if backend enforces it (use exact same value).")
    p.add_argument("--status", default="Available", help="Status to send: Available/Charging/Preparing/SuspendedEV/Finishing/Faulted/etc.")
    p.add_argument("--vendor", default="GPT-Sim", help="chargePointVendor for BootNotification")
    p.add_argument("--model", default="Model-1", help="chargePointModel for BootNotification")
    p.add_argument("--quiet", action="store_true", help="Less verbose output")
    args = p.parse_args()

    url = build_ws_url(args.backend, args.cp, args.token or None)
    asyncio.run(run_once(url, args.status, args.vendor, args.model, verbose=not args.quiet))

if __name__ == "__main__":
    main()
