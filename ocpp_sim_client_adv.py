
import asyncio
import argparse
import json
import uuid
import urllib.parse
from datetime import datetime, timezone

import websockets

def build_ws_url(base: str, cp_id: str, token: str | None):
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    cp_enc = urllib.parse.quote(cp_id, safe="")
    url = f"{base.rstrip('/')}/ocpp/{cp_enc}"
    if token:
        url += f"?token={urllib.parse.quote(token)}"
    return url

def ocpp_call(action: str, payload: dict):
    uid = str(uuid.uuid4())
    return uid, json.dumps([2, uid, action, payload])

async def send_call(ws, action: str, payload: dict, verbose=True):
    uid, frame = ocpp_call(action, payload)
    if verbose:
        print(f"-> {action}: {frame}")
    await ws.send(frame)
    while True:
        msg = await ws.recv()
        if verbose:
            print("<- raw:", msg)
        try:
            data = json.loads(msg)
        except:
            continue
        if isinstance(data, list) and len(data) >= 3 and data[1] == uid:
            if data[0] == 3:
                if verbose:
                    print(f"<- {action}.conf:", json.dumps(data[2]))
                return data[2]
            elif data[0] == 4:
                raise RuntimeError(f"{action} CallError: {data}")
        # else: skip unrelated messages

async def run_session(url, id_tag, vendor, model, verbose=True):
    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        # Boot
        await send_call(ws, "BootNotification",
                        {"chargePointVendor": vendor, "chargePointModel": model},
                        verbose)
        # Authorize
        await send_call(ws, "Authorize", {"idTag": id_tag}, verbose)
        # StartTransaction
        st_payload = {
            "connectorId": 1,
            "idTag": id_tag,
            "meterStart": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        st_conf = await send_call(ws, "StartTransaction", st_payload, verbose)
        txid = st_conf.get("transactionId", 1)
        # MeterValues (simulate 3 steps)
        for i, kwh in enumerate([1, 2, 3], start=1):
            mv_payload = {
                "connectorId": 1,
                "transactionId": txid,
                "meterValue": [{
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "sampledValue": [{
                        "value": str(kwh*1000),  # Wh
                        "measurand": "Energy.Active.Import.Register",
                        "unit": "Wh",
                    }]
                }]
            }
            await send_call(ws, "MeterValues", mv_payload, verbose)
            await asyncio.sleep(1)
        # StopTransaction
        stop_payload = {
            "transactionId": txid,
            "idTag": id_tag,
            "meterStop": 3000,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await send_call(ws, "StopTransaction", stop_payload, verbose)

def main():
    p = argparse.ArgumentParser(description="Advanced OCPP 1.6 simulator (Boot->Authorize->Start->Meter->Stop)")
    p.add_argument("--backend", required=True)
    p.add_argument("--cp", required=True)
    p.add_argument("--idtag", required=True)
    p.add_argument("--token", default="")
    p.add_argument("--vendor", default="GPT-Sim")
    p.add_argument("--model", default="Model-Adv")
    args = p.parse_args()
    url = build_ws_url(args.backend, args.cp, args.token or None)
    asyncio.run(run_session(url, args.idtag, args.vendor, args.model))

if __name__ == "__main__":
    main()
