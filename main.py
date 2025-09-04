from urllib.parse import unquote  # ← 新增

def _normalize_cp_id(cp_id: str) -> str:
    return unquote(cp_id).lstrip("/")

connected_charge_points = {}
live_status_cache = {}
import sys
sys.path.insert(0, "./")

import json
import os
import io
import csv
import uuid
import logging
import sqlite3
import uvicorn
import asyncio
pending_stop_transactions = {}
# 針對每筆交易做「已送停充」去重，避免前端/後端重複送
stop_requested = set()

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, Query, Body, Path, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from dateutil.parser import parse as parse_date
from websockets.exceptions import ConnectionClosedOK
from ocpp.v16 import call, call_result, ChargePoint as OcppChargePoint
from ocpp.v16.enums import Action, RegistrationStatus
from ocpp.routing import on
from urllib.parse import urlparse, parse_qsl
from reportlab.pdfgen import canvas

app = FastAPI()

# === WebSocket 連線驗證設定（可選）===
REQUIRED_TOKEN = os.getenv("OCPP_WS_TOKEN", None)  # 例：設成 "abc123"；不設就不驗證



logging.basicConfig(level=logging.INFO)

# 允許跨域（若前端使用）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ✅ 改為英文半形引號
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


charging_point_status = {}

class FastAPIWebSocketAdapter:
    def __init__(self, websocket):
        self.websocket = websocket

    async def recv(self):
        msg = await self.websocket.receive_text()
        return msg

    async def send(self, data):
        await self.websocket.send_text(data)

    # ocpp 會取 subprotocol 屬性
    @property
    def subprotocol(self):
        return self.websocket.headers.get('sec-websocket-protocol') or "ocpp1.6"


# ---- Key 正規化工具：忽略底線與大小寫，回傳第一個匹配鍵的值 ----
def pick(d: dict, *names, default=None):
    if not isinstance(d, dict):
        return default
    # 建一個 {規範化鍵: 原鍵} 的對照表
    norm_map = {str(k).replace("_", "").lower(): k for k in d.keys()}
    for name in names:
        key_norm = str(name).replace("_", "").lower()
        if key_norm in norm_map:
            return d.get(norm_map[key_norm], default)
    return default



# ✅ 不再需要 connected_devices
# ✅ 直接依據 connected_charge_points 來判定目前「已連線」的樁

@app.get("/api/connections")
def get_active_connections():
    """
    回傳目前已連線的充電樁列表。
    以 connected_charge_points 的 key（cp_id）作為判定。
    """
    return [{"charge_point_id": cp_id} for cp_id in connected_charge_points.keys()]


@app.get("/api/debug/whitelist")
def get_whitelist():
    """
    Debug 用 API：回傳目前允許的 charge_point_id 清單
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT charge_point_id, name FROM charge_points")
        rows = cur.fetchall()

    return {
        "whitelist": [
            {"charge_point_id": row[0], "name": row[1]} for row in rows
        ]
    }




# ==== Live 快取工具 ====
import time

LIVE_TTL = 15  # 秒：視情況調整

def _to_kw(value, unit):
    try:
        v = float(value)
    except Exception:
        return None
    u = (unit or "").lower()
    if u in ("w", "watt", "watts"):
        return v / 1000.0
    return v  # 視為 kW（或已是 kW）

def _energy_to_kwh(value, unit):
    try:
        v = float(value)
    except Exception:
        return None
    u = (unit or "").lower()
    if u in ("wh", "w*h", "w_h"):
        return v / 1000.0
    return v  # 視為 kWh

def _upsert_live(cp_id: str, **kv):
    cur = live_status_cache.get(cp_id, {})
    cur.update(kv)
    cur["updated_at"] = time.time()
    live_status_cache[cp_id] = cur
# ======================



from fastapi import WebSocket, WebSocketDisconnect


async def _accept_or_reject_ws(websocket: WebSocket, raw_cp_id: str):
    # 標準化 CPID
    cp_id = _normalize_cp_id(raw_cp_id)

    # 解析 query token
    url = str(websocket.url)
    qs = dict(parse_qsl(urlparse(url).query))
    supplied_token = qs.get("token")

    # 查白名單
    with get_conn() as _c:
        cur = _c.cursor()
        cur.execute("SELECT charge_point_id FROM charge_points")
        allowed_ids = [row[0] for row in cur.fetchall()]


    # === 驗證檢查 ===
    #if REQUIRED_TOKEN:
     #   if supplied_token is None:
      #      print(f"❌ 拒絕：缺少 token；URL={url}")
       #     await websocket.close(code=1008)
        #    return None
        #if supplied_token != REQUIRED_TOKEN:
         #   print(f"❌ 拒絕：token 不正確；給定={supplied_token}")
          #  await websocket.close(code=1008)
           # return None
    print(f"📝 白名單允許={allowed_ids}, 本次連線={cp_id}")
    if cp_id not in allowed_ids:
        print(f"❌ 拒絕：{cp_id} 不在白名單 {allowed_ids}")
        await websocket.close(code=1008)
        return None

    # 接受連線（OCPP 1.6 子協定）
    await websocket.accept(subprotocol="ocpp1.6")
    print(f"✅ 接受 WebSocket：cp_id={cp_id} | ip={websocket.client.host}")

    now = datetime.utcnow().isoformat()
    with get_conn() as _c:
        cur = _c.cursor()
        cur.execute(
            "INSERT INTO connection_logs (charge_point_id, ip, time) VALUES (?, ?, ?)",
            (cp_id, websocket.client.host, now)
        )
        _c.commit()


    return cp_id



@app.websocket("/{charge_point_id:path}")
async def websocket_endpoint(websocket: WebSocket, charge_point_id: str):
    try:
        # 1) 驗證 + accept(subprotocol="ocpp1.6")，並回傳標準化 cp_id
        cp_id = await _accept_or_reject_ws(websocket, charge_point_id)
        if cp_id is None:
            return

        # 2) 啟動 OCPP handler
        cp = ChargePoint(cp_id, FastAPIWebSocketAdapter(websocket))
        connected_charge_points[cp_id] = cp
        await cp.start()

    except WebSocketDisconnect:
        logger.warning(f"⚠️ Disconnected: {charge_point_id}")
    except Exception as e:
        logger.error(f"❌ WebSocket error for {charge_point_id}: {e}")
        try:
            await websocket.close()
        except Exception:
            pass
    finally:
        # 3) 清理連線狀態
        connected_charge_points.pop(_normalize_cp_id(charge_point_id), None)



# 初始化狀態儲存
#charging_point_status = {}

# HTTP 端點：查詢狀態
@app.get("/status/{cp_id}")
async def get_status(cp_id: str):
    return JSONResponse(charging_point_status.get(cp_id, {}))



# 初始化 SQLite 資料庫
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "ocpp_data.db")  # ✅ 固定資料庫絕對路徑
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()


def get_conn():
    # 為每次查詢建立新的連線與游標，避免共用全域 cursor 造成並發問題
    return sqlite3.connect(DB_FILE, check_same_thread=False)




# ✅ 確保資料表存在（若不存在則建立）
cursor.execute("""
CREATE TABLE IF NOT EXISTS charge_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT UNIQUE NOT NULL,
    name TEXT,
    status TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# 初始化 connection_logs 表格（如不存在就建立）
cursor.execute("""
CREATE TABLE IF NOT EXISTS connection_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    ip TEXT,
    time TEXT
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT NOT NULL,
    id_tag TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time   TEXT NOT NULL,
    status     TEXT NOT NULL
)
""")
conn.commit()




# === 新增 cards 資料表，用於管理卡片餘額 ===
cursor.execute('''
CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id TEXT UNIQUE,
    balance REAL DEFAULT 0
)
''')

# 建立 daily_pricing 表（若尚未存在）
cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_pricing (
    date TEXT PRIMARY KEY,
    price_per_kwh REAL
)
''')



# ★ 新增：每日「多時段」電價規則，供 /api/pricing/price-now 使用
cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_pricing_rules (
    date TEXT,          -- YYYY-MM-DD
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL,         -- 當時段電價
    label TEXT          -- 可選：顯示用標籤（例如 尖峰/離峰/活動價）
)
''')
conn.commit()




cursor.execute('''
CREATE TABLE IF NOT EXISTS stop_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT,
    meter_stop INTEGER,
    timestamp TEXT,
    reason TEXT
)
''')


# ✅ 加在這裡！
cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id INTEGER,
        base_fee REAL,
        energy_fee REAL,
        overuse_fee REAL,
        total_amount REAL,
        paid_at TEXT
    )
""")


conn.commit()


# 測試卡片初始資料（可選）
cursor.execute('INSERT OR IGNORE INTO cards (card_id, balance) VALUES (?, ?)', ("ABC123", 200))
cursor.execute('INSERT OR IGNORE INTO cards (card_id, balance) VALUES (?, ?)', ("TAG001", 50))
cursor.execute('INSERT OR IGNORE INTO cards (card_id, balance) VALUES (?, ?)', ("USER999", 500))
conn.commit()

cursor.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY,
    charge_point_id TEXT,
    connector_id INTEGER,
    id_tag TEXT,
    meter_start INTEGER,
    start_timestamp TEXT,
    meter_stop INTEGER,
    stop_timestamp TEXT,
    reason TEXT
)
''')


cursor.execute('''
CREATE TABLE IF NOT EXISTS id_tags (
    id_tag TEXT PRIMARY KEY,
    status TEXT,
    valid_until TEXT
)
''')

# 測試資料（可移除）：預設三張卡片
cursor.execute('INSERT OR IGNORE INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)', ("ABC123", "Accepted", "2099-12-31T23:59:59"))
cursor.execute('INSERT OR IGNORE INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)', ("TAG001", "Expired", "2022-01-01T00:00:00"))
cursor.execute('INSERT OR IGNORE INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)', ("USER999", "Blocked", "2099-12-31T23:59:59"))


# ✅ 請插入這段
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    id_tag TEXT PRIMARY KEY,
    name TEXT,
    department TEXT,
    card_number TEXT
)
''')

conn.commit()


cursor.execute('''
CREATE TABLE IF NOT EXISTS weekly_pricing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season TEXT,
    weekday TEXT,
    type TEXT,          -- 尖峰、離峰、半尖峰
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL
)
''')
conn.commit()



# ★ 新增：一般季別/日別的時段電價規則，供 /api/pricing-rules 使用
cursor.execute('''
CREATE TABLE IF NOT EXISTS pricing_rules (
    season TEXT,        -- 例如：summer、winter…（你自訂）
    day_type TEXT,      -- 例如：weekday、weekend…（你自訂）
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL
)
''')
conn.commit()



cursor.execute('''
CREATE TABLE IF NOT EXISTS meter_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER,
    charge_point_id TEXT,
    connector_id INTEGER,
    timestamp TEXT,
    value REAL,
    measurand TEXT,
    unit TEXT,
    context TEXT,
    format TEXT,
    phase TEXT               -- ★ 新增：存相別 L1/L2/L3/N 等
)
''')

# ★ 舊庫相容：若既有資料表沒有 phase 欄位，自動補上
cursor.execute("PRAGMA table_info(meter_values)")
_cols = [r[1] for r in cursor.fetchall()]
if "phase" not in _cols:
    cursor.execute("ALTER TABLE meter_values ADD COLUMN phase TEXT")
conn.commit()


cursor.execute('''
CREATE TABLE IF NOT EXISTS status_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    connector_id INTEGER,
    status TEXT,
    timestamp TEXT
)
''')


conn.commit()

from ocpp.v16 import call

class ChargePoint(OcppChargePoint):
    # ...（你的其他方法，例如 on_status_notification, on_meter_values, ...）

    async def send_stop_transaction(self, transaction_id):
        import sqlite3
        from datetime import datetime, timezone

        # 讀取交易資訊
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT meter_stop, id_tag FROM transactions WHERE transaction_id = ?
            ''', (transaction_id,))
            row = cursor.fetchone()
            if not row:
                raise Exception(f"查無 transaction_id: {transaction_id}")
            meter_stop, id_tag = row
            # 補 timestamp
            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            reason = "Remote"

        # 發送 OCPP StopTransaction
        request = call.StopTransaction(
            transaction_id=transaction_id,
            meter_stop=meter_stop or 0,
            timestamp=timestamp,
            id_tag=id_tag,
            reason=reason
        )
        response = await self.call(request)
        return response



    async def send_remote_start_transaction(self, id_tag: str, connector_id: int = 1):
        from ocpp.v16 import call

        request = call.RemoteStartTransaction(
            id_tag=id_tag,
            connector_id=connector_id
        )
        response = await self.call(request)
        return response




    @on(Action.StopTransaction)
    async def on_stop_transaction(self, **kwargs):
        try:
            print(f"🟢【OCPP Handler】StopTransaction kwargs: {kwargs}")
            cp_id = getattr(self, "id", None)
            print(f"🟢【OCPP Handler】StopTransaction self.id: {cp_id}")

            # 正規化傳入欄位
            transaction_id = str(kwargs.get("transaction_id") or kwargs.get("transactionId"))
            meter_stop = kwargs.get("meter_stop")
            timestamp = kwargs.get("timestamp") or datetime.utcnow().isoformat()
            reason = kwargs.get("reason")

            if cp_id is None or transaction_id is None:
                print(f"🔴【OCPP Handler】❌ StopTransaction 欄位缺失 | cp_id={cp_id} | transaction_id={transaction_id}")
                StopTransactionPayload()

            with sqlite3.connect(DB_FILE) as _conn:
                _cur = _conn.cursor()

                # 1) 落 StopTransaction 與更新交易紀錄
                _cur.execute('''
                    INSERT INTO stop_transactions (transaction_id, meter_stop, timestamp, reason)
                    VALUES (?, ?, ?, ?)
                ''', (transaction_id, meter_stop, timestamp, reason))

                _cur.execute('''
                    UPDATE transactions
                    SET meter_stop = ?, stop_timestamp = ?, reason = ?
                    WHERE transaction_id = ?
                ''', (meter_stop, timestamp, reason, transaction_id))

                # 2) 取交易起始資料 → 算本次用電（Wh→kWh）
                _cur.execute('''
                    SELECT meter_start, id_tag FROM transactions
                    WHERE transaction_id = ?
                ''', (transaction_id,))
                row = _cur.fetchone()

                if row:
                    meter_start, id_tag = row
                    energy_kwh = max(0.0, ((meter_stop or 0) - (meter_start or 0)) / 1000.0)

                    # 3) 依「停止時間」求單價（用你 daily_pricing_rules 的規則；找不到時預設 6 元）
                    unit_price = float(_price_for_timestamp(timestamp))

                    # 4) 記 payments（可視需求調整 base/overuse）
                    base_fee = 0.0
                    energy_fee = round(energy_kwh * unit_price, 2)
                    overuse_fee = 0.0
                    total = round(base_fee + energy_fee + overuse_fee, 2)

                    _cur.execute('''
                        INSERT INTO payments (transaction_id, base_fee, energy_fee, overuse_fee, total_amount, paid_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (transaction_id, base_fee, energy_fee, overuse_fee, total, timestamp))

                    # 5) 扣卡片餘額（不足則扣到 0）
                    _cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
                    card_row = _cur.fetchone()
                    if card_row is not None:
                        old_balance = float(card_row[0] or 0)
                        new_balance = max(0.0, round(old_balance - total, 2))
                        _cur.execute("UPDATE cards SET balance = ? WHERE card_id = ?", (new_balance, id_tag))
                        print(f"💳 扣款：{id_tag} | {old_balance} → {new_balance} 元 | txn={transaction_id} | kWh={energy_kwh:.3f} | 單價={unit_price}")

                _conn.commit()


            # 若 /stop API 有在等，回傳結果
            tx_key = str(transaction_id)
            fut = pending_stop_transactions.get(tx_key)
            if fut and not fut.done():
                fut.set_result({"meter_stop": meter_stop, "timestamp": timestamp, "reason": reason})

            # ✅ 清掉已送停充去重旗標，避免殘留影響下一筆交易
            stop_requested.discard(tx_key)
            pending_stop_transactions.pop(tx_key, None)

            return call_result.StopTransactionPayload()

        except Exception as e:
            logging.exception(f"🔴 StopTransaction 儲存/扣款失敗：{e}")
            return call_result.StopTransactionPayload()







    @on(Action.StatusNotification)
    async def on_status_notification(self, connector_id=None, status=None, error_code=None, timestamp=None, **kwargs):
        global charging_point_status

        try:
            cp_id = getattr(self, "id", None)
 
            # 強制轉為 int 並防止 None 造成錯誤
            try:
                connector_id = int(connector_id) if connector_id is not None else 0
            except (ValueError, TypeError):
                connector_id = 0

            status = status or "Unknown"
            error_code = error_code or "NoError"
            timestamp = timestamp or datetime.utcnow().isoformat()

            if cp_id is None or status is None:
                logging.error(f"❌ 欄位遺失 | cp_id={cp_id} | connector_id={connector_id} | status={status}")
                return call_result.StatusNotificationPayload()

            # 寫入資料庫
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO status_logs (charge_point_id, connector_id, status, timestamp)
                    VALUES (?, ?, ?, ?)
                ''', (cp_id, connector_id, status, timestamp))
                conn.commit()

            # 儲存至記憶體
            charging_point_status[cp_id] = {
                "connector_id": connector_id,
                "status": status,
                "timestamp": timestamp,
                "error_code": error_code
            }

            logging.info(f"📡 StatusNotification | CP={cp_id} | connector={connector_id} | errorCode={error_code} | status={status}")
            return call_result.StatusNotificationPayload()

        except Exception as e:
            logging.exception(f"❌ StatusNotification 發生未預期錯誤：{e}")
            return call_result.StatusNotificationPayload()



    from ocpp.v16.enums import RegistrationStatus

    @on(Action.BootNotification)
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        try:
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            logging.info(f"🔌 BootNotification | 模型={charge_point_model} | 廠商={charge_point_vendor}")
            return call_result.BootNotificationPayload(
                current_time=now.isoformat(),
                interval=10,
                status=RegistrationStatus.accepted
            )
        except Exception as e:
            logging.exception(f"BootNotification handler error: {e}")
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            return call_result.BootNotificationPayload(
                current_time=now.isoformat(),
                interval=10,
                status=RegistrationStatus.accepted
            )


    @on(Action.Heartbeat)
    async def on_heartbeat(self):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        logging.info(f"❤️ Heartbeat | CP={self.id}")
        return call_result.HeartbeatPayload(current_time=now.isoformat())




    @on(Action.Authorize)
    async def on_authorize(self, id_tag, **kwargs):
        with get_conn() as _c:
            cur = _c.cursor()
            cur.execute("SELECT status, valid_until FROM id_tags WHERE id_tag = ?", (id_tag,))
            row = cur.fetchone()

        if not row:
            status = "Invalid"
        else:
            status_db, valid_until = row
            try:
                valid_until_dt = datetime.fromisoformat(valid_until).replace(tzinfo=timezone.utc)
            except ValueError:
                logging.warning(f"⚠️ 無法解析 valid_until 格式：{valid_until}")
                valid_until_dt = datetime.min.replace(tzinfo=timezone.utc)
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            status = "Accepted" if status_db == "Accepted" and valid_until_dt > now else "Expired"

        logging.info(f"🆔 Authorize | idTag={id_tag} → {status}")
        return call_result.AuthorizePayload(id_tag_info={"status": status})







    @on(Action.StartTransaction)
    async def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            # 驗證 idTag
            with get_conn() as _c:
                cur = _c.cursor()
                cur.execute("SELECT status, valid_until FROM id_tags WHERE id_tag = ?", (id_tag,))
                row = cur.fetchone()
            if not row:
                return call_result.StartTransactionPayload(transaction_id=0, id_tag_info={"status": "Invalid"})

            status_db, valid_until = row
            try:
                valid_until_dt = datetime.fromisoformat(valid_until).replace(tzinfo=timezone.utc)
            except ValueError:
                logging.warning(f"⚠️ 無法解析 valid_until：{valid_until}")
                valid_until_dt = datetime.min.replace(tzinfo=timezone.utc)
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            status = "Accepted" if status_db == "Accepted" and valid_until_dt > now else "Expired"
            if status != "Accepted":
                return call_result.StartTransactionPayload(transaction_id=0, id_tag_info={"status": status})

            # 預約（可有可無）
            now_str = datetime.utcnow().isoformat()
            cursor.execute("""
                SELECT id FROM reservations
                WHERE charge_point_id=? AND id_tag=? AND status='active'
                  AND start_time<=? AND end_time>=?
            """, (self.id, id_tag, now_str, now_str))
            res = cursor.fetchone()
            if res:
                cursor.execute("UPDATE reservations SET status='completed' WHERE id=?", (res[0],))
                conn.commit()

            # 餘額檢查
            cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
            card = cursor.fetchone()
            if not card:
                return call_result.StartTransactionPayload(transaction_id=0, id_tag_info={"status": "Invalid"})
            balance = float(card[0] or 0)
            if balance <= 0:
                return call_result.StartTransactionPayload(transaction_id=0, id_tag_info={"status": "Blocked"})

            # 建立交易
            transaction_id = int(datetime.utcnow().timestamp() * 1000)
            cursor.execute("""
                INSERT INTO transactions (
                    transaction_id, charge_point_id, connector_id, id_tag,
                    meter_start, start_timestamp, meter_stop, stop_timestamp, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (transaction_id, self.id, connector_id, id_tag, meter_start, timestamp, None, None, None))
            conn.commit()
            logging.info(f"🚗 StartTransaction 成功 | CP={self.id} | idTag={id_tag} | transactionId={transaction_id}")

            return call_result.StartTransactionPayload(transaction_id=transaction_id, id_tag_info={"status": "Accepted"})



    @on(Action.MeterValues)
    async def on_meter_values(self, **kwargs):
        """
        相容多種鍵名風格：
        - connectorId / connector_id
        - transactionId / transaction_id
        - meterValue / meter_value
        - sampledValue / sampled_value
        並在寫 DB 的同時，更新 live_status_cache（power/current/voltage/energy）
        ＊新增：根據本交易累積電費 vs 卡片餘額，自動觸發 RemoteStopTransaction
        """
        try:
            cp_id = getattr(self, "id", None)
            if not cp_id:
                logging.error("❌ 無法識別充電樁 ID（self.id 為空）")
                return call_result.MeterValuesPayload()

            # 取 connector_id
            connector_id = pick(kwargs, "connectorId", "connector_id", default=0)
            try:
                connector_id = int(connector_id or 0)
            except Exception:
                connector_id = 0

            # 取 transaction_id（若沒有就到 DB 補最近一筆未結束）
            transaction_id = pick(kwargs, "transactionId", "transaction_id", "TransactionId", default="")
            if not transaction_id:
                with sqlite3.connect(DB_FILE) as _c:
                    _cur = _c.cursor()
                    _cur.execute("""
                        SELECT transaction_id FROM transactions
                        WHERE charge_point_id=? AND stop_timestamp IS NULL
                        ORDER BY start_timestamp DESC LIMIT 1
                    """, (cp_id,))
                    row = _cur.fetchone()
                    if row:
                        transaction_id = str(row[0])

            # 取 meter_value list（相容多種命名）
            meter_value_list = pick(kwargs, "meterValue", "meter_value", "MeterValue", default=[]) or []
            if not isinstance(meter_value_list, list):
                meter_value_list = [meter_value_list]

            insert_count = 0
            last_ts_in_batch = None  # 用於定價時間點
            with sqlite3.connect(DB_FILE) as _c:
                _cur = _c.cursor()

                for mv in meter_value_list:
                    ts = pick(mv, "timestamp", "timeStamp", "Timestamp")
                    if ts:
                        last_ts_in_batch = ts

                    sampled_list = pick(mv, "sampledValue", "sampled_value", "SampledValue", default=[]) or []
                    if not isinstance(sampled_list, list):
                        sampled_list = [sampled_list]

                    seen = {}

                    for sv in sampled_list:
                        if not isinstance(sv, dict):
                            continue

                        raw_val = pick(sv, "value", "Value")
                        meas = pick(sv, "measurand", "Measurand", default="")
                        unit = pick(sv, "unit", "Unit", default="")
                        phase = pick(sv, "phase", "Phase")

                        if raw_val is None or not meas:
                            continue

                        try:
                            val = float(raw_val)
                        except Exception:
                            logging.warning(f"⚠️ 無法轉換 value 為 float：{raw_val} | measurand={meas}")
                            continue

                        # (1) 寫 DB
                        _cur.execute("""
                            INSERT INTO meter_values
                              (charge_point_id, connector_id, transaction_id,
                               value, measurand, unit, timestamp, phase)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (cp_id, connector_id, transaction_id, val, meas, unit, ts, phase))
                        insert_count += 1

                        # (2) 更新 live cache
                        m = str(meas)
                        if m == "Power.Active.Import":
                            kw = _to_kw(val, unit)
                            if kw is not None:
                                _upsert_live(cp_id, power=round(kw, 3), timestamp=ts, derived=False)

                        elif m in ("Current.Import", "Current.Import.L1", "Current.Import.L2", "Current.Import.L3"):
                            try:
                                cur_a = float(val)
                                _upsert_live(cp_id, current=cur_a, timestamp=ts)
                                seen["current"] = cur_a
                            except Exception:
                                pass

                        elif m in ("Voltage", "Voltage.L1", "Voltage.L2", "Voltage.L3"):
                            try:
                                vv = float(val)
                                _upsert_live(cp_id, voltage=vv, timestamp=ts)
                                seen["voltage"] = vv
                            except Exception:
                                pass

                        elif m in ("Energy.Active.Import.Register", "Energy.Active.Import"):
                            kwh = _energy_to_kwh(val, unit)
                            if kwh is not None:
                                _upsert_live(cp_id, energy=round(kwh, 6), timestamp=ts)

                    # (3) 若本批沒有功率，但有 V 或 I，嘗試以 V×I 推估（單相）
                    live_now = live_status_cache.get(cp_id) or {}
                    if "power" not in live_now:
                        v = seen.get("voltage") or live_now.get("voltage")
                        i = seen.get("current") or live_now.get("current")
                        if isinstance(v, (int, float)) and isinstance(i, (int, float)):
                            vi_kw = max(0.0, (v * i) / 1000.0)
                            _upsert_live(cp_id, power=round(vi_kw, 3), timestamp=ts, derived=True)

                _c.commit()

            logging.info(f"📊 MeterValues 寫入完成，共 {insert_count} 筆 | tx={transaction_id} | keys={list(kwargs.keys())}")

            # === 新增：餘額與累計費用檢查 → 需要時自動下遠端停充 ===
            # 只在有進行中交易時才檢查
            if transaction_id:
                with sqlite3.connect(DB_FILE) as _c:
                    _cur = _c.cursor()
                    # 取得交易起點與 id_tag、是否已結束
                    _cur.execute("""
                        SELECT meter_start, id_tag, stop_timestamp
                        FROM transactions
                        WHERE transaction_id = ?
                    """, (transaction_id,))
                    tr = _cur.fetchone()

                    if tr:
                        meter_start_wh, id_tag, stop_ts = tr
                        if stop_ts is None and id_tag:
                            # 取最新能量值（優先使用 Energy.Active.Import.Register / Import）
                            _cur.execute("""
                                SELECT timestamp, value, unit
                                FROM meter_values
                                WHERE transaction_id = ?
                                  AND measurand IN ('Energy.Active.Import.Register','Energy.Active.Import')
                                ORDER BY timestamp DESC
                                LIMIT 1
                            """, (transaction_id,))
                            ev = _cur.fetchone()

                            if ev:
                                ts_last, val, unit = ev
                                # 轉成 kWh
                                cur_kwh = _energy_to_kwh(val, unit)
                                # 起始表碼是 Wh（你的 transactions.meter_start 即是 Wh）
                                used_kwh = max(0.0, (cur_kwh - (float(meter_start_wh or 0) / 1000.0))) if cur_kwh is not None else 0.0

                                # 依最後一筆時間戳定價（找不到就回 6.0）
                                try:
                                    unit_price = float(_price_for_timestamp(ts_last or last_ts_in_batch or datetime.utcnow().isoformat()))
                                except Exception:
                                    unit_price = 6.0  # 後備價
                                cost_so_far = round(used_kwh * unit_price, 2)

                                # 查卡片餘額
                                _cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
                                cr = _cur.fetchone()


                                if cr is not None:
                                    balance = float(cr[0] or 0.0)

                                    # 計算剩餘餘額（卡片餘額 - 已累積花費）
                                    remaining_balance = balance - cost_so_far

                                    # 若已經沒有餘額 → 送停充（去重，避免重覆送）
                                    if remaining_balance <= 0:
                                        tx_key = str(transaction_id)
                                        if tx_key not in stop_requested:
                                            stop_requested.add(tx_key)
                                            logging.warning(
                                                f"⛔ 餘額不足：已用 {cost_so_far} / 餘額 {balance}，送出 RemoteStopTransaction | tx={tx_key} | cp={cp_id}"
                                            )
                                            try:
                                                # ✅ 正確：用 Payload 物件建立 CALL
                                                req = call.RemoteStopTransactionPayload(transaction_id=int(transaction_id))
                                                async def _fire_and_log():
                                                    try:
                                                        resp = await self.call(req)
                                                        logging.info(f"[AutoStop] RemoteStopTransaction 回應: {getattr(resp, 'status', None)}")
                                                        if getattr(resp, "status", None) != "Accepted":
                                                            stop_requested.discard(tx_key)  # 允許下次重試
                                                    except Exception as e:
                                                        logging.exception(f"[AutoStop] RemoteStopTransaction 送出失敗: {e}")
                                                        stop_requested.discard(tx_key)      # 允許下次重試

                                                asyncio.create_task(_fire_and_log())
    
                                    
                                            except Exception as e:
                                                logging.exception(f"❌ 送出 RemoteStopTransaction 失敗: {e}")
                                                # 若送失敗，允許下一次重試
                                                stop_requested.discard(tx_key)



            return call_result.MeterValuesPayload()

        except Exception as e:
            logging.exception(f"❌ 處理 MeterValues 例外：{e} | payload keys={list(kwargs.keys())}")
            return call_result.MeterValuesPayload()





    @on(Action.RemoteStopTransaction)
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        logging.info(f"✅ 收到遠端停止充電要求，transaction_id={transaction_id}")
        return call_result.RemoteStopTransactionPayload(status="Accepted")


@app.get("/api/charge-points/{charge_point_id}/live-status")
def get_live_status(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    data = live_status_cache.get(cp_id)
    now = time.time()  # ✅ 正確
    if (not data) or (now - data.get("updated_at", 0) > LIVE_TTL):
        return {"message": "尚無資料", "active": False, "status": "stale", "cp_id": cp_id}
    return {
        "power": data.get("power", 0),
        "current": data.get("current", 0),
        "voltage": data.get("voltage", 0),
        "energy": data.get("energy", 0),
        "timestamp": data.get("timestamp"),
        "derived": data.get("derived", False),
        "active": True,
    }


@app.post("/api/debug/force-add-charge-point")
def force_add_charge_point(
    charge_point_id: str = "TW*MSI*E000100",
    name: str = "MSI充電樁"
):
    """
    Debug 用 API：強制新增一個充電樁到白名單 (charge_points 資料表)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO charge_points (charge_point_id, name, status)
            VALUES (?, ?, 'enabled')
            """,
            (charge_point_id, name),
        )
        conn.commit()

    return {
        "message": f"已新增或存在白名單: {charge_point_id}",
        "charge_point_id": charge_point_id,
        "name": name
    }





from fastapi import HTTPException

@app.post("/api/charge-points/{charge_point_id}/stop")
async def stop_transaction_by_charge_point(charge_point_id: str):
    # ← 先正規化，處理星號與 URL 編碼
    cp_id = _normalize_cp_id(charge_point_id)
    print(f"🟢【API呼叫】收到停止充電API請求, charge_point_id = {charge_point_id}")
    cp = connected_charge_points.get(cp_id)

    if not cp:
        print(f"🔴【API異常】找不到連線中的充電樁：{charge_point_id}")
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{charge_point_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))}
        )
    # 查詢進行中的 transaction_id
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT transaction_id FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """, (cp_id,))
        row = cursor.fetchone()
        if not row:
            print(f"🔴【API異常】無進行中交易 charge_point_id={charge_point_id}")
            raise HTTPException(status_code=400, detail="⚠️ 無進行中交易")
        transaction_id = row[0]
        print(f"🟢【API呼叫】找到進行中交易 transaction_id={transaction_id}")


    # 新增同步等待機制
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    pending_stop_transactions[str(transaction_id)] = fut

    # 發送 RemoteStopTransaction
    print(f"🟢【API呼叫】發送 RemoteStopTransaction 給充電樁")
    print(f"🟢【API呼叫】即將送出 RemoteStopTransaction | charge_point_id={charge_point_id} | transaction_id={transaction_id}")
    # 送 RemoteStopTransaction（使用 Payload）
    req = call.RemoteStopTransactionPayload(transaction_id=int(transaction_id))
    resp = await cp.call(req)
    print(f"🟢【API回應】呼叫 RemoteStopTransaction 完成，resp={resp}")

    # 等待 StopTransaction 被觸發（最多 10 秒）
    try:
        stop_result = await asyncio.wait_for(fut, timeout=10)
        print(f"🟢【API回應】StopTransaction 完成: {stop_result}")
        return {"message": "充電已停止", "transaction_id": transaction_id, "stop_result": stop_result}
    except asyncio.TimeoutError:
        print(f"🔴【API異常】等待 StopTransaction 超時")
        return JSONResponse(status_code=504, content={"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"})
    finally:
        pending_stop_transactions.pop(str(transaction_id), None)


@app.post("/api/charge-points/{charge_point_id}/start")
async def start_transaction_by_charge_point(charge_point_id: str, data: dict = Body(...)):
    id_tag = data.get("idTag")
    connector_id = data.get("connectorId", 1)

    if not id_tag:
        raise HTTPException(status_code=400, detail="缺少 idTag")

    cp = connected_charge_points.get(charge_point_id)
    if not cp:
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{charge_point_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))}
        )

    # 發送 RemoteStartTransaction
    print(f"🟢【API】遠端啟動充電 | CP={charge_point_id} | idTag={id_tag} | connector={connector_id}")
    response = await cp.send_remote_start_transaction(id_tag=id_tag, connector_id=connector_id)
    print(f"🟢【API】回應 RemoteStartTransaction: {response}")
    return {"message": "已送出啟動充電請求", "response": response}


from fastapi import FastAPI, HTTPException

# 假設這裡有一個全域變數在存充電樁即時數據
latest_power_data = {}

@app.get("/api/charge-points/{charge_point_id}/latest-power")
def get_latest_power(charge_point_id: str):
    """
    回傳該樁「最新功率(kW)」。
    優先使用 measurand='Power.Active.Import'（單位 W 或 kW）。
    若沒有，則在最近 5 秒內以各相 Voltage × Current.Import 推導 ΣP。
    """
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()

    # 0) 直接取總功率（不分相）
    c.execute("""
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id = ?
          AND measurand = 'Power.Active.Import'
          AND (phase IS NULL OR phase = '')
        ORDER BY timestamp DESC
        LIMIT 1
    """, (charge_point_id,))
    row = c.fetchone()
    if row:
        ts, val, unit = row[0], float(row[1]), (row[2] or "").lower()
        kw = (val / 1000.0) if unit in ("w",) else val
        return {"timestamp": ts, "value": round(kw, 3), "unit": "kW"}

    # 1) 最近 5 秒：各相取「該相最新」的 V 與 I，Σ(V*I)/1000 推得 kW
    c.execute("""
    WITH latest_ts AS (
      SELECT MAX(timestamp) AS ts FROM meter_values WHERE charge_point_id=?
    ),
    win AS (
      SELECT datetime((SELECT ts FROM latest_ts), '-5 seconds') AS from_ts,
             (SELECT ts FROM latest_ts) AS to_ts
    ),
    v_pick AS (   -- 各相最新電壓
      SELECT COALESCE(
               CASE WHEN measurand LIKE 'Voltage.L%' THEN substr(measurand, length('Voltage.')+1) END,
               CASE WHEN measurand='Voltage' THEN phase END
             ) AS ph,
             MAX(timestamp) AS ts
      FROM meter_values, win
      WHERE charge_point_id=? AND (
        measurand IN ('Voltage.L1','Voltage.L2','Voltage.L3')
        OR (measurand='Voltage' AND phase IN ('L1','L2','L3'))
      ) AND timestamp BETWEEN from_ts AND to_ts
      GROUP BY ph
    ),
    i_pick AS (   -- 各相最新電流
      SELECT COALESCE(
               CASE WHEN measurand LIKE 'Current.Import.L%' THEN substr(measurand, length('Current.Import.')+1) END,
               CASE WHEN measurand='Current.Import' THEN phase END
             ) AS ph,
             MAX(timestamp) AS ts
      FROM meter_values, win
      WHERE charge_point_id=? AND (
        measurand IN ('Current.Import.L1','Current.Import.L2','Current.Import.L3')
        OR (measurand='Current.Import' AND phase IN ('L1','L2','L3'))
      ) AND timestamp BETWEEN from_ts AND to_ts
      GROUP BY ph
    ),
    v AS (
      SELECT p.ph, m.value AS v_val, m.timestamp AS v_ts
      FROM v_pick p JOIN meter_values m ON m.timestamp=p.ts
      WHERE m.charge_point_id=? AND (
        (m.measurand='Voltage' AND m.phase=p.ph) OR m.measurand='Voltage.'||p.ph
      )
    ),
    i AS (
      SELECT p.ph, m.value AS i_val, m.timestamp AS i_ts
      FROM i_pick p JOIN meter_values m ON m.timestamp=p.ts
      WHERE m.charge_point_id=? AND (
        (m.measurand='Current.Import' AND m.phase=p.ph) OR m.measurand='Current.Import.'||p.ph
      )
    )
    SELECT
      COALESCE(MAX(v.v_ts), MAX(i.i_ts)) AS ts,
      SUM(CASE WHEN v.v_val IS NOT NULL AND i.i_val IS NOT NULL THEN (v.v_val * i.i_val) ELSE 0 END) AS watt_sum
    FROM v LEFT JOIN i ON v.ph = i.ph
    """, (charge_point_id, charge_point_id, charge_point_id, charge_point_id))
    r = c.fetchone()
    if r and r[1] is not None:
        ts, watt_sum = r[0], float(r[1])
        kw = max(0.0, watt_sum / 1000.0)
        return {"timestamp": ts, "value": round(kw, 3), "unit": "kW", "derived": True}

    # 2) 都沒有資料
    return {}




@app.get("/api/charge-points/{charge_point_id}/latest-voltage")
def get_latest_voltage(charge_point_id: str):
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()

    # 1) 無相別
    c.execute("""
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id=? AND measurand='Voltage' AND (phase IS NULL OR phase='')
        ORDER BY timestamp DESC LIMIT 1
    """, (charge_point_id,))
    row = c.fetchone()
    if row:
        return {"timestamp": row[0], "value": round(float(row[1]), 1), "unit": (row[2] or "V")}

    # 2) 最近 5 秒各相取最新再平均
    c.execute("""
    WITH latest_ts AS (SELECT MAX(timestamp) AS ts FROM meter_values WHERE charge_point_id=?),
    win AS (SELECT datetime((SELECT ts FROM latest_ts), '-5 seconds') AS from_ts, (SELECT ts FROM latest_ts) AS to_ts),
    pick AS (
      SELECT COALESCE(
               CASE WHEN measurand LIKE 'Voltage.L%' THEN substr(measurand, length('Voltage.')+1) END,
               CASE WHEN measurand='Voltage' THEN phase END
             ) AS ph,
             MAX(timestamp) AS ts
      FROM meter_values, win
      WHERE charge_point_id=? AND (
        measurand IN ('Voltage.L1','Voltage.L2','Voltage.L3')
        OR (measurand='Voltage' AND phase IN ('L1','L2','L3'))
      ) AND timestamp BETWEEN from_ts AND to_ts
      GROUP BY ph
    )
    SELECT m.timestamp, AVG(m.value) AS v_avg
    FROM meter_values m JOIN pick p ON m.timestamp=p.ts
    WHERE m.charge_point_id=? AND (
      (m.measurand='Voltage' AND m.phase=p.ph) OR m.measurand='Voltage.'||p.ph
    )
    """, (charge_point_id, charge_point_id, charge_point_id))
    r = c.fetchone()
    if r and r[1] is not None:
        return {"timestamp": r[0], "value": round(float(r[1]), 1), "unit": "V", "derived": True}

    return {}



@app.get("/api/charge-points/{charge_point_id}/latest-current")
def get_latest_current_api(charge_point_id: str):
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()

    # 1) 無相別
    c.execute("""
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id=? AND measurand='Current.Import' AND (phase IS NULL OR phase='')
        ORDER BY timestamp DESC LIMIT 1
    """, (charge_point_id,))
    row = c.fetchone()
    if row:
        return {"timestamp": row[0], "value": round(float(row[1]), 2), "unit": (row[2] or "A")}

    # 2) 最近 5 秒各相取最新再相加
    c.execute("""
    WITH latest_ts AS (SELECT MAX(timestamp) AS ts FROM meter_values WHERE charge_point_id=?),
    win AS (SELECT datetime((SELECT ts FROM latest_ts), '-5 seconds') AS from_ts, (SELECT ts FROM latest_ts) AS to_ts),
    pick AS (
      SELECT COALESCE(
               CASE WHEN measurand LIKE 'Current.Import.L%' THEN substr(measurand, length('Current.Import.')+1) END,
               CASE WHEN measurand='Current.Import' THEN phase END
             ) AS ph,
             MAX(timestamp) AS ts
      FROM meter_values, win
      WHERE charge_point_id=? AND (
        measurand IN ('Current.Import.L1','Current.Import.L2','Current.Import.L3')
        OR (measurand='Current.Import' AND phase IN ('L1','L2','L3'))
      ) AND timestamp BETWEEN from_ts AND to_ts
      GROUP BY ph
    )
    SELECT m.timestamp, SUM(m.value) AS a_sum
    FROM meter_values m JOIN pick p ON m.timestamp=p.ts
    WHERE m.charge_point_id=? AND (
      (m.measurand='Current.Import' AND m.phase=p.ph) OR m.measurand='Current.Import.'||p.ph
    )
    """, (charge_point_id, charge_point_id, charge_point_id))
    r = c.fetchone()
    if r and r[1] is not None:
        return {"timestamp": r[0], "value": round(float(r[1]), 2), "unit": "A", "derived": True}

    return {}







@app.get("/api/charge-points/{charge_point_id}/latest-energy")
def get_latest_energy(charge_point_id: str):
    """
    回傳該充電樁最新的累積用電量（kWh），以及若有進行中交易，回傳本次充電用電量 sessionEnergyKWh。
    - 來源優先：measurand='Energy.Active.Import.Register'（通常為 Wh 或 kWh）
    - 單位處理：Wh → /1000；kWh → 原值
    """
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()

    # 1) 讀取最新的 Energy.Active.Import.Register（不分相）
    c.execute("""
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id = ?
          AND measurand = 'Energy.Active.Import.Register'
          AND (phase IS NULL OR phase = '')
        ORDER BY timestamp DESC
        LIMIT 1
    """, (charge_point_id,))
    row = c.fetchone()
    if not row:
        # 無對應量測，回傳空物件
        return {}

    ts, raw_val, unit = row[0], float(row[1]), (row[2] or "").lower()
    # 轉為 kWh
    if unit == "wh" or unit == "w*h" or unit == "w_h":
        total_kwh = raw_val / 1000.0
    else:
        # 預設已是 kWh（或未知單位時依 kWh 解讀）
        total_kwh = raw_val

    result = {
        "timestamp": ts,
        "totalEnergyKWh": round(total_kwh, 6),  # 累積表值（從樁的電表來）
        "unit": "kWh"
    }

    # 2) 若有「進行中的交易」，計算本次充電用電量（以 meter_start 當基準）
    c.execute("""
        SELECT transaction_id, meter_start
        FROM transactions
        WHERE charge_point_id = ? AND stop_timestamp IS NULL
        ORDER BY start_timestamp DESC
        LIMIT 1
    """, (charge_point_id,))
    tx = c.fetchone()
    if tx:
        _, meter_start = tx
        try:
            # OCPP 的 meter_start 通常是 Wh，若數值看起來偏大就視為 Wh
            # 若想嚴謹，也可加入 unit 欄（目前表設計沒有 unit）
            if meter_start is not None:
                # 以「最新表值（kWh）」換算回 Wh 再相減，或直接假設 meter_start 單位與表值一致
                # 這裡假設 meter_start 為 Wh → 先把 total_kwh 轉回 Wh 再相減
                session_wh = (total_kwh * 1000.0) - float(meter_start)
                session_kwh = max(0.0, session_wh / 1000.0)
                result["sessionEnergyKWh"] = round(session_kwh, 6)
        except Exception:
            # 若有例外就略過 session 欄位
            pass

    return result




# 新增獨立的卡片餘額查詢 API（修正縮排）
@app.get("/api/cards/{id_tag}/balance")
def get_card_balance(id_tag: str):
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
    row = cursor.fetchone()
    if not row:
        return {"balance": 0, "found": False}
    return {"balance": row[0], "found": True}

   

@app.get("/api/charge-points/{charge_point_id}/status")
def get_charge_point_status(charge_point_id: str):
    cp = _normalize_cp_id(charge_point_id)          # ← 先正規化 key
    status = charging_point_status.get(cp)

    # 沒有快取 → 統一回英文 Unknown
    if not status:
        return {"status": "Unknown"}

    # 快取裡萬一混入中文，也強制轉英文
    if status.get("status") == "未知":
        status = {**status, "status": "Unknown"}

    return status




@app.get("/api/charge-points/{charge_point_id}/latest-status")
def get_latest_status(charge_point_id: str):
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()
    # 優先取充電樁傳來的最新 StatusNotification 紀錄
    c.execute(
        """
        SELECT status, timestamp
        FROM status_logs
        WHERE charge_point_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (charge_point_id,),
    )
    row = c.fetchone()
    if row:
        return {"status": row[0], "timestamp": row[1]}
    # 找不到就回 Unknown
    return {"status": "Unknown"}

@app.get("/api/charge-points/{charge_point_id}/last-transaction/summary")
def get_last_tx_summary_by_cp(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        # 找最近「已結束」的交易
        cur.execute("""
            SELECT t.transaction_id, t.id_tag
            FROM transactions t
            WHERE t.charge_point_id = ? AND t.stop_timestamp IS NOT NULL
            ORDER BY t.stop_timestamp DESC
            LIMIT 1
        """, (cp_id,))
        row = cur.fetchone()
        if not row:
            return {"found": False}

        tx_id, id_tag = row
        # 查 payments 總額
        cur.execute("SELECT total_amount FROM payments WHERE transaction_id = ?", (tx_id,))
        pay = cur.fetchone()
        total_amount = float(pay[0]) if pay else 0.0
        # 查卡片目前餘額
        cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        c = cur.fetchone()
        balance = float(c[0]) if c else 0.0

        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "total_amount": round(total_amount, 2),
            "balance": round(balance, 2),
        }


@app.get("/api/transactions/{transaction_id}/summary")
def get_transaction_summary(transaction_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id_tag, p.total_amount
            FROM transactions t
            LEFT JOIN payments p ON p.transaction_id = t.transaction_id
            WHERE t.transaction_id = ?
        """, (transaction_id,))
        row = cur.fetchone()
        if not row:
            return {"found": False}

        id_tag, total_amount = row[0], float(row[1] or 0.0)
        cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        c = cur.fetchone()
        balance = float(c[0]) if c else 0.0

        return {
            "found": True,
            "transaction_id": transaction_id,
            "id_tag": id_tag,
            "total_amount": round(total_amount, 2),
            "balance": round(balance, 2),
        }



@app.get("/api/transactions/{transaction_id}/live-cost")
def get_transaction_live_cost(transaction_id: int):
    """
    計算指定交易的「即時累計電費」。
    - used_kwh：已用電量 (kWh)
    - unit_price：最後一筆電表數據對應的電價
    - cost_so_far：累計電費
    - balance：卡片剩餘餘額
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # 取交易資訊
        cur.execute("""
            SELECT charge_point_id, meter_start, id_tag, stop_timestamp
            FROM transactions
            WHERE transaction_id = ?
        """, (transaction_id,))
        tr = cur.fetchone()
        if not tr:
            return {"found": False, "message": f"找不到交易 {transaction_id}"}

        cp_id, meter_start, id_tag, stop_ts = tr

        # 若交易已經結束，就回傳最終結算結果
        if stop_ts is not None:
            cur.execute("SELECT total_amount FROM payments WHERE transaction_id = ?", (transaction_id,))
            pay = cur.fetchone()
            total = float(pay[0]) if pay else 0.0
            return {
                "transaction_id": transaction_id,
                "id_tag": id_tag,
                "used_kwh": None,
                "unit_price": None,
                "cost_so_far": total,
                "balance": None,
                "final": True
            }

        # 取最新能量值（Wh → kWh）
        cur.execute("""
            SELECT timestamp, value, unit
            FROM meter_values
            WHERE transaction_id = ?
              AND measurand IN ('Energy.Active.Import.Register','Energy.Active.Import')
            ORDER BY timestamp DESC
            LIMIT 1
        """, (transaction_id,))
        ev = cur.fetchone()
        if not ev:
            return {"found": False, "message": "尚無電表數據"}

        ts_last, val, unit = ev
        if unit.lower() in ("wh", "w*h", "w_h"):
            cur_kwh = float(val) / 1000.0
        else:
            cur_kwh = float(val)

        used_kwh = max(0.0, cur_kwh - (float(meter_start or 0) / 1000.0))

        # 依時間戳查單價（找不到就預設 6 元）
        try:
            unit_price = float(_price_for_timestamp(ts_last))
        except Exception:
            unit_price = 6.0

        cost_so_far = round(used_kwh * unit_price, 2)

        # 查卡片餘額
        cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        c = cur.fetchone()
        balance = float(c[0]) if c else 0.0

        return {
            "transaction_id": transaction_id,
            "id_tag": id_tag,
            "used_kwh": round(used_kwh, 6),
            "unit_price": unit_price,
            "cost_so_far": cost_so_far,
            "balance": balance,
            "currency": "TWD",
            "final": False
        }





# ✅ 時段電價設定管理：新增與刪除
@app.post("/api/pricing-rules")
async def add_pricing_rule(rule: dict = Body(...)):
    try:
        cursor.execute('''
            INSERT INTO pricing_rules (season, day_type, start_time, end_time, price)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            rule["season"],
            rule["day_type"],
            rule["start_time"],
            rule["end_time"],
            float(rule["price"])
        ))
        conn.commit()
        return {"message": "新增成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/pricing-rules")
async def delete_pricing_rule(rule: dict = Body(...)):
    try:
        cursor.execute('''
            DELETE FROM pricing_rules
            WHERE season = ? AND day_type = ? AND start_time = ? AND end_time = ? AND price = ?
        ''', (
            rule["season"],
            rule["day_type"],
            rule["start_time"],
            rule["end_time"],
            float(rule["price"])
        ))
        conn.commit()
        return {"message": "刪除成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

conn.commit()


@app.get("/api/payments")
async def list_payments():
    cursor.execute("SELECT transaction_id, base_fee, energy_fee, overuse_fee, total_amount FROM payments ORDER BY transaction_id DESC")
    rows = cursor.fetchall()
    return [
        {
            "transactionId": r[0],
            "baseFee": r[1],
            "energyFee": r[2],
            "overuseFee": r[3],
            "totalAmount": r[4]
        } for r in rows
    ]
...


@app.post("/api/transactions")
async def create_transaction_api(data: dict = Body(...)):
    try:
        txn_id = int(datetime.utcnow().timestamp() * 1000)
        cursor.execute('''
            INSERT INTO transactions (
                transaction_id, charge_point_id, connector_id, id_tag,
                meter_start, start_timestamp, meter_stop, stop_timestamp, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            txn_id,
            data["chargePointId"],
            1,
            data["idTag"],
            data["meter_start"],
            data["start_timestamp"],
            data["meter_stop"],
            data["stop_timestamp"],
            None
        ))
        conn.commit()
        return {"transaction_id": txn_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))





@app.get("/api/transactions")
async def get_transactions(
    idTag: str = Query(None),
    chargePointId: str = Query(None),
    start: str = Query(None),
    end: str = Query(None)
):
    query = "SELECT * FROM transactions WHERE 1=1"
    params = []

    if idTag:
        query += " AND id_tag = ?"
        params.append(idTag)
    if chargePointId:
        query += " AND charge_point_id = ?"
        params.append(chargePointId)
    if start:
        query += " AND start_timestamp >= ?"
        params.append(start)
    if end:
        query += " AND start_timestamp <= ?"
        params.append(end)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    result = {}
    for row in rows:
        txn_id = row[0]
        result[txn_id] = {
            "chargePointId": row[1],
            "connectorId": row[2],
            "idTag": row[3],
            "meterStart": row[4],
            "startTimestamp": row[5],
            "meterStop": row[6],
            "stopTimestamp": row[7],
            "reason": row[8],
            "meterValues": []
        }

        cursor.execute("""
            SELECT timestamp, value, measurand, unit, context, format
            FROM meter_values WHERE transaction_id = ?
        """, (txn_id,))
        mv_rows = cursor.fetchall()
        for mv in mv_rows:
            result[txn_id]["meterValues"].append({
                "timestamp": mv[0],
                "sampledValue": [{
                    "value": mv[1],
                    "measurand": mv[2],
                    "unit": mv[3],
                    "context": mv[4],
                    "format": mv[5]
                }]
            })

    return JSONResponse(content=result)




def compute_transaction_cost(transaction_id: int):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT total_amount
        FROM payments
        WHERE transaction_id = ?
    """, (transaction_id,))
    row = cursor.fetchone()
    if row:
        return {"transaction_id": transaction_id, "total_amount": row[0]}
    else:
        raise HTTPException(status_code=404, detail="Transaction not found")

@app.get("/api/transactions/{transaction_id}/cost")
async def calculate_transaction_cost(transaction_id: int):
    try:
        return compute_transaction_cost(transaction_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

   

@app.get("/api/transactions/{transaction_id}")
async def get_transaction_detail(transaction_id: int):
    # 查詢交易主資料
    cursor.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")

    result = {
        "transactionId": row[0],
        "chargePointId": row[1],
        "connectorId": row[2],
        "idTag": row[3],
        "meterStart": row[4],
        "startTimestamp": row[5],
        "meterStop": row[6],
        "stopTimestamp": row[7],
        "reason": row[8],
        "meterValues": []
    }

    # 查詢對應電錶數據
    cursor.execute("""
        SELECT timestamp, value, measurand, unit, context, format
        FROM meter_values WHERE transaction_id = ?
        ORDER BY timestamp ASC
    """, (transaction_id,))
    mv_rows = cursor.fetchall()
    for mv in mv_rows:
        result["meterValues"].append({
            "timestamp": mv[0],
            "sampledValue": [{
                "value": mv[1],
                "measurand": mv[2],
                "unit": mv[3],
                "context": mv[4],
                "format": mv[5]
            }]
        })

    return JSONResponse(content=result)


@app.get("/api/transactions/export")
async def export_transactions_csv(
    idTag: str = Query(None),
    chargePointId: str = Query(None),
    start: str = Query(None),
    end: str = Query(None)
):
    query = "SELECT * FROM transactions WHERE 1=1"
    params = []

    if idTag:
        query += " AND id_tag = ?"
        params.append(idTag)
    if chargePointId:
        query += " AND charge_point_id = ?"
        params.append(chargePointId)
    if start:
        query += " AND start_timestamp >= ?"
        params.append(start)
    if end:
        query += " AND start_timestamp <= ?"
        params.append(end)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    # 建立 CSV 內容
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "transactionId", "chargePointId", "connectorId", "idTag",
        "meterStart", "startTimestamp", "meterStop", "stopTimestamp", "reason"
    ])
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=transactions_export.csv"
    })





# REST API - 查詢所有充電樁狀態
@app.get("/api/status")
async def get_status():
    return JSONResponse(content=charging_point_status)

@app.get("/api/status/logs")
async def get_status_logs(
    chargePointId: str = Query(None),
    start: str = Query(None),
    end: str = Query(None),
    limit: int = Query(100)
):
    query = "SELECT charge_point_id, connector_id, status, timestamp FROM status_logs WHERE 1=1"
    params = []

    if chargePointId:
        query += " AND charge_point_id = ?"
        params.append(chargePointId)
    if start:
        query += " AND timestamp >= ?"
        params.append(start)
    if end:
        query += " AND timestamp <= ?"
        params.append(end)

    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    return JSONResponse(content=[
        {
            "chargePointId": row[0],
            "connectorId": row[1],
            "status": row[2],
            "timestamp": row[3]
        } for row in rows
    ])



@app.get("/api/id_tags")
async def list_id_tags():
    cursor.execute("SELECT id_tag, status, valid_until FROM id_tags")
    rows = cursor.fetchall()
    return JSONResponse(content=[
        {"idTag": row[0], "status": row[1], "validUntil": row[2]} for row in rows
    ])

@app.post("/api/id_tags")
async def add_id_tag(data: dict = Body(...)):
    print("📥 收到新增卡片資料：", data)

    id_tag = data.get("idTag")
    status = data.get("status", "Accepted")
    valid_until = data.get("validUntil", "2099-12-31T23:59:59")

    if not id_tag:
        print("❌ idTag 缺失")
        raise HTTPException(status_code=400, detail="idTag is required")

    try:
        # ✅ 解析格式（允許無秒的 ISO 格式）
        valid_dt = parse_date(valid_until)
        valid_str = valid_dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"❌ validUntil 格式錯誤：{valid_until}，錯誤訊息：{e}")
        raise HTTPException(status_code=400, detail="Invalid validUntil format")

    try:
        cursor.execute(
            'INSERT INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)',
            (id_tag, status, valid_str)
        )
        conn.commit()
        print(f"✅ 已成功新增卡片：{id_tag}, {status}, {valid_str}")
        # ⬇️ 新增這一行：如果卡片不存在於 cards，則自動新增餘額帳戶（初始餘額0元）
        cursor.execute('INSERT OR IGNORE INTO cards (card_id, balance) VALUES (?, ?)', (id_tag, 0))
        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"❌ 資料庫重複錯誤：{e}")
        raise HTTPException(status_code=409, detail="idTag already exists")
    except Exception as e:
        print(f"❌ 未知新增錯誤：{e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    return {"message": "Added successfully"}


@app.put("/api/id_tags/{id_tag}")
async def update_id_tag(
    id_tag: str = Path(...),
    data: dict = Body(...)
):
    status = data.get("status")
    valid_until = data.get("validUntil")

    if not (status or valid_until):
        raise HTTPException(status_code=400, detail="No update fields provided")

    if status:
        cursor.execute("UPDATE id_tags SET status = ? WHERE id_tag = ?", (status, id_tag))
    if valid_until:
        cursor.execute("UPDATE id_tags SET valid_until = ? WHERE id_tag = ?", (valid_until, id_tag))
    conn.commit()
    return {"message": "Updated successfully"}

@app.delete("/api/id_tags/{id_tag}")
async def delete_id_tag(id_tag: str = Path(...)):
    cursor.execute("SELECT 1 FROM id_tags WHERE id_tag = ?", (id_tag,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="id_tag not found")
    
    cursor.execute("DELETE FROM id_tags WHERE id_tag = ?", (id_tag,))
    cursor.execute("DELETE FROM cards WHERE card_id = ?", (id_tag,))
    conn.commit()
    return {"message": "Deleted successfully"}


@app.get("/api/summary")
async def get_summary(group_by: str = Query("day")):
    if group_by == "day":
        date_expr = "strftime('%Y-%m-%d', start_timestamp)"
    elif group_by == "week":
        date_expr = "strftime('%Y-W%W', start_timestamp)"
    elif group_by == "month":
        date_expr = "strftime('%Y-%m', start_timestamp)"
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid group_by. Use 'day', 'week', or 'month'."})

    cursor.execute(f"""
        SELECT {date_expr} as period,
               COUNT(*) as transaction_count,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY period
        ORDER BY period ASC
    """)
    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append({
            "period": row[0],
            "transactionCount": row[1],
            "totalEnergy": row[2] or 0
        })

    return JSONResponse(content=result)



from fastapi.responses import JSONResponse

import sqlite3
import logging
import threading
import time


@app.post("/api/messaging/test")
async def test_line_messaging(payload: dict = Body(...)):
    logging.info("🔕 已停用 LINE 推播功能，略過發送")
    return {"message": "LINE 通知功能已暫時停用"}

    # 查詢對應的 user_id
    recipient_ids = []
    if targets and isinstance(targets, list):
        query = f"SELECT card_number FROM users WHERE id_tag IN ({','.join(['?']*len(targets))})"
        cursor.execute(query, targets)
        rows = cursor.fetchall()
        recipient_ids = [row[0] for row in rows if row[0]]
    else:
        recipient_ids = LINE_USER_IDS  # 預設全部

    # 發送
    for user_id in recipient_ids:
        try:
            payload = {
                "to": user_id,
                "messages": [{"type": "text", "text": message}]
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_TOKEN}"
            }
            resp = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))
            logging.info(f"🔔 發送至 {user_id}：{resp.status_code} | 回應：{resp.text}")
        except Exception as e:
            logging.error(f"發送至 {user_id} 失敗：{e}")

    return {"message": f"Sent to {len(recipient_ids)} users"}



import requests
LINE_TOKEN = os.getenv("LINE_TOKEN", "")

@app.post("/webhook")
async def webhook(request: Request):
    if not LINE_TOKEN:
        return {"status": "no token"}

    body = await request.json()
    for event in body.get("events", []):
        user_id = event.get("source", {}).get("userId")
        message = event.get("message", {})

        if message.get("type") == "text":
            text = message.get("text", "").strip()
            if text.startswith("綁定 ") or text.startswith("綁定:"):
                id_tag = text.replace("綁定:", "").replace("綁定 ", "").strip()
                cursor.execute("SELECT * FROM users WHERE id_tag = ?", (id_tag,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("UPDATE users SET card_number = ? WHERE id_tag = ?", (user_id, id_tag))
                    conn.commit()
                    reply_text = f"✅ 已成功綁定 {id_tag}"
                else:
                    reply_text = f"❌ 找不到使用者 IDTag：{id_tag}"

            elif text in ["取消綁定", "解除綁定"]:
                cursor.execute("SELECT id_tag FROM users WHERE card_number = ?", (user_id,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("UPDATE users SET card_number = NULL WHERE id_tag = ?", (row[0],))
                    conn.commit()
                    reply_text = f"🔓 已取消綁定：{row[0]}"
                else:
                    reply_text = "⚠️ 尚未綁定任何帳號"

            else:
                reply_text = "請輸入：\n綁定 {IDTag} 來綁定帳號\n取消綁定 來解除綁定"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_TOKEN}"
            }
            reply_payload = {
                "replyToken": event.get("replyToken"),
                "messages": [{"type": "text", "text": reply_text}]
            }
            requests.post("https://api.line.me/v2/bot/message/reply", headers=headers, data=json.dumps(reply_payload))

    return {"status": "ok"}



@app.get("/api/users/{id_tag}")
async def get_user(id_tag: str = Path(...)):
    cursor.execute("SELECT id_tag, name, department, card_number FROM users WHERE id_tag = ?", (id_tag,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "idTag": row[0],
        "name": row[1],
        "department": row[2],
        "cardNumber": row[3]
    }



@app.post("/api/reservations")
async def create_reservation(data: dict = Body(...)):
    cursor.execute('''
        INSERT INTO reservations (charge_point_id, id_tag, start_time, end_time, status)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        data["chargePointId"], data["idTag"],
        data["startTime"], data["endTime"], "active"
    ))
    conn.commit()
    return {"message": "Reservation created"}


@app.get("/api/reservations/{id}")
async def get_reservation(id: int = Path(...)):
    cursor.execute("SELECT * FROM reservations WHERE id = ?", (id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return {
        "id": row[0], "chargePointId": row[1], "idTag": row[2],
        "startTime": row[3], "endTime": row[4], "status": row[5]
    }

@app.put("/api/reservations/{id}")
async def update_reservation(id: int, data: dict = Body(...)):
    fields = []
    values = []
    for field in ["chargePointId", "idTag", "startTime", "endTime", "status"]:
        if field in data:
            fields.append(f"{field.lower()} = ?")
            values.append(data[field])
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    values.append(id)
    cursor.execute(f'''
        UPDATE reservations SET {", ".join(fields)} WHERE id = ?
    ''', values)
    conn.commit()
    return {"message": "Reservation updated"}

@app.delete("/api/reservations/{id}")
async def delete_reservation(id: int = Path(...)):
    cursor.execute("DELETE FROM reservations WHERE id = ?", (id,))
    conn.commit()
    return {"message": "Reservation deleted"}


@app.put("/api/users/{id_tag}")
async def update_user(id_tag: str = Path(...), data: dict = Body(...)):
    name = data.get("name")
    department = data.get("department")
    card_number = data.get("cardNumber")

    if not any([name, department, card_number]):
        raise HTTPException(status_code=400, detail="No fields to update")

    if name:
        cursor.execute("UPDATE users SET name = ? WHERE id_tag = ?", (name, id_tag))
    if department:
        cursor.execute("UPDATE users SET department = ? WHERE id_tag = ?", (department, id_tag))
    if card_number:
        cursor.execute("UPDATE users SET card_number = ? WHERE id_tag = ?", (card_number, id_tag))

    conn.commit()
    return {"message": "User updated successfully"}




@app.get("/api/summary/pricing-matrix")
async def get_pricing_matrix():
    cursor.execute("""
        SELECT season, day_type, start_time, end_time, price
        FROM pricing_rules
        ORDER BY season, day_type, start_time
    """)
    rows = cursor.fetchall()
    return [
        {
            "season": r[0],
            "day_type": r[1],
            "start_time": r[2],
            "end_time": r[3],
            "price": r[4]
        } for r in rows
    ]


@app.get("/api/summary/daily-by-chargepoint")
async def get_daily_by_chargepoint():
    cursor.execute("""
        SELECT strftime('%Y-%m-%d', start_timestamp) as day,
               charge_point_id,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY day, charge_point_id
        ORDER BY day ASC
    """)
    rows = cursor.fetchall()

    result_map = {}
    for day, cp_id, energy in rows:
        if day not in result_map:
            result_map[day] = {"period": day}
        result_map[day][cp_id] = round(energy / 1000, 3)  # kWh

    return list(result_map.values())


@app.get("/api/users/export")
async def export_users_csv():
    cursor.execute("SELECT id_tag, name, department, card_number FROM users")
    rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["idTag", "name", "department", "cardNumber"])
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=users.csv"
    })


@app.get("/api/reservations/export")
async def export_reservations_csv():
    cursor.execute("SELECT id, charge_point_id, id_tag, start_time, end_time, status FROM reservations")
    rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "chargePointId", "idTag", "startTime", "endTime", "status"])
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=reservations.csv"
    })

@app.get("/api/report/monthly")
async def generate_monthly_pdf(month: str):
    # 取得指定月份的起始與結束日期
    try:
        start_date = f"{month}-01"
        end_date = f"{month}-31"
    except:
        return {"error": "Invalid month format"}

    # 查詢交易資料
    cursor.execute("""
        SELECT id_tag, charge_point_id, SUM(meter_stop - meter_start) AS total_energy, COUNT(*) as txn_count
        FROM transactions
        WHERE start_timestamp >= ? AND start_timestamp <= ? AND meter_stop IS NOT NULL
        GROUP BY id_tag, charge_point_id
    """, (start_date, end_date))
    rows = cursor.fetchall()

    # PDF 產出
    buffer = io.BytesIO()
    p = canvas.Canvas(buffer)
    p.setTitle(f"Monthly Report - {month}")

    p.drawString(50, 800, f"🔌 Monthly Electricity Report - {month}")
    p.drawString(50, 780, "----------------------------------------")
    y = 760
    for row in rows:
        id_tag, cp_id, energy, count = row
        kwh = round(energy / 1000, 2)
        p.drawString(50, y, f"ID: {id_tag} | 樁: {cp_id} | 次數: {count} | 用電: {kwh} kWh")
        y -= 20
        if y < 50:
            p.showPage()
            y = 800

    if not rows:
        p.drawString(50, 760, "⚠️ 本月無任何有效交易紀錄")

    p.showPage()
    p.save()
    buffer.seek(0)

    return StreamingResponse(buffer, media_type="application/pdf", headers={
        "Content-Disposition": f"attachment; filename=monthly_report_{month}.pdf"
    })



@app.get("/api/holiday/{date}")
def get_holiday(date: str):
    try:
        year = date[:4]
        weekday = datetime.strptime(date, "%Y-%m-%d").weekday()
        is_weekend = weekday >= 5  # 週六(5)、週日(6)

        with open(f"holidays/{year}.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        holidays = data.get("days", {})
        found = holidays.get(date)

        description = found.get("description", "") if found else ""
        is_holiday_flag = found.get("isHoliday", False) if found else False

        # 假日判定邏輯：只要是週末且不是補班，或明確標示為 isHoliday:true，即為假日
        is_holiday = is_holiday_flag or (is_weekend and "補班" not in description)

        return {
            "date": date,
            "type": description or ("週末" if is_weekend else "平日"),
            "holiday": is_holiday,
            "festival": description if description not in ["週六", "週日", "補班", "平日"] else None
        }
    except FileNotFoundError:
        return {
            "date": date,
            "type": "查無年度資料",
            "holiday": False,
            "festival": None
        }
    except Exception as e:
        return {
            "date": date,
            "type": f"錯誤：{str(e)}",
            "holiday": False,
            "festival": None
        }



@app.get("/api/cards")
async def get_cards():
    cursor.execute("SELECT card_id, balance FROM cards")
    rows = cursor.fetchall()
    return [{"id": row[0], "card_id": row[0], "balance": row[1]} for row in rows]

@app.get("/api/charge-points")
async def list_charge_points():
    cursor.execute("SELECT id, charge_point_id, name, status, created_at FROM charge_points")
    rows = cursor.fetchall()
    return [
        {
            "id": r[0],
            "chargePointId": r[1],  # 注意：這是駝峰命名，對應前端
            "name": r[2],
            "status": r[3],
            "createdAt": r[4]
        } for r in rows
    ]

@app.post("/api/charge-points")
async def add_charge_point(data: dict = Body(...)):
    print("🔥 payload=", data)  # 新增，除錯用
    cp_id = data.get("chargePointId") or data.get("charge_point_id")
    name = data.get("name", "")
    status = (data.get("status") or "enabled").lower()
    if not cp_id:
        raise HTTPException(status_code=400, detail="chargePointId is required")
    try:
        cursor.execute(
            "INSERT INTO charge_points (charge_point_id, name, status) VALUES (?, ?, ?)",
            (cp_id, name, status)
        )
        conn.commit()
        print(f"✅ 新增白名單到資料庫: {cp_id}, {name}, {status}")  # 新增，除錯用
        cursor.execute("SELECT * FROM charge_points")
        print("所有白名單=", cursor.fetchall())  # 新增，除錯用
        return {"message": "新增成功"}
    except sqlite3.IntegrityError as e:
        print("❌ IntegrityError:", e)
        raise HTTPException(status_code=409, detail="充電樁已存在")
    except Exception as e:
        print("❌ 其他新增錯誤:", e)
        raise HTTPException(status_code=500, detail="內部錯誤")



@app.put("/api/charge-points/{cp_id}")
async def update_charge_point(cp_id: str = Path(...), data: dict = Body(...)):
    name = data.get("name")
    status = data.get("status")
    update_fields = []
    params = []
    if name is not None:
        update_fields.append("name = ?")
        params.append(name)
    if status is not None:
        update_fields.append("status = ?")
        params.append(status)
    if not update_fields:
        raise HTTPException(status_code=400, detail="無可更新欄位")
    params.append(cp_id)
    cursor.execute(f"UPDATE charge_points SET {', '.join(update_fields)} WHERE charge_point_id = ?", params)
    conn.commit()
    return {"message": "已更新"}

@app.delete("/api/charge-points/{cp_id}")
async def delete_charge_point(cp_id: str = Path(...)):
    cursor.execute("DELETE FROM charge_points WHERE charge_point_id = ?", (cp_id,))
    conn.commit()
    return {"message": "已刪除"}





@app.delete("/api/cards/{card_id}")
async def delete_card(card_id: str):
    cursor.execute("DELETE FROM cards WHERE card_id = ?", (card_id,))
    conn.commit()
    return {"message": f"Card {card_id} deleted"}


@app.put("/api/cards/{card_id}")
async def update_card(card_id: str, payload: dict):
    new_balance = payload.get("balance")
    if new_balance is None:
        raise HTTPException(status_code=400, detail="Missing balance")
    cursor.execute("UPDATE cards SET balance = ? WHERE card_id = ?", (new_balance, card_id))
    conn.commit()
    return {"message": f"Card {card_id} updated", "new_balance": new_balance}



@app.post("/api/cards/{card_id}/topup")
async def topup_card(card_id: str = Path(...), data: dict = Body(...)):
    amount = data.get("amount")
    if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
        raise HTTPException(status_code=400, detail="儲值金額錯誤")

    cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (card_id,))
    row = cursor.fetchone()

    if not row:
        # ⛳️ 沒有這張卡 → 幫他自動新增，初始餘額就是此次儲值金額
        cursor.execute("INSERT INTO cards (card_id, balance) VALUES (?, ?)", (card_id, amount))
        conn.commit()
        return {"status": "created", "card_id": card_id, "new_balance": round(amount, 2)}
    else:
        # ✅ 已存在 → 正常加值
        new_balance = row[0] + amount
        cursor.execute("UPDATE cards SET balance = ? WHERE card_id = ?", (new_balance, card_id))
        conn.commit()
        return {"status": "success", "card_id": card_id, "new_balance": round(new_balance, 2)}

@app.get("/api/version-check")
def version_check():
    return {"version": "✅ 偵錯用 main.py v1.0 已啟動成功"}




@app.get("/api/summary/daily-by-chargepoint-range")
async def get_daily_by_chargepoint_range(
    start: str = Query(...),
    end: str = Query(...)
):
    result_map = {}
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT strftime('%Y-%m-%d', start_timestamp) as day,
                   charge_point_id,
                   SUM(meter_stop - meter_start) as total_energy
            FROM transactions
            WHERE meter_stop IS NOT NULL
              AND start_timestamp >= ?
              AND start_timestamp <= ?
            GROUP BY day, charge_point_id
            ORDER BY day ASC
        """, (start, end))
        rows = cursor.fetchall()

    for day, cp_id, energy in rows:
        if day not in result_map:
            result_map[day] = {"period": day}
        result_map[day][cp_id] = round((energy or 0) / 1000, 3)

    return list(result_map.values())




from datetime import datetime, timedelta, timezone
from fastapi import Query

TZ_TAIPEI = timezone(timedelta(hours=8))

def _price_time_in_range(now_hm: str, start: str, end: str) -> bool:
    """HH:MM；處理跨日，且 start==end 視為全天。"""
    if start == end:
        return True
    if start < end:
        return start <= now_hm < end
    return now_hm >= start or now_hm < end  # 跨日，如 22:00~06:00


@app.get("/api/pricing/price-now")
def price_now(date: str | None = Query(None), time: str | None = Query(None)):
    now = datetime.now(TZ_TAIPEI)
    d = date or now.strftime("%Y-%m-%d")
    t = time or now.strftime("%H:%M")

    c = conn.cursor()
    c.execute("""
        SELECT start_time, end_time, price, COALESCE(label,'')
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time
    """, (d,))
    rows = c.fetchall()

    hits = [(s, e, float(p), lbl) for (s, e, p, lbl) in rows if _price_time_in_range(t, s, e)]
    if hits:
        s, e, price, lbl = max(hits, key=lambda r: r[2])  # 時段重疊取最高價
        return {"date": d, "time": t, "price": price, "label": lbl}

    # 找不到對應時段 → 回預設（你也可改成 0 或 404）
    return {"date": d, "time": t, "price": 6.0, "fallback": True}



# === Helper: 依時間算電價（找不到就回預設 6.0 元/kWh） ===
def _price_for_timestamp(ts_iso: str) -> float:
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(TZ_TAIPEI)
    except Exception:
        dt = datetime.now(TZ_TAIPEI)
    d = dt.strftime("%Y-%m-%d")
    t = dt.strftime("%H:%M")

    c = conn.cursor()
    c.execute("""
        SELECT start_time, end_time, price
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time
    """, (d,))
    rows = c.fetchall()

    hits = [float(p) for (s, e, p) in rows if _price_time_in_range(t, s, e)]
    return max(hits) if hits else 6.0





# 建表（若已存在會略過）
cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,        -- yyyy-mm-dd
    start_time TEXT,  -- HH:MM
    end_time TEXT,    -- HH:MM
    price REAL,
    label TEXT DEFAULT ''
)
''')
conn.commit()

# 取得指定日期設定
@app.get("/api/daily-pricing")
async def get_daily_pricing(date: str = Query(...)):
    cursor.execute('''
        SELECT id, date, start_time, end_time, price, label
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time ASC
    ''', (date,))
    rows = cursor.fetchall()
    return [
        {"id": r[0], "date": r[1], "startTime": r[2], "endTime": r[3], "price": r[4], "label": r[5]}
        for r in rows
    ]

# 新增設定
@app.post("/api/daily-pricing")
async def add_daily_pricing(data: dict = Body(...)):
    cursor.execute('''
        INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
        VALUES (?, ?, ?, ?, ?)
    ''', (data["date"], data["startTime"], data["endTime"], float(data["price"]), data.get("label", "")))
    conn.commit()
    return {"message": "新增成功"}

# 修改設定
@app.put("/api/daily-pricing/{id}")
async def update_daily_pricing(id: int = Path(...), data: dict = Body(...)):
    cursor.execute('''
        UPDATE daily_pricing_rules
        SET date = ?, start_time = ?, end_time = ?, price = ?, label = ?
        WHERE id = ?
    ''', (data["date"], data["startTime"], data["endTime"], float(data["price"]), data.get("label", ""), id))
    conn.commit()
    return {"message": "更新成功"}

# 刪除單筆
@app.delete("/api/daily-pricing/{id}")
async def delete_daily_pricing(id: int = Path(...)):
    cursor.execute("DELETE FROM daily_pricing_rules WHERE id = ?", (id,))
    conn.commit()
    return {"message": "已刪除"}

# 刪除某日期所有設定
@app.delete("/api/daily-pricing")
async def delete_daily_pricing_by_date(date: str = Query(...)):
    cursor.execute("DELETE FROM daily_pricing_rules WHERE date = ?", (date,))
    conn.commit()
    return {"message": f"已刪除 {date} 所有設定"}

# 複製設定到多日期
@app.post("/api/daily-pricing/duplicate")
async def duplicate_pricing(data: dict = Body(...)):
    source_date = data["sourceDate"]
    target_dates = data["targetDates"]  # list[str]

    cursor.execute("SELECT start_time, end_time, price, label FROM daily_pricing_rules WHERE date = ?", (source_date,))
    rows = cursor.fetchall()

    for target in target_dates:
        for s, e, p, lbl in rows:
            cursor.execute("""
                INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
                VALUES (?, ?, ?, ?, ?)
            """, (target, s, e, p, lbl))
    conn.commit()
    return {"message": f"已複製 {len(rows)} 筆設定至 {len(target_dates)} 天"}







# 🔹 補上 pricing_rules 表
cursor.execute('''
CREATE TABLE IF NOT EXISTS pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season TEXT,
    day_type TEXT,
    start_time TEXT,
    end_time TEXT,
    price REAL
)
''')

# 🔹 補上 reservations 表
cursor.execute('''
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    id_tag TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT
)
''')

conn.commit()
















@app.get("/")
async def root():
    return {"status": "API is running"}




# 刪除
@app.delete("/api/weekly-pricing/{id}")
async def delete_weekly_pricing(id: int = Path(...)):
    cursor.execute('DELETE FROM weekly_pricing WHERE id = ?', (id,))
    conn.commit()
    return {"message": "刪除成功"}


@app.post("/api/internal/meter_values")
async def add_meter_values(data: dict = Body(...)):
    required_fields = ["transaction_id", "charge_point_id", "connector_id", "timestamp", "value"]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        raise HTTPException(
            status_code=422,
            detail=f"❌ 缺少欄位: {', '.join(missing_fields)}"
        )

    try:
        cursor.execute('''
            INSERT INTO meter_values (
                transaction_id, charge_point_id, connector_id, timestamp, value, measurand, unit, context, format
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data["transaction_id"],
            data["charge_point_id"],
            data["connector_id"],
            data["timestamp"],
            data["value"],
            data.get("measurand", "Energy.Active.Import.Register"),
            data.get("unit", "Wh"),
            data.get("context", "Sample.Periodic"),
            data.get("format", "Raw")
        ))
        conn.commit()
        return {"message": "✅ Meter value added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❗資料庫寫入失敗: {str(e)}")



@app.post("/api/internal/mock-daily-pricing")
async def mock_daily_pricing(
    start: str = Query("2025-06-01", description="起始日期（格式 YYYY-MM-DD）"),
    days: int = Query(30, description="建立幾天的電價")
):
    try:
        base = datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid start date format. Use YYYY-MM-DD"})

    count = 0
    for i in range(days):
        day = base + timedelta(days=i)
        date_str = day.strftime("%Y-%m-%d")

        # 跳過已存在的
        cursor.execute('SELECT * FROM daily_pricing WHERE date = ?', (date_str,))
        if cursor.fetchone():
            continue

        cursor.execute('''
            INSERT INTO daily_pricing (date, price_per_kwh)
            VALUES (?, ?)
        ''', (date_str, 10.0))
        count += 1

    conn.commit()
    return {
        "message": f"✅ 已建立 {count} 筆日電價",
        "start": start,
        "days": days
    }



@app.post("/api/internal/recalculate-all-payments")
async def recalculate_all_payments():
    cursor.execute('DELETE FROM payments')
    conn.commit()

    cursor.execute('''
        SELECT transaction_id, charge_point_id, meter_start, meter_stop,
               start_timestamp, stop_timestamp, id_tag
        FROM transactions
        WHERE meter_stop IS NOT NULL
    ''')
    rows = cursor.fetchall()
    created = 0
    skipped = 0

    for row in rows:
        txn_id, cp_id, meter_start, meter_stop, start_ts, stop_ts, id_tag = row

        try:
            if not meter_start or not meter_stop or not start_ts or not stop_ts:
                skipped += 1
                continue

            start_obj = datetime.fromisoformat(start_ts)
            stop_obj = datetime.fromisoformat(stop_ts)

            if start_obj.date() != stop_obj.date():
                skipped += 1
                continue

            date_str = start_obj.strftime("%Y-%m-%d")
            t_start = start_obj.strftime("%H:%M")

            cursor.execute('''
                SELECT start_time, end_time, price FROM daily_pricing_rules
                WHERE date = ?
                ORDER BY start_time ASC
            ''', (date_str,))
            pricing_segments = cursor.fetchall()
            if not pricing_segments:
                skipped += 1
                continue

            price = None
            for seg_start, seg_end, seg_price in pricing_segments:
                if seg_start < seg_end:
                    if seg_start <= t_start < seg_end:
                        price = seg_price
                        break
                else:
                    if t_start >= seg_start or t_start < seg_end:
                        price = seg_price
                        break
            if price is None:
                skipped += 1
                continue

            # 成本計算
            kWh = (meter_stop - meter_start) / 1000
            base_fee = 20.0
            energy_fee = round(kWh * price, 2)
            overuse_fee = round(kWh * 2 if kWh > 5 else 0, 2)
            total_amount = round(base_fee + energy_fee + overuse_fee, 2)

            # 寫入 payments 表
            cursor.execute('''
                INSERT INTO payments (transaction_id, base_fee, energy_fee, overuse_fee, total_amount)
                VALUES (?, ?, ?, ?, ?)
            ''', (txn_id, base_fee, energy_fee, overuse_fee, total_amount))
            created += 1

            # 這裡重點修正：直接用 id_tag 對應卡片卡號
            card_id = id_tag
            cursor.execute('SELECT balance FROM cards WHERE card_id = ?', (card_id,))
            balance_row = cursor.fetchone()
            if balance_row:
                old_balance = balance_row[0]
                if old_balance >= total_amount:
                    new_balance = round(old_balance - total_amount, 2)
                    cursor.execute('UPDATE cards SET balance = ? WHERE card_id = ?', (new_balance, card_id))
                    print(f"💳 扣款成功：{card_id} | {old_balance} → {new_balance} 元 | txn={txn_id}")
                else:
                    print(f"⚠️ 餘額不足：{card_id} | 餘額={old_balance}，費用={total_amount}")
            else:
                print(f"⚠️ 找不到卡片餘額：card_id={card_id}")

        except Exception as e:
            print(f"❌ 錯誤 txn {txn_id} | idTag={id_tag} | {e}")
            skipped += 1

    conn.commit()
    return {
        "message": "✅ 已重新計算所有交易成本（daily_pricing_rules 分段並自動扣款）",
        "created": created,
        "skipped": skipped
    }






@app.get("/api/diagnostic/daily-pricing")
async def diagnostic_daily_pricing():
    cursor.execute('SELECT date, price_per_kwh FROM daily_pricing ORDER BY date ASC')
    rows = cursor.fetchall()
    return [{"date": row[0], "price": row[1]} for row in rows]

@app.get("/api/diagnostic/missing-cost-transactions")
async def missing_cost_transactions():
    cursor.execute('''
        SELECT transaction_id, charge_point_id, meter_start, meter_stop, start_timestamp
        FROM transactions
        WHERE meter_stop IS NOT NULL
    ''')
    rows = cursor.fetchall()

    missing = []

    for row in rows:
        txn_id, cp_id, meter_start, meter_stop, start_ts = row
        try:
            ts_obj = datetime.fromisoformat(start_ts)
            date_str = ts_obj.strftime("%Y-%m-%d")
            cursor.execute('SELECT price_per_kwh FROM daily_pricing WHERE date = ?', (date_str,))
            price_row = cursor.fetchone()
            if not price_row:
                missing.append({
                    "transaction_id": txn_id,
                    "date": date_str,
                    "chargePointId": cp_id,
                    "reason": "No daily pricing found"
                })
        except:
            missing.append({
                "transaction_id": txn_id,
                "date": start_ts,
                "chargePointId": cp_id,
                "reason": "Invalid timestamp format"
            })

    return missing


@app.post("/api/internal/mock-status")
async def mock_status(data: dict = Body(...)):  # 這裡要三個點
    cp_id = data["cp_id"]
    charging_point_status[cp_id] = {
        "connectorId": data.get("connector_id", 1),
        "status": data.get("status", "Available"),
        "timestamp": data.get("timestamp") or datetime.utcnow().isoformat()
    }
    return {"message": f"Mock status for {cp_id} 已注入"}


@app.get("/api/dashboard/rank_by_idTag")
async def get_dashboard_top_idtags(limit: int = Query(10)):
    cursor.execute("""
        SELECT id_tag,
               COUNT(*) as transaction_count,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY id_tag
        ORDER BY total_energy DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()

    return [
        {
            "idTag": row[0],
            "transactionCount": row[1],
            "totalEnergy": round((row[2] or 0) / 1000, 3)  # 換算成 kWh
        } for row in rows
    ]

@app.post("/api/internal/duplicate-daily-pricing")
async def duplicate_by_rule(data: dict = Body(...)):
    """
    根據 weekday/saturday/sunday 套用規則，套用至整月符合條件的日期
    請求內容：
    {
        "type": "weekday" | "saturday" | "sunday",
        "rules": [...],
        "start": "2025-07-01"
    }
    """
    try:
        type = data["type"]
        rules = data["rules"]
        start = datetime.strptime(data["start"], "%Y-%m-%d")
        days_in_month = (start.replace(month=start.month % 12 + 1, day=1) - timedelta(days=1)).day

        inserted = 0
        for d in range(1, days_in_month + 1):
            current = datetime(start.year, start.month, d)
            weekday = current.weekday()  # 0=Mon, ..., 6=Sun

            # 篩選符合類型的日期
            if (type == "weekday" and weekday < 5) or \
               (type == "saturday" and weekday == 5) or \
               (type == "sunday" and weekday == 6):
                date_str = current.strftime("%Y-%m-%d")
                # 先刪除既有設定
                cursor.execute("DELETE FROM daily_pricing_rules WHERE date = ?", (date_str,))
                for r in rules:
                    cursor.execute("""
                        INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
                        VALUES (?, ?, ?, ?, ?)
                    """, (date_str, r["startTime"], r["endTime"], float(r["price"]), r["label"]))
                inserted += 1

        conn.commit()
        return {"message": f"✅ 套用完成，共更新 {inserted} 天"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



from fastapi import HTTPException

@app.post("/api/charge-points/{charge_point_id}/stop")
async def stop_transaction_by_charge_point(charge_point_id: str):
    print(f"🟢【API呼叫】收到停止充電API請求, charge_point_id = {charge_point_id}")

    norm_id = _normalize_cp_id(charge_point_id)
    cp = connected_charge_points.get(norm_id)
    if not cp:
        print(f"🔴【API異常】找不到連線中的充電樁：{norm_id}")
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{norm_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))}
        )

    # 取進行中交易
    with sqlite3.connect(DB_FILE) as lconn:
        c = lconn.cursor()
        c.execute("""
            SELECT transaction_id FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """, (norm_id,))
        r = c.fetchone()
        if not r:
            print(f"🔴【API異常】無進行中交易 charge_point_id={norm_id}")
            raise HTTPException(status_code=400, detail="⚠️ 無進行中交易")
        transaction_id = int(r[0])

    # 等待 StopTransaction 回覆
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    pending_stop_transactions[str(transaction_id)] = fut

    # 發送 RemoteStopTransaction（新版類名，無 Payload）
    print(f"🟢【API呼叫】發送 RemoteStopTransaction 給充電樁")
    req = call.RemoteStopTransaction(transaction_id=transaction_id)
    resp = await cp.call(req)
    print(f"🟢【API回應】呼叫 RemoteStopTransaction 完成，resp={resp}")

    try:
        stop_result = await asyncio.wait_for(fut, timeout=10)
        print(f"🟢【API回應】StopTransaction 完成: {stop_result}")
        return {"message": "充電已停止", "transaction_id": transaction_id, "stop_result": stop_result}
    except asyncio.TimeoutError:
        print(f"🔴【API異常】等待 StopTransaction 超時")
        return JSONResponse(status_code=504, content={"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"})
    finally:
        pending_stop_transactions.pop(str(transaction_id), None)










@app.get("/debug/charge-points")
async def debug_ids():
    cursor.execute("SELECT charge_point_id FROM charge_points")
    return [row[0] for row in cursor.fetchall()]

@app.get("/api/debug/connected-cp")
def debug_connected_cp():
    return list(connected_charge_points.keys())

@app.post("/api/charge-points/{charge_point_id}/stop")
async def stop_transaction_by_charge_point(charge_point_id: str):
    print(f"🟢【API呼叫】收到停止充電API請求, charge_point_id = {charge_point_id}")
    cp = connected_charge_points.get(charge_point_id)

    if not cp:
        print(f"🔴【API異常】找不到連線中的充電樁：{charge_point_id}")
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{charge_point_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))}
        )
    # 查詢進行中的 transaction_id
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT transaction_id FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """, (charge_point_id,))
        row = cursor.fetchone()
        if not row:
            print(f"🔴【API異常】無進行中交易 charge_point_id={charge_point_id}")
            raise HTTPException(status_code=400, detail="⚠️ 無進行中交易")
        transaction_id = row[0]
        print(f"🟢【API呼叫】找到進行中交易 transaction_id={transaction_id}")

    # 新增同步等待機制
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    pending_stop_transactions[str(transaction_id)] = fut

    # 發送 RemoteStopTransaction
    print(f"🟢【API呼叫】發送 RemoteStopTransaction 給充電樁")
    req = call.RemoteStopTransaction(transaction_id=transaction_id)
    resp = await cp.call(req)
    print(f"🟢【API回應】呼叫 RemoteStopTransaction 完成，resp={resp}")

    # 等待 StopTransaction 被觸發（最多 10 秒）
    try:
        stop_result = await asyncio.wait_for(fut, timeout=10)
        print(f"🟢【API回應】StopTransaction 完成: {stop_result}")
        return {"message": "充電已停止", "transaction_id": transaction_id, "stop_result": stop_result}
    except asyncio.TimeoutError:
        print(f"🔴【API異常】等待 StopTransaction 超時")
        return JSONResponse(status_code=504, content={"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"})
    finally:
        pending_stop_transactions.pop(str(transaction_id), None)


from fastapi import Query
from fastapi.responses import JSONResponse

@app.get("/api/charging_status")
async def charging_status(cp_id: str = Query(..., description="Charge Point ID")):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # 查詢最新功率 (W)
            cur.execute("""
                SELECT value FROM meter_values
                WHERE charge_point_id=? AND measurand='Power.Active.Import'
                ORDER BY timestamp DESC LIMIT 1
            """, (cp_id,))
            power_row = cur.fetchone()

            # 查詢最新累積電量 (Wh)
            cur.execute("""
                SELECT value FROM meter_values
                WHERE charge_point_id=? AND measurand='Energy.Active.Import.Register'
                ORDER BY timestamp DESC LIMIT 1
            """, (cp_id,))
            energy_row = cur.fetchone()

            return JSONResponse({
                "power": round(power_row["value"], 2) if power_row else 0.0,
                "kwh": round(energy_row["value"] / 1000.0, 3) if energy_row else 0.0  # 轉為 kWh
            })

    except Exception as e:
        logging.exception(f"❌ 查詢 charging_status 發生錯誤：{e}")
        return JSONResponse({"power": 0.0, "kwh": 0.0}, status_code=500)






@app.get("/api/devtools/last-transactions")
def last_transactions():
    import sqlite3
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT transaction_id, charge_point_id, id_tag, start_timestamp, meter_start,
                stop_timestamp, meter_stop, reason
            FROM transactions
            ORDER BY start_timestamp DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()
        result = [
            {
                "transaction_id": row[0],
                "charge_point_id": row[1],
                "id_tag": row[2],
                "start_time": row[3],
                "meter_start": row[4],
                "stop_time": row[5],
                "meter_stop": row[6],
                "reason": row[7]
            }
            for row in rows
        ]
        return {"last_transactions": result}






if __name__ == "__main__":
    import os, uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

