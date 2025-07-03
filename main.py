import sys
sys.path.insert(0, "./")

import json
import os
import uuid
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Query, Body, Path, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from dateutil.parser import parse as parse_date  # ✅ 加入這行

import uvicorn
from websockets.server import serve
from websockets.exceptions import ConnectionClosedOK

from werkzeug.security import generate_password_hash, check_password_hash

from ocpp.v16 import call
from ocpp.v16.call_result import (
    BootNotificationPayload,
    HeartbeatPayload,
    MeterValuesPayload,
    StartTransactionPayload,
    StopTransactionPayload,
    StatusNotificationPayload
)
from ocpp.v16.enums import Action, RegistrationStatus
from ocpp.routing import on
from threading import Thread



# 建立 FastAPI app
app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://.*\.onrender\.com",  # ✅ 僅保留 regex，前面加 r 原始字串
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# 初始化狀態儲存
charging_point_status = {}

logging.basicConfig(level=logging.INFO)

# HTTP 端點：查詢狀態
@app.get("/status/{cp_id}")
async def get_status(cp_id: str):
    return JSONResponse(charging_point_status.get(cp_id, {}))

# 假設的授權 API
@app.post("/authorize/{cp_id}")
async def authorize(cp_id: str, badge_id: str = Body(..., embed=True)):
    # 查 DB，回傳 AuthorizePayload
    loop = asyncio.get_event_loop()
    return {"idTagInfo": {"status": "Accepted"}}

# 以下開始 WebSocket 與 OCPP 部分



logging.basicConfig(level=logging.INFO)
# 啟用 logging
logging.basicConfig(level=logging.INFO)

# 用於記錄所有充電樁的狀態
charging_point_status = {}


# 初始化 SQLite 資料庫
DB_FILE = "ocpp_data.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()


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


# ⚠️ 請注意：這會清空原本 meter_values 資料
cursor.execute('DROP TABLE IF EXISTS meter_values')

cursor.execute('''
CREATE TABLE meter_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER,
    charge_point_id TEXT,
    connector_id INTEGER,
    timestamp TEXT,
    value REAL,
    measurand TEXT,
    unit TEXT,
    context TEXT,
    format TEXT
)
''')


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




from ocpp.v16 import ChargePoint as OcppChargePoint

class ChargePoint(OcppChargePoint):

    @on(Action.BootNotification)
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        logging.info(f"🔌 BootNotification | 模型={charge_point_model} | 廠商={charge_point_vendor}")
        return BootNotificationPayload(
            current_time=now.isoformat(),
            interval=10,
            status="Accepted"
        )

    @on(Action.Heartbeat)
    async def on_heartbeat(self):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        logging.info(f"❤️ Heartbeat | CP={self.id}")
        return HeartbeatPayload(current_time=now.isoformat())

    @on(Action.Authorize)
    async def on_authorize(self, id_tag, **kwargs):
        cursor.execute("SELECT status, valid_until FROM id_tags WHERE id_tag = ?", (id_tag,))
        row = cursor.fetchone()
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
            logging.info(f"🔎 驗證有效期限valid_until={valid_until_dt.isoformat()} / now={now.isoformat()}")
            status = "Accepted" if status_db == "Accepted" and valid_until_dt > now else "Expired"
        logging.info(f"🆔 Authorize | idTag: {id_tag} | 查詢結果: {status}")
        return call.AuthorizePayload(id_tag_info={"status": status})

    @on(Action.StartTransaction)
    async def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        cursor.execute("SELECT status, valid_until FROM id_tags WHERE id_tag = ?", (id_tag,))
        row = cursor.fetchone()
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
            logging.info(f"🔎 驗證有效期限valid_until={valid_until_dt.isoformat()} / now={now.isoformat()}")
            status = "Accepted" if status_db == "Accepted" and valid_until_dt > now else "Expired"

        # 驗證是否有符合條件的有效預約
        now = datetime.utcnow().isoformat()
        cursor.execute('''
        SELECT id FROM reservations
        WHERE charge_point_id = ? AND id_tag = ? AND status = 'active'
        AND start_time <= ? AND end_time >= ?
        ''', (self.id, id_tag, now, now))
        res = cursor.fetchone()

        if not res:
            logging.warning(f"⛔ StartTransaction 拒絕 | 無有效預約")
            return StartTransaction(transaction_id=0, id_tag_info={"status": "Expired"})
        else:
            cursor.execute("UPDATE reservations SET status = 'completed' WHERE id = ?", (res[0],))
            conn.commit()

        # ✅ 新增：餘額檢查
        cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        card = cursor.fetchone()
        if not card:
            logging.warning(f"⛔ 無此卡片帳戶資料，StartTransaction 拒絕")
            return StartTransaction(transaction_id=0, id_tag_info={"status": "Invalid"})

        balance = card[0]
        if balance < 10:
            logging.warning(f"💳 餘額不足：{balance} 元，StartTransaction 拒絕")
            return StartTransaction(transaction_id=0, id_tag_info={"status": "Blocked"})

        # 🟢 原本的交易建立邏輯繼續執行
        transaction_id = int(datetime.utcnow().timestamp() * 1000)
        ...

        if status != "Accepted":
            logging.warning(f"⛔ StartTransaction 拒絕 | idTag={id_tag} | status={status}")
            return StartTransaction(transaction_id=0, id_tag_info={"status": status})

        # ✅ 新增：確認卡片餘額是否足夠（預設最低 10 元才能啟動）
        cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        balance_row = cursor.fetchone()
        if not balance_row or balance_row[0] < 10:
            logging.warning(f"⛔ StartTransaction 拒絕 | idTag={id_tag} | 餘額不足 {balance_row[0] if balance_row else '無資料'} 元")
            return StartTransaction(transaction_id=0, id_tag_info={"status": "Blocked"})


        transaction_id = int(datetime.utcnow().timestamp() * 1000)
        cursor.execute('''
            INSERT INTO transactions (
                transaction_id, charge_point_id, connector_id, id_tag,
                meter_start, start_timestamp, meter_stop, stop_timestamp, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            transaction_id, self.id, connector_id, id_tag,
            meter_start, timestamp, None, None, None
        ))
        conn.commit()
        logging.info(f"🚗 StartTransaction 成功 | CP={self.id} | idTag={id_tag} | transactionId={transaction_id}")
        return StartTransactionPayload(
            transaction_id=transaction_id,
            id_tag_info={"status": "Accepted"}
        )

    @on(Action.MeterValues)
    async def on_meter_values(self, connector_id, meter_value, **kwargs):
        for entry in meter_value:
            timestamp = entry.get("timestamp")
            for sampled_value in entry.get("sampled_value", []):
                value = float(sampled_value.get("value"))
                measurand = sampled_value.get("measurand", "Energy.Active.Import.Register")
                unit = sampled_value.get("unit", "Wh")
                cursor.execute('''
                    INSERT INTO meter_values (charge_point_id, connector_id, timestamp, measurand, value, unit)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    self.id, connector_id, timestamp, measurand, value, unit
                ))
        conn.commit()
        logging.info(f"📈 MeterValues | CP={self.id} | 筆數={len(meter_value)}")
        return MeterValuesPayload()


    @on(Action.StopTransaction)
    async def on_stop_transaction(self, transaction_id, meter_stop, timestamp, id_tag, reason, **kwargs):
        # 更新交易紀錄
        cursor.execute('''
            UPDATE transactions
            SET meter_stop = ?, stop_timestamp = ?, reason = ?
            WHERE transaction_id = ?
        ''', (meter_stop, timestamp, reason, transaction_id))
        conn.commit()

        # 查詢啟始資料
        cursor.execute("SELECT meter_start, start_timestamp FROM transactions WHERE transaction_id = ?", (transaction_id,))
        row = cursor.fetchone()
        if not row:
            logging.warning("❌ StopTransaction | 查無交易記錄")
            return StopTransaction(id_tag_info={"status": "Expired"})

        meter_start, start_time_str = row
        start_time = datetime.fromisoformat(start_time_str)
        stop_time = datetime.fromisoformat(timestamp)
        kwh = max((meter_stop - meter_start) / 1000, 0)

        # 計算時間點的電價
        def is_summer(dt):
            return datetime(dt.year, 6, 1) <= dt <= datetime(dt.year, 9, 30)

        def is_holiday(dt):
            return dt.weekday() >= 5

        def get_price(dt):
            season = "summer" if is_summer(dt) else "non_summer"
            day_type = "holiday" if is_holiday(dt) else "weekday"
            t = dt.time().strftime("%H:%M")

        # 新增例外處理：00:00–00:00 表示全天
            cursor.execute('''
                SELECT price FROM pricing_rules
                WHERE season = ? AND day_type = ? AND start_time = '00:00' AND end_time = '00:00'
                ORDER BY start_time DESC LIMIT 1
            ''', (season, day_type))
            full_day = cursor.fetchone()
            if full_day:
                return full_day[0]

            cursor.execute('''
                SELECT price FROM pricing_rules
                WHERE season = ? AND day_type = ? AND (
                    (start_time <= end_time AND start_time <= ? AND end_time > ?) OR
                    (start_time > end_time AND (? >= start_time OR ? < end_time))
                )
                ORDER BY start_time DESC LIMIT 1
            ''', (season, day_type, t, t, t, t))
            row = cursor.fetchone()
            return row[0] if row else 0

        price = get_price(start_time)
        cost = round(kwh * price, 2)

        # 扣除卡片餘額
        cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        card = cursor.fetchone()
        if card:
            new_balance = round(card[0] - cost, 2)
            if new_balance < 0:
                new_balance = 0
            cursor.execute("UPDATE cards SET balance = ? WHERE card_id = ?", (new_balance, id_tag))
            conn.commit()
            logging.info(f"💳 扣款完成 | 卡片={id_tag} | 原餘額={card[0]} | 扣款={cost} 元 | 剩餘={new_balance} 元")

            # 儲存扣款紀錄
            cursor.execute('''
                INSERT INTO payments (transaction_id, id_tag, amount, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (transaction_id, id_tag, cost, timestamp))
            conn.commit()

            # 若餘額過低，自動通知
            if new_balance < 100:
                logging.info(f"⚠️ 卡片 {id_tag} 餘額僅剩 {new_balance} 元")

        logging.info(f"🛑 StopTransaction 成功 | CP={self.id} | idTag={id_tag} | transactionId={transaction_id}")
        return StopTransactionPayload(id_tag_info={"status": "Accepted"})





# 建立扣款紀錄表（修正後）
cursor.execute('DROP TABLE IF EXISTS payments')
cursor.execute('''
CREATE TABLE payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER,
    base_fee REAL,
    energy_fee REAL,
    overuse_fee REAL,
    total_amount REAL
)
''')





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
    cursor.execute("SELECT transaction_id, id_tag, amount, timestamp FROM payments ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    return [
        {
            "transactionId": r[0],
            "idTag": r[1],
            "amount": round(r[2], 2),
            "timestamp": r[3]
        } for r in rows
    ]


...

@on(Action.StatusNotification)
async def on_status_notification(self, connector_id, status, timestamp, **kwargs):
    cursor.execute('''
        INSERT INTO status_logs (charge_point_id, connector_id, status, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (self.id, connector_id, status, timestamp))
    conn.commit()

    # ✅ 新增：更新全域狀態變數
    charging_point_status[self.id] = {
        "connectorId": connector_id,
        "status": status,
        "timestamp": timestamp
    }

    logging.info(f"📡 StatusNotification | CP={self.id} | connector={connector_id} | status={status}")
    return StatusNotificationPayload()



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




@app.get("/api/transactions/{transaction_id}/cost")
async def calculate_transaction_cost(transaction_id: int):
    try:
        return compute_transaction_cost(transaction_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

   

@app.get("/api/transactions/cost-summary")
async def transaction_cost_summary():
    query = """
        SELECT t.transaction_id, (t.meter_stop - t.meter_start)/1000.0 as kWh,
               p.base_fee, p.energy_fee, p.overuse_fee, p.total_amount
        FROM transactions t
        JOIN payments p ON t.transaction_id = p.transaction_id
        WHERE t.meter_stop IS NOT NULL
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append({
            "transactionId": row[0],
            "totalKWh": round(row[1], 3),
            "basicFee": row[2],
            "energyCost": row[3],
            "overuseFee": row[4],
            "totalCost": row[5]
        })

    return result




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





from fastapi.responses import StreamingResponse
import io
import csv

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

from fastapi import HTTPException, Body, Path



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


# ✅ 新增：即時電量查詢 API
@app.get("/api/charge-points/{charge_point_id}/latest-meter")
async def get_latest_meter_value(charge_point_id: str):
    query = '''
        SELECT connector_id, timestamp, measurand, value, unit
        FROM meter_values
        WHERE charge_point_id = ?
        ORDER BY datetime(timestamp) DESC
        LIMIT 1
    '''
    cursor.execute(query, (charge_point_id,))
    row = cursor.fetchone()

    if row:
        return {
            "chargePointId": charge_point_id,
            "connectorId": row[0],
            "timestamp": row[1],
            "measurand": row[2],
            "value": float(row[3]),
            "unit": row[4]
        }
    else:
        raise HTTPException(status_code=404, detail="No meter values found.")


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
    cursor.execute("DELETE FROM id_tags WHERE id_tag = ?", (id_tag,))
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



@app.get("/api/summary/top")
async def get_top_consumers(
    group_by: str = Query("idTag"),
    limit: int = Query(10)
):
    if group_by == "idTag":
        group_field = "id_tag"
    elif group_by == "chargePointId":
        group_field = "charge_point_id"
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid group_by. Use 'idTag' or 'chargePointId'."})

    cursor.execute(f"""
        SELECT {group_field} as key,
               COUNT(*) as transaction_count,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY {group_field}
        ORDER BY total_energy DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append({
            "group": row[0],
            "transactionCount": row[1],
            "totalEnergy": row[2] or 0
        })

    return JSONResponse(content=result)  

# WebSocket 充電樁接入時呼叫的處理函式
async def on_connect(websocket, path):
    cp_id = path.strip("/")
    cp = ChargePoint(cp_id, websocket)
    logging.info(f"🔌 充電樁已連線：{cp_id}")
    await cp.start()

# 啟動 WebSocket Server
async def start_websocket():
    server = await serve(
        on_connect,
        "0.0.0.0",  # 可依需求改為 localhost
        9000,
        subprotocols=["ocpp1.6"]
    )
    logging.info("✅ WebSocket Server 已啟動 ws://0.0.0.0:9000")
    await server.wait_closed()



import requests


from datetime import datetime, timedelta
import threading

# 每週定時通知任務
def weekly_notify_task():
    import time
    while True:
        now = datetime.now()
        # 只在每週一上午 9:00 傳送
        if now.weekday() == 0 and now.hour == 9 and now.minute == 0:
            try:
                cursor.execute("""
                    SELECT id_tag, SUM(meter_stop - meter_start) as total_energy
                    FROM transactions
                    WHERE meter_stop IS NOT NULL
                    AND start_timestamp >= datetime('now', '-7 days')
                    GROUP BY id_tag
                    ORDER BY total_energy DESC
                    LIMIT 5
                """)
                rows = cursor.fetchall()
                if rows:
                    message = "📊 一週用電排行（依 idTag）:\n"
                    for idx, (id_tag, energy) in enumerate(rows, start=1):
                        message += f"{idx}. {id_tag}：{round(energy/1000, 2)} kWh\n"
                    logging.info(f"📊 用電排行通知（模擬）：\n{message}")
            except Exception as e:
                logging.error(f"📉 用電排行通知錯誤：{e}")
        time.sleep(60)  # 每分鐘檢查一次是否符合發送條件



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



# Thread 啟動 WebSocket 與 FastAPI 共存
@app.on_event("startup")
async def start_ws_server():
    def run_ws():
        asyncio.run(start_websocket())
    Thread(target=run_ws, daemon=True).start()

    def run_notify():
        weekly_notify_task()
    Thread(target=run_notify, daemon=True).start()

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


@app.get("/api/users")
async def list_users():
    cursor.execute("SELECT id_tag, name, department, card_number FROM users")
    rows = cursor.fetchall()
    return JSONResponse(content=[
        {"idTag": row[0], "name": row[1], "department": row[2], "cardNumber": row[3]} for row in rows
    ])

cursor.execute('''
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    id_tag TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT  -- 'active', 'cancelled', 'completed'
)
''')
conn.commit()


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


@app.post("/api/users")
async def add_user(data: dict = Body(...)):
    id_tag = data.get("idTag")
    name = data.get("name")
    department = data.get("department")
    card_number = data.get("cardNumber")

    if not id_tag:
        raise HTTPException(status_code=400, detail="idTag is required")

    try:
        cursor.execute('''
            INSERT INTO users (id_tag, name, department, card_number)
            VALUES (?, ?, ?, ?)
        ''', (id_tag, name, department, card_number))
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="User already exists")
    return {"message": "User added successfully"}

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

@app.get("/api/reservations")
async def list_reservations():
    cursor.execute("SELECT * FROM reservations")
    rows = cursor.fetchall()
    return [{
        "id": r[0], "chargePointId": r[1], "idTag": r[2],
        "startTime": r[3], "endTime": r[4], "status": r[5]
    } for r in rows]

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

@app.delete("/api/users/{id_tag}")
async def delete_user(id_tag: str = Path(...)):
    cursor.execute("DELETE FROM users WHERE id_tag = ?", (id_tag,))
    conn.commit()
    return {"message": "User deleted successfully"}


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


from fastapi.responses import StreamingResponse
import io
import csv

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


from fastapi.responses import StreamingResponse
import io
import csv

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


from fastapi.responses import StreamingResponse
import io
from reportlab.pdfgen import canvas

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



from fastapi import HTTPException

@app.get("/api/cards")
async def get_cards():
    cursor.execute("SELECT card_id, balance FROM cards")
    rows = cursor.fetchall()
    return [{"id": row[0], "card_id": row[0], "balance": row[1]} for row in rows]

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



@app.get("/api/cards/{card_id}")
async def get_card_balance(card_id: str):
    cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (card_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="卡片不存在")
    return {"cardId": card_id, "balance": round(row[0], 2)}



@app.get("/api/version-check")
def version_check():
    return {"version": "✅ 偵錯用 main.py v1.0 已啟動成功"}



...


@app.get("/api/dashboard/summary")
async def get_dashboard_summary():
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE meter_stop IS NULL")
        charging_count = cursor.fetchone()[0] or 0
    except:
        charging_count = 0

    try:
        cursor.execute("""
            SELECT SUM(value) FROM (
                SELECT MAX(id) as latest_id FROM meter_values GROUP BY charge_point_id
            ) AS latest_ids
            JOIN meter_values ON meter_values.id = latest_ids.latest_id
        """)
        total_power = cursor.fetchone()[0] or 0
    except:
        total_power = 0

    try:
        cursor.execute("""
            SELECT SUM(meter_stop - meter_start) FROM transactions
            WHERE DATE(start_timestamp) = ? AND meter_stop IS NOT NULL
        """, (today,))
        energy_today = cursor.fetchone()[0] or 0
    except:
        energy_today = 0

    return {
        "chargingCount": charging_count,
        "totalPowerW": total_power,
        "energyTodayKWh": round(energy_today / 1000, 2)
    }






@app.get("/api/dashboard/trend")
async def dashboard_trend(group_by: str = Query("day")):
    try:
        if group_by == "day":
            date_expr = "strftime('%Y-%m-%d', start_timestamp)"
        elif group_by == "week":
            date_expr = "strftime('%Y-W%W', start_timestamp)"
        else:
            raise HTTPException(status_code=400, detail="group_by must be 'day' or 'week'")

        cursor.execute(f"""
            SELECT {date_expr} as period,
                   SUM(meter_stop - meter_start) / 1000.0 as total_kwh
            FROM transactions
            WHERE meter_stop IS NOT NULL
            GROUP BY period
            ORDER BY period ASC
        """)
        rows = cursor.fetchall()

        return [
            {
                "period": row[0],
                "kWh": round(row[1] or 0, 2)
            } for row in rows
        ]
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"伺服器錯誤：{str(e)}")



@app.get("/api/summary/daily-by-chargepoint-range")
async def get_daily_by_chargepoint_range(
    start: str = Query(...),
    end: str = Query(...)
):
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

    result_map = {}
    for day, cp_id, energy in rows:
        if day not in result_map:
            result_map[day] = {"period": day}
        result_map[day][cp_id] = round(energy / 1000, 3)  # kWh

    return list(result_map.values())


from fastapi import Query



# 新增：每日電價設定 daily_pricing_rules API 與資料表
from fastapi import Body, Path

# 建立資料表
cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,               -- yyyy-mm-dd
    start_time TEXT,         -- HH:MM
    end_time TEXT,           -- HH:MM
    price REAL,
    label TEXT DEFAULT ''
)
''')
conn.commit()

# 取得指定日期的設定
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
        {
            "id": r[0], "date": r[1], "startTime": r[2],
            "endTime": r[3], "price": r[4], "label": r[5]
        } for r in rows
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

# 刪除設定
@app.delete("/api/daily-pricing/{id}")
async def delete_daily_pricing(id: int = Path(...)):
    cursor.execute("DELETE FROM daily_pricing_rules WHERE id = ?", (id,))
    conn.commit()
    return {"message": "已刪除"}

# 複製到多個日期
@app.post("/api/daily-pricing/duplicate")
async def duplicate_pricing(data: dict = Body(...)):
    source_date = data["sourceDate"]
    target_dates = data["targetDates"]  # list of yyyy-mm-dd

    cursor.execute("SELECT start_time, end_time, price, label FROM daily_pricing_rules WHERE date = ?", (source_date,))
    rows = cursor.fetchall()

    for target in target_dates:
        for r in rows:
            cursor.execute("""
                INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
                VALUES (?, ?, ?, ?, ?)
            """, (target, r[0], r[1], r[2], r[3]))

    conn.commit()
    return {"message": f"已複製 {len(rows)} 筆設定至 {len(target_dates)} 天"}


@app.get("/")
async def root():
    return {"status": "API is running"}


from fastapi import HTTPException

from fastapi import Query

# 取得
@app.get("/api/weekly-pricing")
async def get_weekly_pricing(season: str = Query(...)):
    cursor.execute('''
        SELECT id, season, weekday, type, start_time, end_time, price
        FROM weekly_pricing
        WHERE season = ?
        ORDER BY weekday, start_time
    ''', (season,))
    rows = cursor.fetchall()
    return [
        {
            "id": r[0], "season": r[1], "weekday": r[2],
            "type": r[3], "startTime": r[4], "endTime": r[5], "price": r[6]
        } for r in rows
    ]

# 新增
@app.post("/api/weekly-pricing")
async def add_weekly_pricing(data: dict = Body(...)):
    cursor.execute('''
        INSERT INTO weekly_pricing (season, weekday, type, start_time, end_time, price)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        data["season"], data["weekday"], data["type"],
        data["startTime"], data["endTime"], float(data["price"])
    ))
    conn.commit()
    return {"message": "新增成功"}

# 更新
@app.put("/api/weekly-pricing/{id}")
async def update_weekly_pricing(id: int = Path(...), data: dict = Body(...)):
    cursor.execute('''
        UPDATE weekly_pricing
        SET season = ?, weekday = ?, type = ?, start_time = ?, end_time = ?, price = ?
        WHERE id = ?
    ''', (
        data["season"], data["weekday"], data["type"],
        data["startTime"], data["endTime"], float(data["price"]), id
    ))
    conn.commit()
    return {"message": "更新成功"}

# 刪除
@app.delete("/api/weekly-pricing/{id}")
async def delete_weekly_pricing(id: int = Path(...)):
    cursor.execute('DELETE FROM weekly_pricing WHERE id = ?', (id,))
    conn.commit()
    return {"message": "刪除成功"}

@app.post("/api/internal/meter_values")
async def add_meter_values(data: dict = Body(...)):
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
        return {"message": "Meter value added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/payments")
async def add_payment(data: dict = Body(...)):
    try:
        cursor.execute('''
            INSERT INTO payments (transaction_id, id_tag, amount, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (
            data["transaction_id"],
            data.get("id_tag", "TEST123"),  # 預設卡片 ID
            data["total_amount"],
            datetime.utcnow().isoformat()
        ))
        conn.commit()
        return {"message": "Payment added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


from fastapi import Query

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
    # 清除舊資料
    cursor.execute('DELETE FROM payments')
    conn.commit()

    cursor.execute('SELECT transaction_id, charge_point_id, meter_start, meter_stop, start_timestamp, stop_timestamp FROM transactions WHERE meter_stop IS NOT NULL')
    rows = cursor.fetchall()
    created = 0
    skipped = 0

    for row in rows:
        txn_id, cp_id, meter_start, meter_stop, start_ts, stop_ts = row
        try:
            # 安全轉換日期字串
            ts_obj = datetime.fromisoformat(start_ts)
            date_str = ts_obj.strftime("%Y-%m-%d")

            cursor.execute('SELECT price_per_kwh FROM daily_pricing WHERE date = ?', (date_str,))
            price_row = cursor.fetchone()

            if not price_row:
                print(f"⚠️ Skipped txn {txn_id} | 日期 {date_str} 找不到電價 (原始時間: {start_ts})")
                skipped += 1
                continue

            price_per_kwh = price_row[0]
            kWh = (meter_stop - meter_start) / 1000
            base_fee = 20.0
            energy_fee = round(kWh * price_per_kwh, 2)
            overuse_fee = round(kWh * 2 if kWh > 5 else 0, 2)
            total_amount = round(base_fee + energy_fee + overuse_fee, 2)

            cursor.execute('''
                INSERT INTO payments (transaction_id, base_fee, energy_fee, overuse_fee, total_amount)
                VALUES (?, ?, ?, ?, ?)
            ''', (txn_id, base_fee, energy_fee, overuse_fee, total_amount))
            created += 1
            print(f"✅ Created txn {txn_id} | 日期 {date_str} 成本 {total_amount} 元")

        except Exception as e:
            print(f"❌ 錯誤 txn {txn_id} | {e}")
            skipped += 1

    conn.commit()
    return {
        "message": "✅ 已重新計算所有交易成本（debug 模式）",
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


from fastapi import Body
from datetime import datetime

@app.post("/api/internal/mock-status")
async def mock_status(data: dict = Body(...)):
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

