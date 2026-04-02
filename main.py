import asyncio

from urllib.parse import unquote  # ← 新增


def _normalize_cp_id(cp_id: str) -> str:
    return unquote(cp_id).lstrip("/")


connected_charge_points = {}
live_status_cache = {}



# ===============================
# 🔌 SmartCharging / 限流狀態（後端真實控制狀態）
# ===============================
current_limit_state = {}
# 結構說明（每個 cp_id 一筆）：
# {
#   cp_id: {
#     "requested_limit_a": float,
#     "requested_limit_a": float,
#     "requested_at": iso8601,
#     "applied": bool,
#     "last_try_at": iso8601,
#     "last_ok_at": iso8601 | None,
#     "last_err_at": iso8601 | None,
#     "last_error": str | None,
#     "last_tx_id": int | None,
#     "last_connector_id": int | None,
#   }
# }

# 每支充電樁各自一把 lock，避免同一台 CP 的 OCPP call 併發衝突
cp_call_locks = {}


def get_cp_call_lock(cp_id: str):
    lock = cp_call_locks.get(cp_id)
    if lock is None:
        lock = asyncio.Lock()
        cp_call_locks[cp_id] = lock
    return lock


def is_cp_connection_alive(cp_id: str, cp_obj=None) -> bool:
    """
    檢查這支 CP 是否仍為目前有效連線
    - cp_id 必須仍存在於 connected_charge_points
    - 若提供 cp_obj，還必須是同一個物件，避免舊物件 race condition
    """
    current_cp = connected_charge_points.get(cp_id)
    if current_cp is None:
        return False

    if cp_obj is not None and current_cp is not cp_obj:
        return False

    return True


def is_cp_in_ws_disconnect_grace(cp_id: str) -> bool:
    info = ws_disconnect_grace.get(cp_id)
    if not info:
        return False

    try:
        expire_at = float(info.get("expire_at", 0))
    except Exception:
        expire_at = 0

    if expire_at <= time.time():
        ws_disconnect_grace.pop(cp_id, None)
        return False

    return True


def is_cp_effectively_available_for_allocation(cp_id: str) -> bool:
    return cp_id in connected_charge_points or is_cp_in_ws_disconnect_grace(cp_id)


def cancel_ws_disconnect_cleanup(cp_id: str):
    cp_id = _normalize_cp_id(cp_id)

    # ==================================================
    # 1) 先把 seq +1，讓所有舊的 grace / finalize 立即失效
    # ==================================================
    prev_seq = int(ws_disconnect_seq.get(cp_id, 0) or 0)
    new_seq = prev_seq + 1
    ws_disconnect_seq[cp_id] = new_seq

    # ==================================================
    # 2) 取消舊的 finalize task
    # ==================================================
    task = pending_ws_disconnect_tasks.pop(cp_id, None)
    had_task = task is not None

    if task:
        try:
            task.cancel()
        except Exception as e:
            logger.warning(
                f"[WS_RECONNECT][GRACE_CANCEL_TASK_ERR] "
                f"cp_id={cp_id} | seq={new_seq} | err={e}"
            )

    # ==================================================
    # 3) 清掉 grace 視窗
    # ==================================================
    had_grace = ws_disconnect_grace.pop(cp_id, None) is not None

    if had_task or had_grace:
        logger.warning(
            f"[WS_RECONNECT][GRACE_CANCEL] "
            f"cp_id={cp_id} | new_seq={new_seq} | "
            f"had_task={had_task} | had_grace={had_grace}"
        )


async def finalize_ws_disconnect_cleanup(
    cp_norm: str,
    expected_cp=None,
    expected_seq: int | None = None,
):
    try:
        await asyncio.sleep(WS_DISCONNECT_GRACE_SECONDS)

        current_seq = int(ws_disconnect_seq.get(cp_norm, 0) or 0)

        # ==================================================
        # A) 若 seq 已不同，表示這輪 grace 已被取消或被新事件覆蓋
        # ==================================================
        if expected_seq is not None and current_seq != expected_seq:
            logger.warning(
                f"[WS_DISCONNECT][GRACE_SKIP][STALE_SEQ] "
                f"cp_id={cp_norm} | expected_seq={expected_seq} | current_seq={current_seq}"
            )
            return

        # ==================================================
        # B) 若 grace 視窗已不存在，也不應再繼續 finalize
        # ==================================================
        if cp_norm not in ws_disconnect_grace:
            logger.warning(
                f"[WS_DISCONNECT][GRACE_SKIP][NO_GRACE] cp_id={cp_norm}"
            )
            return

        current_cp = connected_charge_points.get(cp_norm)

        # grace 期間若已被「新連線物件」取代，才算真正重連成功
        if current_cp is not None and current_cp is not expected_cp:
            logger.warning(
                f"[WS_DISCONNECT][GRACE_SKIP] "
                f"cp_id={cp_norm} | reason=reconnected_in_time | expected_seq={expected_seq}"
            )
            return

        # 若 grace 到期後，dict 裡仍是舊物件，這時才正式移除
        if current_cp is expected_cp:
            connected_charge_points.pop(cp_norm, None)

        # ==================================================
        # A) Grace 到期後，只檢查是否仍有 active transaction
        #    ❌ 不再自動 UPDATE stop_timestamp
        # ==================================================
        has_active_tx = False
        latest_tx_id = None

        try:
            with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT transaction_id
                    FROM transactions
                    WHERE charge_point_id = ?
                      AND stop_timestamp IS NULL
                    ORDER BY start_timestamp DESC, transaction_id DESC
                    LIMIT 1
                    """,
                    (cp_norm,),
                )
                row = cur.fetchone()

            if row:
                has_active_tx = True
                latest_tx_id = int(row[0])

        except Exception as e:
            logger.exception(
                f"[WS_DISCONNECT][ACTIVE_TX_CHECK_ERR] cp_id={cp_norm} | err={e}"
            )

        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

        # ==================================================
        # B) 若 grace 到期時仍有 active transaction
        #    → 不再維持 Charging 畫面，改標記為 Unavailable / ConnectionLost
        #    → 但先不自動 stop transaction，避免高風險改動
        # ==================================================
        if has_active_tx:
            prev_status = charging_point_status.get(cp_norm, {}) or {}

            charging_point_status[cp_norm] = {
                **prev_status,
                "connector_id": int(prev_status.get("connector_id") or 1),
                "status": "Unavailable",
                "timestamp": now,
                "error_code": "ConnectionLost",
                "derived": True,
                "ws_grace_expired": True,
                "transaction_id": latest_tx_id,
            }

            prev_live = live_status_cache.get(cp_norm, {}) or {}
            live_status_cache[cp_norm] = {
                **prev_live,
                "status": "Unavailable",
                "power": 0,
                "voltage": 0,
                "current": 0,
                "estimated_energy": prev_live.get("estimated_energy", 0),
                "estimated_amount": prev_live.get("estimated_amount", 0),
                "derived": True,
                "ws_grace_expired": True,
                "updated_at": time.time(),
            }

            logger.warning(
                f"[WS_DISCONNECT][MARK_UNAVAILABLE_AFTER_GRACE] "
                f"cp_id={cp_norm} | tx_id={latest_tx_id} | status=Unavailable"
            )

        # ==================================================
        # C) 若已明確無交易，才允許回 Available
        # ==================================================
        else:
            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO status_logs
                        (charge_point_id, connector_id, status, timestamp)
                        VALUES (?, ?, ?, ?)
                        """,
                        (cp_norm, 0, "Available", now),
                    )
                    conn.commit()

                charging_point_status[cp_norm] = {
                    "connector_id": 0,
                    "status": "Available",
                    "timestamp": now,
                    "error_code": "NoError",
                    "derived": True,
                }

                prev_live = live_status_cache.get(cp_norm, {}) or {}
                live_status_cache[cp_norm] = {
                    **prev_live,
                    "status": "Available",
                    "power": 0,
                    "voltage": 0,
                    "current": 0,
                    "estimated_energy": 0,
                    "estimated_amount": 0,
                    "derived": True,
                    "updated_at": time.time(),
                }

                logger.warning(
                    f"[WS_DISCONNECT][NO_ACTIVE_TX][SET_AVAILABLE] cp_id={cp_norm}"
                )

            except Exception as e:
                logger.exception(
                    f"[WS_DISCONNECT][NO_ACTIVE_TX][SET_AVAILABLE_ERR] cp_id={cp_norm} | err={e}"
                )

        # ==================================================
        # D) Grace 到期後，登記一次全域 rebalance
        # ==================================================
        try:
            request_rebalance(reason="ws_disconnect_finalize")
            logger.warning(
                f"[SMART][WS_DISCONNECT][FINAL_REBALANCE_REQUESTED] cp_id={cp_norm}"
            )
        except Exception as e:
            logger.exception(
                f"[SMART][WS_DISCONNECT][FINAL_REBALANCE_REQUEST_ERR] cp_id={cp_norm} | err={e}"
            )

    except asyncio.CancelledError:
        logger.warning(f"[WS_DISCONNECT][GRACE_CANCELLED] cp_id={cp_norm}")
        raise

    finally:
        pending_ws_disconnect_tasks.pop(cp_norm, None)
        ws_disconnect_grace.pop(cp_norm, None)



import sys

sys.path.insert(0, "./")

import json
import os

# =====================================================
# 🔧 SmartCharging 強制開關（模擬器 / 開發用）
# =====================================================
FORCE_SMART_CHARGING = os.getenv("FORCE_SMART_CHARGING", "0") == "1"
import io
import csv
import uuid
import logging
import sqlite3
import uvicorn
import asyncio

pending_stop_transactions = {}
# 針對每筆交易做「已送停充」去重，避免前端/後端重複送


logger = logging.getLogger(__name__)

from datetime import datetime, timedelta, timezone
from fastapi import (
    FastAPI,
    Request,
    Query,
    Body,
    Path,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from dateutil.parser import parse as parse_date
from websockets.exceptions import ConnectionClosedOK
from ocpp.v16 import call, call_result, ChargePoint as OcppChargePoint
from ocpp.v16.enums import Action, RegistrationStatus
from ocpp.routing import on
from urllib.parse import urlparse, parse_qsl
from reportlab.pdfgen import canvas


# ===============================
# 🔌 WebSocket 斷線寬限（實體樁 / 弱網環境保護）
# ===============================
def _get_ws_disconnect_grace_seconds() -> int:
    raw = os.getenv("WS_DISCONNECT_GRACE_SECONDS", "60")
    try:
        value = int(str(raw).strip())
        return max(0, value)
    except Exception:
        return 60


WS_DISCONNECT_GRACE_SECONDS = _get_ws_disconnect_grace_seconds()
pending_ws_disconnect_tasks = {}
ws_disconnect_grace = {}
ws_disconnect_seq = {}

# ===============================
# 🔁 全域 Rebalance 去抖 / 合併 / 節流
# ===============================
REBALANCE_DEBOUNCE_SECONDS = float(os.getenv("REBALANCE_DEBOUNCE_SECONDS", "2.0"))

rebalance_lock = asyncio.Lock()
pending_rebalance_task = None
rebalance_requested = False
rebalance_last_reason = None
rebalance_last_requested_at = 0.0
rebalance_request_seq = 0


def request_rebalance(reason: str):
    """
    只負責「登記」需要 rebalance，不直接立即重算。
    多個事件在短時間內會共用同一個 pending task。
    """
    global pending_rebalance_task
    global rebalance_requested
    global rebalance_last_reason
    global rebalance_last_requested_at
    global rebalance_request_seq

    rebalance_requested = True
    rebalance_last_reason = reason
    rebalance_last_requested_at = time.time()
    rebalance_request_seq += 1

    seq = rebalance_request_seq

    if pending_rebalance_task is None or pending_rebalance_task.done():
        pending_rebalance_task = asyncio.create_task(
            _debounced_rebalance_runner(expected_seq=seq)
        )
        logger.warning(
            f"[SMART][REBALANCE][SCHEDULED] "
            f"reason={reason} | seq={seq} | debounce={REBALANCE_DEBOUNCE_SECONDS}s"
        )
    else:
        logger.warning(
            f"[SMART][REBALANCE][COALESCED] "
            f"reason={reason} | seq={seq} | debounce={REBALANCE_DEBOUNCE_SECONDS}s"
        )


async def _debounced_rebalance_runner(expected_seq: int):
    """
    將短時間內的多個 rebalance 請求合併。
    若 rebalance 執行期間又有新事件進來，結束後再補跑一輪。
    """
    global pending_rebalance_task
    global rebalance_requested
    global rebalance_last_reason
    global rebalance_last_requested_at
    global rebalance_request_seq

    try:
        while True:
            while True:
                wait_s = REBALANCE_DEBOUNCE_SECONDS - (
                    time.time() - rebalance_last_requested_at
                )
                if wait_s <= 0:
                    break
                await asyncio.sleep(min(wait_s, 0.2))

            async with rebalance_lock:
                if not rebalance_requested:
                    break

                run_reason = rebalance_last_reason or "debounced"
                run_seq = rebalance_request_seq

                # ==================================================
                # 關鍵防呆：
                # ws_disconnect_grace 已經不是最新 request 時，不准執行
                # ==================================================
                if (
                    run_reason == "ws_disconnect_grace"
                    and expected_seq != run_seq
                ):
                    logger.warning(
                        f"[SMART][REBALANCE][SKIP_STALE_GRACE] "
                        f"expected_seq={expected_seq} | current_seq={run_seq}"
                    )
                    break

                # 若 grace 已被取消，也不應再執行這輪 ws_disconnect_grace
                if run_reason == "ws_disconnect_grace" and not ws_disconnect_grace:
                    rebalance_requested = False
                    logger.warning(
                        f"[SMART][REBALANCE][SKIP_CANCELLED_GRACE] "
                        f"reason={run_reason} | expected_seq={expected_seq}"
                    )
                    break

                rebalance_requested = False

                logger.warning(
                    f"[SMART][REBALANCE][RUN] "
                    f"reason={run_reason} | seq={run_seq} | "
                    f"debounce={REBALANCE_DEBOUNCE_SECONDS}s"
                )

                try:
                    await rebalance_all_charging_points(reason=run_reason)
                    logger.warning(
                        f"[SMART][REBALANCE][RUN_OK] "
                        f"reason={run_reason} | seq={run_seq}"
                    )
                except Exception as e:
                    logger.exception(
                        f"[SMART][REBALANCE][RUN_ERR] "
                        f"reason={run_reason} | seq={run_seq} | err={e}"
                    )

            if not rebalance_requested:
                break

    finally:
        pending_rebalance_task = None


# ===============================
# 單機硬體安全上限（保護用，不提供前端修改）
# ===============================
DEVICE_HARD_LIMIT = 32  # 單位：安培

# ===============================
# 第一階段：單機功率管理上限（固定值，不提供前端修改）
# ===============================
SINGLE_CP_MAX_POWER_KW = 7.0


# ===============================
# 🔌 OCPP 電流限制（TxProfile）
# ===============================
async def query_smart_charging_capability(cp):
    """
    ⚠️ 已停用（重要）
    ------------------------------------------------
    python-ocpp 目前不支援 call.GetConfiguration，
    若繼續使用會導致：
      - supports_smart_charging 永遠 False
      - SetChargingProfile 永遠被 SKIP
    ------------------------------------------------
    若未來要恢復能力探測，請改用：
      - 嘗試送 SetChargingProfile
      - 以成功 / 失敗結果回填能力
    """
    raise RuntimeError(
        "query_smart_charging_capability is DISABLED. "
        "Do NOT use call.GetConfiguration with current python-ocpp."
    )


async def send_current_limit_profile(
    cp,
    connector_id: int,
    limit_a: float,
    tx_id: int | None = None,
):
    """
    對充電樁送出 OCPP 1.6 SetChargingProfile（TxProfile）
    """

    cp_id = getattr(cp, "id", "unknown")

    # =====================================================
    # [1] 進入點（確認一定有進來）
    # =====================================================
    logging.warning(
        f"[LIMIT][ENTER] "
        f"cp_id={cp_id} | connector_id={connector_id} | tx_id={tx_id} | "
        f"requested_raw_limit_a={limit_a}A | device_hard_limit={DEVICE_HARD_LIMIT}A"
    )


    # =====================================================
    # [1.2] 先寫入狀態（保底：就算後面送失敗，前端也看的到 requested）
    # =====================================================
    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    st = current_limit_state.setdefault(cp_id, {})
    st.update(
        {
            "requested_limit_a": float(limit_a),
            "requested_at": now_iso,
            "applied": False,
            "last_tx_id": tx_id,
            "last_connector_id": connector_id,
            "last_error": None,
        }
    )




    # =====================================================
    # [1.5] 🧯 SAFETY GUARD：下發值不得高於「功率分配換算後理論值」
    # 第一階段：管理邏輯改成功率，實際下發仍為電流
    # =====================================================
    raw_limit_a = limit_a

    try:
        limit_a = float(limit_a)
    except Exception:
        limit_a = float(DEVICE_HARD_LIMIT)

    # 永遠先套硬體上限
    limit_a = min(limit_a, float(DEVICE_HARD_LIMIT))

    try:
        # 重新抓目前所有 active 交易
        with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT charge_point_id
                FROM transactions
                WHERE stop_timestamp IS NULL
                  AND start_timestamp IS NOT NULL
                """
            )
            rows = cur.fetchall()

        active_cp_ids = []
        for (cpid,) in rows:
            cpid = _normalize_cp_id(str(cpid))
            if cpid not in active_cp_ids:
                active_cp_ids.append(cpid)

        # ✅ 與 rebalance 邏輯統一：
        #    已連線 or grace 中的樁，都要納入功率分配計算
        active_cp_ids = [
            cid for cid in active_cp_ids
            if is_cp_effectively_available_for_allocation(cid)
        ]

        allocated_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
        theory_a = (
            convert_power_kw_to_current_a(allocated_kw, cp_id)
            if allocated_kw is not None
            else None
        )

        if theory_a is not None:
            theory_a = float(theory_a)

            logging.warning(
                f"[LIMIT][THEORY] "
                f"cp_id={cp_id} | tx_id={tx_id} | connector_id={connector_id} | "
                f"active_cp_ids={active_cp_ids} | allocated_kw={allocated_kw} | "
                f"raw_limit_a={raw_limit_a}A | theory_a={theory_a}A"
            )

            if limit_a > theory_a:
                logging.warning(
                    f"[LIMIT][CLAMP] "
                    f"cp_id={cp_id} | tx_id={tx_id} | "
                    f"raw={raw_limit_a}A -> clamp={theory_a}A | "
                    f"allocated_kw={allocated_kw} | active_cp_ids={active_cp_ids}"
                )
                limit_a = theory_a

    except Exception as e:
        logging.warning(
            f"[LIMIT][CLAMP][SKIP] cp_id={cp_id} | raw={raw_limit_a}A | err={e}"
        )



    # =====================================================
    # [1.8] ✅ OCPP schema 要求：limit 必須是 0.1 的倍數
    # 例如 22.73 -> 22.7（避免 ValidationError / FormatViolation）
    # =====================================================
    try:
        limit_a = float(limit_a)
    except Exception:
        limit_a = float(DEVICE_HARD_LIMIT)

    # 量化到 0.1A（符合 multipleOf 0.1）
    limit_a = round(limit_a * 10.0) / 10.0

    # 保底：避免出現 -0.0
    if abs(limit_a) < 1e-9:
        limit_a = 0.0




    # =====================================================
    # [2] 組 payload（TxProfile 需要 transaction_id；沒有 tx_id 就退回 CP Max Profile）
    # =====================================================
    purpose = "TxProfile" if tx_id is not None else "ChargePointMaxProfile"

    cs_profile = {
        "charging_profile_id": 999,
        "stack_level": 10,
        "charging_profile_purpose": purpose,
        "charging_profile_kind": "Absolute",
        "charging_schedule": {
            "charging_rate_unit": "A",
            "charging_schedule_period": [
                {
                    "start_period": 0,
                    "limit": float(limit_a),
                    "number_phases": 1,
                }
            ],
        },
    }

    # 🔥 TxProfile 必須帶 transaction_id（snake_case）
    if tx_id is not None:
        cs_profile["transaction_id"] = int(tx_id)

    payload = call.SetChargingProfilePayload(
        connector_id=int(connector_id),
        cs_charging_profiles=cs_profile,
    )

    # =====================================================
    # [2.5] 🔍 DEBUG：確認 payload / call 型態（Step A）
    # =====================================================
    logging.warning(
        f"[LIMIT][PAYLOAD] "
        f"cp_id={cp_id} | tx_id={tx_id} | connector_id={connector_id} | "
        f"purpose={purpose} | final_limit_a={limit_a}A | payload={payload}"
    )


    # =====================================================
    # [3] 嘗試送出（同一支 CP 必須序列化）
    #     送出前 / 送出中 都要再次確認仍為有效連線
    # =====================================================
    lock = get_cp_call_lock(cp_id)

    # 先做一次快速檢查：若已不在線，直接跳過
    if not is_cp_connection_alive(cp_id, cp):
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": False,
                "last_try_at": now_iso,
                "last_ok_at": None,
                "last_err_at": now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": "cp_disconnected_before_send",
            }
        )

        logging.warning(
            f"[LIMIT][SKIP][DISCONNECTED] "
            f"| cp_id={cp_id} | tx_id={tx_id} | limit={limit_a}A"
        )
        return False

    try:
        async with lock:
            # 進 lock 後再檢查一次，避免排隊期間該樁已斷線
            if not is_cp_connection_alive(cp_id, cp):
                now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
                st = current_limit_state.setdefault(cp_id, {})
                st.update(
                    {
                        "requested_limit_a": float(limit_a),
                        "requested_at": now_iso,
                        "applied": False,
                        "last_try_at": now_iso,
                        "last_ok_at": None,
                        "last_err_at": now_iso,
                        "last_tx_id": tx_id,
                        "last_connector_id": connector_id,
                        "last_error": "cp_disconnected_before_locked_send",
                    }
                )

                logging.warning(
                    f"[LIMIT][SKIP][DISCONNECTED_AFTER_LOCK] "
                    f"| cp_id={cp_id} | tx_id={tx_id} | limit={limit_a}A"
                )
                return False

            logging.warning(
                f"[LIMIT][SEND][TRY] "
                f"| cp_id={cp_id} | tx_id={tx_id} | limit={limit_a}A"
            )

            resp = await asyncio.wait_for(cp.call(payload), timeout=60.0)

        # 回應回來時，再確認一次這個 cp 仍是目前有效連線
        if not is_cp_connection_alive(cp_id, cp):
            now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            st = current_limit_state.setdefault(cp_id, {})
            st.update(
                {
                    "requested_limit_a": float(limit_a),
                    "requested_at": now_iso,
                    "applied": False,
                    "last_try_at": now_iso,
                    "last_ok_at": None,
                    "last_err_at": now_iso,
                    "last_tx_id": tx_id,
                    "last_connector_id": connector_id,
                    "last_error": "cp_disconnected_after_send",
                }
            )

            logging.warning(
                f"[LIMIT][SKIP][DISCONNECTED_AFTER_SEND] "
                f"| cp_id={cp_id} | tx_id={tx_id}"
            )
            return False

        status = getattr(resp, "status", None)
        status_str = str(status) if status is not None else "UNKNOWN"

        status_low = status_str.strip().lower()
        ok = (
            status_low == "accepted"
            or status_low.endswith(".accepted")
            or status_low.endswith("_accepted")
        )

        logging.warning(
            f"[LIMIT][SEND][RESP-RAW] "
            f"cp_id={cp_id} | resp={repr(resp)} | type={type(resp)}"
        )

        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": ok,
                "last_try_at": now_iso,
                "last_ok_at": now_iso if ok else None,
                "last_err_at": None if ok else now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": None if ok else f"status={status_str}",
            }
        )

        logging.warning(
            f"[LIMIT][SEND][RESP] "
            f"| cp_id={cp_id} | tx_id={tx_id} | status={status_str} | applied={ok}"
        )
        return ok

    except asyncio.TimeoutError:
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": False,
                "last_try_at": now_iso,
                "last_ok_at": None,
                "last_err_at": now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": "timeout>60s",
            }
        )

        logging.error(
            f"[LIMIT][SEND][TIMEOUT] "
            f"| cp_id={cp_id} | tx_id={tx_id} | limit={limit_a}A"
        )
        return False

    except (WebSocketDisconnect, ConnectionClosedOK) as e:
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": False,
                "last_try_at": now_iso,
                "last_ok_at": None,
                "last_err_at": now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": f"ws_disconnected:{repr(e)}",
            }
        )

        logging.warning(
            f"[LIMIT][SEND][WS_CLOSED] "
            f"| cp_id={cp_id} | tx_id={tx_id} | err={e}"
        )
        return False

    except RuntimeError as e:
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        err_text = str(e)

        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": False,
                "last_try_at": now_iso,
                "last_ok_at": None,
                "last_err_at": now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": err_text,
            }
        )

        if "websocket.send" in err_text or "websocket.close" in err_text:
            logging.warning(
                f"[LIMIT][SEND][SKIP_RUNTIME_CLOSED] "
                f"| cp_id={cp_id} | tx_id={tx_id} | err={err_text}"
            )
            return False

        logging.exception(
            f"[LIMIT][SEND][RUNTIME_ERR] "
            f"| cp_id={cp_id} | tx_id={tx_id} | error={e}"
        )
        return False

    except Exception as e:
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        st = current_limit_state.setdefault(cp_id, {})
        st.update(
            {
                "requested_limit_a": float(limit_a),
                "requested_at": now_iso,
                "applied": False,
                "last_try_at": now_iso,
                "last_ok_at": None,
                "last_err_at": now_iso,
                "last_tx_id": tx_id,
                "last_connector_id": connector_id,
                "last_error": repr(e),
            }
        )

        logging.exception(
            f"[LIMIT][SEND][ERR] "
            f"| cp_id={cp_id} | tx_id={tx_id} | error={e}"
        )
        return False



# === 時區設定: 台北 ===
from zoneinfo import ZoneInfo

TZ_TAIPEI = ZoneInfo("Asia/Taipei")


app = FastAPI()


# === WebSocket 連線驗證設定（可選）===
REQUIRED_TOKEN = os.getenv("OCPP_WS_TOKEN", None)


logging.basicConfig(level=logging.WARNING)


# ===============================
# Stop API 去重保護（防換頁/重刷）
# ===============================
STOP_DEDUP_WINDOW = 60  # 秒
_recent_stop_requests = {}  # {cp_id: last_stop_time}


# 允許跨域（若前端使用）
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://ocppfortechlux-frontend.onrender.com",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=False,  # ✅ 你目前前端沒用 cookie/session，關掉最穩
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
        try:
            await self.websocket.send_text(data)
        except (WebSocketDisconnect, ConnectionClosedOK):
            raise
        except RuntimeError as e:
            # WebSocket 已 close / 已 completed 時，視為連線已失效
            if "websocket.send" in str(e) or "websocket.close" in str(e):
                raise WebSocketDisconnect(code=1006)
            raise

    # ocpp 會取 subprotocol 屬性
    @property
    def subprotocol(self):
        return self.websocket.headers.get("sec-websocket-protocol") or "ocpp1.6"


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


def _ms_since(start_ts: float) -> int:
    try:
        return int((time.perf_counter() - start_ts) * 1000)
    except Exception:
        return -1


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

    return {"whitelist": [{"charge_point_id": row[0], "name": row[1]} for row in rows]}


# =====================================================
# 🔍 Debug API：卡片 / StartTransaction 判斷
# =====================================================
from fastapi import Query


def _debug_card_state(id_tag: str, cp_id: str | None = None):
    """
    回傳卡片在後端 DB 的實際狀態：
    - id_tags（授權狀態）
    - cards（餘額）
    - card_whitelist（允許哪些充電樁）
    """
    cp_norm = _normalize_cp_id(cp_id) if cp_id else None

    with get_conn() as conn:
        cur = conn.cursor()

        # --- id_tags ---
        cur.execute(
            "SELECT id_tag, status, valid_until FROM id_tags WHERE id_tag = ?",
            (id_tag,),
        )
        row = cur.fetchone()
        id_tag_info = None
        if row:
            id_tag_info = {
                "id_tag": row[0],
                "status": row[1],
                "valid_until": row[2],
            }

        # --- cards ---
        cur.execute(
            "SELECT card_id, balance FROM cards WHERE card_id = ?",
            (id_tag,),
        )
        row = cur.fetchone()
        card_info = None
        if row:
            card_info = {
                "card_id": row[0],
                "balance": float(row[1] or 0),
            }

        # --- whitelist ---
        cur.execute(
            "SELECT charge_point_id FROM card_whitelist WHERE card_id = ?",
            (id_tag,),
        )
        rows = cur.fetchall()
        whitelist = [r[0] for r in rows] if rows else []

    return {
        "idTag": id_tag,
        "cp_id": cp_norm,
        "id_tags": id_tag_info,
        "cards": card_info,
        "card_whitelist": whitelist,
        "cp_allowed": (cp_norm in whitelist) if cp_norm else None,
    }


@app.get("/api/debug/card/{id_tag}")
def debug_card(id_tag: str, cp_id: str | None = Query(default=None)):
    """
    Debug 用 API：
    直接查看某張卡在後端 DB 的真實狀態
    """
    data = _debug_card_state(id_tag=id_tag, cp_id=cp_id)
    logging.warning(f"[DEBUG][CARD] {data}")
    return data

@app.get("/api/debug/start-transaction-check")
def debug_start_transaction_check(
    cp_id: str = Query(...),
    id_tag: str = Query(...),
):
    """
    Debug 用 API：
    不真的建立交易，只模擬 StartTransaction 的判斷流程，
    告訴你為什麼會 Accepted / Blocked / Invalid
    （純社區管理型：不再依賴 SmartCharging enabled 開關）
    """
    cp_norm = _normalize_cp_id(cp_id)

    # ========== Step 1：id_tags ==========
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT status FROM id_tags WHERE id_tag = ?",
            (id_tag,),
        )
        row = cur.fetchone()
        if not row:
            out = {
                "cp_id": cp_norm,
                "idTag": id_tag,
                "decision": "Invalid",
                "reason": "id_tag_not_found",
                "hint": "請確認 id_tags 表中有這張卡",
            }
            logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
            return out

        status_db = row[0]
        if status_db != "Accepted":
            out = {
                "cp_id": cp_norm,
                "idTag": id_tag,
                "decision": "Blocked",
                "reason": "id_tag_status_not_accepted",
                "id_tag_status": status_db,
            }
            logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
            return out

        # ========== Step 2：cards / balance ==========
        cur.execute(
            "SELECT balance FROM cards WHERE card_id = ?",
            (id_tag,),
        )
        row = cur.fetchone()
        if not row:
            out = {
                "cp_id": cp_norm,
                "idTag": id_tag,
                "decision": "Invalid",
                "reason": "card_not_found",
                "hint": "請確認 cards 表有此卡片",
            }
            logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
            return out

        balance = float(row[0] or 0)
        if balance <= 0:
            out = {
                "cp_id": cp_norm,
                "idTag": id_tag,
                "decision": "Blocked",
                "reason": "balance_le_0",
                "balance": balance,
            }
            logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
            return out

    # ========== Step 3：社區 Smart Charging 試算（第一階段：功率分配模式） ==========
    try:
        active_now = get_active_charging_count()
        trial_count = active_now + 1

        community_cfg = get_community_settings()

        # 模擬「若這台也加入充電」後的活躍樁數
        trial_cp_ids = [f"TRIAL_CP_{i+1}" for i in range(trial_count)]
        allocated_kw = calculate_allocated_power_kw_by_cp_ids(trial_cp_ids)
        preview_current_a = (
            convert_power_kw_to_current_a(allocated_kw, cp_norm)
            if allocated_kw is not None
            else None
        )

        if allocated_kw is None:
            out = {
                "cp_id": cp_norm,
                "idTag": id_tag,
                "decision": "Blocked",
                "reason": "contract_kw_invalid",
                "balance": balance,
                "active_now": active_now,
                "trial_count": trial_count,
                "allocated_power_kw": None,
                "preview_current_a": None,
                "single_cp_max_power_kw": SINGLE_CP_MAX_POWER_KW,
                "community_settings": community_cfg,
            }
            logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
            return out

        out = {
            "cp_id": cp_norm,
            "idTag": id_tag,
            "decision": "Accepted",
            "reason": "ok",
            "balance": balance,
            "active_now": active_now,
            "trial_count": trial_count,
            "allocated_power_kw": allocated_kw,
            "preview_current_a": preview_current_a,
            "single_cp_max_power_kw": SINGLE_CP_MAX_POWER_KW,
            "community_settings": community_cfg,
        }
        logging.warning(f"[DEBUG][START_TX_CHECK] {out}")
        return out

    except Exception as e:
        out = {
            "cp_id": cp_norm,
            "idTag": id_tag,
            "decision": "Accepted",
            "reason": "smart_charging_check_error_but_allowed",
            "error": str(e),
        }
        logging.exception(f"[DEBUG][START_TX_CHECK] {out}")
        return out

# =====================================================
# ==== Live 快取工具 ====
# =====================================================
import time

LIVE_TTL = 15  # 秒：視情況調整

DEBUG_TARGET_CP_ID = "TW*MSI*E000100"


def _is_debug_target_cp(cp_id: str | None) -> bool:
    try:
        return _normalize_cp_id(cp_id or "") == DEBUG_TARGET_CP_ID
    except Exception:
        return False

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
    before = dict(cur)

    applied_patch = {}
    skipped_keys = []

    for k, v in kv.items():
        if v is not None:
            cur[k] = v
            applied_patch[k] = v
        else:
            skipped_keys.append(k)

    cur["updated_at"] = time.time()
    live_status_cache[cp_id] = cur

    if _is_debug_target_cp(cp_id):
        logging.warning(
            f"[DEBUG][LIVE_UPSERT] "
            f"cp_id={cp_id} | "
            f"before_ts={before.get('timestamp')} | after_ts={cur.get('timestamp')} | "
            f"before_power={before.get('power')} | after_power={cur.get('power')} | "
            f"before_voltage={before.get('voltage')} | after_voltage={cur.get('voltage')} | "
            f"before_current={before.get('current')} | after_current={cur.get('current')} | "
            f"applied_patch={applied_patch} | skipped_keys={skipped_keys}"
        )


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
    # if REQUIRED_TOKEN:
    #   if supplied_token is None:
    #      print(f"❌ 拒絕：缺少 token；URL={url}")
    #     await websocket.close(code=1008)
    #    return None
    # if supplied_token != REQUIRED_TOKEN:
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
            (cp_id, websocket.client.host, now),
        )
        _c.commit()

    return cp_id



@app.websocket("/{charge_point_id:path}")
async def websocket_endpoint(websocket: WebSocket, charge_point_id: str):
    ws_t0 = time.perf_counter()

    print(
        f"🌐 WS attempt | raw_path={charge_point_id} | headers={dict(websocket.headers)}"
    )

    try:
        # 1) 驗證 + accept(subprotocol="ocpp1.6")，並回傳標準化 cp_id
        cp_id = await _accept_or_reject_ws(websocket, charge_point_id)
        if cp_id is None:
            return

        logging.warning(
            f"[DEBUG][WS][ACCEPTED] cp_id={cp_id} | alive_ms={_ms_since(ws_t0)}"
        )

        # 2) 啟動 OCPP handler
        cp = ChargePoint(cp_id, FastAPIWebSocketAdapter(websocket))
        cp.supports_smart_charging = True
        connected_charge_points[cp_id] = cp
        cancel_ws_disconnect_cleanup(cp_id)
        await cp.start()

    except WebSocketDisconnect:
        logger.warning(
            f"[DEBUG][WS][DISCONNECTED] cp_id={_normalize_cp_id(charge_point_id)} | "
            f"alive_ms={_ms_since(ws_t0)}"
        )

    except Exception as e:
        logger.error(
            f"[DEBUG][WS][ERR] cp_id={_normalize_cp_id(charge_point_id)} | "
            f"alive_ms={_ms_since(ws_t0)} | err={e}"
        )
        try:
            await websocket.close()
        except Exception:
            pass

    finally:
        logger.warning(
            f"[DEBUG][WS][FINALLY] cp_id={_normalize_cp_id(charge_point_id)} | "
            f"alive_ms={_ms_since(ws_t0)}"
        )
        cp_norm = _normalize_cp_id(charge_point_id)

        # ==================================================
        # 3) WebSocket 斷線：先保留這次 endpoint 對應的舊連線物件進入 grace
        #    不立刻 pop，不立刻 auto-stop / force Available / reset live
        # ==================================================
        current_cp = cp if "cp" in locals() else None

        try:
            now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            started_at = time.time()
            expire_at = started_at + float(WS_DISCONNECT_GRACE_SECONDS)

            disconnect_seq = int(ws_disconnect_seq.get(cp_norm, 0) or 0) + 1
            ws_disconnect_seq[cp_norm] = disconnect_seq

            ws_disconnect_grace[cp_norm] = {
                "started_at": started_at,
                "expire_at": expire_at,
                "started_at_iso": now,
                "seq": disconnect_seq,
            }

            # ✅ 這裡只標記 grace 視窗，不直接洗成 Unavailable
            prev_status = charging_point_status.get(cp_norm, {})

            effective_status = prev_status.get("status")
            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT transaction_id
                        FROM transactions
                        WHERE charge_point_id = ?
                          AND stop_timestamp IS NULL
                        ORDER BY start_timestamp DESC
                        LIMIT 1
                        """,
                        (cp_norm,),
                    )
                    active_row = cur.fetchone()

                if active_row and effective_status in (None, "Available", "Unknown", "未知"):
                    effective_status = "Charging"
            except Exception:
                pass

            charging_point_status[cp_norm] = {
                **prev_status,
                "status": effective_status or prev_status.get("status") or "Unknown",
                "timestamp": now,
                "error_code": "ConnectionLost",
                "derived": True,
                "grace_seconds": WS_DISCONNECT_GRACE_SECONDS,
                "ws_grace_until": expire_at,
            }

            # ✅ live cache 保留原本狀態，只補 grace 資訊
            prev_live = live_status_cache.get(cp_norm, {})
            live_status_cache[cp_norm] = {
                **prev_live,
                "derived": True,
                "ws_grace_until": expire_at,
                "updated_at": time.time(),
            }

            logger.warning(
                f"[WS_DISCONNECT][GRACE_START] cp_id={cp_norm} | grace_seconds={WS_DISCONNECT_GRACE_SECONDS}"
            )

            old_task = pending_ws_disconnect_tasks.pop(cp_norm, None)
            if old_task:
                old_task.cancel()

            pending_ws_disconnect_tasks[cp_norm] = asyncio.create_task(
                finalize_ws_disconnect_cleanup(
                    cp_norm,
                    expected_cp=current_cp,
                    expected_seq=disconnect_seq,
                )
            )

            # ✅ grace 期間不直接重算，改成「登記一次全域 rebalance」
            try:
                request_rebalance(reason="ws_disconnect_grace")
                logger.warning(
                    f"[SMART][WS_DISCONNECT][GRACE_REBALANCE_REQUESTED] cp_id={cp_norm}"
                )
            except Exception as rebalance_err:
                logger.exception(
                    f"[SMART][WS_DISCONNECT][GRACE_REBALANCE_REQUEST_ERR] "
                    f"cp_id={cp_norm} | err={rebalance_err}"
                )

        except Exception as e:
            logger.exception(
                f"[WS_DISCONNECT][GRACE_START][ERR] cp_id={cp_norm} | err={e}"
            )

        # ✅ 到這裡就結束
        # grace 期間只做：
        # 1. 保留目前 ws 連線物件進入 grace
        # 2. 建立 ws_disconnect_grace
        # 3. 啟動 finalize_ws_disconnect_cleanup(cp_norm, expected_cp=current_cp)
        # 4. 做一次 grace re-balance

# 初始化狀態儲存
# charging_point_status = {}


# HTTP 端點：查詢狀態
@app.get("/status/{cp_id}")
async def get_status(cp_id: str):
    return JSONResponse(charging_point_status.get(cp_id, {}))


# ===============================
# 初始化 SQLite 資料庫（順序修正）
# ===============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "ocpp_data.db")  # ✅ 固定資料庫絕對路徑


def get_conn():
    """
    為每次查詢建立新的連線與游標，避免共用全域 cursor 造成並發問題
    """
    return sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)


def get_community_settings():
    with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT enabled, contract_kw, voltage_v, phases, min_current_a, max_current_a
            FROM community_settings
            WHERE id = 1
        """
        )
        row = cur.fetchone()

    if not row:
        return {
            "enabled": False,
            "contract_kw": 0,
            "voltage_v": 220,
            "phases": 1,
            "min_current_a": 16,
            "max_current_a": 32,
        }

    return {
        "enabled": bool(row[0]),
        "contract_kw": float(row[1] or 0),
        "voltage_v": float(row[2] or 220),
        "phases": int(row[3] or 1),
        "min_current_a": float(row[4] or 16),
        "max_current_a": float(row[5] or 32),
    }


def _live_voltage_v(cp_id: str, fallback_v: float = 220.0) -> float:
    """
    取得某充電樁目前即時電壓
    優先從 live_status_cache 讀取，取不到就回退到 fallback_v
    """
    try:
        cp_norm = _normalize_cp_id(cp_id)
        live = live_status_cache.get(cp_norm, {}) or {}

        v = live.get("voltage")

        if v is None:
            return float(fallback_v)

        v = float(v)
        if v <= 0:
            return float(fallback_v)

        return v

    except Exception:
        return float(fallback_v)



def calculate_allocated_power_kw_by_cp_ids(active_cp_ids: list[str]):
    """
    社區功率平均分配（不再限制最低充電電流門檻）
    ------------------------------------------------
    規則：
    1. 以 community_settings.contract_kw 作為社區總可分配功率
    2. 對 active_cp_ids 平均分配
    3. 每台上限固定 SINGLE_CP_MAX_POWER_KW
    4. 不再因為低於 min_current_a 而阻擋啟動或停止下發
    ------------------------------------------------
    回傳：
    - float：每台應分配功率（kW）
    - None：僅代表 contract_kw 無效
    """

    cfg = get_community_settings()

    contract_kw = float(cfg.get("contract_kw") or 0)
    if contract_kw <= 0:
        return None

    if not active_cp_ids:
        return round(float(SINGLE_CP_MAX_POWER_KW), 3)

    avg_kw = contract_kw / max(1, len(active_cp_ids))
    allocated_kw = min(float(avg_kw), float(SINGLE_CP_MAX_POWER_KW))

    if allocated_kw <= 0:
        return None

    return round(float(allocated_kw), 3)

def convert_power_kw_to_current_a(power_kw: float, cp_id: str | None = None):
    """
    將功率(kW)換算成電流(A)

    【社區功率控制正式規則】
    - 一律使用 community_settings.voltage_v 作為固定控制基準電壓
    - 不再使用單樁即時電壓參與控制換算
    - 這樣可避免因現場電壓浮動，導致 limit_a 跟著抖動
    - cp_id 參數保留僅為相容既有呼叫，不再參與換算
    """
    cfg = get_community_settings()
    control_voltage_v = float(cfg.get("voltage_v") or 220)

    try:
        power_kw = float(power_kw)
    except Exception:
        return None

    if power_kw <= 0:
        return 0.0

    try:
        if control_voltage_v <= 0:
            return None

        current_a = (power_kw * 1000.0) / control_voltage_v
        current_a = min(float(current_a), float(DEVICE_HARD_LIMIT))
        return round(current_a, 2)

    except Exception:
        return None




def calculate_allowed_current_by_cp_ids(active_cp_ids: list[str]):
    """
    舊版相容函式（保留）

    【統一控制邏輯】
    - 一律使用 community_settings.voltage_v 作為固定控制基準電壓
    - 不再使用各樁即時電壓總和計算 allowed_A
    - 回傳值代表：若所有 active CP 平均分攤契約容量時，每樁理論可下發的電流
    """
    cfg = get_community_settings()

    contract_kw = float(cfg.get("contract_kw") or 0)
    if contract_kw <= 0:
        return None

    per_cp_max_a = float(cfg.get("max_current_a") or DEVICE_HARD_LIMIT)
    control_voltage_v = float(cfg.get("voltage_v") or 220)

    if control_voltage_v <= 0:
        return None

    if not active_cp_ids:
        return min(float(per_cp_max_a), float(DEVICE_HARD_LIMIT))

    cp_count = max(1, len(active_cp_ids))
    allocated_kw = contract_kw / cp_count

    current_a = (allocated_kw * 1000.0) / control_voltage_v

    if current_a <= 0:
        return None

    current_a = min(float(current_a), float(per_cp_max_a), float(DEVICE_HARD_LIMIT))
    return round(current_a, 2)



def get_effective_active_cp_ids():
    """
    取得目前「真正有效參與 Smart Charging」的 CP 清單

    規則：
    1. transactions 內仍為 active（stop_timestamp IS NULL, start_timestamp IS NOT NULL）
    2. 充電樁目前仍在線，或仍在 ws disconnect grace 中
    3. 去除重複 cp_id，保留唯一值
    """
    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT charge_point_id
                FROM transactions
                WHERE stop_timestamp IS NULL
                  AND start_timestamp IS NOT NULL
                """
            )
            rows = cur.fetchall()

        tx_cp_ids = []
        for (cp_id,) in rows:
            cp_id = _normalize_cp_id(str(cp_id))
            if cp_id not in tx_cp_ids:
                tx_cp_ids.append(cp_id)

        effective_cp_ids = [
            cp_id
            for cp_id in tx_cp_ids
            if is_cp_effectively_available_for_allocation(cp_id)
        ]

        if DEBUG_TARGET_CP_ID in tx_cp_ids or DEBUG_TARGET_CP_ID in effective_cp_ids:
            logging.warning(
                f"[DEBUG][ACTIVE_CP_IDS] "
                f"tx_cp_ids={tx_cp_ids} | "
                f"connected_cp_ids={list(connected_charge_points.keys())} | "
                f"grace_cp_ids={list(ws_disconnect_grace.keys())} | "
                f"effective_cp_ids={effective_cp_ids}"
            )

        return effective_cp_ids

    except Exception as e:
        logging.exception(f"[SMART][ACTIVE_CP_IDS][ERR] err={e}")
        return []



def get_active_charging_count():
    """
    取得目前「真正有效參與 Smart Charging」的車輛數

    判定依據：
    1. transactions 內仍為 active
    2. 該 CP 目前仍在線（connected_charge_points）
    """
    try:
        return len(get_effective_active_cp_ids())
    except Exception:
        return 0


async def rebalance_all_charging_points(reason: str):
    """
    第一階段：管理邏輯改成功率，實際下發仍為電流
    ------------------------------------------------
    1. 先找出所有 active 交易
    2. 依 active_cp_ids 平均分配功率
    3. 每樁上限固定 SINGLE_CP_MAX_POWER_KW
    4. 下發前依各樁即時電壓換算成電流
    """

    logger.warning(f"[SMART][REBALANCE][ENTER] reason={reason}")

    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT transaction_id, charge_point_id, connector_id
                FROM transactions
                WHERE stop_timestamp IS NULL
                  AND start_timestamp IS NOT NULL
                ORDER BY transaction_id DESC
                """
            )
            rows = cur.fetchall()

        # 建立 cp_id -> (tx_id, connector_id)（取最新一筆交易）
        tx_map = {}
        active_cp_ids = []
        for tx_id, cp_id, connector_id in rows:
            cp_id = _normalize_cp_id(cp_id)
            if cp_id not in tx_map:
                tx_map[cp_id] = (int(tx_id), int(connector_id or 1))
            if cp_id not in active_cp_ids:
                active_cp_ids.append(cp_id)

        # 只算仍連線的 active CP
        active_cp_ids = [cid for cid in active_cp_ids if is_cp_effectively_available_for_allocation(cid)]

        allocated_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)

        logging.warning(
            f"[SMART][REBALANCE] reason={reason} | "
            f"active_cp_ids={active_cp_ids} | allocated_kw={allocated_kw} | "
            f"single_cp_max_kw={SINGLE_CP_MAX_POWER_KW}"
        )

        if allocated_kw is None:
            return

        allocated_kw = float(allocated_kw)

        for cp_id in list(active_cp_ids):
            cp = connected_charge_points.get(cp_id)
            if not cp:
                logging.warning(f"[SMART][SKIP][OFFLINE] cp_id={cp_id} | reason=no_connected_cp")
                continue

            # 再次確認這支 cp 仍是目前有效連線
            if not is_cp_connection_alive(cp_id, cp):
                logging.warning(f"[SMART][SKIP][STALE_CP] cp_id={cp_id}")
                continue

            if getattr(cp, "supports_smart_charging", True) is False:
                logging.warning(f"[SMART][SKIP][NO_SC] cp_id={cp_id}")
                continue

            tx_id, connector_id = tx_map.get(cp_id, (None, 1))
            limit_a = convert_power_kw_to_current_a(allocated_kw, cp_id)

            if limit_a is None or limit_a <= 0:
                logging.warning(
                    f"[SMART][SKIP] cp_id={cp_id} | allocated_kw={allocated_kw} | limit_a={limit_a}"
                )
                continue

            try:
                logging.warning(
                    f"[SMART][APPLY_PLAN] "
                    f"reason={reason} | cp_id={cp_id} | tx_id={tx_id} | connector_id={connector_id} | "
                    f"allocated_kw={allocated_kw} | planned_limit_a={limit_a}A"
                )
                ok = await send_current_limit_profile(
                    cp=cp,
                    connector_id=int(connector_id or 1),
                    limit_a=float(limit_a),
                    tx_id=tx_id,
                )

                logging.warning(
                    f"[SMART][FORCE_APPLY] "
                    f"cp_id={cp_id} | connector_id={connector_id} | tx_id={tx_id} | "
                    f"allocated_kw={allocated_kw} | limit={limit_a}A | ok={ok}"
                )

            except Exception as e:
                logging.exception(
                    f"[SMART][FORCE_ERR] cp_id={cp_id} | err={e}"
                )

    except Exception as e:
        logging.exception(
            f"[SMART][REBALANCE][FATAL] err={e}"
        )



def ensure_charge_points_table():
    """
    ✅ 保證 charge_points 表一定存在
    - 新 DB 第一次啟動：建立表（直接包含 max_current_a）
    - 舊 DB：若已存在，不影響
    """
    with get_conn() as c:
        cur = c.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS charge_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charge_point_id TEXT UNIQUE NOT NULL,
                name TEXT,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                max_current_a REAL DEFAULT 16
            )
        """
        )
        c.commit()


def ensure_charge_points_schema():
    """
    ✅ 舊 DB 相容用：
    - 表存在但沒有 max_current_a → 自動補欄位
    - 表不存在 → 直接跳過（避免 Deploy 炸掉）
    """
    with get_conn() as c:
        cur = c.cursor()

        cur.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='charge_points'
        """
        )
        if not cur.fetchone():
            logging.warning("⚠️ [MIGRATION] charge_points table not found, skip ALTER")
            return

        cur.execute("PRAGMA table_info(charge_points);")
        cols = [r[1] for r in cur.fetchall()]

        if "max_current_a" not in cols:
            cur.execute(
                "ALTER TABLE charge_points " "ADD COLUMN max_current_a REAL DEFAULT 16"
            )
            c.commit()
            logging.warning(
                "🛠️ [MIGRATION] charge_points add column max_current_a REAL DEFAULT 16"
            )
        else:
            logging.info("✅ [MIGRATION] charge_points.max_current_a exists")


def ensure_community_settings_table():
    """
    ✅ 保證 community_settings 表一定存在
    - 新 DB 第一次啟動：直接建立表
    - 並保證 id=1 這筆預設資料存在
    """
    with get_conn() as c:
        cur = c.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS community_settings (
                id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 1,
                contract_kw REAL DEFAULT 0,
                voltage_v REAL DEFAULT 220,
                phases INTEGER DEFAULT 1,
                min_current_a REAL DEFAULT 16,
                max_current_a REAL DEFAULT 32
            )
            """
        )

        # 保證 id=1 一定存在
        cur.execute("SELECT id FROM community_settings WHERE id = 1")
        row = cur.fetchone()

        if not row:
            cur.execute(
                """
                INSERT INTO community_settings
                (id, enabled, contract_kw, voltage_v, phases, min_current_a, max_current_a)
                VALUES (1, 1, 0, 220, 1, 16, 32)
                """
            )

        c.commit()


def ensure_community_settings_schema():
    """
    ✅ 舊 DB 相容用：
    - 表存在但少欄位 → 自動補欄位
    - 表不存在 → 直接跳過（避免 Deploy 炸掉）
    """
    with get_conn() as c:
        cur = c.cursor()

        cur.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='community_settings'
            """
        )
        if not cur.fetchone():
            logging.warning("⚠️ [MIGRATION] community_settings table not found, skip ALTER")
            return

        cur.execute("PRAGMA table_info(community_settings);")
        cols = [r[1] for r in cur.fetchall()]

        if "enabled" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN enabled INTEGER DEFAULT 1")

        if "contract_kw" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN contract_kw REAL DEFAULT 0")

        if "voltage_v" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN voltage_v REAL DEFAULT 220")

        if "phases" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN phases INTEGER DEFAULT 1")

        if "min_current_a" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN min_current_a REAL DEFAULT 16")

        if "max_current_a" not in cols:
            cur.execute("ALTER TABLE community_settings ADD COLUMN max_current_a REAL DEFAULT 32")

        c.commit()
        logging.info("✅ [MIGRATION] community_settings schema checked")




# 建立一個全域連線（僅供少數 legacy 用途）
conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
cursor = conn.cursor()

# ✅ 正確順序：先確保表存在，再做 migration
ensure_charge_points_table()
ensure_charge_points_schema()

ensure_community_settings_table()
ensure_community_settings_schema()

def _price_for_timestamp(ts: str) -> float:
    """
    查當下 timestamp 所對應的電價，
    修正 24:00 整天規則與跨午夜邏輯。
    """
    try:
        # 1. 解析時間
        dt = (
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if "Z" in ts
            else datetime.fromisoformat(ts)
        )
        dt = dt.astimezone(TZ_TAIPEI)

        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M")

        # 2. 查這一天的規則
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT start_time, end_time, price
                FROM daily_pricing_rules
                WHERE date = ?
                ORDER BY start_time ASC
                """,
                (date_str,),
            )
            rules = cur.fetchall()

        # ------ 沒規則 → 預設 ------
        if not rules:
            return 6.0

        # ------ 將 24:00 → 23:59 ------
        normalized = []
        for s, e, p in rules:
            s2 = "23:59" if s == "24:00" else s
            e2 = "23:59" if e == "24:00" else e
            normalized.append((s2, e2, p))

        # ------ 時段比對 ------
        for s, e, price in normalized:
            if s <= e:
                if s <= time_str <= e:
                    return float(price)
            else:
                # 跨午夜
                if time_str >= s or time_str <= e:
                    return float(price)

        # ------ 整天設定 fallback ------
        overs = [p for s, e, p in normalized if s == "00:00" and e == "23:59"]
        if overs:
            return float(overs[0])

    except Exception as e:
        logging.warning(f"⚠️ 電價查詢失敗: {e}")

    return 6.0


# 📌 預設電價規則資料表（只會存一筆）
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS default_pricing_rules (
    id INTEGER PRIMARY KEY,
    weekday_rules TEXT,
    saturday_rules TEXT,
    sunday_rules TEXT
)
"""
)
conn.commit()


# ============================================================
# 多時段電價分段計算（依據每筆 meter_values 分段累加）
# ============================================================
def _calculate_multi_period_cost(transaction_id: int) -> float:
    import sqlite3

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp, value FROM meter_values
            WHERE transaction_id=? AND measurand LIKE 'Energy.Active.Import%'
            ORDER BY timestamp ASC
        """,
            (transaction_id,),
        )
        rows = cur.fetchall()

    if len(rows) < 2:
        return 0.0

    total = 0.0
    for i in range(1, len(rows)):
        ts_prev, val_prev = rows[i - 1]
        ts_curr, val_curr = rows[i]

        diff_kwh = max(0.0, (float(val_curr) - float(val_prev)) / 1000.0)
        price = _price_for_timestamp(ts_curr)  # 依各時間點電價查價
        total += diff_kwh * price

    return round(total, 2)


def _calculate_multi_period_cost_detailed(transaction_id: int):
    import sqlite3
    from datetime import datetime

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp, value FROM meter_values
            WHERE transaction_id=? AND measurand LIKE 'Energy.Active.Import%'
            ORDER BY timestamp ASC
        """,
            (transaction_id,),
        )
        rows = cur.fetchall()

    if len(rows) < 2:
        return {"total": 0.0, "segments": []}

    segments_map = {}
    total = 0.0

    for i in range(1, len(rows)):
        ts_prev, val_prev = rows[i - 1]
        ts_curr, val_curr = rows[i]

        diff_kwh = max(0.0, (float(val_curr) - float(val_prev)) / 1000.0)
        price = _price_for_timestamp(ts_curr)

        # 正規化 UTC timestamp → 轉換為台北時間
        ts_norm = ts_curr.replace("Z", "+00:00")  # 處理帶 Z 格式
        dt_parsed = datetime.fromisoformat(ts_norm)

        # 若 timestamp 無 tzinfo，視為 UTC，再轉台北
        if dt_parsed.tzinfo is None:
            dt_parsed = dt_parsed.replace(tzinfo=timezone.utc)

        dt_local = dt_parsed.astimezone(TZ_TAIPEI)

        date_key = dt_local.strftime("%Y-%m-%d")
        time_str = dt_local.strftime("%H:%M")

        # ★ 查電價時段（start_time, end_time）
        with sqlite3.connect(DB_FILE) as conn:
            c2 = conn.cursor()
            c2.execute(
                """
                SELECT start_time, end_time
                FROM daily_pricing_rules
                WHERE date = ?
                  AND ? >= start_time
                  AND ? < end_time
                ORDER BY start_time ASC LIMIT 1
            """,
                (date_key, time_str, time_str),
            )
            rule = c2.fetchone()

        if rule:
            start_t, end_t = rule
        else:
            # 🩵 新增：若當天查不到規則，嘗試往前一天查
            prev_date = (dt_local - timedelta(days=1)).strftime("%Y-%m-%d")
            with sqlite3.connect(DB_FILE) as conn_prev:
                c_prev = conn_prev.cursor()
                c_prev.execute(
                    """
                    SELECT start_time, end_time, price
                    FROM daily_pricing_rules
                    WHERE date = ?
                      AND ? >= start_time
                      AND ? < end_time
                    ORDER BY start_time ASC LIMIT 1
                """,
                    (prev_date, time_str, time_str),
                )
                rule_prev = c_prev.fetchone()
            if rule_prev:
                start_t, end_t, price = rule_prev
            else:
                # 最終 fallback（完全查不到）
                start_t, end_t = "00:00", "23:59"
                price = 6.0

        key = (date_key, start_t, end_t, price)

        if key not in segments_map:
            segments_map[key] = {
                "start": f"{date_key}T{start_t}:00",
                "end": f"{date_key}T{end_t}:00",
                "kwh": 0.0,
                "price": price,
                "subtotal": 0.0,
            }

        seg = segments_map[key]
        seg["kwh"] += diff_kwh
        seg["subtotal"] += diff_kwh * price
        total += diff_kwh * price

    # 轉格式：按時間排序
    segments = sorted(segments_map.values(), key=lambda s: s["start"])

    # 🔧 延後 round，避免四捨五入誤差
    raw_total = sum(seg["subtotal"] for seg in segments)
    for seg in segments:
        seg["kwh"] = round(seg["kwh"], 6)
        seg["subtotal"] = round(seg["kwh"] * seg["price"], 2)  # 精確以最新 kWh×單價計算

    return {
        "total": float(round(sum(seg["subtotal"] for seg in segments), 2)),
        "segments": segments,
    }


@app.get("/api/cards/{card_id}/whitelist")
async def get_card_whitelist(card_id: str):
    cursor.execute(
        "SELECT charge_point_id FROM card_whitelist WHERE card_id = ?", (card_id,)
    )
    rows = cursor.fetchall()
    allowed_list = [row[0] for row in rows]

    return {"idTag": card_id, "allowed": allowed_list}


# === 卡片白名單 (card_whitelist) ===
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS card_whitelist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        card_id TEXT NOT NULL,
        charge_point_id TEXT NOT NULL
    )
"""
)
conn.commit()


cursor.execute(
    """
CREATE TABLE IF NOT EXISTS card_owners (
    card_id TEXT PRIMARY KEY,
    name TEXT
)
"""
)
conn.commit()


# 初始化 connection_logs 表格（如不存在就建立）
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS connection_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    ip TEXT,
    time TEXT
)
"""
)
conn.commit()

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT NOT NULL,
    id_tag TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time   TEXT NOT NULL,
    status     TEXT NOT NULL
)
"""
)
conn.commit()


# === 新增 cards 資料表，用於管理卡片餘額 ===
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id TEXT UNIQUE,
    balance REAL DEFAULT 0
)
"""
)

# 建立 daily_pricing 表（若尚未存在）
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS daily_pricing (
    date TEXT PRIMARY KEY,
    price_per_kwh REAL
)
"""
)


# ★ 新增：每日「多時段」電價規則，供 /api/pricing/price-now 使用
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS daily_pricing_rules (
    date TEXT,          -- YYYY-MM-DD
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL,         -- 當時段電價
    label TEXT          -- 可選：顯示用標籤（例如 尖峰/離峰/活動價）
)
"""
)
conn.commit()


cursor.execute(
    """
CREATE TABLE IF NOT EXISTS stop_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT,
    meter_stop INTEGER,
    timestamp TEXT,
    reason TEXT
)
"""
)


# ✅ 加在這裡！
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id INTEGER,
        base_fee REAL,
        energy_fee REAL,
        overuse_fee REAL,
        total_amount REAL,
        paid_at TEXT
    )
"""
)


conn.commit()


cursor.execute(
    """
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
"""
)


cursor.execute(
    """
CREATE TABLE IF NOT EXISTS id_tags (
    id_tag TEXT PRIMARY KEY,
    status TEXT,
    valid_until TEXT
)
"""
)


# ✅ 請插入這段
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS users (
    id_tag TEXT PRIMARY KEY,
    name TEXT,
    department TEXT,
    card_number TEXT
)
"""
)

conn.commit()


cursor.execute(
    """
CREATE TABLE IF NOT EXISTS weekly_pricing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season TEXT,
    weekday TEXT,
    type TEXT,          -- 尖峰、離峰、半尖峰
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL
)
"""
)
conn.commit()


# ★ 新增：一般季別/日別的時段電價規則，供 /api/pricing-rules 使用
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS pricing_rules (
    season TEXT,        -- 例如：summer、winter…（你自訂）
    day_type TEXT,      -- 例如：weekday、weekend…（你自訂）
    start_time TEXT,    -- HH:MM
    end_time TEXT,      -- HH:MM
    price REAL
)
"""
)
conn.commit()


cursor.execute(
    """
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
"""
)

# ★ 舊庫相容：若既有資料表沒有 phase 欄位，自動補上
cursor.execute("PRAGMA table_info(meter_values)")
_cols = [r[1] for r in cursor.fetchall()]
if "phase" not in _cols:
    cursor.execute("ALTER TABLE meter_values ADD COLUMN phase TEXT")
conn.commit()


cursor.execute(
    """
CREATE TABLE IF NOT EXISTS status_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    connector_id INTEGER,
    status TEXT,
    timestamp TEXT
)
"""
)

# ★ 舊庫相容：若既有資料表沒有 error_code 欄位，自動補上
cursor.execute("PRAGMA table_info(status_logs)")
_cols = [r[1] for r in cursor.fetchall()]
if "error_code" not in _cols:
    cursor.execute("ALTER TABLE status_logs ADD COLUMN error_code TEXT")
    logging.warning("[DB][MIGRATE] status_logs.error_code added")
else:
    logging.warning("[DB][MIGRATE] status_logs.error_code already exists")

conn.commit()


# ============================================================
# 🏘️ 社區 Smart Charging 設定（契約容量）
# ============================================================
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS community_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    enabled INTEGER DEFAULT 0,          -- 0=false, 1=true
    contract_kw REAL DEFAULT 0,          -- 契約容量 (kW)
    voltage_v REAL DEFAULT 220,          -- 電壓 (V)
    phases INTEGER DEFAULT 1,            -- 相數（先保留）
    min_current_a REAL DEFAULT 16,       -- 最低充電電流
    max_current_a REAL DEFAULT 32        -- 每樁最大上限
)
"""
)

# ✅ 確保一定有一筆 id=1
cursor.execute(
    """
INSERT OR IGNORE INTO community_settings (id)
VALUES (1)
"""
)

conn.commit()


from ocpp.v16 import call


async def _auto_stop_if_balance_insufficient(
    cp_id: str,
    transaction_id: int,
    estimated_amount: float,
):
    # 已送過停充的交易直接跳過（避免重複）
    if str(transaction_id) in pending_stop_transactions:
        return

    # 取得 id_tag 與餘額
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id_tag
            FROM transactions
            WHERE transaction_id = ?
        """,
            (transaction_id,),
        )
        row = cur.fetchone()
        if not row:
            return

        id_tag = row[0]

        cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
        card = cur.fetchone()

    if not card:
        return

    balance = float(card[0] or 0)

    # ⭐ 關鍵判斷：餘額不足
    if balance > estimated_amount:
        return

    logger.warning(
        f"[AUTO-STOP] balance insufficient "
        f"| cp_id={cp_id} "
        f"| tx_id={transaction_id} "
        f"| balance={balance} "
        f"| estimated={estimated_amount}"
    )

    cp = connected_charge_points.get(cp_id)
    if not cp:
        logger.error(f"[AUTO-STOP] CP not connected | cp_id={cp_id}")
        return

    # 建立等待 StopTransaction 的 future（沿用你原本機制）
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    pending_stop_transactions[str(transaction_id)] = fut

    # 發送 RemoteStopTransaction
    req = call.RemoteStopTransactionPayload(transaction_id=int(transaction_id))

    try:
        await cp.call(req)
        logger.warning(
            f"[AUTO-STOP][SEND] RemoteStopTransaction "
            f"| cp_id={cp_id} | tx_id={transaction_id}"
        )
    except Exception as e:
        logger.error(
            f"[AUTO-STOP][ERR] RemoteStop failed "
            f"| cp_id={cp_id} | tx_id={transaction_id} | err={e}"
        )


class ChargePoint(OcppChargePoint):
    # ...（你的其他方法，例如 on_status_notification, on_meter_values, ...）

    async def send_stop_transaction(self, transaction_id):
        import sqlite3
        from datetime import datetime, timezone

        # 讀取交易資訊
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT meter_stop, id_tag FROM transactions WHERE transaction_id = ?
            """,
                (transaction_id,),
            )
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
            reason=reason,
        )
        response = await self.call(request)
        return response

    async def send_remote_start_transaction(self, id_tag: str, connector_id: int = 1):
        from ocpp.v16 import call

        request = call.RemoteStartTransaction(id_tag=id_tag, connector_id=connector_id)
        response = await self.call(request)
        return response

    @on(Action.StatusNotification)
    async def on_status_notification(
        self, connector_id=None, status=None, error_code=None, timestamp=None, **kwargs
    ):
        global charging_point_status

        try:
            cp_id = getattr(self, "id", None)

            logging.warning(
                f"[DEBUG][STATUS][ENTER] "
                f"cp_id={cp_id} | connector_id={connector_id} | "
                f"status={status} | error_code={error_code} | timestamp={timestamp} | kwargs={kwargs}"
            )

            if cp_id is None:
                logging.error("[STATUS][INVALID] cp_id is None")
                return call_result.StatusNotificationPayload()

            try:
                connector_id = int(connector_id) if connector_id is not None else 0
            except (ValueError, TypeError):
                connector_id = 0

            status = str(status or "Unknown")
            error_code = str(error_code or "NoError")

            # 統一 timestamp 為 UTC ISO8601
            status_ts_utc = None
            if timestamp:
                try:
                    parsed_ts = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
                    if parsed_ts.tzinfo is None:
                        parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                    status_ts_utc = parsed_ts.astimezone(timezone.utc).isoformat()
                except Exception:
                    status_ts_utc = str(timestamp)
            else:
                status_ts_utc = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

            # 1) 寫 status_logs（這才是 StatusNotification 應該做的事）
            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                    cursor = conn.cursor()

                    try:
                        cursor.execute(
                            """
                            INSERT INTO status_logs
                            (charge_point_id, connector_id, status, timestamp, error_code)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (cp_id, connector_id, status, status_ts_utc, error_code),
                        )
                        conn.commit()

                    except sqlite3.OperationalError as e:
                        if "no column named error_code" in str(e):
                            logging.warning(
                                f"[STATUS][DB_LOG_FALLBACK] "
                                f"cp_id={cp_id} | connector_id={connector_id} | err={e}"
                            )
                            cursor.execute(
                                """
                                INSERT INTO status_logs
                                (charge_point_id, connector_id, status, timestamp)
                                VALUES (?, ?, ?, ?)
                                """,
                                (cp_id, connector_id, status, status_ts_utc),
                            )
                            conn.commit()

                            logging.warning(
                                f"[STATUS][DB_LOG_FALLBACK_OK] "
                                f"cp_id={cp_id} | connector_id={connector_id} | "
                                f"status={status} | timestamp={status_ts_utc}"
                            )

                        else:
                            raise

            except Exception as e:
                logging.exception(
                    f"[STATUS][DB_LOG_ERR] cp_id={cp_id} | connector_id={connector_id} | err={e}"
                )

            # 2) 更新記憶體狀態
            prev_status = charging_point_status.get(cp_id, {}) or {}
            charging_point_status[cp_id] = {
                **prev_status,
                "connector_id": connector_id,
                "status": status,
                "timestamp": status_ts_utc,
                "error_code": error_code,
                "derived": False,
            }

            # 3) 順手 patch live cache 的 status，不清空功率/電流
            prev_live = live_status_cache.get(cp_id, {}) or {}
            live_status_cache[cp_id] = {
                **prev_live,
                "status": status,
                "error_code": error_code,
                "timestamp": prev_live.get("timestamp") or status_ts_utc,
                "updated_at": time.time(),
                "derived": prev_live.get("derived", False),
            }

            logging.warning(
                f"[STATUS][OK] "
                f"cp_id={cp_id} | connector_id={connector_id} | "
                f"status={status} | error_code={error_code} | timestamp={status_ts_utc}"
            )

            return call_result.StatusNotificationPayload()

        except Exception as e:
            logging.exception(f"[STATUS][ERR] cp_id={getattr(self, 'id', None)} | err={e}")
            return call_result.StatusNotificationPayload()

    from ocpp.v16.enums import RegistrationStatus
    import os

    # =====================================================
    # SmartCharging 能力判定模式
    # - FORCE_SMART_CHARGING=1 → 強制視為支援（開發 / 模擬器）
    # - FORCE_SMART_CHARGING=0 → 一律視為不支援（正式 / 真實樁）
    #
    # ⚠️ 設計原則：
    # - BootNotification 階段【禁止】呼叫 GetConfiguration
    # - 避免因樁或 library 相容性問題造成誤判或卡樁
    # =====================================================
    FORCE_SMART_CHARGING = os.getenv("FORCE_SMART_CHARGING", "0") == "1"

    @on(Action.BootNotification)
    async def on_boot_notification(
        self, charge_point_model, charge_point_vendor, **kwargs
    ):
        logging.error("🔥🔥 BOOT HANDLER NEW VERSION ACTIVE 🔥🔥")

        """
        OCPP 1.6 BootNotification
        - 永遠回 Accepted（不能擋樁）
        - SmartCharging 僅設定「能力旗標」
        - 不做任何 SmartCharging 通訊（不呼叫 GetConfiguration）
        """
        now = datetime.utcnow().replace(tzinfo=timezone.utc)

        try:
            logging.warning(
                f"🔌 BootNotification | CP={self.id} | 模型={charge_point_model} | 廠商={charge_point_vendor}"
            )

            # =====================================================
            # 🔍 SmartCharging 能力判定（選項 A：模擬器強制）
            # =====================================================
            if FORCE_SMART_CHARGING:
                # ✅ 開發 / 模擬器：強制視為支援
                self.supports_smart_charging = True

                logging.warning(
                    f"[CAPABILITY][FORCE] CP={self.id} | "
                    f"FORCE_SMART_CHARGING=1 | supports_smart_charging=True"
                )
            else:
                # 🟡 正式環境：預設一律不支援（安全）
                self.supports_smart_charging = False

                logging.warning(
                    f"[CAPABILITY][DEFAULT] CP={self.id} | "
                    f"FORCE_SMART_CHARGING=0 | supports_smart_charging=False"
                )
            # =====================================================

            # =====================================================
            # ✅ 正常回應 BootNotification（永遠 Accepted）
            # =====================================================
            return call_result.BootNotificationPayload(
                current_time=now.isoformat(),
                interval=10,
                status=RegistrationStatus.accepted,
            )

        except Exception as e:
            # ❗ BootNotification 永遠不能擋樁
            logging.exception(f"❌ BootNotification handler error: {e}")
            return call_result.BootNotificationPayload(
                current_time=now.isoformat(),
                interval=10,
                status=RegistrationStatus.accepted,
            )

    @on(Action.Heartbeat)
    async def on_heartbeat(self):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        logging.warning(f"❤️ Heartbeat | CP={self.id}")
        return call_result.HeartbeatPayload(current_time=now.isoformat())

    @on(Action.Authorize)
    async def on_authorize(self, id_tag, **kwargs):
        t0 = time.perf_counter()
        cp_id = getattr(self, "id", "unknown")

        logging.warning(
            f"[DEBUG][AUTHORIZE][ENTER] cp_id={cp_id} | idTag={id_tag}"
        )

        try:
            t_db = time.perf_counter()

            with get_conn() as _c:
                cur = _c.cursor()
                cur.execute("SELECT status FROM id_tags WHERE id_tag = ?", (id_tag,))
                row = cur.fetchone()

            db_ms = _ms_since(t_db)

            if not row:
                status = "Invalid"
                status_db = None
            else:
                status_db = row[0]
                status = "Accepted" if status_db == "Accepted" else "Blocked"

            total_ms = _ms_since(t0)

            logging.warning(
                f"[DEBUG][AUTHORIZE][EXIT] "
                f"cp_id={cp_id} | idTag={id_tag} | status_db={status_db} | "
                f"result={status} | db_ms={db_ms} | total_ms={total_ms}"
            )

            return call_result.AuthorizePayload(id_tag_info={"status": status})

        except Exception as e:
            total_ms = _ms_since(t0)
            logging.exception(
                f"[DEBUG][AUTHORIZE][ERR] "
                f"cp_id={cp_id} | idTag={id_tag} | total_ms={total_ms} | err={e}"
            )
            return call_result.AuthorizePayload(id_tag_info={"status": "Invalid"})

    @on(Action.StartTransaction)
    async def on_start_transaction(
        self, connector_id, id_tag, meter_start, timestamp, **kwargs
    ):
        """
        ✅ 修正 A：保證 StartTransaction「一定回 CALLRESULT」
        - 任何情況都回 StartTransactionPayload
        - 絕不裸 return
        - 絕不 await 任何會卡住的動作
        """

        try:
            tx_t0 = time.perf_counter()

            # =================================================
            # [0] SmartCharging 能力保證（debug / 防 cp instance 問題）
            # =================================================
            if not hasattr(self, "supports_smart_charging"):
                self.supports_smart_charging = True
                logging.error(
                    f"🧪 [DEBUG][START_TX][FIX] CP={self.id} | "
                    f"supports_smart_charging defaulted to True"
                )

            logging.error(
                f"🧪 [DEBUG][START_TX][ENTER] "
                f"CP={self.id} | "
                f"supports_smart_charging={getattr(self, 'supports_smart_charging', 'MISSING')}"
            )

            logging.warning(
                f"[DEBUG][START_TX][INPUT] "
                f"cp_id={self.id} | connector_id={connector_id} | idTag={id_tag} | "
                f"meter_start={meter_start} | timestamp={timestamp}"
            )

            # =================================================
            # [1] idTag 驗證
            # =================================================
            t_step = time.perf_counter()
            with get_conn() as _c:
                cur = _c.cursor()
                cur.execute("SELECT status FROM id_tags WHERE id_tag = ?", (id_tag,))
                row = cur.fetchone()

            logging.warning(
                f"[DEBUG][START_TX][STEP] "
                f"cp_id={self.id} | step=id_tags_lookup | ms={_ms_since(t_step)}"
            )

            if not row:
                logging.warning(f"🔴 StartTransaction Invalid：idTag={id_tag} 不存在")
                logging.warning(
                    f"[DEBUG][START_TX][EXIT] "
                    f"cp_id={self.id} | transaction_id=0 | result=Invalid | total_ms={_ms_since(tx_t0)}"
                )
                return call_result.StartTransactionPayload(
                    transaction_id=0, id_tag_info={"status": "Invalid"}
                )

            status_db = row[0]

            if status_db != "Accepted":
                logging.warning(
                    f"🔴 StartTransaction Blocked：idTag={id_tag} | status_db={status_db}"
                )
                logging.warning(
                    f"[DEBUG][START_TX][EXIT] "
                    f"cp_id={self.id} | transaction_id=0 | result=Blocked | total_ms={_ms_since(tx_t0)}"
                )
                return call_result.StartTransactionPayload(
                    transaction_id=0, id_tag_info={"status": "Blocked"}
                )

            status_db = row[0]

            if status_db != "Accepted":
                logging.warning(
                    f"🔴 StartTransaction Blocked：idTag={id_tag} | status_db={status_db}"
                )
                logging.warning(
                    f"[DEBUG][START_TX][EXIT] "
                    f"cp_id={self.id} | transaction_id=0 | result=Blocked | total_ms={_ms_since(tx_t0)}"
                )
                return call_result.StartTransactionPayload(
                    transaction_id=0, id_tag_info={"status": "Blocked"}
                )

            # =================================================
            # [2] 預約檢查（若命中 → completed）
            # =================================================
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

            if _is_debug_target_cp(self.id):
                logging.warning(
                    f"[DEBUG][START_TX][TIME_INPUT] "
                    f"cp_id={self.id} | "
                    f"connector_id={connector_id} | "
                    f"id_tag={id_tag} | "
                    f"ocpp_timestamp={timestamp} | "
                    f"server_now_utc={now_utc} | "
                    f"meter_start={meter_start}"
                )

            t_step = time.perf_counter()
            with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT id FROM reservations
                    WHERE charge_point_id=? AND id_tag=? AND status='active'
                      AND start_time<=? AND end_time>=?
                    """,
                    (self.id, id_tag, now_utc, now_utc),
                )
                res = cursor.fetchone()

                if res:
                    cursor.execute(
                        "UPDATE reservations SET status='completed' WHERE id=?",
                        (res[0],),
                    )
                    conn.commit()

            logging.warning(
                f"[DEBUG][START_TX][STEP] "
                f"cp_id={self.id} | step=reservation_lookup | ms={_ms_since(t_step)} | hit={bool(res)}"
            )

            if res:
                logging.warning(
                    f"🟡 Reservation completed | cp={self.id} | idTag={id_tag}"
                )
            # =================================================
            # [3] 餘額檢查
            # =================================================
            t_step = time.perf_counter()
            with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
                card = cursor.fetchone()

            logging.warning(
                f"[DEBUG][START_TX][STEP] "
                f"cp_id={self.id} | step=card_balance_lookup | ms={_ms_since(t_step)}"
            )

            if not card:
                logging.warning(f"🔴 StartTransaction Invalid：card {id_tag} 不存在")
                logging.warning(
                    f"[DEBUG][START_TX][EXIT] "
                    f"cp_id={self.id} | transaction_id=0 | result=Invalid | total_ms={_ms_since(tx_t0)}"
                )
                return call_result.StartTransactionPayload(
                    transaction_id=0, id_tag_info={"status": "Invalid"}
                )

            balance = float(card[0] or 0)
            if balance <= 0:
                logging.warning(
                    f"🔴 StartTransaction Blocked：idTag={id_tag} | balance={balance}"
                )
                logging.warning(
                    f"[DEBUG][START_TX][EXIT] "
                    f"cp_id={self.id} | transaction_id=0 | result=Blocked | total_ms={_ms_since(tx_t0)}"
                )
                return call_result.StartTransactionPayload(
                    transaction_id=0, id_tag_info={"status": "Blocked"}
                )
            # ==================================================
            # [3.5] 🏘️ Smart Charging：最後一台車輛擋下判斷（第一階段：功率分配模式）
            # ==================================================
            try:
                effective_active_cp_ids_before = get_effective_active_cp_ids()
                active_now = len(effective_active_cp_ids_before)

                cp_norm = _normalize_cp_id(self.id)
                trial_cp_ids = list(effective_active_cp_ids_before)

                if cp_norm not in trial_cp_ids:
                    trial_cp_ids.append(cp_norm)

                trial_count = len(trial_cp_ids)

                allocated_kw = calculate_allocated_power_kw_by_cp_ids(trial_cp_ids)
                preview_current_a = (
                    convert_power_kw_to_current_a(allocated_kw, cp_norm)
                    if allocated_kw is not None
                    else None
                )

                logging.warning(
                    f"[SMART][START_TX][CHECK] "
                    f"cp_id={cp_norm} | "
                    f"effective_active_cp_ids_before={effective_active_cp_ids_before} | "
                    f"active_now={active_now} | "
                    f"trial_cp_ids_after_join={trial_cp_ids} | "
                    f"trial_count={trial_count} | "
                    f"allocated_kw={allocated_kw} | "
                    f"preview_current_a={preview_current_a} | "
                    f"single_cp_max_kw={SINGLE_CP_MAX_POWER_KW}"
                )

                if allocated_kw is None:
                    logging.error(
                        f"[SMART][START_TX][BLOCKED] "
                        f"cp_id={cp_norm} | reason=contract_kw_invalid_or_zero | "
                        f"trial_cp_ids_after_join={trial_cp_ids}"
                    )
                    logging.warning(
                        f"[DEBUG][START_TX][EXIT] "
                        f"cp_id={cp_norm} | transaction_id=0 | result=Blocked | total_ms={_ms_since(tx_t0)}"
                    )
                    return call_result.StartTransactionPayload(
                        transaction_id=0,
                        id_tag_info={"status": "Blocked"},
                    )

            except Exception as e:
                # ⚠️ 保守策略：SmartCharging 出錯時，不影響原本流程
                logging.exception(
                    f"[SMART][START_TX][ERROR] cp_id={self.id} | err={e}"
                )
            # =================================================
            # [4] 建立交易（⚠️ 只做 DB，不做 await）
            # =================================================
            start_ts_to_save = now_utc

            if timestamp:
                try:
                    parsed_start_ts = datetime.fromisoformat(
                        str(timestamp).replace("Z", "+00:00")
                    )
                    if parsed_start_ts.tzinfo is None:
                        parsed_start_ts = parsed_start_ts.replace(
                            tzinfo=timezone.utc
                        )
                    start_ts_to_save = parsed_start_ts.astimezone(
                        timezone.utc
                    ).isoformat()
                except Exception:
                    start_ts_to_save = str(timestamp)
            else:
                start_ts_to_save = datetime.utcnow().replace(
                    tzinfo=timezone.utc
                ).isoformat()

            t_step = time.perf_counter()
            with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO transactions (
                        charge_point_id,
                        connector_id,
                        id_tag,
                        meter_start,
                        start_timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        self.id,
                        connector_id,
                        id_tag,
                        int(meter_start),
                        start_ts_to_save,
                    ),
                )
                tx_id = cursor.lastrowid
                conn.commit()

            logging.warning(
                f"[DEBUG][START_TX][STEP] "
                f"cp_id={self.id} | step=insert_transaction | ms={_ms_since(t_step)} | transaction_id={tx_id}"
            )

            if _is_debug_target_cp(self.id):
                logging.warning(
                    f"[DEBUG][START_TX][DB_WRITE] "
                    f"cp_id={self.id} | "
                    f"tx_id={tx_id} | "
                    f"written_start_timestamp={start_ts_to_save} | "
                    f"ocpp_timestamp={timestamp}"
                )

            logging.warning(
                f"🟢 StartTransaction Accepted | "
                f"cp={self.id} | connector={connector_id} | "
                f"idTag={id_tag} | tx_id={tx_id} | balance={balance}"
            )

            # ==================================================
            # [4.5] 🟦 Smart Charging：StartTransaction 後全場 Rebalance
            # ==================================================
            try:
                asyncio.create_task(
                    rebalance_all_charging_points(reason="start_transaction")
                )
            except Exception as e:
                logging.exception(
                    f"[SMART][START_TX][REBALANCE_ERR] cp_id={self.id} | err={e}"
                )

            # ===============================
            # 🔌 StartTransaction 後立即限流（純社區模式：由 rebalance 接管）
            # ===============================
            try:
                logging.warning(
                    f"[LIMIT][START_TX] cp_id={self.id} | mode=community_only | action=rebalance_managed"
                )
            except Exception as e:
                logging.exception(
                    f"[LIMIT][START_TX][ERROR] cp_id={self.id} | err={e}"
                )

            # =================================================
            # [5] ✅ 正常回覆（最重要）
            # =================================================
            logging.warning(
                f"[DEBUG][START_TX][EXIT] "
                f"cp_id={self.id} | transaction_id={int(tx_id)} | result=Accepted | total_ms={_ms_since(tx_t0)}"
            )
            return call_result.StartTransactionPayload(
                transaction_id=int(tx_id),
                id_tag_info={"status": "Accepted"},
            )

        except Exception as e:
            # =================================================
            # [X] 防爆：任何例外都必須回 CALLRESULT
            # =================================================
            logging.exception(
                f"[DEBUG][START_TX][ERR] "
                f"cp_id={getattr(self, 'id', '?')} | idTag={id_tag} | total_ms={_ms_since(tx_t0)} | err={e}"
            )
            return call_result.StartTransactionPayload(
                transaction_id=0,
                id_tag_info={"status": "Rejected"},
            )

    @on(Action.StopTransaction)
    async def on_stop_transaction(self, **kwargs):
        cp_id = getattr(self, "id", None)

        # ==================================================
        # DEBUG：原始 StopTransaction payload（低噪音）
        # ==================================================
        logger.warning(
            "[STOP][RAW] cp_id=%s | keys=%s | kwargs=%s",
            cp_id,
            list(kwargs.keys()),
            kwargs,
        )

        # ==================================================
        # 取關鍵欄位（相容 camelCase / snake_case）
        # ==================================================
        transaction_id = kwargs.get("transaction_id") or kwargs.get("transactionId")
        meter_stop = kwargs.get("meter_stop") or kwargs.get("meterStop")
        raw_ts = kwargs.get("timestamp")
        reason = kwargs.get("reason")

        logger.warning(
            f"[STOP][DONE] StopTransaction received "
            f"| cp_id={cp_id} "
            f"| tx_id={transaction_id} "
            f"| meter_stop={meter_stop} "
            f"| reason={reason}"
        )

        # ⚠️ 沒有 transaction_id 直接回
        if not transaction_id:
            logger.error("🔴 StopTransaction missing transaction_id")
            return call_result.StopTransactionPayload()

        # ==================================================
        # 確保 stop timestamp
        # ==================================================
        try:
            if raw_ts:
                stop_ts = (
                    datetime.fromisoformat(raw_ts).astimezone(timezone.utc).isoformat()
                )
            else:
                raise ValueError("Empty timestamp")
        except Exception:
            stop_ts = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

        try:
            with sqlite3.connect(DB_FILE) as _conn:
                _cur = _conn.cursor()

                # ==================================================
                # 記錄 StopTransaction
                # ==================================================
                _cur.execute(
                    """
                    INSERT INTO stop_transactions (
                        transaction_id, meter_stop, timestamp, reason
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (transaction_id, meter_stop, stop_ts, reason),
                )

                _cur.execute(
                    """
                    UPDATE transactions
                    SET meter_stop = ?, stop_timestamp = ?, reason = ?
                    WHERE transaction_id = ?
                    """,
                    (meter_stop, stop_ts, reason, transaction_id),
                )

                # ==================================================
                # 補一筆結尾能量紀錄（避免能量斷層）
                # ==================================================
                _cur.execute(
                    """
                    INSERT INTO meter_values (
                        charge_point_id, connector_id, transaction_id,
                        value, measurand, unit, timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cp_id,
                        0,
                        transaction_id,
                        0.0,
                        "Energy.Active.Import.Register",
                        "kWh",
                        stop_ts,
                    ),
                )

                # ==================================================
                # 取得交易與起始電量
                # ==================================================
                _cur.execute(
                    """
                    SELECT id_tag, meter_start
                    FROM transactions
                    WHERE transaction_id = ?
                    """,
                    (transaction_id,),
                )
                row = _cur.fetchone()

                if not row:
                    logger.error(
                        f"[STOP][ERR] transaction not found | tx_id={transaction_id}"
                    )
                else:
                    id_tag, meter_start = row

                    try:
                        used_kwh = max(
                            0.0,
                            (float(meter_stop or 0) - float(meter_start or 0)) / 1000.0,
                        )
                    except Exception:
                        used_kwh = 0.0

                    unit_price = float(_price_for_timestamp(stop_ts))
                    total_amount = round(used_kwh * unit_price, 2)

                    # ==================================================
                    # 多時段電價（若有）
                    # ==================================================
                    try:
                        mp_amount = _calculate_multi_period_cost(transaction_id)
                        if mp_amount > 0:
                            total_amount = round(mp_amount, 2)
                            logger.info(f"🧮 多時段電價計算結果：{total_amount}")
                    except Exception as e:
                        logger.warning(f"⚠️ 多時段電價計算失敗：{e}")

                    logger.error(
                        f"[STOP][BILL] tx_id={transaction_id} "
                        f"| meter_start={meter_start} "
                        f"| meter_stop={meter_stop} "
                        f"| used_kwh={used_kwh} "
                        f"| unit_price={unit_price} "
                        f"| total_amount={total_amount}"
                    )

                    # ==================================================
                    # 卡片扣款（正式結算）
                    # ==================================================
                    _cur.execute(
                        "SELECT balance FROM cards WHERE card_id = ?",
                        (id_tag,),
                    )
                    card = _cur.fetchone()

                    if not card:
                        logger.error(f"[STOP][ERR] card not found | card_id={id_tag}")
                    else:
                        old_balance = float(card[0] or 0)
                        new_balance = max(0.0, old_balance - total_amount)

                        _cur.execute(
                            "UPDATE cards SET balance = ? WHERE card_id = ?",
                            (new_balance, id_tag),
                        )

                        logger.error(
                            f"[STOP][UPDATE] card_id={id_tag} "
                            f"| old={old_balance} "
                            f"| new={new_balance} "
                            f"| cost={total_amount} "
                            f"| rowcount={_cur.rowcount}"
                        )

                    # ==================================================
                    # 紀錄付款
                    # ==================================================
                    _cur.execute(
                        """
                        INSERT INTO payments (
                            transaction_id, base_fee, energy_fee,
                            overuse_fee, total_amount, paid_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            transaction_id,
                            0.0,
                            total_amount,
                            0.0,
                            total_amount,
                            stop_ts,
                        ),
                    )

                _conn.commit()
                logger.error("[STOP][COMMIT] DB commit done")

        except Exception as e:
            logger.exception(f"🔴 StopTransaction DB/計算發生錯誤：{e}")

        finally:
            # ==================================================
            # 更新 live_status（真正 StopTransaction 完成後才回 Available）
            # ==================================================
            prev = live_status_cache.get(cp_id, {})

            live_status_cache[cp_id] = {
                "status": "Available",
                "power": 0,
                "voltage": 0,
                "current": 0,
                "energy": prev.get("energy", 0),
                "estimated_energy": 0,
                "estimated_amount": 0,
                "price_per_kwh": prev.get("price_per_kwh", 0),
                "timestamp": stop_ts,
                "derived": True,
                "updated_at": time.time(),
            }

            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn2:
                    cur2 = conn2.cursor()

                    # 確認此 CP 是否還有其他未結束交易
                    cur2.execute(
                        """
                        SELECT 1
                        FROM transactions
                        WHERE charge_point_id = ?
                          AND stop_timestamp IS NULL
                        LIMIT 1
                        """,
                        (cp_id,),
                    )
                    has_other_active_tx = cur2.fetchone() is not None

                    # 只有真正沒有 active tx，才正式回 Available
                    if not has_other_active_tx:
                        cur2.execute(
                            """
                            INSERT INTO status_logs
                            (charge_point_id, connector_id, status, timestamp)
                            VALUES (?, ?, ?, ?)
                            """,
                            (cp_id, 0, "Available", stop_ts),
                        )
                        conn2.commit()

                        charging_point_status[cp_id] = {
                            "connector_id": 0,
                            "status": "Available",
                            "timestamp": stop_ts,
                            "error_code": "NoError",
                            "derived": True,
                        }

                        logger.warning(
                            f"[STOP][SET_AVAILABLE] cp_id={cp_id} | tx_id={transaction_id}"
                        )
                    else:
                        logger.warning(
                            f"[STOP][KEEP_NON_AVAILABLE] cp_id={cp_id} | tx_id={transaction_id} | reason=other_active_tx_exists"
                        )

            except Exception as e:
                logger.exception(
                    f"[STOP][SET_AVAILABLE_ERR] cp_id={cp_id} | tx_id={transaction_id} | err={e}"
                )

            # ==================================================
            # 解鎖 stop API 等待 future
            # ==================================================
            fut = pending_stop_transactions.get(str(transaction_id))
            logger.debug(
                f"🧾【StopTransaction FUTURE】hit={bool(fut)} "
                f"done={(fut.done() if fut else None)} "
                f"keys={list(pending_stop_transactions.keys())}"
            )

            if fut and not fut.done():
                fut.set_result(
                    {
                        "transaction_id": transaction_id,
                        "meter_stop": meter_stop,
                        "timestamp": stop_ts,
                        "reason": reason,
                    }
                )

        # ==================================================
        # 🟦 Smart Charging：StopTransaction 後全場 Rebalance
        # ==================================================
        try:
            await rebalance_all_charging_points(reason="stop_transaction")
        except Exception as e:
            logging.exception(
                f"[SMART][STOP_TX][REBALANCE_ERR] " f"cp_id={cp_id} | err={e}"
            )

        # ⚠️ 永遠回 CALLRESULT
        return call_result.StopTransactionPayload()

    @on(Action.MeterValues)
    async def on_meter_values(self, **kwargs):
        """
        ✅ 穩定版：
        - 先收齊 voltage / current / power
        - 再一次性更新 live_status_cache
        - 避免多段 upsert 互相覆蓋
        """
        try:
            cp_id = getattr(self, "id", None)
            if not cp_id:
                logging.error("❌ 無法識別充電樁 ID（self.id 為空）")
                return call_result.MeterValuesPayload()

            connector_id = pick(kwargs, "connectorId", "connector_id", default=0)
            try:
                connector_id = int(connector_id or 0)
            except Exception:
                connector_id = 0

            transaction_id = pick(
                kwargs, "transactionId", "transaction_id", "TransactionId", default=""
            )

            meter_values_t0 = time.time()

            if _is_debug_target_cp(cp_id):
                logging.warning(
                    f"[DEBUG][MV][ENTER] "
                    f"cp_id={cp_id} | transaction_id={transaction_id} | "
                    f"connector_id={connector_id}"
                )

            meter_value_list = (
                pick(kwargs, "meterValue", "meter_value", default=[]) or []
            )
            if not isinstance(meter_value_list, list):
                meter_value_list = [meter_value_list]

            # === 本批次即時量測（只取「最新 timestamp 那一組」）===
            batch_voltage = None
            batch_current = None
            batch_power_kw = None
            batch_voltage_rank = 999
            batch_current_rank = 999
            batch_power_rank = 999
            last_ts = None
            latest_live_ts = None

            insert_count = 0

            with sqlite3.connect(DB_FILE) as _c:
                _cur = _c.cursor()

                for mv in meter_value_list:
                    ts = pick(mv, "timestamp", "timeStamp", "Timestamp")
                    if ts:
                        last_ts = ts

                    sampled_list = (
                        pick(
                            mv,
                            "sampledValue",
                            "sampled_value",
                            "SampledValue",
                            default=[],
                        )
                        or []
                    )
                    if not isinstance(sampled_list, list):
                        sampled_list = [sampled_list]

                    # ✅ 只讓「最新 timestamp」那筆 meterValue 參與即時功率/電流/電壓顯示
                    if ts:
                        if latest_live_ts is None or str(ts) >= str(latest_live_ts):
                            if latest_live_ts is None or str(ts) > str(latest_live_ts):
                                batch_voltage = None
                                batch_current = None
                                batch_power_kw = None
                                batch_voltage_rank = 999
                                batch_current_rank = 999
                                batch_power_rank = 999
                            latest_live_ts = ts

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
                            continue

                        # === 寫 DB（你原本的功能，保留）===
                        _cur.execute(
                            """
                            INSERT INTO meter_values
                              (charge_point_id, connector_id, transaction_id,
                               value, measurand, unit, timestamp, phase)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                cp_id,
                                connector_id,
                                transaction_id,
                                val,
                                meas,
                                unit,
                                ts,
                                phase,
                            ),
                        )
                        insert_count += 1

                        # ✅ 只收集最新 timestamp 那一組的即時量測
                        if latest_live_ts is not None and ts is not None and str(ts) != str(latest_live_ts):
                            continue

                        meas_s = str(meas or "")
                        phase_s = str(phase or "").strip()

                        def _phase_rank(p: str) -> int:
                            # 越小越優先：None/空白 最優先，其次 L1，其餘先不採用
                            if p == "":
                                return 0
                            if p.upper() == "L1":
                                return 1
                            return 99

                        phase_rank = _phase_rank(phase_s)

                        if meas_s == "Voltage" or meas_s.startswith("Voltage."):
                            prev_rank = locals().get("batch_voltage_rank", 999)
                            if phase_rank < prev_rank:
                                batch_voltage = val
                                batch_voltage_rank = phase_rank

                        elif meas_s.startswith("Current.Import"):
                            prev_rank = locals().get("batch_current_rank", 999)
                            if phase_rank < prev_rank:
                                batch_current = val
                                batch_current_rank = phase_rank

                        elif meas_s.startswith("Power.Active.Import"):
                            prev_rank = locals().get("batch_power_rank", 999)
                            if phase_rank < prev_rank:
                                batch_power_kw = _to_kw(val, unit)
                                batch_power_rank = phase_rank
                        # === 能量 / 金額（原邏輯保留）===
                        if "Energy.Active.Import" in meas:
                            try:
                                res = _calculate_multi_period_cost_detailed(
                                    transaction_id
                                )
                                used_kwh = (
                                    sum(s["kwh"] for s in res["segments"])
                                    if res["segments"]
                                    else 0
                                )
                                total = res["total"]
                                price = (
                                    res["segments"][-1]["price"]
                                    if res["segments"]
                                    else _price_for_timestamp(ts)
                                )

                                live_patch = {}

                                if batch_voltage is not None:
                                    live_patch["voltage"] = batch_voltage

                                if batch_current is not None:
                                    live_patch["current"] = batch_current

                                if batch_power_kw is not None:
                                    live_patch["power"] = batch_power_kw

                                if last_ts is not None:
                                    live_patch["timestamp"] = last_ts

                                _upsert_live(cp_id, **live_patch)

                                await _auto_stop_if_balance_insufficient(
                                    cp_id=cp_id,
                                    transaction_id=int(transaction_id),
                                    estimated_amount=float(total),
                                )
                            except Exception as e:
                                logging.warning(f"⚠️ 能量/金額即時計算失敗：{e}")

                _c.commit()

            # === 推算功率（若樁沒送 Power）===
            power_derived_from_vi = False

            if batch_power_kw is None:
                if isinstance(batch_voltage, (int, float)) and isinstance(
                    batch_current, (int, float)
                ):
                    batch_power_kw = round((batch_voltage * batch_current) / 1000.0, 3)
                    power_derived_from_vi = True

            if _is_debug_target_cp(cp_id):
                logging.warning(
                    f"[DEBUG][MV][FINAL] "
                    f"cp_id={cp_id} | tx_id={transaction_id} | connector_id={connector_id} | "
                    f"last_ts={last_ts} | "
                    f"final_voltage={batch_voltage} | "
                    f"final_current={batch_current} | "
                    f"final_power_kw={batch_power_kw} | "
                    f"power_derived_from_vi={power_derived_from_vi} | "
                    f"meter_rows={insert_count}"
                )

            # =====================================================
            # ✅ 先更新即時狀態（讓 ΣV 計算拿得到最新 voltage）
            # =====================================================
            allocated_kw_for_live = None
            preview_current_a_for_live = None

            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT charge_point_id
                        FROM transactions
                        WHERE stop_timestamp IS NULL
                          AND start_timestamp IS NOT NULL
                        """
                    )
                    rows = cur.fetchall()

                active_cp_ids = []
                for (cpid,) in rows:
                    cpid = _normalize_cp_id(str(cpid))
                    if cpid not in active_cp_ids:
                        active_cp_ids.append(cpid)

                active_cp_ids = [
                    cid for cid in active_cp_ids
                    if is_cp_effectively_available_for_allocation(cid)
                ]

                if cp_id in active_cp_ids:
                    allocated_kw_for_live = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
                    preview_current_a_for_live = (
                        convert_power_kw_to_current_a(allocated_kw_for_live, cp_id)
                        if allocated_kw_for_live is not None
                        else None
                    )
            except Exception as e:
                logging.warning(f"[LIVE][ALLOC_PATCH_ERR] cp_id={cp_id} | err={e}")

            _upsert_live(
                cp_id,
                status="Charging",
                voltage=batch_voltage,
                current=batch_current,
                power=batch_power_kw,
                timestamp=last_ts,
                allocated_power_kw=allocated_kw_for_live,
                preview_current_a=preview_current_a_for_live,
            )


            # =====================================================
            # 🔥 強制電流壓制（實測值 > 功率分配換算後理論值）
            # 第一階段：先算 allocated_kw，再換算 theory_a
            # =====================================================
            try:
                # 取出所有 active 交易的 cp_id 清單
                with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT charge_point_id
                        FROM transactions
                        WHERE stop_timestamp IS NULL
                          AND start_timestamp IS NOT NULL
                        """
                    )
                    rows = cur.fetchall()

                active_cp_ids = []
                for (cpid,) in rows:
                    cpid = _normalize_cp_id(str(cpid))
                    if cpid not in active_cp_ids:
                        active_cp_ids.append(cpid)

                # ✅ 與 rebalance / send_current_limit_profile 統一口徑：
                #    已連線 or grace 中的樁，都要納入功率分配計算
                active_cp_ids = [
                    cid for cid in active_cp_ids
                    if is_cp_effectively_available_for_allocation(cid)
                ]

                allocated_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
                theory_a = (
                    convert_power_kw_to_current_a(allocated_kw, cp_id)
                    if allocated_kw is not None
                    else None
                )

                if theory_a is not None and batch_current is not None:
                    theory_a = float(theory_a)

                    if float(batch_current) > theory_a + 0.5:  # 容忍 0.5A 誤差
                        logging.warning(
                            f"[FORCE-CLAMP] "
                            f"cp_id={cp_id} | tx_id={transaction_id} | connector_id={connector_id} | "
                            f"measured_current_a={float(batch_current):.2f} | theory_a={theory_a:.2f} | "
                            f"measured_power_kw={batch_power_kw} | allocated_kw={allocated_kw} | "
                            f"active_cp_ids={active_cp_ids} | action=re_send_limit"
                        )

                        asyncio.create_task(
                            send_current_limit_profile(
                                cp=connected_charge_points[cp_id],
                                connector_id=int(connector_id or 1),
                                limit_a=theory_a,
                                tx_id=int(transaction_id) if str(transaction_id).isdigit() else None,
                            )
                        )

                        logging.warning(
                            f"[FORCE-CLAMP][ASYNC_DISPATCH] "
                            f"cp_id={cp_id} | connector_id={connector_id} | "
                            f"transaction_id={transaction_id} | theory_a={theory_a}"
                        )

            except Exception as e:
                logging.warning(
                    f"[FORCE-CLAMP][ERR] cp_id={cp_id} | err={e}"
                )

            logging.info(
                f"[LIVE][OK] cp={cp_id} "
                f"V={batch_voltage}V "
                f"I={batch_current}A "
                f"P={batch_power_kw}kW "
                f"| meter_rows={insert_count}"
            )

            if _is_debug_target_cp(cp_id):
                logging.warning(
                    f"[DEBUG][MV][EXIT] "
                    f"cp_id={cp_id} | transaction_id={transaction_id} | "
                    f"elapsed_ms={(time.time() - meter_values_t0) * 1000:.1f}"
                )

            return call_result.MeterValuesPayload()

        except Exception as e:
            logging.exception(f"❌ 處理 MeterValues 例外：{e}")

            if 'cp_id' in locals() and _is_debug_target_cp(cp_id):
                logging.warning(
                    f"[DEBUG][MV][EXIT_ERR] "
                    f"cp_id={cp_id} | transaction_id={transaction_id} | "
                    f"elapsed_ms={(time.time() - meter_values_t0) * 1000:.1f}"
                )

            return call_result.MeterValuesPayload()

    @on(Action.RemoteStopTransaction)
    async def on_remote_stop_transaction(self, transaction_id, **kwargs):
        logging.info(f"✅ 收到遠端停止充電要求，transaction_id={transaction_id}")
        return call_result.RemoteStopTransactionPayload(status="Accepted")


from fastapi import Body


@app.post("/api/card-owners/{card_id}")
def update_card_owner(card_id: str, data: dict = Body(...)):
    name = data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="名稱不可空白")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO card_owners (card_id, name)
            VALUES (?, ?)
            ON CONFLICT(card_id) DO UPDATE SET name=excluded.name
        """,
            (card_id, name),
        )
        conn.commit()

    return {"message": "住戶名稱已更新", "card_id": card_id, "name": name}


@app.post("/api/debug/force-add-charge-point")
def force_add_charge_point(
    charge_point_id: str = "TW*MSI*E000100", name: str = "MSI充電樁"
):
    """
    Debug 用 API：強制新增一個充電樁到白名單 (charge_points 資料表)。
    不會自動建立任何卡片或餘額。
    """
    with get_conn() as conn:
        cur = conn.cursor()
        # 只建立白名單，不建立卡片
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
        "name": name,
    }


# ------------------------------------------------------------
# ✅ 修正版：充電樁斷線時，不主動改寫交易狀態
# ------------------------------------------------------------
async def on_disconnect(self, websocket, close_code):
    try:
        cp_id = getattr(websocket, "cp_id", None)

        if not cp_id:
            logging.warning("⚠️ on_disconnect：無法取得 cp_id")
            return

        # 僅移除連線控制權，不代表交易結束
        connected_charge_points.pop(cp_id, None)

        logging.warning(
            f"⚠️ 充電樁斷線 | cp_id={cp_id} | "
            f"保持原有交易狀態，等待重連或 StopTransaction"
        )

        # ❗❗ 重要原則：
        # WebSocket 斷線 ≠ 交易結束
        # 狀態只允許由 StopTransaction / StatusNotification 結束

    except Exception as e:
        logging.error(f"❌ on_disconnect 發生例外: {e}")


from fastapi import Body


@app.post("/api/cards/{card_id}/whitelist")
async def update_card_whitelist(card_id: str, data: dict = Body(...)):
    allowed = data.get("allowed", [])

    with get_conn() as conn:
        cur = conn.cursor()

        # 卡片必須存在
        cur.execute("SELECT 1 FROM cards WHERE card_id = ?", (card_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="card not found")

        # 檢查所有 cp_id 都存在於 charge_points
        for cp_id in allowed:
            cur.execute(
                "SELECT 1 FROM charge_points WHERE charge_point_id = ?",
                (cp_id,),
            )
            if not cur.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail=f"charge point not found: {cp_id}",
                )

        # 清掉舊白名單
        cur.execute("DELETE FROM card_whitelist WHERE card_id = ?", (card_id,))

        # 寫入新的
        for cp_id in allowed:
            cur.execute(
                "INSERT INTO card_whitelist (card_id, charge_point_id) VALUES (?, ?)",
                (card_id, cp_id),
            )

        conn.commit()

    return {"message": "Whitelist updated", "allowed": allowed}


from ocpp.exceptions import OCPPError


@app.post("/api/charge-points/{charge_point_id:path}/stop")
async def stop_transaction_by_charge_point(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    logger.info(
        f"🟢【API呼叫】收到停止充電API請求 raw={charge_point_id} normalized={cp_id}"
    )
    logger.debug(
        f"🧭【DEBUG】connected_charge_points keys={list(connected_charge_points.keys())}"
    )

    cp = connected_charge_points.get(cp_id)
    if not cp:
        logger.error(f"🔴【API異常】找不到連線中的充電樁：{cp_id}")
        raise HTTPException(status_code=404, detail=f"⚠️ 找不到連線中的充電樁：{cp_id}")

    # === 找進行中交易 ===
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT transaction_id
            FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC
            LIMIT 1
            """,
            (cp_id,),
        )
        row = cursor.fetchone()
        if not row:
            logger.error(f"🔴【API異常】無進行中交易 cp_id={cp_id}")
            raise HTTPException(status_code=400, detail="⚠️ 無進行中交易")

        transaction_id = row[0]

    logger.info(
        f"🟢【API呼叫】找到進行中交易 transaction_id={transaction_id} "
        f"(type={type(transaction_id)})"
    )

    # === 建立 StopTransaction 等待 future ===
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    pending_stop_transactions[str(transaction_id)] = fut
    logger.debug(
        f"🧩【DEBUG】pending_stop_transactions add key={transaction_id} "
        f"size={len(pending_stop_transactions)}"
    )

    # === 發送 RemoteStopTransaction（⚠️ 不管回傳結果）===
    req = call.RemoteStopTransactionPayload(transaction_id=int(transaction_id))
    logger.debug(
        f"📤【DEBUG】RemoteStopTransaction payload tx_id={int(transaction_id)}"
    )

    logger.warning(
        f"[STOP][SEND] RemoteStopTransaction "
        f"| cp_id={cp_id} "
        f"| tx_id={transaction_id} "
        f"| ws_connected={cp_id in connected_charge_points}"
    )

    try:
        await cp.call(req)
        logger.warning(
            f"[STOP][ACK] RemoteStopTransaction accepted "
            f"| cp_id={cp_id} | tx_id={transaction_id}"
        )
    except Exception as e:
        logger.error(
            f"[STOP][ERR] RemoteStopTransaction failed "
            f"| cp_id={cp_id} | tx_id={transaction_id} | err={repr(e)}"
        )

    # === 唯一成功依據：等待 StopTransaction ===
    try:
        stop_result = await asyncio.wait_for(fut, timeout=15)
        logger.info(f"🟢【API回應】StopTransaction 完成 tx={transaction_id}")
        return {
            "message": "充電已停止",
            "transaction_id": transaction_id,
            "stop_result": stop_result,
        }

    except asyncio.TimeoutError:
        logger.error(
            f"[STOP][TIMEOUT] StopTransaction not received "
            f"| cp_id={cp_id} "
            f"| tx_id={transaction_id} "
            f"| pending_keys={list(pending_stop_transactions.keys())}"
        )
        return JSONResponse(
            status_code=504,
            content={"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"},
        )

    finally:
        pending_stop_transactions.pop(str(transaction_id), None)
        logger.debug(
            f"🧹【DEBUG】pending_stop_transactions pop key={transaction_id} "
            f"size={len(pending_stop_transactions)}"
        )


from fastapi import Body, HTTPException


@app.put("/api/charge-points/{charge_point_id:path}")
def update_charge_point(charge_point_id: str, data: dict = Body(...)):
    # 1️⃣ 正規化 CP ID（處理 TW*MSI*E000100 / URL encode）
    cp_id = _normalize_cp_id(charge_point_id)

    # 2️⃣ 前端欄位（⚠️ 關鍵：maxCurrent）
    name = data.get("name")
    status = data.get("status")
    max_current = data.get("maxCurrent")  # ← 前端送來的電流上限（A）

    # 3️⃣ maxCurrent 基本驗證（有給才驗）
    if max_current is not None:
        try:
            max_current = float(max_current)
            if max_current <= 0:
                raise ValueError()
        except Exception:
            raise HTTPException(status_code=400, detail="maxCurrent 必須為正數 (A)")

    with get_conn() as conn:
        cur = conn.cursor()

        # 4️⃣ 確認充電樁存在
        cur.execute(
            "SELECT charge_point_id FROM charge_points WHERE charge_point_id = ?",
            (cp_id,),
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404, detail=f"Charge point not found: {cp_id}"
            )

        # 5️⃣ 動態組 UPDATE（只更新有給的欄位）
        fields = []
        params = []

        if name is not None:
            fields.append("name = ?")
            params.append(name)

        if status is not None:
            fields.append("status = ?")
            params.append(status)

        if max_current is not None:
            fields.append("max_current_a = ?")
            params.append(max_current)

        if not fields:
            return {
                "message": "No fields updated",
                "charge_point_id": cp_id,
            }

        params.append(cp_id)

        sql = f"""
            UPDATE charge_points
            SET {", ".join(fields)}
            WHERE charge_point_id = ?
        """

        cur.execute(sql, params)
        conn.commit()

    # 6️⃣ 回傳結果（方便前端確認）
    return {
        "message": "Charge point updated",
        "charge_point_id": cp_id,
        "updated": {
            "name": name,
            "status": status,
            "max_current_a": max_current,
        },
    }


@app.get("/api/charge-points/{charge_point_id:path}/current-limit")
async def get_current_limit(charge_point_id: str):
    # 1️⃣ 正規化 CP ID（處理 URL encode / 前綴 / slash）
    cp_id = _normalize_cp_id(charge_point_id)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT max_current_a
            FROM charge_points
            WHERE charge_point_id = ?
            """,
            (cp_id,),
        )
        row = cur.fetchone()

    if not row:

        raise HTTPException(status_code=404, detail=f"Charge point not found: {cp_id}")

    raw_val = row[0]

    # 3️⃣ ⚠️ 關鍵修正點（避免 6A 被當 False）
    if raw_val is None:
        limit_a = 16.0  # 預設值
    else:
        try:
            limit_a = float(raw_val)
        except Exception:
            limit_a = 16.0

    return {"maxCurrentA": limit_a}


@app.post("/api/charge-points/{charge_point_id:path}/current-limit")
async def set_current_limit(
    charge_point_id: str,
    data: dict = Body(...),
):
    cp_id = _normalize_cp_id(charge_point_id)
    limit_a = data.get("limit_amps")

    if limit_a is None:
        raise HTTPException(status_code=400, detail="missing limit_amps")

    try:
        limit_a = float(limit_a)
        if limit_a <= 0:
            raise ValueError()
    except Exception:
        raise HTTPException(
            status_code=400, detail="limit_amps must be positive number"
        )

    # ===============================
    # 🔧 關鍵修正：UPDATE → INSERT if not exists
    # ===============================
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE charge_points
            SET max_current_a = ?
            WHERE charge_point_id = ?
            """,
            (limit_a, cp_id),
        )

        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO charge_points (charge_point_id, max_current_a)
                VALUES (?, ?)
                """,
                (cp_id, limit_a),
            )

        conn.commit()

    cp = connected_charge_points.get(cp_id)
    applied = False

    if cp and getattr(cp, "supports_smart_charging", False):
        # 找目前進行中的交易
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT transaction_id, connector_id
                FROM transactions
                WHERE charge_point_id = ?
                  AND stop_timestamp IS NULL
                ORDER BY start_timestamp DESC
                LIMIT 1
                """,
                (cp_id,),
            )
            row = cur.fetchone()

        if row:
            tx_id, connector_id = row
            try:
                await send_current_limit_profile(
                    cp=cp,
                    connector_id=connector_id or 1,
                    limit_a=limit_a,
                    tx_id=tx_id,
                )
                applied = True
            except Exception as e:
                logging.error(
                    f"[LIMIT][ERR] immediate apply failed | cp_id={cp_id} | err={e}"
                )

    return {
        "message": "current limit updated",
        "charge_point_id": cp_id,
        "limit_a": limit_a,
        "applied_immediately": applied,
    }


@app.get("/api/charge-points/{charge_point_id:path}/current-limit-status")
async def get_current_limit_status(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)

    cp = connected_charge_points.get(cp_id)

    return {
        "charge_point_id": cp_id,
        "connected": bool(cp),
        "supports_smart_charging": (
            bool(getattr(cp, "supports_smart_charging", False)) if cp else False
        ),
        "limit_state": current_limit_state.get(cp_id),
    }


@app.post("/api/charge-points/{charge_point_id}/start")
async def start_transaction_by_charge_point(
    charge_point_id: str, data: dict = Body(...)
):
    id_tag = data.get("idTag")
    connector_id = data.get("connectorId", 1)

    if not id_tag:
        raise HTTPException(status_code=400, detail="缺少 idTag")

    cp = connected_charge_points.get(charge_point_id)
    if not cp:
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{charge_point_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))},
        )

    # 發送 RemoteStartTransaction
    print(
        f"🟢【API】遠端啟動充電 | CP={charge_point_id} | idTag={id_tag} | connector={connector_id}"
    )
    response = await cp.send_remote_start_transaction(
        id_tag=id_tag, connector_id=connector_id
    )
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
    c.execute(
        """
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id = ?
          AND measurand = 'Power.Active.Import'
          AND (phase IS NULL OR phase = '')
        ORDER BY timestamp DESC
        LIMIT 1
    """,
        (charge_point_id,),
    )
    row = c.fetchone()
    if row:
        ts, val, unit = row[0], float(row[1]), (row[2] or "").lower()
        kw = (val / 1000.0) if unit in ("w",) else val
        return {"timestamp": ts, "value": round(kw, 3), "unit": "kW"}

    # 1) 最近 5 秒：各相取「該相最新」的 V 與 I，Σ(V*I)/1000 推得 kW
    c.execute(
        """
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
    """,
        (charge_point_id, charge_point_id, charge_point_id, charge_point_id),
    )
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
    c.execute(
        """
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id=? AND measurand='Voltage' AND (phase IS NULL OR phase='')
        ORDER BY timestamp DESC LIMIT 1
    """,
        (charge_point_id,),
    )
    row = c.fetchone()
    if row:
        return {
            "timestamp": row[0],
            "value": round(float(row[1]), 1),
            "unit": (row[2] or "V"),
        }

    # 2) 最近 5 秒各相取最新再平均
    c.execute(
        """
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
    """,
        (charge_point_id, charge_point_id, charge_point_id),
    )
    r = c.fetchone()
    if r and r[1] is not None:
        return {
            "timestamp": r[0],
            "value": round(float(r[1]), 1),
            "unit": "V",
            "derived": True,
        }

    return {}


@app.get("/api/charge-points/{charge_point_id}/latest-current")
def get_latest_current_api(charge_point_id: str):
    charge_point_id = _normalize_cp_id(charge_point_id)
    c = conn.cursor()

    # 1) 無相別
    c.execute(
        """
        SELECT timestamp, value, unit
        FROM meter_values
        WHERE charge_point_id=? AND measurand='Current.Import' AND (phase IS NULL OR phase='')
        ORDER BY timestamp DESC LIMIT 1
    """,
        (charge_point_id,),
    )
    row = c.fetchone()
    if row:
        return {
            "timestamp": row[0],
            "value": round(float(row[1]), 2),
            "unit": (row[2] or "A"),
        }

    # 2) 最近 5 秒各相取最新再相加
    c.execute(
        """
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
    """,
        (charge_point_id, charge_point_id, charge_point_id),
    )
    r = c.fetchone()
    if r and r[1] is not None:
        return {
            "timestamp": r[0],
            "value": round(float(r[1]), 2),
            "unit": "A",
            "derived": True,
        }

    return {}


# ✅ 原本 API（加上最終電量 / 電費，不動結構）
@app.get("/api/charge-points/{charge_point_id}/last-transaction/summary")
def get_last_tx_summary_by_cp(charge_point_id: str):
    print(
        "[WARN] /last-transaction/summary 已過時，建議改用 /current-transaction/summary 或 /last-finished-transaction/summary"
    )
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        # 找最近「最後一筆交易」(可能是進行中，也可能是已結束)
        cur.execute(
            """
            SELECT t.transaction_id, t.id_tag, t.start_timestamp, t.stop_timestamp,
                   t.meter_start, t.meter_stop
            FROM transactions t
            WHERE t.charge_point_id = ?
            ORDER BY t.transaction_id DESC
            LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        print(f"[DEBUG last-transaction] cp_id={cp_id} | row={row}")
        if not row:
            return {"found": False}

        # unpack 六個欄位
        tx_id, id_tag, start_ts, stop_ts, meter_start, meter_stop = row

        # 先抓 payments 總額（若交易已正式結帳可直接使用）
        cur.execute(
            "SELECT total_amount FROM payments WHERE transaction_id = ?", (tx_id,)
        )
        pay = cur.fetchone()
        payment_total_amount = float(pay[0]) if pay else 0.0

        # 交易進行中：優先用即時分段電價重算目前累計金額
        try:
            breakdown = _calculate_multi_period_cost_detailed(tx_id)
            live_total_amount = float(breakdown.get("total") or 0.0)
            pricing_segments = breakdown.get("segments") or []
        except Exception as e:
            logging.exception(
                f"[CURRENT_TX_SUMMARY][PRICE_BREAKDOWN_ERR] "
                f"cp_id={cp_id} | tx_id={tx_id} | err={e}"
            )
            live_total_amount = 0.0
            pricing_segments = []

        # 進行中頁面要顯示「目前累計費用」
        # 若 payments 已有正式值可保留，但通常 stop 前會是 0
        total_amount = live_total_amount if live_total_amount > 0 else payment_total_amount

        # 計算最終電量（kWh）
        final_energy = None
        if meter_start is not None and meter_stop is not None:
            try:
                final_energy = max(
                    0.0, (float(meter_stop) - float(meter_start)) / 1000.0
                )
            except Exception:
                final_energy = None

        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "start_timestamp": start_ts,
            "stop_timestamp": stop_ts,
            "total_amount": round(float(total_amount), 2),
            "estimated_amount": round(float(live_total_amount), 2),
            "payment_total_amount": round(float(payment_total_amount), 2),
            "final_energy_kwh": final_energy,
            "final_cost": round(float(total_amount), 2),
            "pricing_breakdown_total": round(float(live_total_amount), 2),
            "pricing_segments": pricing_segments,
        }


# ✅ 新增 API：回傳單次充電的累積電量
@app.get("/api/charge-points/{charge_point_id}/latest-energy")
def get_latest_energy(charge_point_id: str):
    """
    回傳該樁「目前交易的累積電量 (kWh)」。
    算法：最新 Energy.Active.Import.Register - meter_start。
    """
    cp_id = _normalize_cp_id(charge_point_id)

    with get_conn() as conn:
        cur = conn.cursor()
        # 找出該樁進行中的交易
        cur.execute(
            """
            SELECT transaction_id, meter_start
            FROM transactions
            WHERE charge_point_id=? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False, "sessionEnergyKWh": 0.0}

        tx_id, meter_start = row
        meter_start = float(meter_start or 0)

        # 找最新的能量值
        cur.execute(
            """
            SELECT value, unit, timestamp
            FROM meter_values
            WHERE charge_point_id=? AND transaction_id=? 
              AND (measurand='Energy.Active.Import.Register' OR measurand='Energy.Active.Import')
            ORDER BY timestamp DESC LIMIT 1
        """,
            (cp_id, tx_id),
        )
        mv = cur.fetchone()
        if not mv:
            return {"found": True, "transaction_id": tx_id, "sessionEnergyKWh": 0.0}

        val, unit, ts = mv
        try:
            total_kwh = float(val)
            if unit and unit.lower() in ("wh", "w*h", "w_h"):
                total_kwh = total_kwh / 1000.0
        except Exception:
            total_kwh = 0.0

        session_kwh = max(0.0, total_kwh - (meter_start / 1000.0))

        return {
            "found": True,
            "transaction_id": tx_id,
            "timestamp": ts,
            "sessionEnergyKWh": round(session_kwh, 6),
        }


@app.get("/api/charge-points/{charge_point_id}/current-transaction/summary")
def get_current_tx_summary_by_cp(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.transaction_id, t.id_tag, t.start_timestamp, t.stop_timestamp,
                   t.meter_start, t.meter_stop
            FROM transactions t
            WHERE t.charge_point_id = ? AND t.stop_timestamp IS NULL
            ORDER BY t.transaction_id DESC
            LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False}

        tx_id, id_tag, start_ts, stop_ts, meter_start, meter_stop = row

        # 先抓 payments 總額（若交易已正式結帳可直接使用）
        cur.execute(
            "SELECT total_amount FROM payments WHERE transaction_id = ?", (tx_id,)
        )
        pay = cur.fetchone()
        payment_total_amount = float(pay[0]) if pay else 0.0

        # 交易進行中：優先用即時分段電價重算目前累計金額
        try:
            breakdown = _calculate_multi_period_cost_detailed(tx_id)
            live_total_amount = float(breakdown.get("total") or 0.0)
            pricing_segments = breakdown.get("segments") or []
        except Exception as e:
            logging.exception(
                f"[CURRENT_TX_SUMMARY][PRICE_BREAKDOWN_ERR] "
                f"cp_id={cp_id} | tx_id={tx_id} | err={e}"
            )
            live_total_amount = 0.0
            pricing_segments = []

        # 進行中頁面要顯示「目前累計費用」
        total_amount = live_total_amount if live_total_amount > 0 else payment_total_amount

        # 計算最終電量（進行中可能還在增加）
        final_energy = None
        if meter_start is not None and meter_stop is not None:
            try:
                final_energy = max(
                    0.0, (float(meter_stop) - float(meter_start)) / 1000.0
                )
            except Exception:
                final_energy = None

        if _is_debug_target_cp(cp_id):
            logging.warning(
                f"[DEBUG][CURRENT_TX_SUMMARY] "
                f"cp_id={cp_id} | tx_id={tx_id} | "
                f"start_ts={start_ts} | stop_ts={stop_ts} | "
                f"meter_start={meter_start} | meter_stop={meter_stop} | "
                f"payment_total_amount={payment_total_amount} | "
                f"live_total_amount={live_total_amount} | "
                f"returned_total_amount={total_amount} | "
                f"segments_count={len(pricing_segments)}"
            )

        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "start_timestamp": start_ts,
            "stop_timestamp": stop_ts,
            "total_amount": round(float(total_amount), 2),
            "estimated_amount": round(float(live_total_amount), 2),
            "payment_total_amount": round(float(payment_total_amount), 2),
            "final_energy_kwh": final_energy,
            "final_cost": round(float(total_amount), 2),
            "pricing_breakdown_total": round(float(live_total_amount), 2),
            "pricing_segments": pricing_segments,
        }

@app.get("/api/charge-points/{charge_point_id}/last-finished-transaction/summary")
def get_last_finished_tx_summary_by_cp(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.transaction_id, t.id_tag, t.start_timestamp, t.stop_timestamp,
                   t.meter_start, t.meter_stop
            FROM transactions t
            WHERE t.charge_point_id = ? AND t.stop_timestamp IS NOT NULL
            ORDER BY t.transaction_id DESC
            LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False}

        tx_id, id_tag, start_ts, stop_ts, meter_start, meter_stop = row

        # 查 payments 總額
        cur.execute(
            "SELECT total_amount FROM payments WHERE transaction_id = ?", (tx_id,)
        )
        pay = cur.fetchone()
        total_amount = float(pay[0]) if pay else 0.0

        # 計算最終電量（已結束交易必定有完整值）
        final_energy = None
        if meter_start is not None and meter_stop is not None:
            try:
                final_energy = max(
                    0.0, (float(meter_stop) - float(meter_start)) / 1000.0
                )
            except Exception:
                final_energy = None

        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "start_timestamp": start_ts,
            "stop_timestamp": stop_ts,
            "total_amount": total_amount,
            "final_energy_kwh": final_energy,
            "final_cost": total_amount,
        }


@app.get("/api/charge-points/{charge_point_id}/current-transaction")
def get_current_transaction(charge_point_id: str):
    """
    回傳該充電樁進行中的交易（尚未結束）。
    - 若有，會包含 start_timestamp。
    - 若無，回傳 found=False。
    """
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT transaction_id, id_tag, start_timestamp
            FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY transaction_id DESC
            LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False}

        tx_id, id_tag, start_ts = row
        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "start_timestamp": start_ts,
        }


from datetime import datetime, timezone

# 若原本沒有，請在檔案上方加上
# LIVE_TTL = 15  # seconds


@app.get("/api/charge-points/{charge_point_id}/live-status")
def get_live_status(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    live = live_status_cache.get(cp_id)

    if live is None:
        allocated_power_kw = None
        preview_current_a = None
        active_cp_ids = []

        try:
            active_cp_ids = get_effective_active_cp_ids()
            if cp_id in active_cp_ids:
                allocated_power_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
                preview_current_a = convert_power_kw_to_current_a(allocated_power_kw, cp_id)
        except Exception as e:
            logging.exception(f"[LIVE_STATUS][INIT_FALLBACK_ERR] cp={cp_id} err={e}")

        if _is_debug_target_cp(cp_id):
            logging.warning(
                f"[DEBUG][LIVE_API][INIT_NONE] "
                f"cp_id={cp_id} | "
                f"active_cp_ids={active_cp_ids} | "
                f"allocated_power_kw={allocated_power_kw} | "
                f"preview_current_a={preview_current_a}"
            )

        return {
            "timestamp": None,
            "power": 0,
            "voltage": 0,
            "current": 0,
            "energy": 0,
            "estimated_energy": 0,
            "estimated_amount": 0,
            "price_per_kwh": 0,
            "derived": False,
            "stale": True,
            "managed_by": "power",
            "single_cp_max_power_kw": float(SINGLE_CP_MAX_POWER_KW),
            "allocated_power_kw": allocated_power_kw,
            "preview_current_a": preview_current_a,
        }

    # ✅ 用後端收到資料的時間做 TTL 判斷（最穩，避免樁端 timestamp 不準造成立刻 stale）
    updated_at = live.get("updated_at")
    age_sec = None

    if isinstance(updated_at, (int, float)):
        age_sec = time.time() - updated_at
    else:
        # ✅ 沒有 updated_at / 格式異常，直接視為 stale
        age_sec = LIVE_TTL + 1

    if age_sec > LIVE_TTL:
        ts = live.get("timestamp")

        # ✅ stale 狀態下也不要優先沿用 cache 舊 allocation
        allocated_power_kw = None
        preview_current_a = None

        try:
            active_cp_ids = get_effective_active_cp_ids()
            if cp_id in active_cp_ids:
                allocated_power_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
                preview_current_a = (
                    convert_power_kw_to_current_a(allocated_power_kw, cp_id)
                    if allocated_power_kw is not None
                    else None
                )
        except Exception as e:
            logging.exception(f"[LIVE_STATUS][STALE_ALLOC_ERR] cp={cp_id} err={e}")
            allocated_power_kw = None
            preview_current_a = None

        # ✅ 若目前不在 active 清單，才退回 cache
        if allocated_power_kw is None:
            allocated_power_kw = live.get("allocated_power_kw")

        if preview_current_a is None:
            preview_current_a = live.get("preview_current_a")

        if preview_current_a is None:
            preview_current_a = (
                convert_power_kw_to_current_a(allocated_power_kw, cp_id)
                if allocated_power_kw is not None
                else None
            )

            try:
                active_cp_ids = get_effective_active_cp_ids()
                if cp_id in active_cp_ids:
                    allocated_power_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
                    preview_current_a = (
                        convert_power_kw_to_current_a(allocated_power_kw, cp_id)
                        if allocated_power_kw is not None
                        else None
                    )
            except Exception as e:
                logging.exception(f"[LIVE_STATUS][STALE_ALLOC_ERR] cp={cp_id} err={e}")
                allocated_power_kw = None
                preview_current_a = None

            # ✅ 若目前不在 active 清單，才退回 cache
            if allocated_power_kw is None:
                allocated_power_kw = live.get("allocated_power_kw")

            if preview_current_a is None:
                preview_current_a = live.get("preview_current_a")

            # ==================================================
            # 關鍵補強：
            # stale 時若 cache 沒有 allocation，就至少回單槍管理上限
            # 避免前端永遠看到 None / None
            # ==================================================
            if allocated_power_kw is None:
                try:
                    allocated_power_kw = float(SINGLE_CP_MAX_POWER_KW)
                except Exception:
                    allocated_power_kw = None

            if preview_current_a is None:
                preview_current_a = (
                    convert_power_kw_to_current_a(allocated_power_kw, cp_id)
                    if allocated_power_kw is not None
                    else None
                )

            stale = {
                "timestamp": ts,
                "power": live.get("power", 0),
                "voltage": live.get("voltage", 0),
                "current": live.get("current", 0),
                "energy": live.get("energy", 0),
                "estimated_energy": live.get("estimated_energy", 0),
                "estimated_amount": live.get("estimated_amount", 0),
                "price_per_kwh": live.get("price_per_kwh", 0),
                "derived": True,
                "stale": True,
                "managed_by": "power",
                "single_cp_max_power_kw": float(SINGLE_CP_MAX_POWER_KW),
                "allocated_power_kw": allocated_power_kw,
                "preview_current_a": preview_current_a,
            }

            if _is_debug_target_cp(cp_id):
                logging.warning(
                    f"[DEBUG][LIVE_API][STALE_RETURN] "
                    f"cp_id={cp_id} | "
                    f"timestamp={stale.get('timestamp')} | "
                    f"power={stale.get('power')} | "
                    f"voltage={stale.get('voltage')} | "
                    f"current={stale.get('current')} | "
                    f"allocated_power_kw={stale.get('allocated_power_kw')} | "
                    f"preview_current_a={stale.get('preview_current_a')} | "
                    f"live_cache_timestamp={live.get('timestamp')} | "
                    f"live_updated_at={live.get('updated_at')}"
                )

            return stale

    # ✅ 一般正常回傳時，也不要優先沿用 cache 舊 allocation
    #    只要目前這支樁仍在有效 active 清單中，就每次即時計算真正分配值
    allocated_power_kw = None
    preview_current_a = None

    try:
        active_cp_ids = get_effective_active_cp_ids()

        if cp_id in active_cp_ids:
            allocated_power_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)
            preview_current_a = (
                convert_power_kw_to_current_a(allocated_power_kw, cp_id)
                if allocated_power_kw is not None
                else None
            )
    except Exception as e:
        logging.exception(f"[LIVE_STATUS][ALLOC_REALTIME_ERR] cp={cp_id} err={e}")
        allocated_power_kw = None
        preview_current_a = None

    # ✅ 若目前不在 active 清單中，才退回 cache
    if allocated_power_kw is None:
        allocated_power_kw = live.get("allocated_power_kw")

    if preview_current_a is None:
        preview_current_a = live.get("preview_current_a")

    if preview_current_a is None:
        preview_current_a = (
            convert_power_kw_to_current_a(allocated_power_kw, cp_id)
            if allocated_power_kw is not None
            else None
        )

    result = {
        "timestamp": live.get("timestamp"),
        "power": live.get("power", 0),
        "voltage": live.get("voltage", 0),
        "current": live.get("current", 0),
        "energy": live.get("energy", 0),
        "estimated_energy": live.get("estimated_energy", 0),
        "estimated_amount": live.get("estimated_amount", 0),
        "price_per_kwh": live.get("price_per_kwh", 0),
        "derived": live.get("derived", False),
        "stale": False,
        "managed_by": "power",
        "single_cp_max_power_kw": float(SINGLE_CP_MAX_POWER_KW),
        "allocated_power_kw": allocated_power_kw,
        "preview_current_a": preview_current_a,
    }

    if _is_debug_target_cp(cp_id):
        logging.warning(
            f"[DEBUG][LIVE_API][RETURN] "
            f"cp_id={cp_id} | "
            f"timestamp={result.get('timestamp')} | "
            f"power={result.get('power')} | "
            f"voltage={result.get('voltage')} | "
            f"current={result.get('current')} | "
            f"allocated_power_kw={result.get('allocated_power_kw')} | "
            f"preview_current_a={result.get('preview_current_a')} | "
            f"derived={result.get('derived')} | "
            f"live_cache_timestamp={live.get('timestamp')} | "
            f"live_updated_at={live.get('updated_at')}"
        )

    return result

from ocpp.v16 import call as ocpp_call


@app.get("/api/charge-points/{charge_point_id}/current-limit")
def get_current_limit(charge_point_id: str):
    """
    第一階段相容 API
    ------------------------------------------------
    前端舊畫面還可能會打這支 API，
    但邏輯已改為：
    - 管理上限固定 SINGLE_CP_MAX_POWER_KW
    - 回傳 currentLimitA 僅作為「由 7kW 換算出的參考電流」
    """
    cp_id = _normalize_cp_id(charge_point_id)

    preview_current_a = convert_power_kw_to_current_a(SINGLE_CP_MAX_POWER_KW, cp_id)

    return {
        "chargePointId": cp_id,
        "maxCurrentA": float(preview_current_a or 0),
        "managedBy": "power",
        "singleCpMaxPowerKw": float(SINGLE_CP_MAX_POWER_KW),
        "note": "phase1_power_managed_preview_current",
    }

def _build_cs_charging_profiles(limit_a: float, purpose: str = "ChargePointMaxProfile"):
    # OCPP 1.6 SetChargingProfile → csChargingProfiles
    # chargingRateUnit 用 "A" 表示電流限制
    return {
        "chargingProfileId": 1,
        "stackLevel": 0,
        "chargingProfilePurpose": purpose,  # "ChargePointMaxProfile" / "TxProfile"
        "chargingProfileKind": "Absolute",
        "chargingSchedule": {
            "chargingRateUnit": "A",
            "chargingSchedulePeriod": [{"startPeriod": 0, "limit": float(limit_a)}],
        },
    }


@app.post("/api/charge-points/{charge_point_id}/apply-current-limit")
async def apply_current_limit(charge_point_id: str, data: dict = Body(...)):
    """
    前端 LiveStatus slider 用：
    - data.maxCurrentA: float/int
    - data.connectorId: optional (default 1)
    - data.applyNow: optional (default True)
    """
    cp_id = _normalize_cp_id(charge_point_id)

    max_current_a = data.get("maxCurrentA") or data.get("max_current_a")
    if max_current_a is None:
        raise HTTPException(status_code=400, detail="maxCurrentA is required")

    try:
        max_current_a = float(max_current_a)
    except Exception:
        raise HTTPException(status_code=400, detail="maxCurrentA must be a number")

    connector_id = data.get("connectorId", 1)
    try:
        connector_id = int(connector_id)
    except Exception:
        connector_id = 1

    apply_now = data.get("applyNow", True)

    # 1) 先存 DB（作為預設值）
    with get_conn() as c:
        cur = c.cursor()
        cur.execute(
            "UPDATE charge_points SET max_current_a = ? WHERE charge_point_id = ?",
            (max_current_a, cp_id),
        )
        c.commit()

    # 2) 若不需要立刻下發，就到此結束
    if not apply_now:
        return {
            "ok": True,
            "chargePointId": cp_id,
            "maxCurrentA": max_current_a,
            "applied": False,
        }

    # 3) 取已連線的 CP
    cp = connected_charge_points.get(cp_id)
    if not cp:
        return {
            "ok": True,
            "chargePointId": cp_id,
            "maxCurrentA": max_current_a,
            "applied": False,
            "message": "CP not connected (已存DB，但未下發到樁)",
        }


@app.get("/api/cards/{card_id}/history")
def get_card_history(card_id: str, limit: int = 20):
    """
    回傳指定卡片的扣款紀錄（從 payments 表）。
    預設顯示最近 20 筆，可透過 limit 參數調整。
    """
    card_id = card_id.strip()
    with get_conn() as conn:
        cur = conn.cursor()

        # 找出該卡片相關的交易 ID
        cur.execute(
            "SELECT transaction_id FROM transactions WHERE id_tag=? ORDER BY start_timestamp DESC",
            (card_id,),
        )
        tx_ids = [r[0] for r in cur.fetchall()]
        if not tx_ids:
            return {"card_id": card_id, "history": []}

        # 查詢扣款紀錄
        q_marks = ",".join("?" * len(tx_ids))
        cur.execute(
            f"""
            SELECT p.transaction_id, p.total_amount, p.paid_at,
                   t.start_timestamp, t.stop_timestamp
            FROM payments p
            LEFT JOIN transactions t ON p.transaction_id = t.transaction_id
            WHERE p.transaction_id IN ({q_marks})
            ORDER BY p.paid_at DESC
            LIMIT ?
        """,
            (*tx_ids, limit),
        )

        rows = cur.fetchall()

    history = []
    for row in rows:
        history.append(
            {
                "transaction_id": row[0],
                "amount": float(row[1] or 0),
                "paid_at": row[2],
                "start_timestamp": row[3],
                "stop_timestamp": row[4],
            }
        )

    return {"card_id": card_id, "history": history}


# === 每日電價 API ===

from fastapi import Query


@app.get("/api/daily-pricing")
def get_daily_pricing(date: str = Query(..., description="查詢的日期 YYYY-MM-DD")):
    """
    查詢某一天的電價設定
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_time, end_time, price, label
            FROM daily_pricing_rules
            WHERE date = ?
            ORDER BY start_time
        """,
            (date,),
        )
        rows = cur.fetchall()
    return [
        {"startTime": r[0], "endTime": r[1], "price": r[2], "label": r[3]} for r in rows
    ]


@app.post("/api/daily-pricing")
def add_daily_pricing(data: dict = Body(...)):
    """
    新增一天的時段電價規則
    """
    date = data.get("date")
    start_time = data.get("startTime")
    end_time = data.get("endTime")
    price = data.get("price")
    label = data.get("label")

    if not (date and start_time and end_time and price is not None):
        raise HTTPException(status_code=400, detail="缺少必要欄位")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
            VALUES (?, ?, ?, ?, ?)
        """,
            (date, start_time, end_time, price, label),
        )
        conn.commit()
    return {"message": "✅ 新增成功"}


@app.delete("/api/daily-pricing")
def delete_daily_pricing(date: str = Query(..., description="要刪除的日期 YYYY-MM-DD")):
    """
    刪除某一天的所有電價規則
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM daily_pricing_rules WHERE date=?", (date,))
        conn.commit()
    return {"message": f"✅ 已刪除 {date} 的所有規則"}


@app.get("/api/default-pricing-rules")
def get_default_pricing_rules():
    c = conn.cursor()
    c.execute(
        "SELECT weekday_rules, saturday_rules, sunday_rules FROM default_pricing_rules WHERE id = 1"
    )
    row = c.fetchone()

    if not row:
        return {"weekday": [], "saturday": [], "sunday": []}

    return {
        "weekday": json.loads(row[0]) if row[0] else [],
        "saturday": json.loads(row[1]) if row[1] else [],
        "sunday": json.loads(row[2]) if row[2] else [],
    }


@app.post("/api/default-pricing-rules")
def save_default_pricing_rules(data: dict):
    weekday = data.get("weekday", [])
    saturday = data.get("saturday", [])
    sunday = data.get("sunday", [])

    c = conn.cursor()

    # 檢查是否存在 id=1
    c.execute("SELECT id FROM default_pricing_rules WHERE id = 1")
    exists = c.fetchone()

    if exists:
        c.execute(
            """
            UPDATE default_pricing_rules
            SET weekday_rules = ?, saturday_rules = ?, sunday_rules = ?
            WHERE id = 1
        """,
            (json.dumps(weekday), json.dumps(saturday), json.dumps(sunday)),
        )
    else:
        c.execute(
            """
            INSERT INTO default_pricing_rules (id, weekday_rules, saturday_rules, sunday_rules)
            VALUES (1, ?, ?, ?)
        """,
            (json.dumps(weekday), json.dumps(saturday), json.dumps(sunday)),
        )

    conn.commit()
    return {"status": "ok"}


# === 刪除卡片（完整刪除：card_whitelist + card_owners + cards + id_tags） ===
@app.delete("/api/cards/{id_tag}")
async def delete_card(id_tag: str):
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            # 先檢查卡片是否存在（任一表存在都算）
            cur.execute(
                """
                SELECT 1
                FROM id_tags
                WHERE id_tag = ?
                UNION
                SELECT 1
                FROM cards
                WHERE card_id = ?
                UNION
                SELECT 1
                FROM card_owners
                WHERE card_id = ?
                LIMIT 1
                """,
                (id_tag, id_tag, id_tag),
            )
            exists = cur.fetchone()

            if not exists:
                raise HTTPException(status_code=404, detail="card not found")

            # 刪除白名單
            cur.execute("DELETE FROM card_whitelist WHERE card_id = ?", (id_tag,))

            # 刪除住戶名稱
            cur.execute("DELETE FROM card_owners WHERE card_id = ?", (id_tag,))

            # 刪除餘額卡片資料
            cur.execute("DELETE FROM cards WHERE card_id = ?", (id_tag,))

            # 刪除 id_tags 主表
            cur.execute("DELETE FROM id_tags WHERE id_tag = ?", (id_tag,))

            conn.commit()

        return {"message": f"Card {id_tag} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    cp = _normalize_cp_id(charge_point_id)

    cached_status = charging_point_status.get(cp) or {}

    # ✅ 若已經是 grace 到期後的 ws 斷線狀態，就不要再被 DB active tx 強制洗回 Charging
    if (
        cached_status.get("ws_grace_expired") is True
        and cp not in connected_charge_points
        and not is_cp_in_ws_disconnect_grace(cp)
    ):
        if cached_status.get("status") == "未知":
            return {**cached_status, "status": "Unknown"}
        return cached_status

    # ✅ 1) 優先用 DB 判斷：只要有未結束交易，就視為 Charging
    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT transaction_id, connector_id, start_timestamp
                FROM transactions
                WHERE charge_point_id = ?
                  AND stop_timestamp IS NULL
                ORDER BY start_timestamp DESC
                LIMIT 1
            """,
                (cp,),
            )
            row = cur.fetchone()

        if row:
            tx_id, connector_id, start_ts = row
            return {
                "connector_id": int(connector_id or 0),
                "status": "Charging",
                "timestamp": start_ts,
                "error_code": "NoError",
                "derived": True,
                "transaction_id": int(tx_id),
            }
    except Exception as e:
        logging.exception(f"❌ status derive from DB failed | cp_id={cp} | err={e}")

    # ✅ 2) DB 沒有進行中交易才回快取
    status = charging_point_status.get(cp)
    if not status:
        return {"status": "Unknown"}

    if status.get("status") == "未知":
        status = {**status, "status": "Unknown"}

    return status


@app.get("/api/charge-points/{charge_point_id}/latest-status")
def get_latest_status(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)

    # ✅ 優先從 status_logs 抓最新狀態（你本來就有在 StatusNotification INSERT status_logs）
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status, timestamp
                FROM status_logs
                WHERE charge_point_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """,
                (cp_id,),
            )
            row = cur.fetchone()

        if row:
            return {"status": row[0] or "Unknown", "timestamp": row[1]}

    except Exception as e:
        logging.exception(f"❌ get_latest_status failed | cp_id={cp_id} | err={e}")

    # ✅ DB 沒資料 or 例外 → fallback 用記憶體快取（你程式裡已有 charging_point_status）
    st = charging_point_status.get(cp_id) or {}
    return {"status": st.get("status", "Unknown"), "timestamp": st.get("timestamp")}


@app.get("/api/transactions/{transaction_id}/summary")
def get_transaction_summary(transaction_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.id_tag, p.total_amount
            FROM transactions t
            LEFT JOIN payments p ON p.transaction_id = t.transaction_id
            WHERE t.transaction_id = ?
        """,
            (transaction_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False}

        t_step = time.perf_counter()
        with get_conn() as _c:
            cur = _c.cursor()
            cur.execute("SELECT balance FROM cards WHERE card_id = ?", (id_tag,))
            card_row = cur.fetchone()

        logging.warning(
            f"[DEBUG][START_TX][STEP] "
            f"cp_id={self.id} | step=card_balance_lookup | ms={_ms_since(t_step)}"
        )

        return {
            "found": True,
            "transaction_id": transaction_id,
            "id_tag": id_tag,
            "total_amount": round(total_amount, 2),
            "balance": round(balance, 2),
        }


# ✅ 時段電價設定管理：新增與刪除
@app.post("/api/pricing-rules")
async def add_pricing_rule(rule: dict = Body(...)):
    try:
        cursor.execute(
            """
            INSERT INTO pricing_rules (season, day_type, start_time, end_time, price)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                rule["season"],
                rule["day_type"],
                rule["start_time"],
                rule["end_time"],
                float(rule["price"]),
            ),
        )
        conn.commit()
        return {"message": "新增成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/pricing-rules")
async def delete_pricing_rule(rule: dict = Body(...)):
    try:
        cursor.execute(
            """
            DELETE FROM pricing_rules
            WHERE season = ? AND day_type = ? AND start_time = ? AND end_time = ? AND price = ?
        """,
            (
                rule["season"],
                rule["day_type"],
                rule["start_time"],
                rule["end_time"],
                float(rule["price"]),
            ),
        )
        conn.commit()
        return {"message": "刪除成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


conn.commit()


@app.get("/api/payments")
async def list_payments():
    cursor.execute(
        "SELECT transaction_id, base_fee, energy_fee, overuse_fee, total_amount FROM payments ORDER BY transaction_id DESC"
    )
    rows = cursor.fetchall()
    return [
        {
            "transactionId": r[0],
            "baseFee": r[1],
            "energyFee": r[2],
            "overuseFee": r[3],
            "totalAmount": r[4],
        }
        for r in rows
    ]


...


@app.post("/api/transactions")
async def create_transaction_api(data: dict = Body(...)):
    try:
        txn_id = int(datetime.utcnow().timestamp() * 1000)
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id, charge_point_id, connector_id, id_tag,
                meter_start, start_timestamp, meter_stop, stop_timestamp, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                txn_id,
                data["chargePointId"],
                1,
                data["idTag"],
                data["meter_start"],
                data["start_timestamp"],
                data["meter_stop"],
                data["stop_timestamp"],
                None,
            ),
        )
        conn.commit()
        return {"transaction_id": txn_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/transactions")
async def get_transactions(
    idTag: str = Query(None),
    chargePointId: str = Query(None),
    start: str = Query(None),
    end: str = Query(None),
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
            "meterValues": [],
        }

        cursor.execute(
            """
            SELECT timestamp, value, measurand, unit, context, format
            FROM meter_values WHERE transaction_id = ?
        """,
            (txn_id,),
        )
        mv_rows = cursor.fetchall()
        for mv in mv_rows:
            result[txn_id]["meterValues"].append(
                {
                    "timestamp": mv[0],
                    "sampledValue": [
                        {
                            "value": mv[1],
                            "measurand": mv[2],
                            "unit": mv[3],
                            "context": mv[4],
                            "format": mv[5],
                        }
                    ],
                }
            )

    return JSONResponse(content=result)


def compute_transaction_cost(transaction_id: int):
    with get_conn() as conn:
        cur = conn.cursor()

        # 1. 先查交易主資料，確認 transaction 存在
        cur.execute(
            """
            SELECT transaction_id, charge_point_id, connector_id, id_tag,
                   meter_start, start_timestamp, meter_stop, stop_timestamp, reason
            FROM transactions
            WHERE transaction_id = ?
            """,
            (transaction_id,),
        )
        tx = cur.fetchone()

        if not tx:
            raise HTTPException(status_code=404, detail="Transaction not found")

        (
            tx_id,
            charge_point_id,
            connector_id,
            id_tag,
            meter_start,
            start_timestamp,
            meter_stop,
            stop_timestamp,
            reason,
        ) = tx

        # 2. 查 payments 總金額（若已有正式帳單）
        cur.execute(
            """
            SELECT total_amount
            FROM payments
            WHERE transaction_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (transaction_id,),
        )
        pay_row = cur.fetchone()
        payment_total = float(pay_row[0]) if pay_row and pay_row[0] is not None else None

        # 3. 用既有 helper 算分段明細
        breakdown = _calculate_multi_period_cost_detailed(transaction_id)
        breakdown_total = float(breakdown.get("total") or 0.0)
        segments = breakdown.get("segments") or []

        # 4. 這裡就是「只做欄位轉名」的地方
        details = [
            {
                "from": seg.get("start"),
                "to": seg.get("end"),
                "kWh": round(float(seg.get("kwh") or 0.0), 6),
                "price": float(seg.get("price") or 0.0),
                "cost": round(float(seg.get("subtotal") or 0.0), 2),
            }
            for seg in segments
        ]

        # 5. cost 優先用 payments，沒有才退回 breakdown total
        final_cost = round(
            float(payment_total if payment_total is not None else breakdown_total), 2
        )

        # 6. 查卡片剩餘儲值金額
        cur.execute(
            """
            SELECT balance
            FROM cards
            WHERE card_id = ?
            """,
            (id_tag,),
        )
        card_row = cur.fetchone()
        remaining_balance = (
            round(float(card_row[0]), 2)
            if card_row and card_row[0] is not None
            else None
        )

        return {
            "transactionId": tx_id,
            "chargePointId": charge_point_id,
            "connectorId": connector_id,
            "idTag": id_tag,
            "startTimestamp": start_timestamp,
            "stopTimestamp": stop_timestamp,
            "cost": final_cost,
            "details": details,
            "remainingBalance": remaining_balance,
        }


@app.get("/api/transactions/{transaction_id}/cost")
async def calculate_transaction_cost(transaction_id: int):
    try:
        return compute_transaction_cost(transaction_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/transactions/{transaction_id}")
async def get_transaction_detail(transaction_id: int):
    # 查詢交易主資料
    cursor.execute(
        "SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,)
    )
    row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")

    energy_kwh = None
    if row[4] is not None and row[6] is not None:
        try:
            energy_kwh = round(max(0.0, (float(row[6]) - float(row[4])) / 1000.0), 6)
        except Exception:
            energy_kwh = None

    result = {
        "transactionId": row[0],
        "chargePointId": row[1],
        "connectorId": row[2],
        "idTag": row[3],
        "cardNumber": row[3],
        "meterStart": row[4],
        "startTimestamp": row[5],
        "meterStop": row[6],
        "stopTimestamp": row[7],
        "reason": row[8],
        "energyKwh": energy_kwh,
        "meterValues": [],
    }

    # 查詢對應電錶數據
    cursor.execute(
        """
        SELECT timestamp, value, measurand, unit, context, format
        FROM meter_values WHERE transaction_id = ?
        ORDER BY timestamp ASC
    """,
        (transaction_id,),
    )
    mv_rows = cursor.fetchall()
    for mv in mv_rows:
        result["meterValues"].append(
            {
                "timestamp": mv[0],
                "sampledValue": [
                    {
                        "value": mv[1],
                        "measurand": mv[2],
                        "unit": mv[3],
                        "context": mv[4],
                        "format": mv[5],
                    }
                ],
            }
        )

    return JSONResponse(content=result)


@app.get("/api/transactions/export")
async def export_transactions_csv(
    idTag: str = Query(None),
    chargePointId: str = Query(None),
    start: str = Query(None),
    end: str = Query(None),
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
    writer.writerow(
        [
            "transactionId",
            "chargePointId",
            "connectorId",
            "idTag",
            "meterStart",
            "startTimestamp",
            "meterStop",
            "stopTimestamp",
            "reason",
        ]
    )
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=transactions_export.csv"},
    )


# REST API - 查詢所有充電樁狀態
@app.get("/api/status")
async def get_status():
    return JSONResponse(content=charging_point_status)


@app.get("/api/id_tags")
async def list_id_tags():
    cursor.execute("SELECT id_tag, status, valid_until FROM id_tags")
    rows = cursor.fetchall()
    return JSONResponse(
        content=[
            {"idTag": row[0], "status": row[1], "validUntil": row[2]} for row in rows
        ]
    )


@app.post("/api/id_tags")
async def add_id_tag(data: dict = Body(...)):
    print("📥 收到新增卡片資料：", data)

    id_tag = (data.get("idTag") or "").strip()
    status = data.get("status", "Accepted")
    valid_until = data.get("validUntil", "2099-12-31T23:59:59")

    if not id_tag:
        print("❌ idTag 缺失")
        raise HTTPException(status_code=400, detail="idTag is required")

    try:
        valid_dt = parse_date(valid_until)
        valid_str = valid_dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"❌ validUntil 格式錯誤：{valid_until}，錯誤訊息：{e}")
        raise HTTPException(status_code=400, detail="Invalid validUntil format")

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute(
                "INSERT INTO id_tags (id_tag, status, valid_until) VALUES (?, ?, ?)",
                (id_tag, status, valid_str),
            )

            cur.execute(
                "INSERT OR IGNORE INTO cards (card_id, balance) VALUES (?, ?)",
                (id_tag, 0),
            )

            conn.commit()

        print(f"✅ 已成功新增卡片：{id_tag}, {status}, {valid_str}")
        return {"message": "Added successfully"}

    except sqlite3.IntegrityError as e:
        print(f"❌ 資料庫重複錯誤：{e}")
        raise HTTPException(status_code=409, detail="idTag already exists")
    except Exception as e:
        print(f"❌ 未知新增錯誤：{e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/api/id_tags/{id_tag}")
async def update_id_tag(id_tag: str = Path(...), data: dict = Body(...)):
    status = data.get("status")
    valid_until = data.get("validUntil")

    if not (status or valid_until):
        raise HTTPException(status_code=400, detail="No update fields provided")

    if status:
        cursor.execute(
            "UPDATE id_tags SET status = ? WHERE id_tag = ?", (status, id_tag)
        )
    if valid_until:
        cursor.execute(
            "UPDATE id_tags SET valid_until = ? WHERE id_tag = ?", (valid_until, id_tag)
        )
    conn.commit()
    return {"message": "Updated successfully"}


@app.delete("/api/id_tags/{id_tag}")
async def delete_id_tag(id_tag: str = Path(...)):
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM id_tags WHERE id_tag = ?", (id_tag,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="id_tag not found")

        cur.execute("DELETE FROM card_whitelist WHERE card_id = ?", (id_tag,))
        cur.execute("DELETE FROM card_owners WHERE card_id = ?", (id_tag,))
        cur.execute("DELETE FROM cards WHERE card_id = ?", (id_tag,))
        cur.execute("DELETE FROM id_tags WHERE id_tag = ?", (id_tag,))
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
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid group_by. Use 'day', 'week', or 'month'."},
        )

    cursor.execute(
        f"""
        SELECT {date_expr} as period,
               COUNT(*) as transaction_count,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY period
        ORDER BY period ASC
    """
    )
    rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append(
            {"period": row[0], "transactionCount": row[1], "totalEnergy": row[2] or 0}
        )

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
            payload = {"to": user_id, "messages": [{"type": "text", "text": message}]}
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_TOKEN}",
            }
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                headers=headers,
                data=json.dumps(payload),
            )
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
                    cursor.execute(
                        "UPDATE users SET card_number = ? WHERE id_tag = ?",
                        (user_id, id_tag),
                    )
                    conn.commit()
                    reply_text = f"✅ 已成功綁定 {id_tag}"
                else:
                    reply_text = f"❌ 找不到使用者 IDTag：{id_tag}"

            elif text in ["取消綁定", "解除綁定"]:
                cursor.execute(
                    "SELECT id_tag FROM users WHERE card_number = ?", (user_id,)
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        "UPDATE users SET card_number = NULL WHERE id_tag = ?",
                        (row[0],),
                    )
                    conn.commit()
                    reply_text = f"🔓 已取消綁定：{row[0]}"
                else:
                    reply_text = "⚠️ 尚未綁定任何帳號"

            else:
                reply_text = "請輸入：\n綁定 {IDTag} 來綁定帳號\n取消綁定 來解除綁定"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_TOKEN}",
            }
            reply_payload = {
                "replyToken": event.get("replyToken"),
                "messages": [{"type": "text", "text": reply_text}],
            }
            requests.post(
                "https://api.line.me/v2/bot/message/reply",
                headers=headers,
                data=json.dumps(reply_payload),
            )

    return {"status": "ok"}


@app.get("/api/users/{id_tag}")
async def get_user(id_tag: str = Path(...)):
    cursor.execute(
        "SELECT id_tag, name, department, card_number FROM users WHERE id_tag = ?",
        (id_tag,),
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {"idTag": row[0], "name": row[1], "department": row[2], "cardNumber": row[3]}


@app.post("/api/reservations")
async def create_reservation(data: dict = Body(...)):
    cursor.execute(
        """
        INSERT INTO reservations (charge_point_id, id_tag, start_time, end_time, status)
        VALUES (?, ?, ?, ?, ?)
    """,
        (
            data["chargePointId"],
            data["idTag"],
            data["startTime"],
            data["endTime"],
            "active",
        ),
    )
    conn.commit()
    return {"message": "Reservation created"}


@app.get("/api/reservations/{id}")
async def get_reservation(id: int = Path(...)):
    cursor.execute("SELECT * FROM reservations WHERE id = ?", (id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return {
        "id": row[0],
        "chargePointId": row[1],
        "idTag": row[2],
        "startTime": row[3],
        "endTime": row[4],
        "status": row[5],
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
    cursor.execute(
        f"""
        UPDATE reservations SET {", ".join(fields)} WHERE id = ?
    """,
        values,
    )
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
        cursor.execute(
            "UPDATE users SET department = ? WHERE id_tag = ?", (department, id_tag)
        )
    if card_number:
        cursor.execute(
            "UPDATE users SET card_number = ? WHERE id_tag = ?", (card_number, id_tag)
        )

    conn.commit()
    return {"message": "User updated successfully"}


@app.get("/api/summary/pricing-matrix")
async def get_pricing_matrix():
    cursor.execute(
        """
        SELECT season, day_type, start_time, end_time, price
        FROM pricing_rules
        ORDER BY season, day_type, start_time
    """
    )
    rows = cursor.fetchall()
    return [
        {
            "season": r[0],
            "day_type": r[1],
            "start_time": r[2],
            "end_time": r[3],
            "price": r[4],
        }
        for r in rows
    ]


@app.get("/api/summary/daily-by-chargepoint")
async def get_daily_by_chargepoint():
    cursor.execute(
        """
        SELECT strftime('%Y-%m-%d', start_timestamp) as day,
               charge_point_id,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY day, charge_point_id
        ORDER BY day ASC
    """
    )
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
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"},
    )


@app.get("/api/reservations/export")
async def export_reservations_csv():
    cursor.execute(
        "SELECT id, charge_point_id, id_tag, start_time, end_time, status FROM reservations"
    )
    rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "chargePointId", "idTag", "startTime", "endTime", "status"])
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=reservations.csv"},
    )


@app.get("/api/report/monthly")
async def generate_monthly_pdf(month: str):
    # 取得指定月份的起始與結束日期
    try:
        start_date = f"{month}-01"
        end_date = f"{month}-31"
    except:
        return {"error": "Invalid month format"}

    # 查詢交易資料
    cursor.execute(
        """
        SELECT id_tag, charge_point_id, SUM(meter_stop - meter_start) AS total_energy, COUNT(*) as txn_count
        FROM transactions
        WHERE start_timestamp >= ? AND start_timestamp <= ? AND meter_stop IS NOT NULL
        GROUP BY id_tag, charge_point_id
    """,
        (start_date, end_date),
    )
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
        p.drawString(
            50, y, f"ID: {id_tag} | 樁: {cp_id} | 次數: {count} | 用電: {kwh} kWh"
        )
        y -= 20
        if y < 50:
            p.showPage()
            y = 800

    if not rows:
        p.drawString(50, 760, "⚠️ 本月無任何有效交易紀錄")

    p.showPage()
    p.save()
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=monthly_report_{month}.pdf"
        },
    )


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
            "festival": (
                description
                if description not in ["週六", "週日", "補班", "平日"]
                else None
            ),
        }
    except FileNotFoundError:
        return {
            "date": date,
            "type": "查無年度資料",
            "holiday": False,
            "festival": None,
        }
    except Exception as e:
        return {
            "date": date,
            "type": f"錯誤：{str(e)}",
            "holiday": False,
            "festival": None,
        }


@app.get("/api/cards")
def get_cards():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT 
                c.card_id, 
                c.balance,
                t.status,
                t.valid_until,
                o.name
            FROM cards c
            LEFT JOIN id_tags t ON c.card_id = t.id_tag
            LEFT JOIN card_owners o ON c.card_id = o.card_id
            ORDER BY c.card_id
        """
        )

        rows = cur.fetchall()

    result = []
    for r in rows:
        result.append(
            {
                "card_id": r[0],
                "balance": r[1],
                "status": r[2],
                "validUntil": r[3],
                "name": r[4],  # ⭐ 新增住戶名稱
            }
        )

    return result


@app.get("/api/charge-points")
async def list_charge_points():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, charge_point_id, name, status, created_at, max_current_a
            FROM charge_points
            """
        )
        rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "chargePointId": r[1],
            "name": r[2],
            "status": r[3],
            "createdAt": r[4],
            "maxCurrent": r[5],
        }
        for r in rows
    ]


@app.post("/api/charge-points")
async def add_charge_point(data: dict = Body(...)):
    print("🔥 payload=", data)

    cp_id = (data.get("chargePointId") or data.get("charge_point_id") or "").strip()
    name = (data.get("name") or "").strip()
    status = str(data.get("status") or "enabled").lower().strip()

    max_current_a = (
        data.get("maxCurrent")
        or data.get("maxCurrentA")
        or data.get("max_current_a")
        or 16
    )
    try:
        max_current_a = float(max_current_a)
    except Exception:
        max_current_a = 16.0

    if not cp_id:
        raise HTTPException(status_code=400, detail="chargePointId is required")

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO charge_points (charge_point_id, name, status, max_current_a)
                VALUES (?, ?, ?, ?)
                """,
                (cp_id, name, status, max_current_a),
            )
            conn.commit()

            cur.execute(
                """
                SELECT id, charge_point_id, name, status, created_at, max_current_a
                FROM charge_points
                WHERE charge_point_id = ?
                """,
                (cp_id,),
            )
            row = cur.fetchone()

        print(f"✅ 新增白名單到資料庫: {cp_id}, {name}, {status}")

        return {
            "message": "新增成功",
            "item": {
                "id": row[0],
                "chargePointId": row[1],
                "name": row[2],
                "status": row[3],
                "createdAt": row[4],
                "maxCurrent": row[5],
            },
        }

    except sqlite3.IntegrityError as e:
        print("❌ IntegrityError:", e)
        raise HTTPException(status_code=409, detail="充電樁已存在")
    except Exception as e:
        print("❌ 其他新增錯誤:", e)
        raise HTTPException(status_code=500, detail=f"內部錯誤: {e}")


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

    # ✅【關鍵修正】前端送的是 maxCurrent，不是 maxCurrentA
    max_current = data.get("maxCurrent")
    if max_current is not None:
        try:
            max_current = float(max_current)
            update_fields.append("max_current_a = ?")
            params.append(max_current)
        except Exception:
            raise HTTPException(status_code=400, detail="maxCurrent 必須是數字")

    if not update_fields:
        raise HTTPException(status_code=400, detail="無可更新欄位")

    params.append(cp_id)

    # ✅ 改用 get_conn（避免多請求共用 cursor 問題）
    with get_conn() as _conn:
        _cur = _conn.cursor()
        _cur.execute(
            f"""
            UPDATE charge_points
            SET {', '.join(update_fields)}
            WHERE charge_point_id = ?
            """,
            params,
        )
        _conn.commit()

    return {"message": "已更新"}


@app.delete("/api/charge-points/{cp_id}")
async def delete_charge_point(cp_id: str = Path(...)):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM charge_points WHERE charge_point_id = ?", (cp_id,))
        conn.commit()

    return {"message": "已刪除"}


@app.put("/api/cards/{card_id}")
async def update_card(card_id: str, payload: dict):
    new_balance = payload.get("balance")
    if new_balance is None:
        raise HTTPException(status_code=400, detail="Missing balance")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE cards SET balance = ? WHERE card_id = ?",
            (new_balance, card_id),
        )
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
        cursor.execute(
            "INSERT INTO cards (card_id, balance) VALUES (?, ?)", (card_id, amount)
        )
        conn.commit()
        return {
            "status": "created",
            "card_id": card_id,
            "new_balance": round(amount, 2),
        }
    else:
        # ✅ 已存在 → 正常加值
        new_balance = row[0] + amount
        cursor.execute(
            "UPDATE cards SET balance = ? WHERE card_id = ?", (new_balance, card_id)
        )
        conn.commit()
        return {
            "status": "success",
            "card_id": card_id,
            "new_balance": round(new_balance, 2),
        }


@app.get("/api/version-check")
def version_check():
    return {"version": "✅ 偵錯用 main.py v1.0 已啟動成功"}


@app.get("/api/summary/daily-by-chargepoint-range")
async def get_daily_by_chargepoint_range(
    start: str = Query(...), end: str = Query(...)
):
    result_map = {}
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT strftime('%Y-%m-%d', start_timestamp) as day,
                   charge_point_id,
                   SUM(meter_stop - meter_start) as total_energy
            FROM transactions
            WHERE meter_stop IS NOT NULL
              AND start_timestamp >= ?
              AND start_timestamp <= ?
            GROUP BY day, charge_point_id
            ORDER BY day ASC
        """,
            (start, end),
        )
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
    c.execute(
        """
        SELECT start_time, end_time, price, COALESCE(label,'')
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time
    """,
        (d,),
    )
    rows = c.fetchall()

    hits = [
        (s, e, float(p), lbl)
        for (s, e, p, lbl) in rows
        if _price_time_in_range(t, s, e)
    ]
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
    c.execute(
        """
        SELECT start_time, end_time, price
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time
    """,
        (d,),
    )
    rows = c.fetchall()

    hits = [float(p) for (s, e, p) in rows if _price_time_in_range(t, s, e)]
    return max(hits) if hits else 6.0


# 建表（若已存在會略過）
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS daily_pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,        -- yyyy-mm-dd
    start_time TEXT,  -- HH:MM
    end_time TEXT,    -- HH:MM
    price REAL,
    label TEXT DEFAULT ''
)
"""
)
conn.commit()


# 取得指定日期設定
@app.get("/api/daily-pricing")
async def get_daily_pricing(date: str = Query(...)):
    cursor.execute(
        """
        SELECT id, date, start_time, end_time, price, label
        FROM daily_pricing_rules
        WHERE date = ?
        ORDER BY start_time ASC
    """,
        (date,),
    )
    rows = cursor.fetchall()
    return [
        {
            "id": r[0],
            "date": r[1],
            "startTime": r[2],
            "endTime": r[3],
            "price": r[4],
            "label": r[5],
        }
        for r in rows
    ]


# 新增設定
@app.post("/api/daily-pricing")
async def add_daily_pricing(data: dict = Body(...)):
    cursor.execute(
        """
        INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
        VALUES (?, ?, ?, ?, ?)
    """,
        (
            data["date"],
            data["startTime"],
            data["endTime"],
            float(data["price"]),
            data.get("label", ""),
        ),
    )
    conn.commit()
    return {"message": "新增成功"}


# 修改設定
@app.put("/api/daily-pricing/{id}")
async def update_daily_pricing(id: int = Path(...), data: dict = Body(...)):
    cursor.execute(
        """
        UPDATE daily_pricing_rules
        SET date = ?, start_time = ?, end_time = ?, price = ?, label = ?
        WHERE id = ?
    """,
        (
            data["date"],
            data["startTime"],
            data["endTime"],
            float(data["price"]),
            data.get("label", ""),
            id,
        ),
    )
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

    cursor.execute(
        "SELECT start_time, end_time, price, label FROM daily_pricing_rules WHERE date = ?",
        (source_date,),
    )
    rows = cursor.fetchall()

    for target in target_dates:
        for s, e, p, lbl in rows:
            cursor.execute(
                """
                INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
                VALUES (?, ?, ?, ?, ?)
            """,
                (target, s, e, p, lbl),
            )
    conn.commit()
    return {"message": f"已複製 {len(rows)} 筆設定至 {len(target_dates)} 天"}


# 🔹 補上 pricing_rules 表
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season TEXT,
    day_type TEXT,
    start_time TEXT,
    end_time TEXT,
    price REAL
)
"""
)

# 🔹 補上 reservations 表
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_point_id TEXT,
    id_tag TEXT,
    start_time TEXT,
    end_time TEXT,
    status TEXT
)
"""
)

conn.commit()


@app.get("/")
async def root():
    return {"status": "API is running"}


# 刪除
@app.delete("/api/weekly-pricing/{id}")
async def delete_weekly_pricing(id: int = Path(...)):
    cursor.execute("DELETE FROM weekly_pricing WHERE id = ?", (id,))
    conn.commit()
    return {"message": "刪除成功"}


@app.post("/api/internal/meter_values")
async def add_meter_values(data: dict = Body(...)):
    required_fields = [
        "transaction_id",
        "charge_point_id",
        "connector_id",
        "timestamp",
        "value",
    ]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        raise HTTPException(
            status_code=422, detail=f"❌ 缺少欄位: {', '.join(missing_fields)}"
        )

    try:
        cursor.execute(
            """
            INSERT INTO meter_values (
                transaction_id, charge_point_id, connector_id, timestamp, value, measurand, unit, context, format
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                data["transaction_id"],
                data["charge_point_id"],
                data["connector_id"],
                data["timestamp"],
                data["value"],
                data.get("measurand", "Energy.Active.Import.Register"),
                data.get("unit", "Wh"),
                data.get("context", "Sample.Periodic"),
                data.get("format", "Raw"),
            ),
        )
        conn.commit()
        return {"message": "✅ Meter value added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❗資料庫寫入失敗: {str(e)}")


@app.post("/api/internal/mock-daily-pricing")
async def mock_daily_pricing(
    start: str = Query("2025-06-01", description="起始日期（格式 YYYY-MM-DD）"),
    days: int = Query(30, description="建立幾天的電價"),
):
    try:
        base = datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid start date format. Use YYYY-MM-DD"},
        )

    count = 0
    for i in range(days):
        day = base + timedelta(days=i)
        date_str = day.strftime("%Y-%m-%d")

        # 跳過已存在的
        cursor.execute("SELECT * FROM daily_pricing WHERE date = ?", (date_str,))
        if cursor.fetchone():
            continue

        cursor.execute(
            """
            INSERT INTO daily_pricing (date, price_per_kwh)
            VALUES (?, ?)
        """,
            (date_str, 10.0),
        )
        count += 1

    conn.commit()
    return {"message": f"✅ 已建立 {count} 筆日電價", "start": start, "days": days}


@app.post("/api/internal/recalculate-all-payments")
async def recalculate_all_payments():
    cursor.execute("DELETE FROM payments")
    conn.commit()

    cursor.execute(
        """
        SELECT transaction_id, charge_point_id, meter_start, meter_stop,
               start_timestamp, stop_timestamp, id_tag
        FROM transactions
        WHERE meter_stop IS NOT NULL
    """
    )
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

            cursor.execute(
                """
                SELECT start_time, end_time, price FROM daily_pricing_rules
                WHERE date = ?
                ORDER BY start_time ASC
            """,
                (date_str,),
            )
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
            cursor.execute(
                """
                INSERT INTO payments (transaction_id, base_fee, energy_fee, overuse_fee, total_amount)
                VALUES (?, ?, ?, ?, ?)
            """,
                (txn_id, base_fee, energy_fee, overuse_fee, total_amount),
            )
            created += 1

            # 這裡重點修正：直接用 id_tag 對應卡片卡號
            card_id = id_tag
            cursor.execute("SELECT balance FROM cards WHERE card_id = ?", (card_id,))
            balance_row = cursor.fetchone()
            if balance_row:
                old_balance = balance_row[0]
                if old_balance >= total_amount:
                    new_balance = round(old_balance - total_amount, 2)
                    cursor.execute(
                        "UPDATE cards SET balance = ? WHERE card_id = ?",
                        (new_balance, card_id),
                    )
                    print(
                        f"💳 扣款成功：{card_id} | {old_balance} → {new_balance} 元 | txn={txn_id}"
                    )
                else:
                    print(
                        f"⚠️ 餘額不足：{card_id} | 餘額={old_balance}，費用={total_amount}"
                    )
            else:
                print(f"⚠️ 找不到卡片餘額：card_id={card_id}")

        except Exception as e:
            print(f"❌ 錯誤 txn {txn_id} | idTag={id_tag} | {e}")
            skipped += 1

    conn.commit()
    return {
        "message": "✅ 已重新計算所有交易成本（daily_pricing_rules 分段並自動扣款）",
        "created": created,
        "skipped": skipped,
    }


@app.get("/api/diagnostic/daily-pricing")
async def diagnostic_daily_pricing():
    cursor.execute("SELECT date, price_per_kwh FROM daily_pricing ORDER BY date ASC")
    rows = cursor.fetchall()
    return [{"date": row[0], "price": row[1]} for row in rows]


@app.get("/api/diagnostic/missing-cost-transactions")
async def missing_cost_transactions():
    cursor.execute(
        """
        SELECT transaction_id, charge_point_id, meter_start, meter_stop, start_timestamp
        FROM transactions
        WHERE meter_stop IS NOT NULL
    """
    )
    rows = cursor.fetchall()

    missing = []

    for row in rows:
        txn_id, cp_id, meter_start, meter_stop, start_ts = row
        try:
            ts_obj = datetime.fromisoformat(start_ts)
            date_str = ts_obj.strftime("%Y-%m-%d")
            cursor.execute(
                "SELECT price_per_kwh FROM daily_pricing WHERE date = ?", (date_str,)
            )
            price_row = cursor.fetchone()
            if not price_row:
                missing.append(
                    {
                        "transaction_id": txn_id,
                        "date": date_str,
                        "chargePointId": cp_id,
                        "reason": "No daily pricing found",
                    }
                )
        except:
            missing.append(
                {
                    "transaction_id": txn_id,
                    "date": start_ts,
                    "chargePointId": cp_id,
                    "reason": "Invalid timestamp format",
                }
            )

    return missing


@app.post("/api/internal/mock-status")
async def mock_status(data: dict = Body(...)):  # 這裡要三個點
    cp_id = data["cp_id"]
    charging_point_status[cp_id] = {
        "connectorId": data.get("connector_id", 1),
        "status": data.get("status", "Available"),
        "timestamp": data.get("timestamp") or datetime.utcnow().isoformat(),
    }
    return {"message": f"Mock status for {cp_id} 已注入"}


@app.get("/api/dashboard/rank_by_idTag")
async def get_dashboard_top_idtags(limit: int = Query(10)):
    cursor.execute(
        """
        SELECT id_tag,
               COUNT(*) as transaction_count,
               SUM(meter_stop - meter_start) as total_energy
        FROM transactions
        WHERE meter_stop IS NOT NULL
        GROUP BY id_tag
        ORDER BY total_energy DESC
        LIMIT ?
    """,
        (limit,),
    )
    rows = cursor.fetchall()

    return [
        {
            "idTag": row[0],
            "transactionCount": row[1],
            "totalEnergy": round((row[2] or 0) / 1000, 3),  # 換算成 kWh
        }
        for row in rows
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
        days_in_month = (
            start.replace(month=start.month % 12 + 1, day=1) - timedelta(days=1)
        ).day

        inserted = 0
        for d in range(1, days_in_month + 1):
            current = datetime(start.year, start.month, d)
            weekday = current.weekday()  # 0=Mon, ..., 6=Sun

            # 篩選符合類型的日期
            if (
                (type == "weekday" and weekday < 5)
                or (type == "saturday" and weekday == 5)
                or (type == "sunday" and weekday == 6)
            ):
                date_str = current.strftime("%Y-%m-%d")
                # 先刪除既有設定
                cursor.execute(
                    "DELETE FROM daily_pricing_rules WHERE date = ?", (date_str,)
                )
                for r in rules:
                    cursor.execute(
                        """
                        INSERT INTO daily_pricing_rules (date, start_time, end_time, price, label)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            date_str,
                            r["startTime"],
                            r["endTime"],
                            float(r["price"]),
                            r["label"],
                        ),
                    )
                inserted += 1

        conn.commit()
        return {"message": f"✅ 套用完成，共更新 {inserted} 天"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


from fastapi import HTTPException


@app.post("/api/charge-points/{charge_point_id}/stop")
async def stop_charge_point(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)

    logger.error(
        f"🛑 [STOP API CALLED] cp_id={cp_id} | "
        f"connected_keys={list(connected_charge_points.keys())}"
    )

    cp = connected_charge_points.get(cp_id)
    if not cp:
        logger.error(f"❌ [STOP API FAIL] cp_id={cp_id} NOT in connected_charge_points")
        raise HTTPException(status_code=404, detail="Charge point not connected")

    fut = asyncio.get_event_loop().create_future()
    pending_stop_transactions[str(cp_id)] = fut

    try:
        logger.error(f"📤 [REMOTE STOP SEND] cp_id={cp_id}")
        await cp.send_remote_stop_transaction()

        logger.error(f"⏳ [WAIT StopTransaction] cp_id={cp_id} timeout=20s")

        result = await asyncio.wait_for(fut, timeout=20)

        logger.error(f"✅ [STOP SUCCESS] cp_id={cp_id} | result={result}")
        return result

    except asyncio.TimeoutError:
        logger.error(f"⏰ [STOP TIMEOUT] cp_id={cp_id} | no StopTransaction received")
        raise HTTPException(status_code=504, detail="StopTransaction timeout")

    except Exception as e:
        logger.exception(f"🔥 [STOP EXCEPTION] cp_id={cp_id} | error={e}")
        raise

    finally:
        pending_stop_transactions.pop(str(cp_id), None)


@app.get("/debug/charge-points")
async def debug_ids():
    cursor.execute("SELECT charge_point_id FROM charge_points")
    return [row[0] for row in cursor.fetchall()]


@app.get("/api/debug/connected-cp")
def debug_connected_cp():
    return list(connected_charge_points.keys())

from fastapi import Query
from datetime import datetime, timezone

@app.get("/api/debug/current-limit-state")
def debug_current_limit_state(cp_id: str | None = Query(default=None)):
    """
    Debug 用 API：回傳後端「實際限流下發狀態」current_limit_state
    - 若帶 cp_id 參數：只回傳單台
    - 不帶 cp_id：回傳全部
    """
    server_time = datetime.now(timezone.utc).isoformat()

    def _sanitize_state_item(_cp_id: str, st: dict):
        requested_limit_a = st.get("requested_limit_a")
        requested_power_kw = None
        try:
            if requested_limit_a is not None:
                control_voltage_v = float(get_community_settings().get("voltage_v", 220) or 220)
                requested_power_kw = round((float(requested_limit_a) * control_voltage_v) / 1000.0, 3)
        except Exception:
            requested_power_kw = None

        return {
            "cp_id": _cp_id,
            "managed_by": "power",
            "single_cp_max_power_kw": float(SINGLE_CP_MAX_POWER_KW),
            "requested_limit_a": requested_limit_a,
            "requested_power_kw": requested_power_kw,
            "requested_at": st.get("requested_at"),
            "applied": st.get("applied", False),
            "last_try_at": st.get("last_try_at"),
            "last_ok_at": st.get("last_ok_at"),
            "last_err_at": st.get("last_err_at"),
            "last_error": st.get("last_error"),
            "last_tx_id": st.get("last_tx_id"),
            "last_connector_id": st.get("last_connector_id"),
        }

    # 單台查詢
    if cp_id:
        cp_norm = _normalize_cp_id(cp_id)
        st = current_limit_state.get(cp_norm)
        if not st:
            return {"server_time": server_time, "items": []}
        return {"server_time": server_time, "items": [_sanitize_state_item(cp_norm, st)]}

    # 全部
    items = []
    for _cp_id, st in current_limit_state.items():
        items.append(_sanitize_state_item(_cp_id, st))

    # 排序：有 requested_at 的排前面、再依時間新到舊
    def _sort_key(x):
        ra = x.get("requested_at") or ""
        return ra
    items.sort(key=_sort_key, reverse=True)

    return {"server_time": server_time, "items": items}


@app.get("/api/community-settings")
def api_get_community_settings():
    try:
        cfg = get_community_settings()

        # ✅ 改用真實有效 active CP 清單
        active_cp_ids = get_effective_active_cp_ids()
        active_count = len(active_cp_ids)

        total_current_a = 0.0
        max_cars_by_min = 0
        allocated_power_kw = None
        preview_current_a = None
        blocked_reason = ""

        if cfg["enabled"] and cfg["contract_kw"] > 0:
            total_current_a = (cfg["contract_kw"] * 1000) / cfg["voltage_v"]
            max_cars_by_min = int(total_current_a // cfg["min_current_a"])

            try:
                # ✅ 改用真實 active cp_id 清單，不再用假的 ACTIVE_CP_1...
                allocated_power_kw = calculate_allocated_power_kw_by_cp_ids(active_cp_ids)

                # ✅ 社區總覽維持用 community 電壓做 preview
                #    這代表它是「社區平均預估值」，不是單樁即時值
                preview_current_a = (
                    convert_power_kw_to_current_a(allocated_power_kw, None)
                    if allocated_power_kw is not None
                    else None
                )

                if active_count > 0 and allocated_power_kw is None:
                    blocked_reason = "contract_kw_invalid"

            except Exception as e:
                logging.exception(f"[COMMUNITY_SETTINGS][ALLOC_ERR] {e}")

        return {
            "enabled": bool(cfg["enabled"]),
            "contract_kw": float(cfg["contract_kw"]),
            "voltage_v": float(cfg["voltage_v"]),
            "phases": int(cfg["phases"]),
            "min_current_a": float(cfg["min_current_a"]),
            "max_current_a": float(cfg["max_current_a"]),
            "managed_by": "power",
            "single_cp_max_power_kw": float(SINGLE_CP_MAX_POWER_KW),
            "active_charging_count": int(active_count),
            "active_cp_ids": active_cp_ids,   # ✅ 新增，方便前端或 debug 確認
            "total_current_a": round(float(total_current_a), 2),
            "max_cars_by_min": int(max_cars_by_min),
            "allocated_power_kw": allocated_power_kw,
            "preview_current_a": preview_current_a,
            "blocked_reason": blocked_reason,
        }
    except Exception as e:
        logging.exception(f"[COMMUNITY_SETTINGS][ERR] {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/community-settings")
def api_update_community_settings(payload: dict = Body(...)):
    enabled = 1 if payload.get("enabled") else 0
    contract_kw = float(payload.get("contractKw", 0) or 0)
    voltage_v = float(payload.get("voltageV", 220) or 220)
    phases = int(payload.get("phases", 1) or 1)
    min_current_a = float(payload.get("minCurrentA", 16) or 16)
    max_current_a = float(payload.get("maxCurrentA", 32) or 32)

    with sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE community_settings
            SET enabled = ?,
                contract_kw = ?,
                voltage_v = ?,
                phases = ?,
                min_current_a = ?,
                max_current_a = ?
            WHERE id = 1
        """,
            (enabled, contract_kw, voltage_v, phases, min_current_a, max_current_a),
        )
        conn.commit()

    return {"ok": True}


@app.post("/api/charge-points/{charge_point_id}/stop")
async def stop_transaction_by_charge_point(charge_point_id: str):
    print(f"🟢【API呼叫】收到停止充電API請求, charge_point_id = {charge_point_id}")
    cp = connected_charge_points.get(charge_point_id)

    if not cp:
        print(f"🔴【API異常】找不到連線中的充電樁：{charge_point_id}")
        raise HTTPException(
            status_code=404,
            detail=f"⚠️ 找不到連線中的充電樁：{charge_point_id}",
            headers={"X-Connected-CPs": str(list(connected_charge_points.keys()))},
        )
    # 查詢進行中的 transaction_id
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT transaction_id FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """,
            (charge_point_id,),
        )
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
    req = call.RemoteStopTransactionPayload(transaction_id=transaction_id)
    resp = await cp.call(req)
    print(f"🟢【API回應】呼叫 RemoteStopTransaction 完成，resp={resp}")

    # 等待 StopTransaction 被觸發（最多 10 秒）
    try:
        stop_result = await asyncio.wait_for(fut, timeout=10)
        print(f"🟢【API回應】StopTransaction 完成: {stop_result}")
        return {
            "message": "充電已停止",
            "transaction_id": transaction_id,
            "stop_result": stop_result,
        }
    except asyncio.TimeoutError:
        print(f"🔴【API異常】等待 StopTransaction 超時")
        return JSONResponse(
            status_code=504,
            content={"message": "等待充電樁停止回覆逾時 (StopTransaction timeout)"},
        )
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
            cur.execute(
                """
                SELECT value FROM meter_values
                WHERE charge_point_id=? AND measurand='Power.Active.Import'
                ORDER BY timestamp DESC LIMIT 1
            """,
                (cp_id,),
            )
            power_row = cur.fetchone()

            # 查詢最新累積電量 (Wh)
            cur.execute(
                """
                SELECT value FROM meter_values
                WHERE charge_point_id=? AND measurand='Energy.Active.Import.Register'
                ORDER BY timestamp DESC LIMIT 1
            """,
                (cp_id,),
            )
            energy_row = cur.fetchone()

            return JSONResponse(
                {
                    "power": round(power_row["value"], 2) if power_row else 0.0,
                    "kwh": (
                        round(energy_row["value"] / 1000.0, 3) if energy_row else 0.0
                    ),  # 轉為 kWh
                }
            )

    except Exception as e:
        logging.exception(f"❌ 查詢 charging_status 發生錯誤：{e}")
        return JSONResponse({"power": 0.0, "kwh": 0.0}, status_code=500)


# ==============================
# 🔌 Charge Point - Transaction APIs
# ==============================


# 抓最新「進行中」的交易 (只有 start_timestamp)
@app.get("/api/charge-points/{charge_point_id}/current-transaction")
def get_current_tx_by_cp(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT transaction_id, id_tag, start_timestamp
            FROM transactions
            WHERE charge_point_id = ? AND stop_timestamp IS NULL
            ORDER BY transaction_id DESC
            LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()
        print(f"[DEBUG current-transaction] cp_id={cp_id} | row={row}")  # ★ Debug
        if not row:
            return {"found": False}

        tx_id, id_tag, start_ts = row
        return {
            "found": True,
            "transaction_id": tx_id,
            "id_tag": id_tag,
            "start_timestamp": start_ts,
        }


@app.get("/api/devtools/last-transactions")
def last_transactions():
    import sqlite3

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT transaction_id, charge_point_id, id_tag, start_timestamp, meter_start,
                stop_timestamp, meter_stop, reason
            FROM transactions
            ORDER BY start_timestamp DESC
            LIMIT 5
        """
        )
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
                "reason": row[7],
            }
            for row in rows
        ]
        return {"last_transactions": result}


# ⭐ 新增：查詢該樁進行中交易的分段電價明細
@app.get("/api/charge-points/{charge_point_id}/current-transaction/price-breakdown")
def get_current_price_breakdown(charge_point_id: str):
    cp_id = _normalize_cp_id(charge_point_id)

    with get_conn() as conn:
        cur = conn.cursor()
        # 取得進行中的交易
        cur.execute(
            """
            SELECT transaction_id
            FROM transactions
            WHERE charge_point_id=? AND stop_timestamp IS NULL
            ORDER BY start_timestamp DESC LIMIT 1
        """,
            (cp_id,),
        )
        row = cur.fetchone()

        if not row:
            return {"found": False, "segments": []}

        tx_id = row[0]

    # 分段電價計算
    try:
        result = _calculate_multi_period_cost_detailed(tx_id)
        return {
            "found": True,
            "transaction_id": tx_id,
            **result,  # 合併 total, segments
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ 要讓除錯更直觀，在 /api/debug/price 增加目前伺服器時間顯示
@app.get("/api/debug/price")
def debug_price():
    now = datetime.now(TZ_TAIPEI)
    price = _price_for_timestamp(now.isoformat())
    return {"now": now.strftime("%Y-%m-%d %H:%M:%S"), "current_price": price}


# ✅ 除錯用：查詢目前資料庫中尚未結束的交易紀錄
@app.get("/api/debug/active-transactions")
def get_active_transactions():
    import sqlite3

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT transaction_id, charge_point_id, id_tag, start_timestamp, stop_timestamp
                FROM transactions
                ORDER BY start_timestamp DESC
                LIMIT 10
            """
            )
            rows = cur.fetchall()
        result = [
            {
                "transaction_id": r[0],
                "charge_point_id": r[1],
                "id_tag": r[2],
                "start_timestamp": r[3],
                "stop_timestamp": r[4],
                "status": "active" if r[4] is None else "stopped",
            }
            for r in rows
        ]
        return {"count": len(result), "transactions": result}
    except Exception as e:
        return {"error": str(e)}


import asyncio


async def monitor_balance_and_auto_stop():
    """
    後端監控任務：每 5 秒檢查所有進行中交易，
    若發現卡片餘額 <= 0，自動呼叫停止充電 API。
    """
    while True:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                # 找出所有進行中交易
                cur.execute(
                    """
                    SELECT t.charge_point_id, t.id_tag
                    FROM transactions t
                    WHERE t.stop_timestamp IS NULL
                """
                )
                active_tx = cur.fetchall()

            for cp_id, id_tag in active_tx:
                # 查詢卡片餘額
                with get_conn() as conn:
                    c2 = conn.cursor()
                    c2.execute("SELECT balance FROM cards WHERE card_id=?", (id_tag,))
                    row = c2.fetchone()

                if not row:
                    continue

                balance = float(row[0] or 0)
                if balance <= 0:
                    logger.warning(
                        f"[STOP][TRIGGER] balance_zero "
                        f"| cp_id={cp_id} "
                        f"| idTag={id_tag} "
                        f"| balance={balance}"
                    )

                    try:
                        await stop_transaction_by_charge_point(cp_id)
                    except Exception as e:
                        logger.error(
                            f"[STOP][TRIGGER_ERR] auto_stop failed "
                            f"| cp_id={cp_id} "
                            f"| idTag={id_tag} "
                            f"| err={repr(e)}"
                        )

            await asyncio.sleep(5)

        except Exception as e:
            print(f"❌ [監控例外] {e}")
            await asyncio.sleep(10)


# 啟動背景任務
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(monitor_balance_and_auto_stop())


if __name__ == "__main__":
    import os, uvicorn

    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)