from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable


ACK_ACCEPTED = "accepted"
ACK_REJECTED = "rejected"
ACK_TIMEOUT = "timeout"
ACK_ERROR = "error"
ACK_PENDING = "pending"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StopContext:
    transaction_id: int
    cp_id: str
    trigger: str
    stop_request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=_utc_now_iso)
    connection_seq: int | None = None
    connection_instance_id: str | None = None
    remote_stop_unique_id: str | None = None
    websocket_replaced: bool = False
    ack_status: str = ACK_PENDING
    ack_elapsed_ms: int | None = None
    stop_status: str = "pending"
    stop_received: bool = False
    stop_reason: str | None = None
    stop_timestamp: str | None = None
    settlement_status: str = "pending"
    final_outcome: str = "pending"
    auto_stop_reason: str | None = None
    mark_succeeded: bool | None = None
    mark_error: str | None = None
    task: asyncio.Task | None = None
    stop_future: asyncio.Future = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.transaction_id = int(self.transaction_id)
        self.stop_future = asyncio.get_running_loop().create_future()

    def result(self) -> dict[str, Any]:
        stop_result = None
        if self.stop_future.done() and not self.stop_future.cancelled():
            try:
                stop_result = self.stop_future.result()
            except Exception:
                stop_result = None

        return {
            "stop_request_id": self.stop_request_id,
            "transaction_id": self.transaction_id,
            "cp_id": self.cp_id,
            "trigger": self.trigger,
            "connection_seq": self.connection_seq,
            "connection_instance_id": self.connection_instance_id,
            "remote_stop_unique_id": self.remote_stop_unique_id,
            "websocket_replaced": self.websocket_replaced,
            "ack_status": self.ack_status,
            "ack_elapsed_ms": self.ack_elapsed_ms,
            "stop_status": self.stop_status,
            "stop_received": self.stop_received,
            "stop_reason": self.stop_reason,
            "stop_timestamp": self.stop_timestamp,
            "settlement_status": self.settlement_status,
            "final_outcome": self.final_outcome,
            "auto_stop_reason": self.auto_stop_reason,
            "mark_succeeded": self.mark_succeeded,
            "mark_error": self.mark_error,
            "stop_result": stop_result,
        }


class StopRegistry:
    """Transaction-keyed registry used to deduplicate stop requests."""

    def __init__(self, contexts: dict[int, StopContext] | None = None) -> None:
        self.contexts = contexts if contexts is not None else {}
        self._lock = asyncio.Lock()

    async def get_or_create(
        self,
        transaction_id: int,
        cp_id: str,
        trigger: str,
        *,
        auto_stop_reason: str | None = None,
    ) -> tuple[StopContext, bool]:
        tx_id = int(transaction_id)
        async with self._lock:
            current = self.contexts.get(tx_id)
            if current is not None:
                return current, False

            context = StopContext(
                transaction_id=tx_id,
                cp_id=cp_id,
                trigger=trigger,
                auto_stop_reason=auto_stop_reason,
            )
            self.contexts[tx_id] = context
            return context, True

    def get(self, transaction_id: int) -> StopContext | None:
        try:
            return self.contexts.get(int(transaction_id))
        except (TypeError, ValueError):
            return None

    def complete_settlement(
        self,
        transaction_id: int,
        result: dict[str, Any],
        *,
        committed: bool,
        already_stopped: bool = False,
        error: str | None = None,
    ) -> StopContext | None:
        context = self.get(transaction_id)
        if context is None:
            return None

        context.stop_received = True
        context.stop_status = "already_stopped" if already_stopped else "received"
        context.stop_reason = result.get("reason")
        context.stop_timestamp = result.get("timestamp")
        context.settlement_status = "committed" if committed else "failed"

        completion = dict(result)
        completion.update(
            {
                "settlement_committed": bool(committed),
                "settlement_status": context.settlement_status,
                "already_stopped": bool(already_stopped),
                "settlement_error": error,
            }
        )
        if not context.stop_future.done():
            context.stop_future.set_result(completion)
        return context

    async def remove(self, transaction_id: int, expected: StopContext) -> bool:
        tx_id = int(transaction_id)
        async with self._lock:
            if self.contexts.get(tx_id) is not expected:
                return False
            self.contexts.pop(tx_id, None)
            return True


def classify_ack_status(response: Any) -> str:
    value = response
    if response is not None and not isinstance(response, str):
        value = getattr(response, "status", None)
    normalized = str(value or "").strip().lower()
    if normalized == ACK_ACCEPTED:
        return ACK_ACCEPTED
    if normalized == ACK_REJECTED:
        return ACK_REJECTED
    return ACK_ERROR


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _apply_stop_completion(context: StopContext, completion: dict[str, Any]) -> None:
    committed = bool(completion.get("settlement_committed"))
    already_stopped = bool(completion.get("already_stopped"))
    context.stop_received = True
    context.stop_status = "already_stopped" if already_stopped else "received"
    context.stop_reason = completion.get("reason")
    context.stop_timestamp = completion.get("timestamp")
    context.settlement_status = "committed" if committed else "failed"
    if committed:
        context.final_outcome = "already_stopped" if already_stopped else "stopped"
    else:
        context.final_outcome = "settlement_failed"


async def execute_stop_request(
    context: StopContext,
    send_remote_stop: Callable[[], Awaitable[Any]],
    is_transaction_stopped: Callable[[], Any],
    *,
    ack_timeout: float = 30.0,
    stop_timeout: float = 45.0,
    ack_after_stop_grace: float = 0.5,
) -> dict[str, Any]:
    """Send RemoteStop and independently wait for committed StopTransaction."""

    if await _maybe_await(is_transaction_stopped()):
        context.stop_status = "already_stopped"
        context.settlement_status = "committed"
        context.final_outcome = "already_stopped"
        return context.result()

    started = time.monotonic()
    ack_task = asyncio.create_task(send_remote_stop())

    async def finish_ack() -> None:
        ack_started = time.monotonic()
        try:
            response = await asyncio.wait_for(ack_task, timeout=max(0.01, ack_timeout))
            context.ack_status = classify_ack_status(response)
        except asyncio.TimeoutError:
            context.ack_status = ACK_TIMEOUT
        except asyncio.CancelledError:
            if context.ack_status == ACK_PENDING:
                context.ack_status = ACK_TIMEOUT
            raise
        except Exception:
            context.ack_status = ACK_ERROR
        finally:
            context.ack_elapsed_ms = int((time.monotonic() - ack_started) * 1000)

    ack_waiter = asyncio.create_task(finish_ack())

    try:
        deadline = started + max(0.01, stop_timeout)
        done, _ = await asyncio.wait(
            {ack_waiter, context.stop_future},
            timeout=max(0.01, stop_timeout),
            return_when=asyncio.FIRST_COMPLETED,
        )

        if context.stop_future in done:
            completion = context.stop_future.result()
            _apply_stop_completion(context, completion)
            if not ack_waiter.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(ack_waiter),
                        timeout=max(0.0, ack_after_stop_grace),
                    )
                except asyncio.TimeoutError:
                    ack_waiter.cancel()
                    await asyncio.gather(ack_waiter, return_exceptions=True)
            return context.result()

        if ack_waiter in done:
            await asyncio.gather(ack_waiter, return_exceptions=True)

        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0 and not context.stop_future.done():
            try:
                completion = await asyncio.wait_for(
                    asyncio.shield(context.stop_future), timeout=remaining
                )
                _apply_stop_completion(context, completion)
                return context.result()
            except asyncio.TimeoutError:
                pass

        if context.stop_future.done():
            _apply_stop_completion(context, context.stop_future.result())
            return context.result()

        if await _maybe_await(is_transaction_stopped()):
            context.stop_status = "already_stopped"
            context.settlement_status = "committed"
            context.final_outcome = "already_stopped"
            return context.result()

        context.stop_status = "timeout"
        context.settlement_status = "not_committed"
        context.final_outcome = "stop_timeout"
        if context.ack_status == ACK_PENDING:
            context.ack_status = ACK_TIMEOUT
            context.ack_elapsed_ms = int((time.monotonic() - started) * 1000)
        return context.result()
    finally:
        if not ack_waiter.done():
            ack_waiter.cancel()
            await asyncio.gather(ack_waiter, return_exceptions=True)
        if not ack_task.done():
            ack_task.cancel()
            await asyncio.gather(ack_task, return_exceptions=True)


def sqlite_write_with_retry(
    connect: Callable[[], Any],
    operation: Callable[[Any], Any],
    *,
    attempts: int = 4,
    backoff_seconds: tuple[float, ...] = (0.05, 0.15, 0.35),
    on_retry: Callable[[int, float, Exception], None] | None = None,
) -> Any:
    """Run one SQLite write, retrying only locked/busy operational errors."""

    attempts = max(1, int(attempts))
    started = time.monotonic()
    for attempt in range(1, attempts + 1):
        connection = None
        try:
            connection = connect()
            result = operation(connection)
            connection.commit()
            return result
        except Exception as exc:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
            message = str(exc).lower()
            retryable = "database is locked" in message or "database is busy" in message
            if not retryable or attempt >= attempts:
                raise
            delay = backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)]
            if on_retry is not None:
                on_retry(attempt, (time.monotonic() - started) * 1000.0, exc)
            time.sleep(max(0.0, delay))
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
