import time
from typing import Any

import httpx
import structlog
from prometheus_client import Counter, Histogram
from sqlalchemy.ext.asyncio import AsyncSession

from orders.db import async_db
from orders.events import set_status
from orders.models import Order, OrderStatus
from shared.events import current_correlation_id
from shared.metrics import BUCKETS
from shared.middleware import HEADER
from shared.settings import settings

chain_call_duration_seconds = Histogram(
    "chain_call_duration_seconds",
    "Outbound call duration in the HTTP chain, failed calls included",
    ["call"],
    buckets=BUCKETS,
)
chain_failures_total = Counter(
    "chain_failures_total", "Reserve and pay calls that failed, by cause", ["call", "cause"]
)
chain_releases_total = Counter("chain_releases_total", "Releases triggered by a failed pay call")
chain_release_failures_total = Counter(
    "chain_release_failures_total",
    "Release calls that failed, the stock may still be reserved",
    ["cause"],
)

logger = structlog.get_logger()


class CallFailed(Exception):
    def __init__(self, cause: str) -> None:
        super().__init__(cause)
        self.cause = cause


async def post(
    client: httpx.AsyncClient,
    call: str,
    url: str,
    body: dict[str, Any] | None,
    timeout_seconds: float,
    expected: tuple[int, ...],
) -> httpx.Response:
    started = time.perf_counter()
    try:
        response = await client.post(
            url,
            json=body,
            headers={HEADER: str(current_correlation_id())},
            timeout=httpx.Timeout(timeout_seconds, connect=settings.http_connect_timeout_seconds),
        )
    except httpx.HTTPError as error:
        raise CallFailed(type(error).__name__) from error
    finally:
        chain_call_duration_seconds.labels(call).observe(time.perf_counter() - started)
    if response.status_code not in expected:
        raise CallFailed(f"status_{response.status_code}")
    return response


async def release(client: httpx.AsyncClient, order_id: int) -> None:
    chain_releases_total.inc()
    try:
        await post(
            client,
            "release",
            f"{settings.inventory_url}/reservations/{order_id}/release",
            None,
            settings.release_timeout_seconds,
            (200,),
        )
    except CallFailed as failed:
        chain_release_failures_total.labels(failed.cause).inc()
        logger.error("release failed, stock may still be reserved", cause=failed.cause)


async def place(client: httpx.AsyncClient, order: Order) -> OrderStatus:
    lines = [{"product_id": line.product_id, "quantity": line.quantity} for line in order.lines]
    try:
        response = await post(
            client,
            "reserve",
            f"{settings.inventory_url}/reservations",
            {"order_id": order.id, "lines": lines},
            settings.inventory_timeout_seconds,
            (201, 409),
        )
    except CallFailed as failed:
        chain_failures_total.labels("reserve", failed.cause).inc()
        logger.error("reserve call failed, order rejected", cause=failed.cause)
        return OrderStatus.rejected
    if response.status_code == 409:
        logger.info("order rejected by inventory", **response.json())
        return OrderStatus.rejected

    try:
        await post(
            client,
            "pay",
            f"{settings.payments_url}/payments",
            {"order_id": order.id, "amount": str(order.total_amount)},
            settings.payments_timeout_seconds,
            (201,),
        )
    except CallFailed as failed:
        chain_failures_total.labels("pay", failed.cause).inc()
        logger.error("pay call failed, releasing the reservation", cause=failed.cause)
        await release(client, order.id)
        return OrderStatus.rejected
    return OrderStatus.paid


async def run(client: httpx.AsyncClient, session: AsyncSession, order: Order) -> Order:
    structlog.contextvars.bind_contextvars(order_id=order.id)
    status = await place(client, order)
    async with session.begin():
        counter = await set_status(session, order.id, status)
        updated = await async_db.get_order(session, order.id)
    counter.inc()
    assert updated is not None
    return updated
