from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import partial

import structlog
from prometheus_client import Counter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from orders.models import Order, OrderStatus, can_transition
from shared.events import Event
from shared.idempotency import idempotent
from shared.kafka import consumer

order_status_transitions_total = Counter(
    "order_status_transitions_total", "Order status changes", ["from_status", "to_status"]
)
order_status_transitions_rejected_total = Counter(
    "order_status_transitions_rejected_total",
    "Status events that left the order unchanged",
    ["reason"],
)

logger = structlog.get_logger()


async def set_status(session: AsyncSession, event: Event, status: OrderStatus) -> Counter:
    order_id = event.payload["order_id"]
    order = await session.get(Order, order_id, with_for_update=True)
    if order is None:
        logger.error("event for unknown order", order_id=order_id)
        return order_status_transitions_rejected_total.labels("unknown_order")
    if not can_transition(order.status, status):
        logger.warning(
            "illegal order status transition",
            order_id=order_id,
            current_status=order.status,
            attempted_status=status,
        )
        return order_status_transitions_rejected_total.labels("illegal_transition")
    logger.info(
        "order status changed", order_id=order_id, from_status=order.status, to_status=status
    )
    transition = order_status_transitions_total.labels(order.status, status)
    order.status = status
    return transition


@idempotent
async def on_inventory_rejected(session: AsyncSession, event: Event) -> Counter:
    return await set_status(session, event, OrderStatus.rejected)


@idempotent
async def on_payment_completed(session: AsyncSession, event: Event) -> Counter:
    return await set_status(session, event, OrderStatus.paid)


async def handle(
    handler: Callable[[async_sessionmaker[AsyncSession], Event], Awaitable[Counter | None]],
    session_factory: async_sessionmaker[AsyncSession],
    event: Event,
) -> None:
    counter = await handler(session_factory, event)
    if counter is None:
        counter = order_status_transitions_rejected_total.labels("duplicate_event")
    # Counted after the commit, so an event redelivered because its commit failed counts once
    counter.inc()


@asynccontextmanager
async def consumers(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[None]:
    async with (
        consumer("inventory.rejected", partial(handle, on_inventory_rejected, session_factory)),
        consumer("payment.completed", partial(handle, on_payment_completed, session_factory)),
    ):
        yield
