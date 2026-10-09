import asyncio
import time
from decimal import Decimal

import structlog
from prometheus_client import Counter, Histogram
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.models import Payment, PaymentStatus
from shared.db import OutboxEvent
from shared.events import Event
from shared.idempotency import idempotent


PROVIDER_DELAY_SECONDS = 0.01

# The provider delay plus a few ms in the database, seconds when waiting on a lock
DURATION_BUCKETS = (
    0.01, 0.0105, 0.011, 0.0115, 0.012, 0.0125, 0.013, 0.014, 0.015, 0.0175,
    0.02, 0.025, 0.03, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)  
payments_total = Counter("payments_total", "Payments recorded")
payment_handler_duration_seconds = Histogram(
    "payment_handler_duration_seconds",
    "inventory.reserved handler run time, from the start of the provider delay to the commit",
    buckets=DURATION_BUCKETS,
)
payment_duplicates_total = Counter(
    "payment_duplicates_total",
    "inventory.reserved events skipped as already processed or for an order already paid",
)

logger = structlog.get_logger()


@idempotent
async def on_inventory_reserved(session: AsyncSession, event: Event) -> Counter:
    order_id = event.payload["order_id"]
    amount = Decimal(event.payload["total_amount"])
    payment = (
        await session.execute(
            insert(Payment)
            .values(order_id=order_id, amount=amount, status=PaymentStatus.completed)
            .on_conflict_do_nothing(index_elements=[Payment.order_id])
            .returning(Payment.id, Payment.created_at)
        )
    ).one_or_none()
    if payment is None:
        logger.debug("order already paid")
        return payment_duplicates_total
    logger.info("payment recorded", payment_id=payment.id)
    outcome = Event(
        event_type="payment.completed",
        occurred_at=payment.created_at,
        correlation_id=event.correlation_id,
        payload={"order_id": order_id, "amount": amount},
    )
    session.add(
        OutboxEvent(
            topic=outcome.event_type,
            message_key=str(order_id),
            payload=outcome.model_dump(mode="json"),
        )
    )
    return payments_total


async def handle(session_factory: async_sessionmaker[AsyncSession], event: Event) -> None:
    structlog.contextvars.bind_contextvars(order_id=event.payload["order_id"])
    started = time.perf_counter()
    try:
        await asyncio.sleep(PROVIDER_DELAY_SECONDS)
        counter = await on_inventory_reserved(session_factory, event)
    finally:
        payment_handler_duration_seconds.observe(time.perf_counter() - started)
    if counter is None:
        counter = payment_duplicates_total
    # Counted after the commit, so an event redelivered because its commit failed counts once
    counter.inc()
