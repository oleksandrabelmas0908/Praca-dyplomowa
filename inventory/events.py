import time

import structlog
from prometheus_client import Counter, Histogram
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from inventory.db import reserve_all
from inventory.schema import RejectionReason
from shared.db import OutboxEvent
from shared.events import Event
from shared.idempotency import idempotent

# 1 to 3 ms for most events on local Docker, seconds when waiting on a row lock
DURATION_BUCKETS = (
    0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.003, 0.004, 0.005, 0.0075, 0.01,
    0.015, 0.02, 0.03, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)  # fmt: skip

reservations_total = Counter(
    "reservations_total", "order.created events handled, by outcome", ["outcome"]
)
reservation_handler_duration_seconds = Histogram(
    "reservation_handler_duration_seconds",
    "order.created handler run time, from the idempotency insert to the commit",
    buckets=DURATION_BUCKETS,
)
reservation_duplicates_total = Counter(
    "reservation_duplicates_total", "order.created events skipped as already processed"
)

logger = structlog.get_logger()


@idempotent
async def on_order_created(session: AsyncSession, event: Event) -> Counter:
    order_id = event.payload["order_id"]
    lines = [(line["product_id"], line["quantity"]) for line in event.payload["lines"]]
    rejection = await reserve_all(session, lines)
    if rejection is None:
        logger.info("stock reserved", lines=len(lines))
        outcome = Event(
            event_type="inventory.reserved",
            correlation_id=event.correlation_id,
            payload={"order_id": order_id, "total_amount": event.payload["total_amount"]},
        )
        counter = reservations_total.labels("reserved")
    else:
        details = {
            "reason": rejection.reason,
            "product_id": rejection.product_id,
            "requested": rejection.requested,
            "available": rejection.available,
            "missing": rejection.requested - rejection.available,
        }
        if rejection.reason is RejectionReason.product_not_found:
            logger.error("order rejected, unknown product", **details)
        else:
            logger.info("order rejected", **details)
        outcome = Event(
            event_type="inventory.rejected",
            correlation_id=event.correlation_id,
            payload={"order_id": order_id, **details},
        )
        counter = reservations_total.labels("rejected")
    session.add(
        OutboxEvent(
            topic=outcome.event_type,
            message_key=str(order_id),
            payload=outcome.model_dump(mode="json"),
        )
    )
    return counter


async def handle(session_factory: async_sessionmaker[AsyncSession], event: Event) -> None:
    structlog.contextvars.bind_contextvars(order_id=event.payload["order_id"])
    started = time.perf_counter()
    try:
        counter = await on_order_created(session_factory, event)
    except Exception:
        reservations_total.labels("error").inc()
        raise
    finally:
        reservation_handler_duration_seconds.observe(time.perf_counter() - started)
    if counter is None:
        counter = reservation_duplicates_total
    # Counted after the commit, so an event redelivered because its commit failed counts once
    counter.inc()
