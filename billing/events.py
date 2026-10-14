import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from kombu.exceptions import OperationalError
from prometheus_client import Counter, Histogram
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from billing.invoice import invoice_number, invoice_path
from billing.models import Invoice, InvoiceStatus
from billing.tasks import queue_invoice_pdf
from shared.events import Event
from shared.idempotency import idempotent

# 2 to 5 ms for most events on local Docker, seconds when waiting on a row lock or a down broker
DURATION_BUCKETS = (
    0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.003, 0.004, 0.005, 0.0075, 0.01,
    0.015, 0.02, 0.03, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
)  # fmt: skip

invoices_requested_total = Counter(
    "invoices_requested_total", "Invoices written as pending with their PDF task queued"
)
invoice_handler_duration_seconds = Histogram(
    "invoice_handler_duration_seconds",
    "payment.completed handler run time, from the idempotency insert to the task being queued",
    buckets=DURATION_BUCKETS,
)
invoice_duplicates_total = Counter(
    "invoice_duplicates_total",
    "payment.completed events skipped as already processed or for an order already invoiced",
)

logger = structlog.get_logger()


@idempotent
async def on_payment_completed(session: AsyncSession, event: Event) -> dict[str, Any] | None:
    order_id = event.payload["order_id"]
    order_data = {
        "order_id": order_id,
        "customer_id": event.payload["customer_id"],
        "amount": event.payload["amount"],
        "lines": event.payload["lines"],
    }
    requested_at = datetime.now(UTC)
    invoice_id = await session.scalar(
        insert(Invoice)
        .values(
            order_id=order_id,
            invoice_number=invoice_number(order_id, requested_at),
            amount=Decimal(order_data["amount"]),
            file_path=str(invoice_path(order_id)),
            status=InvoiceStatus.pending,
            created_at=requested_at,
        )
        .on_conflict_do_nothing(index_elements=[Invoice.order_id])
        .returning(Invoice.id)
    )
    if invoice_id is None:
        logger.debug("order already invoiced")
        return None
    logger.info("invoice requested", invoice_id=invoice_id)
    return order_data


async def handle(session_factory: async_sessionmaker[AsyncSession], event: Event) -> None:
    structlog.contextvars.bind_contextvars(order_id=event.payload["order_id"])
    started = time.perf_counter()
    try:
        order_data = await on_payment_completed(session_factory, event)
        if order_data is None:
            invoice_duplicates_total.inc()
            return
        try:
            # In a thread, because publishing to Redis blocks the event loop
            await asyncio.to_thread(queue_invoice_pdf, order_data, event.correlation_id)
        except OperationalError as error:
            # Not raised: the row is committed, so a redelivered event would only be skipped
            logger.error("invoice task not queued, invoice stays pending", error=repr(error))
            return
        invoices_requested_total.inc()
    finally:
        invoice_handler_duration_seconds.observe(time.perf_counter() - started)
