from typing import Any
from uuid import UUID

import structlog
from celery import Celery, Task, signals
from sqlalchemy import update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

# Connects the publish, start, end, retry and failure signals and the worker's metrics server
import shared.celery_metrics  # noqa: F401
from billing.invoice import engine, generate_invoice
from billing.models import Invoice, InvoiceStatus
from shared.logging import setup_logging
from shared.middleware import HEADER
from shared.settings import settings

logger = structlog.get_logger()

app = Celery("billing", broker=settings.redis_broker_url)
app.conf.update(
    task_default_queue="cpu",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_pool="prefork",
    worker_concurrency=settings.celery_concurrency,
    broker_transport_options={"visibility_timeout": 120},
    broker_connection_retry_on_startup=True,
)


@signals.setup_logging.connect
def configure_logging(**kwargs: Any) -> None:
    setup_logging(settings.service_name, settings.log_level)


@app.task(
    bind=True,
    autoretry_for=(OperationalError, OSError),
    max_retries=5,
    retry_backoff=2,
    retry_jitter=True,
)
def generate_invoice_pdf(self: Task, order_data: dict[str, Any]) -> None:
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        correlation_id=self.request.get(HEADER), order_id=order_data["order_id"]
    )
    generate_invoice(order_data)


@signals.task_failure.connect(sender=generate_invoice_pdf)
def mark_failed(sender: Task, exception: Exception, args: tuple[Any, ...], **_: Any) -> None:
    logger.error(
        "invoice generation failed, invoice marked failed",
        error=repr(exception),
        retries=sender.request.retries,
    )
    with Session(engine) as session, session.begin():
        session.execute(
            update(Invoice)
            .where(Invoice.order_id == args[0]["order_id"])
            .values(status=InvoiceStatus.failed, error=repr(exception))
        )


def queue_invoice_pdf(order_data: dict[str, Any], correlation_id: UUID) -> None:
    generate_invoice_pdf.apply_async((order_data,), headers={HEADER: str(correlation_id)})
