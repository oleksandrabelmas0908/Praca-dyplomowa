from typing import Any
from uuid import UUID

from celery import Celery

# Connects before_task_publish, which stamps every task with its publish time for the queue wait
import shared.celery_metrics  # noqa: F401
from shared.middleware import HEADER
from shared.settings import settings

app = Celery("billing", broker=settings.redis_broker_url)
app.conf.update(task_default_queue="cpu")


def queue_invoice_pdf(order_data: dict[str, Any], correlation_id: UUID) -> None:
    # Sent by name, the task is defined together with the billing worker
    app.send_task(
        "billing.tasks.generate_invoice_pdf",
        args=(order_data,),
        headers={HEADER: str(correlation_id)},
    )
