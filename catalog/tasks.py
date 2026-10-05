from pathlib import Path
from typing import Any

import structlog
from celery import Celery, Task, signals
from PIL import Image
from sqlalchemy import create_engine, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from catalog.models import Product
from shared.db import database_url
from shared.logging import setup_logging
from shared.middleware import HEADER
from shared.settings import settings

IMAGE_DIR = Path("/images")
DEAD_LETTER_QUEUE = "dead_letter"
# Longest side in pixels
SIZES = {"thumbnail": 150, "small": 300, "medium": 600, "large": 1200}

logger = structlog.get_logger()

app = Celery("catalog", broker=settings.redis_broker_url)
app.conf.update(
    task_default_queue="cpu",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_pool="prefork",
    worker_concurrency=settings.celery_concurrency,
    broker_transport_options={"visibility_timeout": 120},
    broker_connection_retry_on_startup=True,
)

engine = create_engine(database_url().set(drivername="postgresql+psycopg2"), poolclass=NullPool)


@signals.setup_logging.connect
def configure_logging(**kwargs: Any) -> None:
    setup_logging(settings.service_name, settings.log_level)


@app.task(
    bind=True,
    autoretry_for=(OperationalError,),
    max_retries=5,
    retry_backoff=2,
    retry_jitter=True,
)
def process_product_image(self: Task, product_id: int, source_path: str) -> None:
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        correlation_id=self.request.get(HEADER), product_id=product_id
    )
    directory = IMAGE_DIR / str(product_id)
    with Image.open(source_path) as original:
        image = original.convert("RGB")

    paths = []
    for name, size in SIZES.items():
        variant = image.copy()
        variant.thumbnail((size, size), Image.Resampling.LANCZOS)
        variant.save(directory / f"{name}.jpg")
        paths.append(str(directory / f"{name}.jpg"))
    image.save(directory / "original.webp")
    paths.append(str(directory / "original.webp"))

    with Session(engine) as session, session.begin():
        session.execute(update(Product).where(Product.id == product_id).values(image_paths=paths))
    logger.info("product image processed", paths=paths)


@signals.task_failure.connect(sender=process_product_image)
def move_to_dead_letter_queue(
    sender: Task,
    exception: Exception,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    **_: Any,
) -> None:
    logger.error(
        "image processing failed, task moved to the dead-letter queue",
        error=repr(exception),
        retries=sender.request.retries,
    )
    sender.apply_async(
        args,
        kwargs,
        queue=DEAD_LETTER_QUEUE,
        headers={HEADER: sender.request.get(HEADER)},
    )
