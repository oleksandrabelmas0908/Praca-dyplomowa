# TEMPORARY test harness, delete once inventory and payments publish these events themselves.
# Publishes inventory.rejected or payment.completed for one order, with the correlation ID of
# the request that created it:
#   docker compose -f infra/docker-compose.yml --project-directory . exec orders \
#       python -m orders.fake_events payment.completed 42 [--event-id <uuid of an earlier one>]
import argparse
import asyncio
from uuid import UUID, uuid4

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from shared import kafka
from shared.db import OutboxEvent, database_url
from shared.events import Event
from shared.logging import setup_logging
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)
logger = structlog.get_logger()


async def order_correlation_id(order_id: int) -> UUID:
    engine = create_async_engine(database_url())
    async with engine.connect() as connection:
        correlation_id = await connection.scalar(
            select(OutboxEvent.payload["correlation_id"].astext).where(
                OutboxEvent.topic == "order.created", OutboxEvent.message_key == str(order_id)
            )
        )
    await engine.dispose()
    return UUID(correlation_id) if correlation_id else uuid4()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("event_type", choices=["inventory.rejected", "payment.completed"])
    parser.add_argument("order_id", type=int)
    parser.add_argument("--event-id", type=UUID, default=None)
    args = parser.parse_args()

    event = Event(
        event_id=args.event_id or uuid4(),
        event_type=args.event_type,
        correlation_id=await order_correlation_id(args.order_id),
        payload={"order_id": args.order_id},
    )
    async with kafka.producer() as producer:
        await kafka.publish(producer, event.event_type, event)
    logger.info(
        "published",
        event_type=event.event_type,
        event_id=str(event.event_id),
        correlation_id=str(event.correlation_id),
        order_id=args.order_id,
    )


asyncio.run(main())
