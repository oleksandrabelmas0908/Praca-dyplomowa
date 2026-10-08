import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.db import OutboxEvent
from shared.events import Event, to_json
from shared.settings import settings

BATCH_SIZE = 100
BACKOFF_MAX_SECONDS = 30.0
ERROR_AFTER_FAILURES = 5
GAUGE_INTERVAL_SECONDS = 5.0
CLEANUP_INTERVAL_SECONDS = 60.0
CLEANUP_BATCH_SIZE = 1000

# Around the poll interval while Kafka keeps up, minutes while it is down
LAG_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75,
    1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
)

outbox_lag_seconds = Histogram(
    "outbox_lag_seconds",
    "Time from writing an outbox row to Kafka confirming its publish",
    buckets=LAG_BUCKETS,
)
outbox_undispatched_events = Gauge(
    "outbox_undispatched_events", "Outbox rows not yet published to Kafka"
)
outbox_publish_attempts_total = Counter(
    "outbox_publish_attempts_total", "Outbox publish attempts by outcome", ["outcome"]
)

logger = structlog.get_logger()

failures: dict[int, int] = {}


def backoff_seconds(failed_passes: int) -> float:
    return min(settings.outbox_poll_interval_seconds * 2**failed_passes, BACKOFF_MAX_SECONDS)


def record_failure(row: OutboxEvent, error: BaseException) -> None:
    failures[row.id] = failures.get(row.id, 0) + 1
    outbox_publish_attempts_total.labels("failure").inc()
    log = logger.error if failures[row.id] >= ERROR_AFTER_FAILURES else logger.warning
    log(
        "outbox publish failed",
        outbox_event_id=row.id,
        correlation_id=row.payload["correlation_id"],
        failures=failures[row.id],
        error=repr(error),
    )


async def start_producer() -> AIOKafkaProducer:
    failed = 0
    while True:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        try:
            await producer.start()
        except KafkaError as error:
            await producer.stop()
            failed += 1
            logger.warning("kafka unreachable, outbox poller not started", error=repr(error))
            await asyncio.sleep(backoff_seconds(failed))
            continue
        return producer


async def dispatch_batch(
    session_factory: async_sessionmaker[AsyncSession],
    producer: AIOKafkaProducer,
    topics: list[str],
) -> tuple[int, int]:
    async with session_factory() as session, session.begin():
        rows = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.dispatched_at.is_(None), OutboxEvent.topic.in_(topics))
                .order_by(OutboxEvent.id)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
        ).all()
        sent: list[OutboxEvent] = []
        futures: list[asyncio.Future[Any]] = []
        for row in rows:
            try:
                future = await producer.send(
                    row.topic,
                    to_json(Event.model_validate(row.payload)),
                    key=row.message_key.encode(),
                )
            except KafkaError as error:
                # The rest of the batch waits for the next pass, so no row overtakes an older one
                record_failure(row, error)
                break
            sent.append(row)
            futures.append(future)
        results = await asyncio.gather(*futures, return_exceptions=True)
        published = []
        for row, result in zip(sent, results, strict=True):
            if isinstance(result, BaseException):
                record_failure(row, result)
            else:
                published.append(row)
        if not published:
            return len(rows), 0
        # Marked only once Kafka has confirmed the publish. A crash between the two leaves the row
        # undispatched, so it is published a second time on the next pass. That duplicate is
        # expected: Kafka delivery is at-least-once anyway and every consumer is idempotent
        dispatched_at = datetime.now(UTC)
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_([row.id for row in published]))
            .values(dispatched_at=dispatched_at)
        )
    for row in published:
        failures.pop(row.id, None)
        outbox_lag_seconds.observe((dispatched_at - row.created_at).total_seconds())
    outbox_publish_attempts_total.labels("success").inc(len(published))
    return len(rows), len(published)


async def dispatch_loop(
    session_factory: async_sessionmaker[AsyncSession], topics: list[str]
) -> None:
    producer = await start_producer()
    failed_passes = 0
    try:
        while True:
            claimed = 0
            try:
                claimed, published = await dispatch_batch(session_factory, producer, topics)
                failed = claimed > 0 and published == 0
            except Exception as error:  # noqa: BLE001 - e.g. Postgres unreachable, the loop must go on
                logger.warning("outbox poll failed", error=repr(error))
                failed = True
            if failed:
                failed_passes += 1
                await asyncio.sleep(backoff_seconds(failed_passes))
            else:
                failed_passes = 0
                if claimed < BATCH_SIZE:
                    await asyncio.sleep(settings.outbox_poll_interval_seconds)
    finally:
        await producer.stop()


async def gauge_loop(session_factory: async_sessionmaker[AsyncSession], topics: list[str]) -> None:
    while True:
        try:
            async with session_factory() as session:
                count = (
                    await session.execute(
                        select(func.count())
                        .select_from(OutboxEvent)
                        .where(OutboxEvent.dispatched_at.is_(None), OutboxEvent.topic.in_(topics))
                    )
                ).scalar_one()
            outbox_undispatched_events.set(count)
        except Exception as error:  # noqa: BLE001 - e.g. Postgres unreachable, the loop must go on
            logger.warning("counting undispatched outbox rows failed", error=repr(error))
        await asyncio.sleep(GAUGE_INTERVAL_SECONDS)


async def cleanup_loop(
    session_factory: async_sessionmaker[AsyncSession], topics: list[str]
) -> None:
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        cutoff = datetime.now(UTC) - timedelta(seconds=settings.outbox_retention_seconds)
        total = 0
        try:
            deleted = CLEANUP_BATCH_SIZE
            while deleted == CLEANUP_BATCH_SIZE:
                async with session_factory() as session, session.begin():
                    result = await session.scalars(
                        delete(OutboxEvent)
                        .where(
                            OutboxEvent.id.in_(
                                select(OutboxEvent.id)
                                .where(
                                    OutboxEvent.dispatched_at < cutoff,
                                    OutboxEvent.topic.in_(topics),
                                )
                                .limit(CLEANUP_BATCH_SIZE)
                                .with_for_update(skip_locked=True)
                            )
                        )
                        .returning(OutboxEvent.id)
                    )
                    deleted = len(result.all())
                total += deleted
        except Exception as error:  # noqa: BLE001 - e.g. Postgres unreachable, the loop must go on
            logger.warning("deleting dispatched outbox rows failed", error=repr(error))
        if total:
            logger.info("deleted dispatched outbox rows", count=total)


@asynccontextmanager
async def poller(
    session_factory: async_sessionmaker[AsyncSession], topics: list[str]
) -> AsyncIterator[None]:
    tasks = asyncio.gather(
        dispatch_loop(session_factory, topics),
        gauge_loop(session_factory, topics),
        cleanup_loop(session_factory, topics),
    )
    try:
        yield
    finally:
        tasks.cancel()
        with suppress(asyncio.CancelledError):
            await tasks
