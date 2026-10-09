import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager

import structlog
from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRebalanceListener,
    ConsumerRecord,
    TopicPartition,
)
from aiokafka.errors import KafkaError

from shared.events import Event, from_json, to_json
from shared.settings import settings

POLL_TIMEOUT_SECONDS = 1.0
RETRY_DELAY_SECONDS = 1.0

logger = structlog.get_logger()

Handler = Callable[[Event], Awaitable[None]]


@asynccontextmanager
async def producer() -> AsyncIterator[AIOKafkaProducer]:
    kafka = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await kafka.start()
    try:
        yield kafka
    finally:
        await kafka.stop()


async def publish(kafka: AIOKafkaProducer, topic: str, event: Event) -> None:
    key = str(event.payload["order_id"]).encode()
    await kafka.send_and_wait(topic, to_json(event), key=key)


@asynccontextmanager
async def consumer(topic: str, handler: Handler) -> AsyncIterator[None]:
    kafka = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.service_name,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    stopping = asyncio.Event()
    partitions = PartitionTasks(kafka, handler, stopping)
    kafka.subscribe([topic], listener=partitions)
    await kafka.start()
    try:
        yield
    finally:
        stopping.set()
        await asyncio.gather(*partitions.tasks.values(), return_exceptions=True)
        await kafka.stop()


class PartitionTasks(ConsumerRebalanceListener):
    def __init__(self, kafka: AIOKafkaConsumer, handler: Handler, stopping: asyncio.Event) -> None:
        self.kafka = kafka
        self.handler = handler
        self.stopping = stopping
        self.tasks: dict[TopicPartition, asyncio.Task[None]] = {}

    async def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        # Cancelled rather than awaited: a handler stuck on a lock would otherwise hold up the
        # whole group's rebalance. Its transaction rolls back and the new owner redelivers the event
        tasks = [self.tasks.pop(partition) for partition in revoked if partition in self.tasks]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        for partition in assigned:
            self.tasks[partition] = asyncio.create_task(
                consume(self.kafka, self.handler, partition, self.stopping)
            )


async def consume(
    kafka: AIOKafkaConsumer,
    handler: Handler,
    partition: TopicPartition,
    stopping: asyncio.Event,
) -> None:
    while not stopping.is_set():
        try:
            async with asyncio.timeout(POLL_TIMEOUT_SECONDS):
                message = await kafka.getone(partition)
        except TimeoutError:
            continue
        await process(kafka, handler, message)


async def process(kafka: AIOKafkaConsumer, handler: Handler, message: ConsumerRecord) -> None:
    partition = TopicPartition(message.topic, message.partition)
    structlog.contextvars.clear_contextvars()
    try:
        assert message.value is not None
        event = from_json(message.value)
        structlog.contextvars.bind_contextvars(
            correlation_id=str(event.correlation_id), payload=event.payload
        )
        await handler(event)
    except Exception:
        logger.exception(
            "handler failed, event will be redelivered",
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        )
        await asyncio.sleep(RETRY_DELAY_SECONDS)
        if partition in kafka.assignment():
            kafka.seek(partition, message.offset)
        return

    try:
        await kafka.commit({partition: message.offset + 1})
    except KafkaError as error:
        logger.warning("offset commit failed", error=repr(error), offset=message.offset)
