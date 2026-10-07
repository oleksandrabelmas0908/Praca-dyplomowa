from collections.abc import Awaitable, Callable

import structlog
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.db import ProcessedEvent
from shared.events import Event
from shared.settings import settings

logger = structlog.get_logger()


# Decorator for event handlers that ensures an event is processed only once. Returns the handler's
# result once the transaction has committed, or None if the event was already processed
def idempotent[T](
    handler: Callable[[AsyncSession, Event], Awaitable[T]],
) -> Callable[[async_sessionmaker[AsyncSession], Event], Awaitable[T | None]]:
    async def wrapper(session_factory: async_sessionmaker[AsyncSession], event: Event) -> T | None:
        async with session_factory() as session, session.begin():
            inserted = await session.scalar(
                insert(ProcessedEvent)
                .values(event_id=event.event_id, consumer_group=settings.service_name)
                .on_conflict_do_nothing()
                .returning(ProcessedEvent.event_id)
            )
            if inserted is None:
                logger.debug("event already processed", event_id=str(event.event_id))
                return None
            return await handler(session, event)

    return wrapper
