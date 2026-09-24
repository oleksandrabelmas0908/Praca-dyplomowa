from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import structlog
from pydantic import AwareDatetime, BaseModel, Field, field_validator


def current_correlation_id() -> UUID:
    correlation_id = structlog.contextvars.get_contextvars().get("correlation_id")
    if correlation_id is None:
        raise RuntimeError("no correlation_id in the logging context")
    return UUID(correlation_id)


class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID = Field(default_factory=current_correlation_id)
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


def to_json(event: Event) -> bytes:
    return event.model_dump_json().encode()


def from_json(data: bytes) -> Event:
    return Event.model_validate_json(data)
