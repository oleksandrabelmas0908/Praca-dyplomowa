from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Enum, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class ReservationStatus(StrEnum):
    reserved = "reserved"
    released = "released"


class Reservation(Base):
    __tablename__ = "reservations"

    order_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    lines: Mapped[list[dict[str, int]]] = mapped_column(JSONB)
    status: Mapped[ReservationStatus] = mapped_column(
        Enum(ReservationStatus, name="reservation_status")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
