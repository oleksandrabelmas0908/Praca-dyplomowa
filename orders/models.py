from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import DateTime, Enum, ForeignKey, Numeric, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.db import Base


class OrderStatus(StrEnum):
    pending = "pending"
    reserved = "reserved"
    paid = "paid"
    rejected = "rejected"


def can_transition(current: OrderStatus, new: OrderStatus) -> bool:
    return (current, new) in {
        (OrderStatus.pending, OrderStatus.reserved),
        (OrderStatus.pending, OrderStatus.rejected),
        (OrderStatus.reserved, OrderStatus.paid),
        (OrderStatus.reserved, OrderStatus.rejected),
    }


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[int]
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, name="order_status"), index=True, default=OrderStatus.pending
    )
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    lines: Mapped[list["OrderLine"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True
    )


class OrderLine(Base):
    __tablename__ = "order_lines"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), index=True)
    product_id: Mapped[int]
    quantity: Mapped[int]
    unit_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
