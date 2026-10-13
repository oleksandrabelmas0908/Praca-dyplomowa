from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Numeric, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base


class Invoice(Base):
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(unique=True)
    invoice_number: Mapped[str] = mapped_column(Text, unique=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    file_path: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
