import asyncio
from datetime import datetime
from decimal import Decimal

import structlog
from sqlalchemy import Row
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models import Payment, PaymentStatus

PROVIDER_DELAY_SECONDS = 0.01

logger = structlog.get_logger()


async def call_provider() -> None:
    await asyncio.sleep(PROVIDER_DELAY_SECONDS)


async def record_payment(
    session: AsyncSession, order_id: int, amount: Decimal
) -> Row[tuple[int, datetime]] | None:
    payment = (
        await session.execute(
            insert(Payment)
            .values(order_id=order_id, amount=amount, status=PaymentStatus.completed)
            .on_conflict_do_nothing(index_elements=[Payment.order_id])
            .returning(Payment.id, Payment.created_at)
        )
    ).one_or_none()
    if payment is None:
        logger.debug("order already paid")
        return None
    logger.info("payment recorded", payment_id=payment.id)
    return payment
