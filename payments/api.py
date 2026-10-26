from decimal import Decimal
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models import PaymentStatus
from payments.payment import call_provider, record_payment
from shared.db import get_session

router = APIRouter()


class PaymentCreate(BaseModel):
    order_id: int
    amount: Decimal


@router.post("/payments", status_code=201)
async def create_payment(
    payment: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, int | str]:
    structlog.contextvars.bind_contextvars(order_id=payment.order_id)
    await call_provider()
    async with session.begin():
        await record_payment(session, payment.order_id, payment.amount)
    return {"order_id": payment.order_id, "status": PaymentStatus.completed}
