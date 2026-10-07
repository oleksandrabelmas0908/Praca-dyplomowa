from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from orders import db
from orders.schemas import OrderCreate, OrderResponse
from shared.db import get_session

router = APIRouter()


@router.post("/orders", status_code=201)
async def create_order(
    order: OrderCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> OrderResponse:
    product_ids = [line.product_id for line in order.lines]
    prices = await db.get_prices(session, product_ids)
    unknown = [product_id for product_id in product_ids if product_id not in prices]
    if unknown:
        raise HTTPException(
            status_code=422, detail=f"unknown product_id: {', '.join(map(str, unknown))}"
        )
    created = await db.create_order(session, order.customer_id, order.lines, prices)
    return OrderResponse.model_validate(created)


@router.get("/orders/{order_id}")
async def get_order(
    order_id: int,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> OrderResponse:
    order = await db.get_order(session, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderResponse.model_validate(order)
