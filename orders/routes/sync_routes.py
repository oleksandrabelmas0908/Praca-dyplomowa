from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from orders.db import sync_db
from orders.schemas import OrderCreate, OrderResponse
from shared.db import get_sync_session

# Plain def handlers run in AnyIO's threadpool
router = APIRouter()


@router.post("/orders", status_code=201)
def create_order(
    order: OrderCreate,
    session: Annotated[Session, Depends(get_sync_session)],
) -> OrderResponse:
    product_ids = [line.product_id for line in order.lines]
    prices = sync_db.get_prices(session, product_ids)
    unknown = [product_id for product_id in product_ids if product_id not in prices]
    if unknown:
        raise HTTPException(
            status_code=422, detail=f"unknown product_id: {', '.join(map(str, unknown))}"
        )
    created = sync_db.create_order(session, order.customer_id, order.lines, prices)
    return OrderResponse.model_validate(created)


@router.get("/orders/{order_id}")
def get_order(
    order_id: int,
    session: Annotated[Session, Depends(get_sync_session)],
) -> OrderResponse:
    order = sync_db.get_order(session, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderResponse.model_validate(order)
