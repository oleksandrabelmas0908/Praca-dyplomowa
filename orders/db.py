from decimal import Decimal

from sqlalchemy import Integer, Numeric, column, select, table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from orders.models import Order, OrderLine
from orders.schemas import OrderLineCreate
from shared.db import OutboxEvent
from shared.events import Event

products = table("products", column("id", Integer), column("price", Numeric(10, 2)))


async def get_prices(session: AsyncSession, product_ids: list[int]) -> dict[int, Decimal]:
    result = await session.execute(
        select(products.c.id, products.c.price).where(products.c.id.in_(product_ids))
    )
    return dict(result.tuples().all())


async def create_order(
    session: AsyncSession,
    customer_id: int,
    lines: list[OrderLineCreate],
    prices: dict[int, Decimal],
) -> Order:
    order = Order(
        customer_id=customer_id,
        total_amount=sum((line.quantity * prices[line.product_id] for line in lines), Decimal(0)),
        lines=[
            OrderLine(
                product_id=line.product_id,
                quantity=line.quantity,
                unit_price=prices[line.product_id],
            )
            for line in lines
        ],
    )
    session.add(order)
    await session.flush()
    event = Event(
        event_type="order.created",
        occurred_at=order.created_at,
        payload={
            "order_id": order.id,
            "customer_id": order.customer_id,
            "total_amount": order.total_amount,
            "lines": [
                {
                    "product_id": line.product_id,
                    "quantity": line.quantity,
                    "unit_price": line.unit_price,
                }
                for line in order.lines
            ],
        },
    )
    session.add(
        OutboxEvent(
            topic=event.event_type,
            message_key=str(order.id),
            payload=event.model_dump(mode="json"),
        )
    )
    await session.commit()
    return order


async def get_order(session: AsyncSession, order_id: int) -> Order | None:
    result = await session.execute(
        select(Order).where(Order.id == order_id).options(selectinload(Order.lines))
    )
    return result.scalar_one_or_none()
