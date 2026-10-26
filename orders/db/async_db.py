from decimal import Decimal

from sqlalchemy import Integer, Numeric, Row, Text, column, select, table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from orders.models import Order, OrderLine
from orders.schemas import OrderLineCreate
from shared.db import OutboxEvent
from shared.events import Event
from shared.settings import settings

products = table(
    "products",
    column("id", Integer),
    column("name", Text),
    column("price", Numeric(10, 2)),
)


async def get_products(
    session: AsyncSession, product_ids: list[int]
) -> dict[int, Row[tuple[int, Decimal, str]]]:
    result = await session.execute(
        select(products.c.id, products.c.price, products.c.name).where(
            products.c.id.in_(product_ids)
        )
    )
    return {row.id: row for row in result}


async def create_order(
    session: AsyncSession,
    customer_id: int,
    lines: list[OrderLineCreate],
    products: dict[int, Row[tuple[int, Decimal, str]]],
) -> Order:
    order = Order(
        customer_id=customer_id,
        total_amount=sum(
            (line.quantity * products[line.product_id].price for line in lines), Decimal(0)
        ),
        lines=[
            OrderLine(
                product_id=line.product_id,
                quantity=line.quantity,
                unit_price=products[line.product_id].price,
            )
            for line in lines
        ],
    )
    session.add(order)
    await session.flush()
    if settings.transport == "http":
        await session.commit()
        return order
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
                    "product_name": products[line.product_id].name,
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
