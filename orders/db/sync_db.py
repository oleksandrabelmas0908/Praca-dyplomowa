# psycopg2 copy of orders/db/async_db.py for DB_DRIVER=sync. The duplication is intentional and
# permanent: the two copies are what experiment E1 compares, so a change to one must be made to
# both
from decimal import Decimal

from sqlalchemy import Integer, Numeric, Row, Text, column, select, table
from sqlalchemy.orm import Session, selectinload

from orders.models import Order, OrderLine
from orders.schemas import OrderLineCreate
from shared.db import OutboxEvent
from shared.events import Event

products = table(
    "products",
    column("id", Integer),
    column("name", Text),
    column("price", Numeric(10, 2)),
)


def get_products(
    session: Session, product_ids: list[int]
) -> dict[int, Row[tuple[int, Decimal, str]]]:
    result = session.execute(
        select(products.c.id, products.c.price, products.c.name).where(
            products.c.id.in_(product_ids)
        )
    )
    return {row.id: row for row in result}


def create_order(
    session: Session,
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
    session.flush()
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
    session.commit()
    return order


def get_order(session: Session, order_id: int) -> Order | None:
    result = session.execute(
        select(Order).where(Order.id == order_id).options(selectinload(Order.lines))
    )
    return result.scalar_one_or_none()
