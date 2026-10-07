# psycopg2 copy of orders/db/async_db.py for DB_DRIVER=sync. The duplication is intentional and
# permanent: the two copies are what experiment E1 compares, so a change to one must be made to
# both
from decimal import Decimal

from sqlalchemy import Integer, Numeric, column, select, table
from sqlalchemy.orm import Session, selectinload

from orders.models import Order, OrderLine
from orders.schemas import OrderLineCreate
from shared.db import OutboxEvent
from shared.events import Event

products = table("products", column("id", Integer), column("price", Numeric(10, 2)))


def get_prices(session: Session, product_ids: list[int]) -> dict[int, Decimal]:
    result = session.execute(
        select(products.c.id, products.c.price).where(products.c.id.in_(product_ids))
    )
    return dict(result.tuples().all())


def create_order(
    session: Session,
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
