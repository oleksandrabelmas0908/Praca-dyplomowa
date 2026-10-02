from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import ColumnElement, Numeric, func, literal, select
from sqlalchemy.orm import Session

from catalog.models import Product


def get_product(session: Session, product_id: int) -> Product | None:
    result = session.execute(select(Product).where(Product.id == product_id))
    return result.scalar_one_or_none()


def list_products(
    session: Session,
    category: str | None,
    min_price: Decimal | None,
    max_price: Decimal | None,
    limit: int,
    offset: int,
) -> tuple[Sequence[Product], int]:
    filters: list[ColumnElement[bool]] = []
    if category is not None:
        filters.append(Product.category == category)
    # Plain NUMERIC: bound with the column's NUMERIC(10, 2), asyncpg would round the value to two
    # decimals and fail above 10^8, while psycopg2 sends it unchanged
    if min_price is not None:
        filters.append(Product.price >= literal(min_price, Numeric()))
    if max_price is not None:
        filters.append(Product.price <= literal(max_price, Numeric()))

    products = session.execute(
        select(Product).where(*filters).order_by(Product.id).limit(limit).offset(offset)
    )
    total = session.execute(select(func.count()).select_from(Product).where(*filters))
    return products.scalars().all(), total.scalar_one()
