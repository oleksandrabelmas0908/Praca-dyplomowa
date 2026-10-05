from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from catalog.models import Product


async def get_product(session: AsyncSession, product_id: int) -> Product | None:
    result = await session.execute(select(Product).where(Product.id == product_id))
    return result.scalar_one_or_none()


async def list_products(
    session: AsyncSession,
    category: str | None,
    min_price: Decimal | None,
    max_price: Decimal | None,
    limit: int,
    offset: int,
) -> tuple[Sequence[Product], int]:
    filters: list[ColumnElement[bool]] = []
    if category is not None:
        filters.append(Product.category == category)
    if min_price is not None:
        filters.append(Product.price >= min_price)
    if max_price is not None:
        filters.append(Product.price <= max_price)

    products = await session.execute(
        select(Product).where(*filters).order_by(Product.id).limit(limit).offset(offset)
    )
    total = await session.execute(select(func.count()).select_from(Product).where(*filters))
    return products.scalars().all(), total.scalar_one()


async def update_product(
    session: AsyncSession, product_id: int, values: dict[str, Any]
) -> Product | None:
    if not values:
        return await get_product(session, product_id)
    # RETURNING brings back updated_at, which Postgres sets, without a second query
    result = await session.execute(
        update(Product).where(Product.id == product_id).values(**values).returning(Product)
    )
    product = result.scalar_one_or_none()
    await session.commit()
    return product
