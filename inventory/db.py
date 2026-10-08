from enum import StrEnum

from sqlalchemy import Integer, column, select, table, update
from sqlalchemy.ext.asyncio import AsyncSession

products = table(
    "products",
    column("id", Integer),
    column("stock_quantity", Integer),
    column("stock_reserved", Integer),
)


class ReservationResult(StrEnum):
    success = "success"
    insufficient_stock = "insufficient_stock"
    product_not_found = "product_not_found"


async def reserve(session: AsyncSession, product_id: int, quantity: int) -> ReservationResult:
    reserved = await session.scalar(
        update(products)
        .where(products.c.id == product_id, products.c.stock_quantity >= quantity)
        .values(
            stock_quantity=products.c.stock_quantity - quantity,
            stock_reserved=products.c.stock_reserved + quantity,
        )
        .returning(products.c.id)
    )
    if reserved is not None:
        return ReservationResult.success
    exists = await session.scalar(select(products.c.id).where(products.c.id == product_id))
    if exists is None:
        return ReservationResult.product_not_found
    return ReservationResult.insufficient_stock


async def reserve_all(session: AsyncSession, lines: list[tuple[int, int]]) -> ReservationResult:
    savepoint = await session.begin_nested()
    for product_id, quantity in sorted(lines):
        result = await reserve(session, product_id, quantity)
        if result is not ReservationResult.success:
            await savepoint.rollback()
            return result
    await savepoint.commit()
    return ReservationResult.success
