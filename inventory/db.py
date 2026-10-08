import structlog
from sqlalchemy import Integer, column, select, table, update
from sqlalchemy.ext.asyncio import AsyncSession

from inventory.schema import Rejection, RejectionReason

products = table(
    "products",
    column("id", Integer),
    column("stock_quantity", Integer),
    column("stock_reserved", Integer),
)


async def reserve(session: AsyncSession, product_id: int, quantity: int) -> Rejection | None:
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
        return None
    available = await session.scalar(
        select(products.c.stock_quantity).where(products.c.id == product_id)
    )
    if available is None:
        return Rejection(
            reason=RejectionReason.product_not_found,
            product_id=product_id,
            requested=quantity,
            available=0,
        )
    return Rejection(
        reason=RejectionReason.insufficient_stock,
        product_id=product_id,
        requested=quantity,
        available=available,
    )


async def reserve_all(session: AsyncSession, lines: list[tuple[int, int]]) -> Rejection | None:
    savepoint = await session.begin_nested()
    for product_id, quantity in sorted(lines):
        # Left bound if reserve raises, so the consumer's failure log names the product
        structlog.contextvars.bind_contextvars(product_id=product_id)
        rejection = await reserve(session, product_id, quantity)
        if rejection is not None:
            await savepoint.rollback()
            return rejection
    structlog.contextvars.unbind_contextvars("product_id")
    await savepoint.commit()
    return None
