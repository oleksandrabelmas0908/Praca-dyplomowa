import structlog
from sqlalchemy import Integer, column, func, select, table, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from inventory.models import Reservation, ReservationStatus
from inventory.schema import Rejection, RejectionReason

products = table(
    "products",
    column("id", Integer),
    column("stock_quantity", Integer),
    column("stock_reserved", Integer),
)

logger = structlog.get_logger()


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
            missing=quantity,
        )
    return Rejection(
        reason=RejectionReason.insufficient_stock,
        product_id=product_id,
        requested=quantity,
        available=available,
        missing=quantity - available,
    )


async def reserve_all(
    session: AsyncSession, order_id: int, lines: list[tuple[int, int]]
) -> Rejection | None:
    lines = sorted(lines)
    savepoint = await session.begin_nested()
    inserted = await session.scalar(
        insert(Reservation)
        .values(
            order_id=order_id,
            lines=[
                {"product_id": product_id, "quantity": quantity} for product_id, quantity in lines
            ],
            status=ReservationStatus.reserved,
        )
        .on_conflict_do_nothing()
        .returning(Reservation.order_id)
    )
    if inserted is None:
        await savepoint.commit()
        logger.debug("order already reserved")
        return None
    for product_id, quantity in lines:
        # Left bound if reserve raises, so the failure log names the product
        structlog.contextvars.bind_contextvars(product_id=product_id)
        rejection = await reserve(session, product_id, quantity)
        if rejection is not None:
            await savepoint.rollback()
            structlog.contextvars.unbind_contextvars("product_id")
            if rejection.reason is RejectionReason.product_not_found:
                logger.error("order rejected, unknown product", **rejection.model_dump())
            else:
                logger.info("order rejected", **rejection.model_dump())
            return rejection
    structlog.contextvars.unbind_contextvars("product_id")
    await savepoint.commit()
    logger.info("stock reserved", lines=len(lines))
    return None


async def release(session: AsyncSession, order_id: int) -> bool:
    lines = await session.scalar(
        update(Reservation)
        .where(Reservation.order_id == order_id, Reservation.status == ReservationStatus.reserved)
        .values(status=ReservationStatus.released, released_at=func.now())
        .returning(Reservation.lines)
    )
    if lines is None:
        logger.info("nothing to release")
        return False
    # Stored in product ID order, the same lock order as reserve_all
    for line in lines:
        await session.execute(
            update(products)
            .where(products.c.id == line["product_id"])
            .values(
                stock_quantity=products.c.stock_quantity + line["quantity"],
                stock_reserved=products.c.stock_reserved - line["quantity"],
            )
        )
    logger.info("reservation released", lines=len(lines))
    return True
