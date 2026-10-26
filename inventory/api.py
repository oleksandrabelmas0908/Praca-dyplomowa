from typing import Annotated

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from inventory.db import release, reserve_all
from inventory.schema import ReservationCreate
from shared.db import get_session

router = APIRouter()


@router.post("/reservations", status_code=201)
async def create_reservation(
    reservation: ReservationCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> JSONResponse:
    structlog.contextvars.bind_contextvars(order_id=reservation.order_id)
    lines = [(line.product_id, line.quantity) for line in reservation.lines]
    async with session.begin():
        rejection = await reserve_all(session, reservation.order_id, lines)
    if rejection is not None:
        return JSONResponse(rejection.model_dump(mode="json"), status_code=409)
    return JSONResponse({"order_id": reservation.order_id, "status": "reserved"}, status_code=201)


@router.post("/reservations/{order_id}/release")
async def release_reservation(
    order_id: int,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, int | bool]:
    structlog.contextvars.bind_contextvars(order_id=order_id)
    async with session.begin():
        released = await release(session, order_id)
    return {"order_id": order_id, "released": released}
