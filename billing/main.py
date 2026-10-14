from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from billing import events
from billing.models import Invoice, InvoiceStatus
from shared.db import database_ready, get_session
from shared.db import lifespan as db_lifespan
from shared.kafka import consumer
from shared.logging import setup_logging
from shared.metrics import CONTENT_TYPE, generate_metrics
from shared.middleware import CorrelationId
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with (
        db_lifespan(app),
        consumer("payment.completed", partial(events.handle, app.state.session_factory)),
    ):
        yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CorrelationId)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/ready")
async def ready(request: Request) -> JSONResponse:
    if await database_ready(request.app.state.engine):
        return JSONResponse({"status": "ready", "service": settings.service_name})
    return JSONResponse(
        {"status": "not ready", "service": settings.service_name, "failed": "database"},
        status_code=503,
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_metrics(), media_type=CONTENT_TYPE)


@app.get("/invoices/{order_id}/pdf")
async def get_invoice_pdf(
    order_id: int,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> FileResponse:
    invoice = await session.scalar(select(Invoice).where(Invoice.order_id == order_id))
    if invoice is None:
        raise HTTPException(status_code=404, detail=f"No invoice for order {order_id}")
    if invoice.status != InvoiceStatus.completed:
        raise HTTPException(
            status_code=409, detail=f"Invoice for order {order_id} is {invoice.status}"
        )
    return FileResponse(
        invoice.file_path, media_type="application/pdf", filename=f"{invoice.invoice_number}.pdf"
    )
