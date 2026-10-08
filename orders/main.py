from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from orders import events
from orders.routes import async_routes, sync_routes
from shared import outbox
from shared.db import database_ready
from shared.db import lifespan as db_lifespan
from shared.logging import setup_logging
from shared.metrics import CONTENT_TYPE, generate_metrics
from shared.middleware import CorrelationId
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Async under both DB_DRIVER values: the flag compares the HTTP request path, which is what the
    # load generator drives, and these background loops are not part of that comparison
    async with (
        db_lifespan(app),
        outbox.poller(app.state.session_factory, ["order.created"]),
        events.consumers(app.state.session_factory),
    ):
        yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CorrelationId)

orders_router = sync_routes.router if settings.db_driver == "sync" else async_routes.router
app.include_router(orders_router, tags=["orders"])
logger.info("database driver", driver=settings.db_driver)


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
