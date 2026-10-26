from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
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

if settings.transport == "http" and settings.db_driver == "sync":
    raise RuntimeError("TRANSPORT=http needs DB_DRIVER=async, the HTTP chain has no sync copy")

# httpx keeps only 20 idle connections by default, so under load most calls would open a new one
HTTP_LIMITS = httpx.Limits(max_connections=100, max_keepalive_connections=100)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with db_lifespan(app):
        if settings.transport == "http":
            async with httpx.AsyncClient(limits=HTTP_LIMITS) as client:
                app.state.http_client = client
                yield
            return
        # Async under both DB_DRIVER values: the flag compares the HTTP request path, which is what
        # the load generator drives, and these background loops are not part of that comparison
        async with (
            outbox.poller(app.state.session_factory, ["order.created"]),
            events.consumers(app.state.session_factory),
        ):
            yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CorrelationId)

orders_router = sync_routes.router if settings.db_driver == "sync" else async_routes.router
app.include_router(orders_router, tags=["orders"])
logger.info("database driver", driver=settings.db_driver)
logger.info("transport", transport=settings.transport)


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
