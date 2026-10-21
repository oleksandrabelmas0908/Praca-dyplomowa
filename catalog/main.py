from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from anyio import to_thread
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from catalog import cache
from catalog.routes import async_routes, sync_routes
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
    # Per worker process: each one has its own event loop and so its own threadpool
    to_thread.current_default_thread_limiter().total_tokens = settings.threadpool_size
    async with db_lifespan(app):
        # None when CACHE_ENABLED=false, so no Redis connection is ever made
        app.state.cache = await cache.connect() if settings.cache_enabled else None
        yield
        if app.state.cache is not None:
            await app.state.cache.aclose()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CorrelationId)

catalog_router = sync_routes.router if settings.db_driver == "sync" else async_routes.router
app.include_router(catalog_router, prefix="/catalog", tags=["catalog"])
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
