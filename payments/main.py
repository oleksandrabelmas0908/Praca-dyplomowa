from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from shared.db import database_ready, lifespan
from shared.logging import setup_logging
from shared.metrics import CONTENT_TYPE, generate_metrics
from shared.middleware import CorrelationId
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)

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
