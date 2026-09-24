from fastapi import FastAPI, Response

from shared.logging import setup_logging
from shared.metrics import CONTENT_TYPE, generate_metrics
from shared.middleware import CorrelationId
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)

app = FastAPI()
app.add_middleware(CorrelationId)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_metrics(), media_type=CONTENT_TYPE)
