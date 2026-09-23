from fastapi import FastAPI

from shared.logging import setup_logging
from shared.middleware import CorrelationId
from shared.settings import settings

setup_logging(settings.service_name, settings.log_level)

app = FastAPI()
app.add_middleware(CorrelationId)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}
