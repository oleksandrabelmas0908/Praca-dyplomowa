from fastapi import FastAPI

from shared import service_info

app = FastAPI()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", **service_info("orders")}
