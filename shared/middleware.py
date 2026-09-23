import time
from uuid import UUID, uuid4

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

HEADER = "x-correlation-id"

logger = structlog.get_logger()


def read_correlation_id(scope: Scope) -> str:
    for name, value in scope["headers"]:
        if name.decode("latin-1").lower() == HEADER:
            try:
                # str(UUID(...)) so only a canonical UUID ever reaches the logs
                return str(UUID(value.decode("latin-1")))
            except ValueError:
                break
    return str(uuid4())


class CorrelationId:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        correlation_id = read_correlation_id(scope)
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        status = 500
        started = time.perf_counter()

        async def send_with_header(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [
                    *message.get("headers", []),
                    (HEADER.encode(), correlation_id.encode()),
                ]
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            logger.info(
                "request",
                method=scope["method"],
                path=scope["path"],
                status=status,
                duration_ms=round((time.perf_counter() - started) * 1000, 2),
            )
            structlog.contextvars.clear_contextvars()
