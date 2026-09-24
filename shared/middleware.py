import time
from uuid import UUID, uuid4

import structlog
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from shared.metrics import (
    http_request_duration_seconds,
    http_requests_in_progress,
    http_requests_total,
)

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


def endpoint_label(scope: Scope) -> str:
    partial: str | None = None
    for route in scope["app"].routes:
        match, _ = route.matches(scope)
        if match == Match.FULL:
            return route.path
        if match == Match.PARTIAL and partial is None:
            partial = route.path
    return partial or "unmatched"


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

        method = scope["method"]
        endpoint = endpoint_label(scope)
        measured = endpoint != "/metrics"
        if measured:
            http_requests_in_progress.labels(method, endpoint).inc()

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
            duration = time.perf_counter() - started
            if measured:
                http_requests_in_progress.labels(method, endpoint).dec()
                http_request_duration_seconds.labels(method, endpoint).observe(duration)
                http_requests_total.labels(method, endpoint, status).inc()
            logger.info(
                "request",
                method=method,
                path=scope["path"],
                status=status,
                duration_ms=round(duration * 1000, 2),
            )
            structlog.contextvars.clear_contextvars()
