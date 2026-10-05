import functools
import hashlib
import inspect
import json
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import structlog
from anyio import from_thread
from fastapi import Response
from prometheus_client import Counter, Histogram
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from shared.settings import settings

BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
)  # fmt: skip

cache_hits_total = Counter("cache_hits_total", "Cache reads that found the key", ["cache"])
cache_misses_total = Counter(
    "cache_misses_total", "Cache reads that found nothing, failed reads included", ["cache"]
)
cache_operation_duration_seconds = Histogram(
    "cache_operation_duration_seconds",
    "Redis round trip",
    ["operation", "cache"],
    buckets=BUCKETS,
)

logger = structlog.get_logger()


def product_key(product_id: int) -> str:
    return f"product:{product_id}"


def list_key(
    category: str | None,
    min_price: Decimal | None,
    max_price: Decimal | None,
    limit: int,
    offset: int,
) -> str:
    query = {
        "category": category,
        # normalize() so that 100 and 100.00, the same filter, give the same key
        "min_price": None if min_price is None else format(min_price.normalize(), "f"),
        "max_price": None if max_price is None else format(max_price.normalize(), "f"),
        "limit": limit,
        "offset": offset,
    }
    normalised = json.dumps({k: v for k, v in query.items() if v is not None}, sort_keys=True)
    return f"product_list:{hashlib.sha256(normalised.encode()).hexdigest()}"


async def connect() -> Redis:
    # Blocking pool: with all 100 connections busy a request waits for one, where the default
    # pool fails with "Too many connections" and turns the request into a cache miss.
    # No retries: with Redis down, redis-py's default of 10 retries with backoff would add seconds
    # to every request instead of failing straight into a cache miss
    pool = BlockingConnectionPool.from_url(
        settings.redis_cache_url, max_connections=100, retry=Retry(NoBackoff(), 0)
    )
    client = Redis.from_pool(pool)
    try:
        await client.ping()
    except RedisError as error:
        logger.warning("cache unreachable at startup", error=repr(error))
    return client


async def get(client: Redis, key: str) -> bytes | str | None:
    cache = key.split(":", 1)[0]
    started = time.perf_counter()
    try:
        value: bytes | str | None = await client.get(key)
    except RedisError as error:
        logger.warning("cache read failed", key=key, error=repr(error))
        value = None
    cache_operation_duration_seconds.labels("get", cache).observe(time.perf_counter() - started)
    (cache_hits_total if value is not None else cache_misses_total).labels(cache).inc()
    return value


async def set(client: Redis, key: str, value: str) -> None:
    started = time.perf_counter()
    try:
        await client.set(key, value, ex=settings.cache_ttl_seconds)
    except RedisError as error:
        logger.warning("cache write failed", key=key, error=repr(error))
    cache_operation_duration_seconds.labels("set", key.split(":", 1)[0]).observe(
        time.perf_counter() - started
    )


async def delete(client: Redis, key: str) -> None:
    started = time.perf_counter()
    try:
        await client.delete(key)
    except RedisError as error:
        logger.warning("cache delete failed", key=key, error=repr(error))
    cache_operation_duration_seconds.labels("delete", key.split(":", 1)[0]).observe(
        time.perf_counter() - started
    )


Handler = Callable[..., Any]


def cached(key: Callable[..., str]) -> Callable[[Handler], Handler]:
    key_params = list(inspect.signature(key).parameters)

    def decorator(handler: Handler) -> Handler:
        def key_for(kwargs: dict[str, Any]) -> str:
            return key(**{name: kwargs[name] for name in key_params})

        if inspect.iscoroutinefunction(handler):

            @functools.wraps(handler)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                client: Redis | None = kwargs["request"].app.state.cache
                if client is None:
                    return await handler(*args, **kwargs)
                cache_key = key_for(kwargs)
                value = await get(client, cache_key)
                if value is not None:
                    return Response(value, media_type="application/json")
                body = (await handler(*args, **kwargs)).model_dump_json()
                await set(client, cache_key, body)
                return Response(body, media_type="application/json")

            return async_wrapper

        # A plain def wrapper, so FastAPI still runs sync handlers in its threadpool. The client
        # is async, so from_thread.run hands each call to the event loop and waits for it
        @functools.wraps(handler)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            client: Redis | None = kwargs["request"].app.state.cache
            if client is None:
                return handler(*args, **kwargs)
            cache_key = key_for(kwargs)
            value = from_thread.run(get, client, cache_key)
            if value is not None:
                return Response(value, media_type="application/json")
            body = handler(*args, **kwargs).model_dump_json()
            from_thread.run(set, client, cache_key, body)
            return Response(body, media_type="application/json")

        return sync_wrapper

    return decorator
