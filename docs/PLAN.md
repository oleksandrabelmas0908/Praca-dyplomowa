# Plan

## Threadpool (TASK-7)

With `DB_DRIVER=sync`, FastAPI runs the plain `def` handlers in AnyIO's threadpool. Its size is
set per Uvicorn worker process by `THREADPOOL_SIZE`, which defaults to **40, AnyIO's own
default**. The E1 baseline keeps 40.

## Redis cache (TASK-8)

### Instance

`redis-cache`, Redis 8.10.2, `maxmemory 64mb`, `maxmemory-policy allkeys-lru`, no RDB snapshots
and no AOF. It is a separate instance from the future Celery broker, because a cache evicts under
memory pressure and a broker must never do that.

### Why 64 MB

One cached product takes about 1.8 kB in Redis, so 64 MB holds about **32,600 of the 100,000
seeded products, roughly one third**, when it holds nothing else (measured by requesting 50,000
distinct products: 32,620 keys remained, 17,368 evicted). List entries are much larger: 41 kB
for `limit=20` and 197 kB for `limit=100`. They compete for the same 64 MB, so fewer products
fit when lists are cached too.

Under sustained load Redis stays at the limit and evicts rather than growing. After 61,200
requests over 60,000 distinct products plus list pages, memory use was 67.0 MB of 67.1 MB and
50,152 keys had been evicted.

### Key scheme

| Key | Holds | Invalidation |
|---|---|---|
| `product:{id}` | one product, as served by `GET /catalog/products/{id}` | deleted by `PATCH /catalog/products/{id}`, otherwise TTL |
| `product_list:{sha256}` | one list response, as served by `GET /catalog/products` | TTL only |

The list hash covers `category`, `min_price`, `max_price`, `limit` and `offset` as a JSON object
with sorted keys. Unset filters are left out, and prices are normalised so that `100` and `100.00`
give the same key. The order of parameters in the query string therefore does not matter.

The value is the response JSON exactly as served, so a hit returns it without touching Pydantic.

### Expiry and staleness

Every write sets the TTL from `CACHE_TTL_SECONDS`. List keys are never invalidated: finding the
lists that contain an updated product would mean tracking list membership, which is not worth it
for a service whose writes are rare and whose experiment is about reads.

The staleness this allows:

- After a product is updated, a cached list can show its old values for up to
  `CACHE_TTL_SECONDS`.
- A single product is normally fresh on the next read. Its key can still be stale for up to
  `CACHE_TTL_SECONDS` if a read that missed before the update commits writes the old value back
  after the delete. This is the usual cache-aside race.
- `make seed` reloads the products table without touching Redis. Clear `redis-cache` or wait one
  TTL after reseeding.

### Switching the cache off

With `CACHE_ENABLED=false`, no Redis client is created, so there is no connection and no round
trip, and the handlers run the same database path as before the cache existed.

## Image worker (TASK-9)

### Two Redis instances

| | `redis-cache` | `redis-broker` |
|---|---|---|
| Holds | cached responses | queued Celery tasks |
| `maxmemory` | 64 MB | 128 MB |
| `maxmemory-policy` | `allkeys-lru` | `noeviction` |
| Persistence | none | append-only file, fsync every second, named volume |

The two need opposite behaviour when memory runs out. A cache that drops keys just takes a few
extra database reads. A broker that drops keys silently loses tasks, so the request that queued
them got a 202 for work that will never run. With `noeviction` a full broker refuses writes:
the publish raises `command not allowed when used memory > 'maxmemory'`, the upload returns 500,
and every task already queued stays. Measured by filling the broker to 128 MB: the publish was
refused, `evicted_keys` stayed 0, and the 7 queued tasks were all processed afterwards.

One queued image task takes about 1.2 kB, so 128 MB holds about 110,000 queued tasks. Redis does
not count its append-only file buffer against `maxmemory`, and rewriting that file forks the
process, so the container limit is 512 MB.

### Delivery guarantees

- `task_acks_late`: a task is acknowledged after it returns, not when it is received.
- Redis has no real acknowledgements. Kombu keeps every delivered message in the `unacked` hash
  and puts it back on the queue once it is older than the **visibility timeout of 120 s**.
  Kombu checks only every 100 s, so a task whose worker was killed comes back 120 to 230 s
  after delivery (measured: 192 s). It is not redelivered when the worker restarts.
- The visibility timeout must be longer than the longest task plus the longest retry
  countdown (32 s), or a task that is still running is delivered a second time. The task is
  idempotent, so a duplicate costs only CPU.
- `worker_prefetch_multiplier=1`: each process holds only the task it is running. A killed
  worker strands at most `CELERY_CONCURRENCY` tasks, and replicas added with `--scale` find the
  backlog still in Redis instead of buffered in the first worker.
- A prefork child that dies mid-task (for example OOM-killed) while its worker survives fails the
  task with `WorkerLostError`, which goes to the dead-letter queue. It is not requeued, because an
  image that kills one child would kill every child it was redelivered to.

### Retries and the dead-letter queue

Only `sqlalchemy.exc.OperationalError` (database unreachable) is retried: up to 5 times,
exponential backoff from 2 s capped at 2, 4, 8, 16, 32 s, with full jitter (each wait is random
between 0 and the cap). Any other error is permanent, for example an upload that claims
`image/jpeg` but is not an image.

A task that fails for good, after retries or straight away, is published again unchanged to the
`dead_letter` queue. No worker consumes it. `LLEN dead_letter` on `redis-broker` counts the dead
tasks, and `LMOVE dead_letter cpu RIGHT LEFT` replays one.

### Files

On the `images` volume, shared by `catalog` and `catalog-worker`:

| Path | Content |
|---|---|
| `/images/{product_id}/original` | the upload as received |
| `/images/{product_id}/{thumbnail,small,medium,large}.jpg` | longest side 150, 300, 600, 1200 px |
| `/images/{product_id}/original.webp` | the full-size image as WebP |

Each upload replaces `original`, and each run overwrites the five outputs and replaces
`products.image_paths` with their paths.

### Flower

`make obs` starts Flower on `127.0.0.1:${FLOWER_PORT}`; `make up` does not. Flower needs task
events and switches them on in every worker it finds (`Events of group {task} enabled by remote`
in the worker log). Each task then publishes received, started and succeeded or failed events to
`redis-broker`, broker traffic that a measured run should not include. Events stay on after
Flower stops, until the worker restarts, so before a measured run stop Flower and restart
`catalog-worker`.

Tasks still waiting in the queue are not in Flower's task list, because that needs `task-sent`
events from `catalog`, which are off. The broker page shows the queue length instead. The REST
API under `/api` answers 401 because no authentication is configured; the web UI does not use
it. The port is bound to localhost because Flower can shut down workers.
