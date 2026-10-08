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

## Worker metrics (TASK-10)

### Where they come from

`shared/celery_metrics.py` collects everything through Celery signals; importing it registers
them. `before_task_publish` writes `published_at` (Unix time) into the task headers, `task_prerun`
observes the queue wait from it, `task_postrun` the duration and successes, `task_retry` and
`task_failure` the other two outcomes.

Prefork runs every task, and so every task signal, in a pool child, never in the parent. Each
child tries to bind port 9000 at `worker_process_init`; the first one serves `/metrics` from its own
registry and polls the queue depth, the others skip. **With `CELERY_CONCURRENCY=1` the metrics
cover every task of the container; above 1 they cover only that one child's tasks** (measured with
2: four uploads, two counted). Scaling therefore goes through replicas, each scraped on its own.
A task whose child is killed fails in the parent (`WorkerLostError`), so that failure is not
counted either.

The port is reachable on the `shop` network only (`catalog-worker:9000`), not from the host.
`catalog` passes it through at `GET /worker/metrics`, so it can be read at
`localhost:${CATALOG_PORT}/worker/metrics`. With several worker replicas, each request reaches
one of them.

### Buckets

| Histogram | Range | Why |
|---|---|---|
| `celery_task_duration_seconds` | 10 ms to 30 s | image processing takes hundreds of ms to seconds, the 600 px test image about 20 ms |
| `celery_task_queue_wait_seconds` | 5 ms to 300 s | near zero while idle, minutes under backlog; a task redelivered after its worker was killed waits 120 to 230 s |

### Queue wait caveats

- Publisher and worker are separate processes, so the wait is only right if their clocks agree.
  All containers run on one host and share its clock. If publisher and worker ever run on two
  machines, the wait includes the offset between their clocks.
- A retry is published again with its backoff countdown, so the wait of a retried run includes
  that countdown (measured: four retries, 10 s of countdown, 10.1 s of wait).

### Queue depth

A thread in the serving child reads `LLEN` of every queue the worker consumes plus `dead_letter`
from `redis-broker` every `QUEUE_DEPTH_INTERVAL_SECONDS` (default 5). Nothing serves the gauge
while the worker container is stopped. To watch a backlog build, stop consumption instead:
`celery -A catalog.tasks control cancel_consumer cpu`, then `add_consumer cpu`.

Measured with the worker stopped, five uploads 5 s apart, then the worker started: the
histogram's sum was 109.0 s against 109.1 s from the logs, with waits of 11.7 to 31.9 s each in
the matching bucket.

## Orders (TASK-11)

### Unit price from the products table

Each line in `POST /orders` has only `product_id` and `quantity`. Orders reads `price` for every
line from `products` and copies it into `order_lines.unit_price`, in the same transaction that
writes the order, so a later price change does not alter an order already placed. It reads the
table directly instead of calling catalog's API: every service shares one database, so this adds
one primary-key query to the write path and no second service. A `product_id` that is not in
`products` rejects the whole order with 422, and nothing is written.

## Outbox (TASK-12)

### Write path

`POST /orders` flushes the order to get its ID, then adds one `outbox_events` row in the same
transaction: topic `order.created`, `message_key` the order ID, `payload` the full event envelope
as JSONB. `occurred_at` in the envelope, `orders.created_at` and `outbox_events.created_at` are the
same value, Postgres `now()`, which is the start of the request's transaction (the price lookup).
The outbox lag therefore also includes the request's own few milliseconds in the database.

Measured: with a check constraint that rejects every outbox row, `POST /orders` returned 500 and
left no order, no order lines and no outbox row.

### Poller

Every Uvicorn worker process runs one poller, started in the lifespan next to the gauge and
cleanup loops. A pass is one transaction: claim up to 100 of the oldest undispatched rows with
`FOR UPDATE SKIP LOCKED`, hand them all to the producer in ID order, wait for every confirmation,
mark the confirmed ones with one `UPDATE`, commit. The row locks last until the commit, so a
second poller skips those rows instead of publishing them again. After a pass that claimed fewer
than 100 rows the poller sleeps `OUTBOX_POLL_INTERVAL_SECONDS` (0.1 s); after a full batch it
starts the next pass at once, so a backlog is not capped at 100 rows per interval.

`dispatched_at` comes from the orders container's clock after the confirmation, `created_at` from
Postgres. Both run on one host and share its clock, the same caveat as the Celery queue wait.

### Ordering

Within a pass rows are sent in ID order, and aiokafka keeps at most one request in flight per
partition, so a partition receives them in that order. If a send fails, the rest of the batch is
left for the next pass, so no later row overtakes it. The price is that a row Kafka never
accepts holds back every row behind it. It is never dropped, and the error log after 5 failures is
how it shows up.

Across pollers this is not guaranteed: two pollers could each claim a row with the same key and
publish them in either order. Orders writes exactly one row per order (`order.created`), so this
cannot happen today. It has to be revisited if orders ever emits a second event per order.

### Failures

- A failed publish leaves `dispatched_at` NULL and logs a warning with `outbox_event_id` and
  `correlation_id`; from the 5th failure of the same row it is an error. The count is kept in
  process memory, so a restart resets it and several workers split it between their pollers.
- A pass that claimed rows but published none, or that failed on the database, is followed by a
  backoff: poll interval × 2ⁿ, capped at 30 s. Kafka unreachable at startup gets the same backoff
  around starting the producer; the API is not affected.
- aiokafka fails a send only after `request_timeout_ms` (default 40 s), so while Kafka is down
  each pass takes about 40 s and holds its transaction, its row locks and one pool connection for
  that long. aiokafka also logs `Unable to update metadata` at error level every 100 ms while sends
  are pending.

Measured with Kafka stopped at 10:38:25: 31 orders were accepted (6 ms per `POST`), passes failed
every ~40 s, the oldest row's 5th failure at 10:41:49 was logged at error, and
`outbox_undispatched_events` stayed at 31. Kafka was started at 10:42:02; the 31 events were
published by 10:42:05, each exactly once, and the gauge went back to 0.

### Retention

Every 60 s each worker deletes dispatched rows older than `OUTBOX_RETENTION_SECONDS` (24 h),
1000 per transaction, with `SKIP LOCKED` so workers do not wait on each other. An undispatched row
has `dispatched_at` NULL and never matches the age condition. There is no index on
`dispatched_at`, so every cleanup pass is a sequential scan over the rows the window holds.

Measured with a 30 s window and Kafka stopped: one cleanup deleted 74,339 dispatched rows and kept
the 5 undispatched rows, which were 58 s old.

### Metrics

| Metric | Type | Content |
|---|---|---|
| `outbox_lag_seconds` | histogram | `created_at` to `dispatched_at`, buckets 5 ms to 600 s |
| `outbox_undispatched_events` | gauge | count of undispatched rows, every 5 s |
| `outbox_publish_attempts_total{outcome}` | counter | `success` / `failure`, one per row attempt |

With several Uvicorn workers each process has its own registry and `/metrics` answers from one of
them, as for every other metric.

### Lag under load

One Uvicorn worker, `POST /orders` at a constant rate for 20 s per step, lag read from the table,
local Docker:

| Orders/s | p50 | p95 | p99 | orders CPU |
|---|---|---|---|---|
| 20 | 62 ms | 111 ms | 118 ms | |
| 100 | 56 ms | 103 ms | 107 ms | |
| 400 | 56 ms | 102 ms | 112 ms | |
| 600 | 58 ms | 106 ms | 123 ms | 84 % |
| 800 | 67 ms | 128 ms | 144 ms | 85 % |
| 1000 | 74 ms | 136 ms | 157 ms | 98 % |

Below saturation the lag is the poll interval: a wait spread evenly over 0 to 100 ms plus a few
milliseconds to publish. It rises once the single CPU saturates, because the poller shares the
event loop with the request handlers.

With 4 workers at 300 orders/s (6000 orders) every order reached Kafka exactly once, with p50
47 ms, p95 95 ms, p99 101 ms.

## Status consumers (TASK-13)

### Consumers

The orders lifespan starts two consumers next to the outbox poller, both in consumer group
`orders`: `inventory.rejected` moves an order to `rejected`, `payment.completed` to `paid`. Each is
its own group member subscribed to one topic, so each gets all 3 partitions of its topic. Both
handlers are wrapped in `@idempotent`, so the `processed_events` insert, `SELECT ... FOR UPDATE` on
the order and the status update are one transaction, and the offset is committed after it.

The row lock makes the check and the update one step: two events for the same order, from the two
consumers or from two Uvicorn workers, are applied one after the other, and the second is checked
against the status the first left.

| Case | Order | Log | Acknowledged | Counted as |
|---|---|---|---|---|
| allowed by `can_transition` | changed | info | yes | `order_status_transitions_total{from_status, to_status}` |
| not allowed by `can_transition` | unchanged | warning | yes | `order_status_transitions_rejected_total{reason="illegal_transition"}` |
| order ID not in `orders` | none | error | yes | `{reason="unknown_order"}` |
| `event_id` already in `processed_events` | unchanged | debug | yes | `{reason="duplicate_event"}` |
| exception, e.g. Postgres unreachable | rolled back | error | no, retried after 1 s | not counted |

Both counters are incremented after the commit, so an event retried because its transaction
failed is counted once. Every line logged while handling carries the `correlation_id` from the
envelope, so it joins the `POST /orders` request line.

### Measured

Local Docker, events from the test harness:

- `inventory.rejected` for a pending order and `payment.completed` for a reserved one: both changed
  status about 3 ms after the publish.
- The same `event_id` published twice: one status change, one `processed_events` row,
  `duplicate_event` 1.
- `payment.completed` for a pending order, `inventory.rejected` for a paid one, `payment.completed`
  for order 999999: orders unchanged, two warnings and one error, consumer lag 0 afterwards.
- Orders SIGKILLed while its handler waited on a row lock held from psql: the transaction rolled
  back with no `processed_events` row and the offset was not committed. After `docker compose start
  orders` the event was redelivered and the order was `paid` 7 s later, with one row. Most of
  those 7 s is two group joins of about 3 s each, Kafka's `group.initial.rebalance.delay.ms`.
- The handler's connection terminated with `pg_terminate_backend` mid-transaction: one
  `handler failed`, then the retry changed the status, and the transitions counter rose by 1.

### Test harness (temporary)

`orders/fake_events.py` stands in for inventory and payments until they exist, and is deleted
then. It publishes `inventory.rejected` or `payment.completed` with payload `{"order_id": N}`, taking
the correlation ID from the order's `order.created` row in `outbox_events`, or a new one if there
is none. `--event-id` reuses an earlier event's ID to publish a duplicate.

```
docker compose -f infra/docker-compose.yml --project-directory . exec orders \
    python -m orders.fake_events payment.completed 42 [--event-id <uuid>]
```

## Stock (TASK-15)

### Ownership

Stock lives on `products` in two columns: `stock_quantity` is what is still available,
`stock_reserved` what orders have claimed. A reservation moves units from the first to the second,
so for every product `stock_quantity + stock_reserved` stays equal to its seeded quantity. After a
resilience run, `SELECT sum(stock_quantity + stock_reserved) FROM products` must equal the
`stock_total` that `make seed` logged.

Stock is owned by inventory; catalog does not serve it, so cached product entries are unaffected by
order activity. Neither column is in `ProductResponse`, no catalog endpoint filters or sorts on
them, and a reservation leaves `updated_at` alone because catalog serves that column. If stock were
in the cached payload, every reservation would leave the cached product stale for up to
`CACHE_TTL_SECONDS`, the value the caching experiment sweeps, and the two experiments would be
entangled.

### Seed

`make seed` gives every product 1 to 10 units (`MAX_STOCK`), then sets a share of products,
`SEED_ZERO_STOCK_FRACTION` (default 0.1), to zero, so orders for those are rejected at any load.
`stock_reserved` starts at 0. Both random draws happen for every product, so changing the fraction
only moves more or fewer products to zero: names, prices, descriptions and the other stock levels
stay the same (checked by seeding with 0.1 and 0.2).

With the defaults: 100,000 products, 10,032 at zero (10.03 %), 495,578 units in total, the same on
every run.

Nothing ever returns reserved stock (payments always succeeds, and a rejected order reserves
nothing), so stock only falls during a run and the rejection rate rises with it. Reseed before each
measured run.

### Reservation

In `inventory/db.py`. `reserve(product_id, quantity)` is one conditional statement:

```sql
UPDATE products
SET stock_quantity = stock_quantity - :quantity, stock_reserved = stock_reserved + :quantity
WHERE id = :id AND stock_quantity >= :quantity
RETURNING id
```

No row back means nothing was updated, and only then a second query checks whether the product
exists, to tell `product_not_found` from `insufficient_stock`. Under Postgres' default READ
COMMITTED, a second `UPDATE` of the same row waits for the first transaction to end and then
re-evaluates its `WHERE` against the committed row, so the check and the decrement are one step
without `SELECT ... FOR UPDATE`.

`reserve_all(lines)` reserves the lines in product ID order inside a savepoint. When a line fails it
rolls back to the savepoint, so none of the order's lines stay reserved, and returns that line's
result. It runs in the caller's transaction rather than its own, so the consumer can still commit
its `processed_events` row in the same transaction when it rejects an order. The savepoint costs
two extra statements per order (`SAVEPOINT`, `RELEASE`).

The ID order prevents deadlocks. Without it, two orders for the same two products listed in
opposite order can each lock one row and wait for the other's.

### Measured

Temporary scripts against a separate seeded database, local Docker, two runs with the same result:

- 20 transactions reserving 1 unit of a product with 1 left, released together by a barrier:
  exactly one success in each of 200 rounds, and in 50 more rounds where the winner held its lock
  for 50 ms before committing. With 3 left, exactly 3 successes in each of 100 rounds.
- An order whose last line by ID is short or unknown returned `insufficient_stock` or
  `product_not_found`, and its earlier lines were back at their previous values. A
  `processed_events` row inserted before it in the same transaction was committed.
- Two orders for the same two products in opposite line order, with a third transaction holding the
  first product so that both queue up: both succeeded. The same interleaving without the sort
  deadlocked, and Postgres aborted one of them after `deadlock_timeout` (1 s).
- 5,000 orders of 2 to 6 lines over the same 6 products in random line order, 20 at a time: no
  errors, `pg_stat_database.deadlocks` unchanged, totals exact.
- 5,000 random orders over 300 seeded products, 2 % with an unknown product: available + reserved
  equal to the seeded quantity for all 100,000 products, nothing negative, and the columns catalog
  serves, `updated_at` included, unchanged.

## Inventory (TASK-16)

### Handler

The inventory lifespan starts one consumer on `order.created` in consumer group `inventory`, next to
an outbox poller, as in orders. The handler is wrapped in `@idempotent`, so the `processed_events`
insert, `reserve_all` and the outcome's `outbox_events` row are one transaction, and the offset is
committed after it. A handler that raises leaves all three rolled back and the offset uncommitted,
and the shared consumer seeks back and redelivers the event 1 s later.

`reserve_all` now returns `None` when every line is reserved, or a `Rejection` with `reason`,
`product_id`, `requested` and `available` for the line that failed. It stops at the first failing
line in product ID order, so a rejection names one product even if later lines are short too. The
check after a failed `UPDATE` reads `stock_quantity` instead of `id`, so the shortfall costs no
extra query. That read sees a newer snapshot than the `UPDATE` did, but committed stock only
falls, so `available` is never above what the `UPDATE` saw and `missing` is at least 1.

### Events

Key is the order ID, `correlation_id` is copied from the `order.created` envelope, and
`occurred_at` is taken when the handler builds the event, right after `reserve_all`. That is about
a millisecond after the outbox row's `created_at`, which is the start of the transaction.

| Topic | Payload |
|---|---|
| `inventory.reserved` | `order_id` |
| `inventory.rejected` | `order_id`, `reason`, `product_id`, `requested`, `available`, `missing` |

`reason` is `insufficient_stock` or `product_not_found`. For an unknown product `available` is 0
and `missing` equals `requested`.

### Logs and metrics

| Case | Log | Counted as |
|---|---|---|
| every line reserved | info `stock reserved` | `reservations_total{outcome="reserved"}` |
| a line short | info `order rejected` | `{outcome="rejected"}` |
| a product ID not in `products` | error `order rejected, unknown product` | `{outcome="rejected"}` |
| `event_id` already in `processed_events` | debug | `reservation_duplicates_total` |
| exception | error `handler failed, event will be redelivered` | `{outcome="error"}`, once per failed attempt |

`reserved`, `rejected` and duplicates are counted after the commit, `error` before the redelivery.
`reservation_handler_duration_seconds` covers the whole wrapped handler, from the idempotency insert
to the commit, for every attempt including duplicates and failures, with buckets from 0.5 ms to 5 s.

Every line carries `correlation_id`, bound by the shared consumer, and `order_id`, bound before the
idempotency insert. `product_id` is bound while each line is reserved and stays bound if the
reservation raises, so the failure line names the product it was reserving.

### Outbox per service

`outbox_events` is one table for every service, so `outbox.poller` takes the topics its service
publishes and filters the claim, the undispatched gauge and the cleanup by them: orders passes
`order.created`, inventory `inventory.reserved` and `inventory.rejected`. Without the filter each
poller published the other service's rows as well, and the outbox metrics described whichever
process claimed a row. Inventory replicas still share their own rows through `SKIP LOCKED`, so one
replica's `outbox_publish_attempts_total` does not match its own `reservations_total`.

Inventory writes one row per order, like orders, so the cross-poller ordering caveat from TASK-12
still does not apply.

### Scaling

`INVENTORY_REPLICAS` sets the number of inventory containers, applied by `make up`. The host port is
picked by Docker, so the replicas do not collide; `docker compose port --index N inventory 8000`
finds replica N, and `make health` checks replica 1.
All replicas join group `inventory`, so with `KAFKA_PARTITIONS=3` at most 3 consume and any further
replica stays idle. A replica handles one event at a time.

### Measured

Local Docker, one replica unless stated:

- 3 units of a product with 9: stock 9/0 to 6/3 (available/reserved), `inventory.reserved` on Kafka
  with the order ID as key and the request's correlation ID. 6 of a product with 5: rejected with
  `missing` 1, stock unchanged. Three lines where the highest product ID has no stock: the two lower
  lines unchanged, the rejection names the third.
- `order.created` published by hand with an unknown product: `product_not_found`, an error log, the
  other line unchanged.
- An already handled `order.created` published twice more: `reservation_duplicates_total` 2, no
  second outbox row, stock unchanged.
- Handler blocked on a row lock held from psql, then its connection terminated with
  `pg_terminate_backend`: no `processed_events` row, no outbox row, and the line it had already
  reserved was back at its previous value. The failure log carried `order_id`, `correlation_id` and
  `product_id`. The event was redelivered 1 s later and reserved once after the lock was released.
- Inventory SIGKILLed while its handler waited on that lock: nothing written. After `docker compose
  start inventory` the event was processed 5 s later, once, with one `inventory.reserved` message.
- Kafka stopped while inventory worked through a backlog of 3000 `order.created`: it handled the 438
  it had already fetched, and 22 outcome rows stayed undispatched (gauge 22). Kafka was started 18 s
  later. All 3000 were processed and the outbox drained within 8 s, with exactly one outcome row and
  one Kafka message per order. aiokafka logged about 1900 connection errors while Kafka was down.
- 3 replicas, `POST /orders` at 300/s for 60 s (18,000 orders of 1 to 4 lines over 2000 products),
  replica 2 SIGKILLed after 25 s and started again 10 s later. Consumer lag was 0 afterwards. Over all
  21,000 orders since the seed: no product with available + reserved different from its seeded
  quantity, none negative, `stock_reserved` equal to the summed lines of the orders that got
  `inventory.reserved` for every product, and exactly one outcome row and one Kafka message per
  order. The handler averaged 2.1 to 2.4 ms per replica, 88 % of events between 1 and 3 ms.
- `docker compose down -v`, `make up`, `make seed`: an order with stock was reserved and one over
  stock rejected, with no manual steps.
