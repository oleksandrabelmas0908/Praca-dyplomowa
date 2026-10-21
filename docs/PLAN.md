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
| `inventory.reserved` | `order_id`, `total_amount` (copied from `order.created`, since TASK-17), `customer_id` and `lines` (copied, since TASK-19) |
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
replica stays idle. A replica handles one event at a time per assigned partition (since TASK-17,
one at a time in total before).

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

## Payments (TASK-17)

### Simulated provider

Payments always succeed. `PROVIDER_DELAY_SECONDS` in `payments/events.py` stands in for the round
trip to a payment provider: **10 ms**, waited with `asyncio.sleep`. It is a constant, not a setting,
so every run uses the same value and every paid order's end-to-end latency includes it once.

The delay runs in `handle()` before the idempotent transaction opens. No pool connection or lock is
held while it runs; inside the transaction, one handler per partition would hold 3 of the 4 pool
connections of a replica for the whole delay. The payment's `created_at` (Postgres `now()`) is
therefore taken after the delay. The cost is that a redelivered event waits the delay again before
the idempotency check skips it.

### Throughput cap

Each partition's events are handled one at a time, so payments handles at most
`KAFKA_PARTITIONS` / handler duration events/s, however many replicas run: about 3 / 13 ms, roughly
225/s with 3 partitions (measured 201/s, see below). Above that the consumer lag on
`inventory.reserved` grows without bound, and end-to-end latency measures that queue rather than
the saga. About 70 % of orders get reserved, so 300 orders/s is about 210 payments/s, at the cap.
Measured runs that order faster need more partitions.

### Handler

The consumer group is `payments`. `on_inventory_reserved` is wrapped in `@idempotent`, so the
`processed_events` insert, the `payments` row and the `payment.completed` outbox row are one
transaction, and the offset is committed after it.

`payments.order_id` is unique, and the insert is `ON CONFLICT (order_id) DO NOTHING`. A second event
for an order already paid, under a different `event_id` that the idempotency check cannot catch,
commits only its `processed_events` row, logs `order already paid` at debug, and publishes nothing.

`inventory.reserved` now carries `total_amount`, copied from `order.created`. Payments stores it as
`amount` and publishes it.

| Topic | Key | Payload |
|---|---|---|
| `payment.completed` | order ID | `order_id`, `amount`, `customer_id` and `lines` (copied, since TASK-19) |

`occurred_at` is the payment's `created_at`, the same value as the outbox row's `created_at`.
`correlation_id` is copied from `inventory.reserved`.

### Orders reaches paid

Orders consumes only `inventory.rejected` and `payment.completed`, so no event moves an order to
`reserved`. `can_transition` now also allows `pending → paid`. `reserved` stays in the enum, but no
event sets it. The TASK-13 result "`payment.completed` for a pending order leaves it unchanged" no
longer holds.

### One task per partition

`shared/kafka.py` used to fetch and handle one message at a time per consumer. A rebalance listener
now starts one task per assigned partition and cancels those tasks when the partitions are revoked.
Within a partition, events are still handled one at a time in offset order, with the offset
committed after each one; partitions run concurrently. This applies to every consumer, so orders and
inventory also handle up to `KAFKA_PARTITIONS` events at once per replica.

On revoke, a partition's task is cancelled rather than awaited, because a handler blocked on a lock
would hold up the whole group's rebalance. Its transaction rolls back, and the partition's new owner
redelivers the event. On shutdown each task finishes the event it is handling.

### Logs and metrics

| Case | Log | Counted as |
|---|---|---|
| payment written | info `payment recorded` | `payments_total` |
| `event_id` already in `processed_events` | debug `event already processed` | `payment_duplicates_total` |
| order already has a payment | debug `order already paid` | `payment_duplicates_total` |
| exception | error `handler failed, event will be redelivered` | not counted |

`payment_handler_duration_seconds` covers the delay and the transaction for every attempt. Buckets
run from 10 ms to 5 s, in 0.5 ms steps from 10 to 13 ms. Every line carries `correlation_id`, bound
by the shared consumer, and `order_id`.

### Scaling

`PAYMENTS_REPLICAS` sets the number of payments containers, as for inventory, and
`docker compose up --scale payments=N` works too. `PAYMENTS_PORT` is gone: the host port is picked
by Docker, and `make health` checks replica 1.

### Measured

Local Docker, one replica unless stated:

- An order with stock went `pending` → `paid` through events alone. The payment amount equalled the
  order total, and `payment.completed` was on Kafka with the order ID as key, the request's
  correlation ID, and `occurred_at` equal to `payments.created_at`.
- The same `inventory.reserved` published twice more, and once with a new `event_id`: one payment,
  one `payment.completed`, `payment_duplicates_total` 3. With `LOG_LEVEL=DEBUG` both duplicate lines
  were logged at debug.
- Handler blocked on an uncommitted payment row for the same order held from psql, then its
  connection terminated with `pg_terminate_backend`: no payment, no `processed_events` row and no
  outbox row. The error log carried `order_id` and `correlation_id`. The event was redelivered 1 s
  later and paid once after the lock was released.
- Payments SIGKILLed while its handler waited on that lock: nothing written. After
  `docker compose start payments` the order was paid 5 s later, with one payment and one Kafka
  message.
- Kafka stopped while payments worked through a backlog of 3000 `inventory.reserved`: it handled the
  399 it had already fetched, and 24 outcome rows stayed undispatched (gauge 24). Kafka was started
  28 s later. All 3000 were processed and the outbox drained within 15 s, with exactly one payment,
  one outbox row and one Kafka message per order, and every amount equal to the published one.
- 600 events on one partition: 75/s, one at a time at 13.3 ms each. 1800 events over three
  partitions on the same single replica: 201/s, so several events were in their delay at once. It
  falls short of 3 × 75 because the run ends with the fullest partition.
- 3 replicas (`--scale payments=3`), `POST /orders` at 150/s for 60 s (9000 orders of 1 to 3 random
  products), replica 2 SIGKILLed after 25 s and started again 10 s later. Consumer lag was 0 and the
  outbox empty 1 s after the last request. 6439 orders were paid and 2561 rejected. Every reserved
  order had exactly one payment, with amount equal to the order total, no rejected order had one,
  and Kafka held exactly one `payment.completed` per paid order. Handler duration on replica 3: 81 %
  between 11.5 and 13 ms, 97 % at or under 14 ms.
- `docker compose down -v`, `make up`, `make seed`, `make health`: an order with stock reached
  `paid` and one over stock was `rejected`, with no manual steps.

## Invoices (TASK-19)

### Where billing gets the order

The invoice needs the order ID, the customer, every line with product, quantity and unit price,
and the total. Three ways to get them to billing were considered:

| Approach | Cost |
|---|---|
| billing calls `GET /orders/{id}` when `payment.completed` arrives | an HTTP call inside an event handler: billing fails or stalls whenever orders does, and the call adds orders' latency to every invoice |
| billing reads `orders` and `order_lines` directly | possible because every service shares one database, but billing would depend on another service's tables, and the product name is not stored there |
| **the events carry the lines** (chosen) | the payload grows by about 100 bytes per line, and inventory and payments copy fields they do not use |

The events carry them. `order.created` has the full line detail, inventory copies it into
`inventory.reserved`, payments copies it into `payment.completed`, and billing has everything it
needs in the one event it consumes. No handler makes a call to another service or reads another
service's rows.

The product name is read from `products` at order time, in the same query as the price, so it
costs no extra query. An invoice is a historical document and must show the product as it was
ordered, not as it is called after a later rename. This is the usual reason invoices embed data
instead of referencing it. The name is only in the event; `order_lines` does not store it and
`GET /orders` does not return it.

### Payloads

| Topic | Payload |
|---|---|
| `order.created` | `order_id`, `customer_id`, `total_amount`, `lines` |
| `inventory.reserved` | the same four, copied from `order.created` |
| `payment.completed` | `order_id`, `customer_id`, `amount` (equal to `total_amount`), `lines` |

Each line is `product_id`, `product_name`, `quantity`, `unit_price`. Prices are JSON strings with two
decimals, as everywhere else in the envelopes. Checked with both `DB_DRIVER=async` and `sync`: the
lines arrived in `payment.completed` unchanged.

Events published before this change have no `customer_id` or `lines`. Inventory and payments fail
on them and redeliver them forever, so start from a clean stack (`make down`, `make up`).

### Invoice table

`invoices`: `id`, `order_id` (unique, one invoice per order), `invoice_number` (unique), `amount`,
`file_path`, `status` (`pending`, `completed` or `failed`, see TASK-21), `error`, `created_at`. The model is
`billing/models.py`, the migrations `5b2e9a7c41d8_invoices` and `8d4f2a6c13e5_invoices_status`.

`invoice_number` is `INV-{YYYYMMDD}-{order_id, 6 digits}`, for example `INV-20261013-000042`, with
the date the row is first written in UTC (since TASK-20 by the consumer, before the PDF exists). It is readable and separate from the primary key, and it
needs no sequence: a sequence would skip a number every time a repeated run hit the unique
constraint. Numbers are not gapless, because rejected orders get no invoice.

### Rendering

`generate_invoice(order_data)` in `billing/invoice.py` takes the `payment.completed` payload and
returns the path of the PDF. `billing/invoice.html` is the template, filled with `string.Template`
(so no template library) and rendered by WeasyPrint. It contains the invoice number, issue date,
order ID, customer ID, one row per line with quantity, unit price and line total, and the order
total. The table header repeats on every page; the total is printed once, after the last line.
Product names are HTML-escaped.

The function opens one transaction:

1. `INSERT ... ON CONFLICT (order_id) DO UPDATE SET amount, file_path, status`, returning the invoice
   number and `created_at`. On a repeat run both are kept from the first run, so the PDF gets the
   same number and date.
2. Render the PDF and write it over the file.
3. Commit.

The upsert's row lock is held until the commit, so two runs for the same order write the file one
after the other. If rendering fails, the row rolls back with it. A crash after the write but before
the commit leaves a file with no row, which the retry then overwrites.

This makes the operation safe to repeat, which it has to be: Celery retries and Kafka redelivery
both mean it can run more than once for one order. Measured: two runs one after the other, then
two processes running it twice each at the same time, for one order: one row, one file, the
number and `created_at` from the first run, and a complete PDF.

### Files

The `invoices` volume is mounted at `/invoices` in `billing`:

| Path | Content |
|---|---|
| `/invoices/{order_id}.pdf` | the invoice of that order, overwritten on every run |

`billing-worker` mounts the same volume (since TASK-21). Reading the file from a second container (`docker compose run --rm --no-deps billing`) with the volume mounted
returned the same bytes.

### Cost

WeasyPrint needs Pango, HarfBuzz and a font, which the Dockerfile installs only when
`SERVICE=billing` (`libpango-1.0-0 libpangoft2-1.0-0 libharfbuzz-subset0 fonts-dejavu-core`). The
Python package is in `pyproject.toml` and so installed in every image, but only billing imports it.

Render time inside the `billing` container (1 CPU), median of 7 runs, local Docker:

| Lines | Wall | CPU | PDF |
|---|---|---|---|
| 1 | 24 ms | 24 ms | 9 kB |
| 5 | 32 ms | 32 ms | 10 kB |
| 10 | 39 ms | 39 ms | 10 kB |
| 50 | 128 ms | 128 ms | 16 kB |
| 100 | 251 ms | 251 ms | 24 kB |
| 200 | 465 ms | 463 ms | 39 kB |
| 500 | 1183 ms | 1175 ms | 84 kB |

CPU time equals wall time, so rendering is CPU-bound with no waiting. It grows linearly, at about
22 ms plus 2.3 ms per line. Orders of 1 to 4 lines therefore cost about 25 to 30 ms of CPU each,
so one CPU saturates at roughly 35 to 40 invoices per second.

## Billing consumer (TASK-20)

### Handler

The billing lifespan starts one consumer on `payment.completed` in consumer group `billing`. Billing
publishes no events, so it has no outbox poller. `on_payment_completed` is wrapped in `@idempotent`:
the `processed_events` insert and the invoice row are one transaction. The row is written as
`pending` with its invoice number and file path, `INSERT ... ON CONFLICT (order_id) DO NOTHING`, so
a second event for an order already invoiced, under a different `event_id`, commits only its
`processed_events` row and queues nothing. `generate_invoice` sets `completed` in its upsert (`generated` before TASK-21).

Only after the commit does `handle()` queue `generate_invoice_pdf` with `order_id`, `customer_id`,
`amount` and `lines` from the payload, and the event's correlation ID in the `x-correlation-id` task
header, as catalog does for images. Queued inside the transaction, a worker could start before the
row is visible, or a rollback could leave a task for an invoice that does not exist. The publish
runs in a thread (`asyncio.to_thread`), because kombu's Redis publish blocks, and while Redis is down
it retries for about a second, stalling the other partitions and the HTTP endpoints.

The task is `billing.tasks.generate_invoice_pdf` in `billing/tasks.py`, run by `billing-worker`
(TASK-21). One task takes about 3 kB of broker memory, so the 128 MB broker fills after roughly
40,000 unconsumed tasks.

`billing/tasks.py` imports `shared.celery_metrics`, so `before_task_publish` stamps `published_at`
on every task and the worker can report queue wait.

### Broker database

Billing publishes to `redis://redis-broker:6379/1`, queue `cpu`. catalog-worker consumes `cpu` on
db 0 and would take billing's tasks there, find no such task registered, and drop them. Separate db
numbers keep the queue name from the task table and need no second instance. Flower, `make obs`,
watches db 0 only.

### Lost task after the commit

The row commits before the task is queued, so if billing dies between the two, or redis-broker
refuses the publish, the event is already in `processed_events`. A redelivery is skipped as a
duplicate and the invoice stays `pending` with no task. Accepted: the window is the length of one
publish, and nothing recovers it. A failed publish is logged as error `invoice task not queued,
invoice stays pending` and not raised, so the offset is committed: redelivering would only be
skipped. `SELECT order_id FROM invoices WHERE status = 'pending'` finds such invoices once the
queue is empty.

### Logs and metrics

| Case | Log | Counted as |
|---|---|---|
| invoice row written and task queued | info `invoice requested` | `invoices_requested_total` |
| `event_id` already in `processed_events` | debug `event already processed` | `invoice_duplicates_total` |
| order already has an invoice | debug `order already invoiced` | `invoice_duplicates_total` |
| task publish failed after the commit | error `invoice task not queued, invoice stays pending` | not counted |
| exception before the commit | error `handler failed, event will be redelivered` | not counted |

`invoice_handler_duration_seconds` covers the transaction and the publish, for every attempt, with
the same buckets as inventory. Every line carries `correlation_id`, bound by the shared consumer,
and `order_id`.

### Measured

Local Docker, one billing replica, from `docker compose down -v` and `make up`:

- An order of 3 lines reached `paid`, its invoice was `pending` with number `INV-20261014-000001`,
  and one message was in `cpu` on db 1, none on db 0. The message carried task
  `billing.tasks.generate_invoice_pdf`, the order data with the lines, the request's correlation ID
  in `x-correlation-id`, and `published_at`. `generate_invoice` run by hand on it set `generated`
  and kept the number and `created_at`.
- The same `payment.completed` published twice more, and once with a new `event_id`: one invoice,
  one queued task, `invoice_duplicates_total` 3.
- Handler blocked on an uncommitted invoice row for the same order held from psql, then its
  connection terminated with `pg_terminate_backend`: no invoice, no `processed_events` row, no task.
  The error log carried `order_id` and `correlation_id`. The event was redelivered and handled once
  after the lock was released, with one task.
- Billing SIGKILLed while its handler waited on that lock: nothing written, nothing queued. After
  `docker compose start billing` the event was handled 2 s later, once, with one task.
- redis-broker stopped: the invoice was committed `pending`, the publish failed within the
  handler's 2.5 s bucket, the error was logged, the offset committed (lag 0) and no task queued.
- Handler duration per batch, 10 orders in flight, the saga running alongside:

| Orders | Lines each | Mean | ≤ 3 ms | ≤ 5 ms | ≤ 10 ms |
|---|---|---|---|---|---|
| 300 | 1 | 3.7 ms | 36 % | 88 % | 98 % |
| 200 | 50 | 4.0 ms | 20 % | 82 % | 99 % |
| 300 | 1 | 3.2 ms | 43 % | 94 % | 100 % |

The duration does not follow the line count, while rendering the same orders costs 24 ms (1 line)
and 128 ms (50 lines). Task duration in the worker's metrics: see TASK-21.

## Invoice worker (TASK-21)

### Worker

`billing-worker` is the billing image running `celery -A billing.tasks worker`, like
`catalog-worker`: prefork, `CELERY_CONCURRENCY` processes, queue `cpu` on redis-broker db 1, no
result backend, the `invoices` volume, 2 CPUs and 1 GB. More replicas:
`docker compose up --scale billing-worker=N`. The Celery settings are catalog's:
`task_acks_late`, `worker_prefetch_multiplier=1` (so added replicas find the backlog still in Redis)
and a visibility timeout of 120 s. The longest retry countdown is 32 s and a 500-line invoice
renders in about 1.2 s, so a running task is never delivered twice.

Broker check: `redis-cli config get maxmemory-policy` on redis-broker answers `noeviction`, with
`maxmemory` 128 MB (see TASK-9).

### Task

`generate_invoice_pdf(order_data)` binds `correlation_id` from the `x-correlation-id` header and
`order_id`, and calls `generate_invoice`, whose upsert sets `completed`, the file path and clears
`error`. A repeat run overwrites `/invoices/{order_id}.pdf`, so files never accumulate. billing now
queues it with `apply_async` instead of sending it by name.

Invoice status is `pending` (row written by the consumer), `completed` or `failed`. Migration
`2e7b9c4d5f18_invoices_failed` renames `generated` to `completed`, adds `failed` and the nullable
`error` column.

### Retries and failure

`sqlalchemy.exc.OperationalError` (database unreachable) and `OSError` (volume full, unwritable
path) are retried, up to 5 times, backoff from 2 s capped at 2, 4, 8, 16, 32 s with full jitter, as
for images. Celery keeps the task headers on a retry, so every retry logs the same correlation ID.

Any task that fails for good, after its retries or at once for any other error, sets the row to
`failed` with `repr()` of the exception in `error`, from a `task_failure` handler. This replaces the
dead-letter queue catalog has: `SELECT * FROM invoices WHERE status = 'failed'` lists every invoice
that never completed. `generate_invoice` rolls back with the failure, so nothing else is left
behind. Running the task again for a failed invoice overwrites both fields. If the database itself
is the cause, the `failed` update fails too and the row stays `pending`.

### Shared instrumentation

`billing/tasks.py` imports `shared.celery_metrics` and nothing in `shared/` changed. The worker
serves `celery_task_queue_wait_seconds`, `celery_task_duration_seconds`, `celery_tasks_total` and
`celery_queue_depth` on `billing-worker:9000/metrics`, reachable on the `shop` network only. Billing
has no endpoint passing it through like catalog's `GET /worker/metrics`. The TASK-10 caveat applies
as it is: above `CELERY_CONCURRENCY=1` one child's tasks are counted. The depth gauge also reports
`dead_letter`, always 0 here, because the shared code polls it for every worker.

WeasyPrint logs seven info lines per render (`Step 1 - Fetching and parsing HTML` …), each carrying
the correlation ID, so the worker writes about nine log lines per task.

### Measured

Local Docker, from `docker compose down -v` and `make up`, `CELERY_CONCURRENCY=1`:

- An order of 3 lines went to `paid` and its invoice to `completed` with `/invoices/1.pdf`, through
  events alone. The request's correlation ID was in the logs of orders, inventory, payments,
  billing and billing-worker.
- A directory at the invoice's file path: five retries after 1, 0, 2, 10 and 15 s, then `failed`
  with `IsADirectoryError(21, 'Is a directory')`, all under the request's correlation ID.
  `celery_tasks_total` counted 5 `retry` and 1 `failure`.
- Consumption cancelled (`celery -A billing.tasks control cancel_consumer cpu`), 20 orders: the
  depth gauge read 20, the invoices stayed `pending`, and `add_consumer cpu` drained them.
- Worker container stopped, 20 orders: 20 tasks stayed in Redis, all completed 2.2 s after
  `docker compose start billing-worker`, worker startup included.
- Worker SIGKILLed while rendering a 400-line invoice: the task stayed in `unacked`, the invoice
  `pending`. It was restored to the queue and completed 260 s after the kill, once, under its
  correlation ID: longer than the 120 to 230 s measured for images.
- A backlog of 1500 three-line invoices queued with consumption cancelled, then released, two runs
  each:

| Replicas | Drain time | Tasks/s |
|---|---|---|
| 1 | 65.3 s, 66.9 s | 23, 22 |
| 2 | 33.6 s, 34.7 s | 45, 43 |
| 3 | 23.8 s, 25.3 s | 63, 59 |

A task took 43 ms on average, against 3 to 4 ms for the billing handler that queued it.

## Prometheus (TASK-23)

### Container

`prometheus`, v3.15.0, behind the `obs` profile with Flower: `make obs` starts both, `make up`
neither. Configuration is `infra/prometheus/prometheus.yml` and `rules.yml`, mounted read-only;
the time series are in the `prometheus_data` volume. The UI listens on
`127.0.0.1:${PROMETHEUS_PORT}` only; on the server it is reached with
`ssh -L 9090:localhost:9090`.

### Scrape interval: 5 s

Every target is scraped every 5 s, and the rules are evaluated every 5 s. The events the
experiments look at, a container killed, lag building up, a worker stopped, last from seconds to
tens of seconds, and a 30 s rate window then still holds 6 samples. A 15 s interval would leave 2
per window, and a spike shorter than the interval would show up only smeared over the next one.

The cost is small. One scrape took 3 to 10 ms and returned 6 to 20 kB (catalog the largest),
about 830 series in total. For orders, on 1 CPU, that is roughly 0.15 % of its CPU. The longest
scrape seen was 0.6 s, from orders during the burst and Postgres-outage tests below. Going to 1 s
would multiply the cost by five and add no resolution the 30 s rate windows can use.

The scrape timeout is 3 s. It has to be below the interval, and it is far above any scrape seen
so far, so a timeout means the target is stuck, and it reports `up` 0.

### Retention: 30 days

`--storage.tsdb.retention.time=30d`. About 830 series at one sample per 5 s come to roughly
20 MB a day, before the exporters of TASK-24 add theirs. 30 days covers re-analysing a whole
benchmark campaign. But `make down` runs `down -v` and deletes `prometheus_data` along with every
other volume. In practice the data lives from one `make up` to the next `make down`, so a run's
series have to be exported before then.

### Targets

| Job | Discovery | Port | Instance |
|---|---|---|---|
| `catalog`, `orders`, `billing` | static | 8000 | `catalog:8000` |
| `inventory`, `payments` | DNS, type A | 8000 | `<container IP>:8000` |
| `catalog-worker`, `billing-worker` | DNS, type A | 9000 | `<container IP>:9000` |

Compose DNS returns one address per container of a scaled service, and DNS discovery turns each
into its own target, refreshed every 5 s. A static `inventory:8000` would reach one random replica
per scrape. The three single-container services stay static, because a static target that stops
is still listed and reports `up` 0. A stopped replica, by contrast, drops out of DNS, and its target
disappears instead of going down. The IP changes when a container is recreated, so the instance
label is stable only for the container's lifetime.

`catalog`'s `GET /worker/metrics` is not scraped, since it would count the worker twice. The
earlier caveats still hold: with `UVICORN_WORKERS` above 1 a scrape reaches one process, and with
`CELERY_CONCURRENCY` above 1 one pool child.

### Recording rules

| Rule | Content |
|---|---|
| `service_endpoint:http_requests:rate30s` | requests/s by `service`, `method`, `endpoint` |
| `service:http_requests_errors:rate30s` | 5xx responses/s by `service`, 0 for a service without one |
| `service_endpoint:http_request_duration_seconds:p50_30s`, `p95_30s`, `p99_30s` | latency quantiles by `service`, `method`, `endpoint` |
| `service_task:celery_task_queue_wait_seconds:p50_30s`, `p95_30s`, `p99_30s` | queue wait quantiles by `service`, `task`, `queue` |

Every rule sums over the replicas of a service. The 30 s window holds 6 scrapes, so one missed
scrape still leaves a rate. The quantiles are interpolated between bucket boundaries, so they are
only as fine as the buckets, and they are NaN while an endpoint or task has no traffic.

A labelled series appears only once a service handles its first request with those labels, so
whatever happens before that series' first scrape is invisible to `rate()`. 300 orders sent in
0.7 s into a fresh stack peaked at 0.55 requests/s. The warm-up before each measured run creates
the series.

### Measured

Local Docker, `make down` then `make obs`:

- 7 targets, all `up`. `--scale inventory=3` made 9, each inventory replica with its own
  instance, all `up` within 12 s. The two runs below used the 3 replicas.
- 60 s of 17 orders/s and 42 product reads/s from the client: the rules recorded 16.7 and 41.1
  requests/s, p50 2.6 ms and p99 9.4 ms for `POST /orders`, a billing-worker queue wait p95 of
  8 ms. Reservations ran at 6.0, 5.5 and 5.7/s on the three replicas, payments at 15.5/s (the
  rest rejected for zero stock).
- Postgres stopped, 40 `POST /orders` answered 500: the error rule read 1.04/s for orders and 0
  for every other service.
- After `make down` and `make obs`: all targets `up`, all 8 rules `ok`, retention 30d, interval
  5 s.

## Exporters (TASK-24)

### Containers

| Container | Version | Job | Instance |
|---|---|---|---|
| `cadvisor` | v0.60.6, from `ghcr.io/google/cadvisor` (`gcr.io` stops at v0.52) | `cadvisor` | `cadvisor:8080` |
| `postgres-exporter` | v0.20.1 | `postgres` | `postgres-exporter:9187` |
| `redis-exporter` | v1.94.0, `-alpine` because the default image has no `wget` for the healthcheck | `redis` | `redis-cache`, `redis-broker` |
| `kafka-exporter` | v1.10.0 | `kafka` | `kafka-exporter:9308` |

All four are behind the `obs` profile, started by `make obs` and not published to the host.

### cAdvisor

| Flag | Why |
|---|---|
| `--docker_only=true` | containers plus the root cgroup, no other system cgroups |
| `--housekeeping_interval=5s` | the scrape interval; collecting more often gives nothing Prometheus sees |
| `--store_container_labels=false`, `--whitelisted_container_labels=com.docker.compose.service` | only the Compose service as label `container_label_com_docker_compose_service`, which groups the replicas of a service; `name` tells them apart |

The Docker and containerd sockets are mounted one by one instead of `/var/run`. With Docker 29's
containerd image store, cAdvisor's Docker support needs both. Docker Desktop does not pass the
VM's `/var/run` through a directory mount. The paths are the same on Linux.

Limits sit next to usage: `container_spec_cpu_quota / container_spec_cpu_period` is `cpus` and
`container_spec_memory_limit_bytes` is `mem_limit`, and both matched the Compose file for every
container.

```
sum by (container_label_com_docker_compose_service) (rate(container_cpu_usage_seconds_total{container_label_com_docker_compose_service!=""}[30s]))
sum by (container_label_com_docker_compose_service) (container_memory_working_set_bytes{container_label_com_docker_compose_service!=""})
```

The series without a service label are the root cgroup: the whole host (locally, the Docker
Desktop VM) and its network interfaces.

### postgres_exporter

The exporter connects as `exporter`, a login role with `pg_monitor`. That role reads every
statistics view and setting but no table (`has_table_privilege('exporter', 'products', 'SELECT')`
is false). `infra/postgres/create-exporter-role.sh` creates it from `/docker-entrypoint-initdb.d`,
which only runs on an empty volume, so a volume from before this change has no role until
`make down`. The script is executable because the entrypoint executes it: on Docker Desktop it
counts as executable regardless of its mode, and sourcing it there failed.

| Signal | Metric |
|---|---|
| connections | `pg_stat_database_numbackends`, `pg_stat_activity_count{state}` |
| transactions | `rate(pg_stat_database_xact_commit[30s])`, `xact_rollback` |
| lock waits | `pg_backends_waiting_for_lock` |
| locks held | `pg_locks_count{mode}`, `pg_stat_database_deadlocks` |
| size | `pg_database_size_bytes` |

None of the built-in collectors counts sessions waiting for a lock. `pg_locks_count` counts locks
held, and it leaves out transaction ID locks, which are what a row lock waits on. So
`infra/postgres/queries.yml` adds one query that counts the sessions with
`wait_event_type = 'Lock'`. It goes through the extended queries file, which v0.20.1 marks as
deprecated in its startup log but still runs. The other startup warning, about a missing
`postgres_exporter.yml`, refers to auth modules, which are not used. The gauge is a snapshot at
each scrape, so a wait that starts and ends between two scrapes is not seen.

Every scrape runs the exporter's queries on the measured database: about 820 samples, 150 ms on
average. One scrape timed out (3 s, `up` 0) while `make seed` was replacing the catalogue, because
the seed's lock on `products` blocks the per-table size queries.

### redis_exporter

The exporter has no Redis address of its own. Prometheus scrapes `/scrape?target=redis://…`, once
per instance, and relabels `instance` to `redis-cache` or `redis-broker`.

| Signal | Metric |
|---|---|
| memory | `redis_memory_used_bytes` against `redis_memory_max_bytes` |
| evictions | `redis_evicted_keys_total` |
| clients | `redis_connected_clients` |
| keyspace | `redis_db_keys{db}` |

On the broker `redis_evicted_keys_total` has to stay 0, and with `noeviction` it can only change if
the policy does. A full broker refuses new tasks instead, which shows as `redis_memory_used_bytes`
reaching `redis_memory_max_bytes`.

### kafka-exporter

| Signal | Metric |
|---|---|
| consumer lag | `kafka_consumergroup_lag{consumergroup, topic, partition}` |
| consumers | `kafka_consumergroup_members{consumergroup}` |
| throughput | `rate(kafka_topic_partition_current_offset[30s])`, messages produced per partition |
| partitions | `kafka_topic_partitions{topic}` |

A group's lag appears once it has committed an offset on the partition, and it stays reported
while every consumer of the group is stopped. Under load the lag of a healthy consumer reads 0
to 2: messages in flight at the scrape. `__consumer_offsets` (50 partitions) is included.
The exporter exits at startup if the broker does not answer, so it waits for Kafka to be healthy.

### Cost

At idle, local Docker:

| Container | CPU | Memory | Scrape avg / max | Samples |
|---|---|---|---|---|
| `cadvisor` | 0.09 | 168 MB | 96 / 520 ms | 3232 |
| `postgres-exporter` | 0.01 | 29 MB | 156 ms / timeout once (seed) | 823 |
| `redis-exporter` | 0.01 | 14 MB | 49 / 256 ms, both targets | 1356 |
| `kafka-exporter` | 0.01 | 17 MB | 136 / 515 ms | 534 |

cAdvisor used 0.34 CPU in its first minute. Prometheus now holds about 7,000 series, roughly
180 MB a day at 5 s, against about 830 before the exporters.

### Measured

Local Docker, `make down` then `make obs`, one replica each:

- 12 targets `up`: the 7 application targets, `cadvisor`, `postgres`, `kafka`, and `redis` twice.
- 150 s of 17 orders/s and 42 product reads/s, inventory stopped at 30 s and started at 77 s.
  Lag of group `inventory` on `order.created`, every 10 s:

| | 30 s | 40 s | 60 s | 80 s | 90 s | 100 s | 110 s |
|---|---|---|---|---|---|---|---|
| sum of 3 partitions | 2 | 138 | 460 | 828 | 633 | 37 | 1 |

  Each partition grew alike (268 to 290 at 80 s). Production on `order.created` held at 17/s
  throughout. Once inventory caught up, `inventory.reserved` peaked at 49 messages/s, and the
  lag of `payments` on it at 136, both back to normal by 120 s.
- Over the run the broker processed 18,400 commands with 0 evictions; the cache had 0 too. Up to
  47 Postgres connections and 270 commits/s.
- A row held with `FOR UPDATE` in one session and updated from a second:
  `pg_backends_waiting_for_lock` read 1. It read 0 throughout the load run.

## Grafana (TASK-25)

### Container

`grafana`, 13.2.4, behind the `obs` profile, on `127.0.0.1:${GRAFANA_PORT}` only (on the server
`ssh -L 3000:localhost:3000`). It logs in as `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`, and
has 1 CPU and 512 MB. It has no volume: the datasource and dashboards come from `infra/grafana`,
and nothing else Grafana stores is kept, so every `make down` gives back exactly the committed
state.

| File | Content |
|---|---|
| `provisioning/datasources/prometheus.yml` | Prometheus, uid `prometheus`, `timeInterval` 5 s (the scrape interval) |
| `provisioning/dashboards/dashboards.yml` | loads every JSON in `infra/grafana/dashboards`, `allowUiUpdates: false` |
| `dashboards/red.json`, `kafka.json`, `celery.json`, `resources.json` | the four dashboards |

Saving a dashboard from the UI fails with `Cannot save provisioned dashboard`. A change to a JSON
file shows up within about 10 s without a restart. Every panel uses fixed 30 s windows like the
recording rules, so one graph means the same thing at every zoom level.

### Time range and refresh

Every dashboard opens on the last 15 minutes and refreshes every 10 s. 15 minutes holds the warm-up
and one measured run; a whole campaign is a wider range picked by hand. 10 s is two scrapes, so each
refresh has new data, at half the query load of refreshing at the scrape interval. Grafana only
queries while a dashboard is open in a browser, so close it during a measured run.

### Dashboards

| Dashboard | Panels |
|---|---|
| RED per service | requests/s, 5xx/s, p50, p95, p99 by endpoint, from the TASK-23 rules (5xx from `http_requests_total`, 0 where there are none); event handlers: events/s, errors/s, latency p50/p95/p99 |
| Kafka | lag by group and topic, lag by partition, produced/s by topic, consumed/s by group and topic, group members, partitions by topic |
| Celery queues | depth per queue, queue wait p50/p95/p99, task duration p50/p95/p99, outcomes/s |
| Resources | CPU and memory per container as a share of their limits and absolute; Postgres connections by state against `max_connections`, commits and rollbacks/s; Redis memory against `maxmemory`, evictions/s and in the time range |

The RED dashboard and the Resources dashboard have a multi-select variable, Service and Container,
defaulting to All. `/health` and `/ready` are left out of the HTTP panels.

inventory and payments serve no HTTP besides health checks; their work is events. So the RED
dashboard has a row for the handlers, from the existing metrics:

| | inventory | payments | billing | orders |
|---|---|---|---|---|
| rate | `reservation_handler_duration_seconds_count` | `payment_handler_duration_seconds_count` | `invoice_handler_duration_seconds_count` | `order_status_transitions_total` |
| errors | `reservations_total{outcome="error"}` | not counted | not counted | not counted |
| latency | the same histograms | | | no histogram |

The handler rates count every attempt, including duplicates and failures; the orders rate counts
the status changes applied. payments and billing log a failed handler without a metric, so the
errors panel covers inventory only.

Consumed messages/s is the rate of the committed offset, which matches consumption because every
consumer commits after each event.

### Queue depth from the broker

The Celery dashboard reads queue depth from `redis_key_size`, not from the workers'
`celery_queue_depth`. That gauge is served by the worker, so it stops while the worker is down,
which is exactly when the backlog builds. redis-exporter now runs with
`--check-single-keys=db0=cpu,db0=dead_letter,db1=cpu`: `LLEN` of catalog's `cpu` and
`dead_letter` on db 0 and of billing's `cpu` on db 1, on every scrape, and 0 for an empty queue.
It checks the same keys on redis-cache too, where they never exist, so the panel filters on
`instance="redis-broker"`. If a queue name or db changes, this flag has to change with it.

Task counts, durations and waits still come from the workers, with the TASK-10 caveat: with
`CELERY_CONCURRENCY=5`, 2 of 10 image tasks were counted. A worker's first task series is created by
that task, so `rate()` does not see it, as for the HTTP series (see TASK-23).

### Measured

Local Docker, `make down` then `make obs`, `CELERY_CONCURRENCY=5`:

- The datasource and the four dashboards were there with no manual step, and the datasource
  health check passed. Every panel query, run through Grafana's `/api/ds/query`, returned data
  without errors. The exceptions were catalog-worker's waits and durations, for the reason above.
- 160 s of 17 orders/s and 42 product reads/s, inventory stopped from 30 to 75 s, billing-worker
  from 80 to 120 s, 10 image uploads at 95 s. Lag of `inventory` on `order.created` peaked at 865,
  billing's queue at 1,780, its queue wait p95 at 87 s.
- billing-worker was at 100 % of its 2 CPUs from 30 s on: billing's queue had already grown to 202
  before the worker was stopped. At this load and concurrency, invoice rendering is the
  bottleneck, and the Resources dashboard shows it at its CPU limit.
- After `make down` and `make obs` the four dashboards returned by the API were identical to
  those before, apart from `id` and `version`.

## Logs (TASK-26)

### Containers

| Container | Version | Role |
|---|---|---|
| `loki` | 3.7.9, built from `infra/loki/Dockerfile` | stores logs on the `loki_data` volume, not published to the host |
| `alloy` | v1.20.1 | reads every container's log through the Docker API and pushes it to Loki |

Both are behind the `obs` profile, and `make obs` builds the Loki image before starting them. The
Loki image is distroless, with no shell or HTTP client, so the Dockerfile copies a static busybox
(1.36.1-musl) to `/busybox` for the healthcheck's `wget`. Alloy's image has bash but no HTTP
client, so its healthcheck sends `GET /-/ready` through bash's `/dev/tcp`.

Containers keep Docker's `json-file` logging driver, so `docker compose logs` works as before. Alloy
mounts the Docker socket read-only, since it only lists containers and reads their logs; a
writable socket is root on the host. The `alloy_data` volume holds how far each log has been read.
Without it a recreated Alloy sent every log from the start again, doubling the stored lines
(17,520 to 32,925). With it, a recreate sent nothing twice.

### Collection

`infra/alloy/config.alloy`: `discovery.docker` lists the containers with label
`com.docker.compose.project` equal to the project name every 5 s, so a replica added with
`--scale` is picked up within 5 s and other projects on the same Docker are left out. The project
name reaches Alloy as `COMPOSE_PROJECT`, from Compose's `COMPOSE_PROJECT_NAME`. `cadvisor` and the
`*-exporter` containers are dropped. Everything else is collected, the observability containers
included.

### Labels and fields

| Where | Fields |
|---|---|
| labels (one stream per combination) | `service` (Compose service), `container` (container name, e.g. `pracadyplomowa-inventory-2`) |
| structured metadata, parsed by Alloy | `level`, `correlation_id`, `order_id` |
| line, unchanged | the whole JSON, every other field |

The correlation ID and order ID are one value per request, so as labels they would make one stream
per request. As structured metadata they are stored next to each line and filtered without
parsing: `{service=~".+"} | correlation_id="…"` and `| level="error"` need no `| json`. Other
fields (`event`, `status`, `duration_ms`, `product_id` …) are read with `| json` at query time.
Lines that are not JSON (Postgres, Celery's start and stop banners) are kept without the three
fields.

Loki 3 would add a `service_name` label and a `detected_level` field by itself. Both are switched
off (`discover_service_name: []`, `discover_log_levels: false`), so the labels are exactly the two
above. When Loki's API returns a stream, it lists structured metadata next to the labels;
`/loki/api/v1/labels` answers `container` and `service` only.

### Retention: 7 days

`retention_period: 168h`, applied by the compactor (`retention_enabled: true`). Logs answer what
went wrong in a run, which is asked soon after the run; the results come from Prometheus, kept for
30 days. At 17 orders/s and 42 product reads/s Loki received 245 lines/s, 79 KiB/s before
compression, about 6.6 GB a day under continuous load. As for Prometheus, `make down` deletes
`loki_data`, so in practice the logs last until the next `make down`.

Loki's default per-tenant limit is 4 MB/s, which at 331 bytes per line is about 12,000 lines/s.
Above that Loki rejects pushes, Alloy retries, and lines arrive late or are dropped.

### Where the lines come from

Lines in 65 s of that load: billing-worker 8,842, catalog 2,470, orders 1,981, inventory 1,022,
payments 897, billing 896. billing-worker writes about 9 lines per invoice, 7 of them WeasyPrint's
render steps (see TASK-21). Every service also logs a `request` line for each Prometheus scrape of
`/metrics`, one per service every 5 s.

### Trace in Grafana

Loki is provisioned as a second datasource (uid `loki`). The `Order trace` dashboard has a
Correlation ID text box and one logs panel, oldest line first, over the last hour:

```
{service=~".+"} | correlation_id!="" | correlation_id="$correlation_id"
```

The `!= ""` makes an empty box match nothing instead of every line without an ID. The dashboard does
not refresh on its own, because a trace does not change once the order has finished.

The correlation ID is the `x-correlation-id` response header of `POST /orders`. catalog and
catalog-worker are not on an order's path, since orders reads prices from `products` directly. An
image upload traces through them instead.

### Measured

Local Docker, `make down` then `make obs`:

- `/loki/api/v1/label/service/values`: every service except cadvisor and the exporters.
- One order of 2 lines, by its correlation ID: 14 lines from 6 containers, in order. orders
  `request` 0 ms, inventory `stock reserved` 105 ms, payments `payment recorded` 210 ms, orders
  `order status changed` 303 ms, billing `invoice requested` 313 ms, billing-worker 9 lines to
  `succeeded` at 453 ms. Every line after the first carried `order_id` 1.
- An image upload: catalog `request`, then catalog-worker `product image processed` and
  `succeeded`, 82 ms apart.
- `--scale inventory=3` under 60 s of load: 326, 360 and 337 lines from `pracadyplomowa-inventory-1`,
  `-2` and `-3`.
- Loki at 1.4 % of a CPU and 87 MB, Alloy at 2.6 % and 68 MB, during that load.

## Compose files and OBS

The stack is split in two files:

| File | Content |
|---|---|
| `infra/docker-compose.yml` | Postgres, Kafka, both Redis, the migrations, the five services and both workers, the `shop` network and the application volumes |
| `infra/docker-compose.obs.yml` | Flower, Prometheus, Grafana, Loki, Alloy, cAdvisor and the three exporters, with `prometheus_data`, `loki_data` and `alloy_data` |

The monitoring file uses the network and services of the first, so it is only ever used together
with it. The `obs` profile is gone: what the sections above call "behind the `obs` profile" now
means "in `docker-compose.obs.yml`". Merged, the two files give the same configuration as the
single file did with `--profile obs` (`docker compose config` compared, minus the `profiles`
lines).

`OBS` in `.env`, `true` by default, picks the files for every make target:

| Command | Files |
|---|---|
| `make up` with `OBS=true` | both: the application and the monitoring |
| `make up` with `OBS=false`, or `make up OBS=false` | the application only. Monitoring containers left running are removed (`--remove-orphans`); their volumes stay |
| `make obs` | `make up OBS=true`, whatever `.env` says |
| `make down` | both, always, so every container and volume goes |

Only the exact value `true` includes the monitoring; anything else, `True` included, leaves it
out.

Flower now starts with every `make up` while `OBS=true`, and it switches task events on in every
worker (see TASK-9). A measured run with monitoring therefore includes that broker traffic, and a
run with `OBS=false` does not. With `OBS=false` the `exporter` Postgres role still exists, because
Postgres creates it on an empty volume whichever files are used.
