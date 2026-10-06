import errno
import threading
import time
from typing import Any

import redis
import structlog
from celery import Task, current_app, signals, states
from celery.app.task import Context
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from shared.metrics import WithServiceLabel
from shared.settings import settings

DEAD_LETTER_QUEUE = "dead_letter"
PUBLISHED_AT = "published_at"
METRICS_PORT = 9000

# Image processing takes hundreds of milliseconds to seconds, the 600 px test image about 20 ms
DURATION_BUCKETS = (
    0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75,
    1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0, 20.0, 30.0,
)  # fmt: skip
# Near zero while idle, minutes under backlog: fine steps from 1 s up, where saturation shows,
# and up to 300 s so a backlog stays out of +Inf
QUEUE_WAIT_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0,
    7.5, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 180.0, 300.0,
)  # fmt: skip

celery_task_queue_wait_seconds = Histogram(
    "celery_task_queue_wait_seconds",
    "Time from publishing a task to a worker starting it",
    ["task", "queue"],
    buckets=QUEUE_WAIT_BUCKETS,
)
celery_task_duration_seconds = Histogram(
    "celery_task_duration_seconds",
    "Celery task run time",
    ["task", "queue"],
    buckets=DURATION_BUCKETS,
)
celery_tasks_total = Counter(
    "celery_tasks_total", "Celery task runs by outcome", ["task", "queue", "outcome"]
)
celery_queue_depth = Gauge("celery_queue_depth", "Tasks waiting in the queue", ["queue"])

logger = structlog.get_logger()

started: dict[str, float] = {}


def queue_name(request: Context) -> str:
    return (request.delivery_info or {}).get("routing_key", "")


@signals.before_task_publish.connect
def record_publish_time(headers: dict[str, Any], **_: Any) -> None:
    headers[PUBLISHED_AT] = time.time()


@signals.task_prerun.connect
def record_start(task_id: str, task: Task, **_: Any) -> None:
    started[task_id] = time.perf_counter()
    published_at = task.request.get(PUBLISHED_AT)
    if published_at is None:
        return
    # Publisher and worker are different processes, so this is only right if their clocks agree.
    # Here both are containers on one host and share its clock. Across two machines it would
    # also include the offset between their clocks
    celery_task_queue_wait_seconds.labels(task.name, queue_name(task.request)).observe(
        time.time() - published_at
    )


@signals.task_postrun.connect
def record_end(task_id: str, task: Task, state: str, **_: Any) -> None:
    queue = queue_name(task.request)
    celery_task_duration_seconds.labels(task.name, queue).observe(
        time.perf_counter() - started.pop(task_id)
    )
    if state == states.SUCCESS:
        celery_tasks_total.labels(task.name, queue, "success").inc()


@signals.task_retry.connect
def count_retry(sender: Task, request: Context, **_: Any) -> None:
    celery_tasks_total.labels(sender.name, queue_name(request), "retry").inc()


@signals.task_failure.connect
def count_failure(sender: Task, **_: Any) -> None:
    celery_tasks_total.labels(sender.name, queue_name(sender.request), "failure").inc()


def poll_queue_depth(queues: list[str]) -> None:
    client = redis.Redis.from_url(settings.redis_broker_url)
    while True:
        try:
            for queue in queues:
                celery_queue_depth.labels(queue).set(client.llen(queue))
        except redis.RedisError as error:
            logger.warning("reading the queue depth failed", error=repr(error))
        time.sleep(settings.queue_depth_interval_seconds)


# Prefork runs every task, and so every task signal, in a pool child, never in the parent. Each
# child keeps its own registry and tries to bind the port: the first one serves its registry and
# polls the queue depth, the others skip. With CELERY_CONCURRENCY above 1 the metrics therefore
# cover only that child's tasks. Multiprocess mode with a directory shared by the children would
# merge them, which the current experiments do not need
@signals.worker_process_init.connect
def start_metrics_server(**_: Any) -> None:
    try:
        start_http_server(METRICS_PORT, registry=WithServiceLabel())
    except OSError as error:
        if error.errno != errno.EADDRINUSE:
            raise
        return
    queues = [*current_app.amqp.queues, DEAD_LETTER_QUEUE]
    threading.Thread(target=poll_queue_depth, args=(queues,), daemon=True).start()
