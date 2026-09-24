import copy
from collections.abc import Iterator

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    disable_created_metrics,
    generate_latest,
)
from prometheus_client.metrics_core import Metric
from prometheus_client.registry import Collector

from shared.settings import settings

# Only HTTP metrics and process_* (CPU, memory, fds) are exposed: *_created timestamps and
# python_gc_* / python_info are nothing the experiments read
disable_created_metrics()
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)


CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# Roughly 1.5x steps from 5 ms to 5 s: finer than the library defaults (which jump 10 -> 25 ms
# and 100 -> 250 ms) and the same relative resolution across the whole range
BUCKETS = (
    0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15,
    0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0,
)  # fmt: skip

http_requests_total = Counter(
    "http_requests_total", "HTTP requests handled", ["method", "endpoint", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["method", "endpoint"],
    buckets=BUCKETS,
)
http_requests_in_progress = Gauge(
    "http_requests_in_progress", "HTTP requests being handled", ["method", "endpoint"]
)


class WithServiceLabel(Collector):
    def collect(self) -> Iterator[Metric]:
        for metric in REGISTRY.collect():
            metric = copy.copy(metric)
            metric.samples = [
                sample._replace(labels={"service": settings.service_name, **sample.labels})
                for sample in metric.samples
            ]
            yield metric


def generate_metrics() -> bytes:
    return generate_latest(WithServiceLabel())
