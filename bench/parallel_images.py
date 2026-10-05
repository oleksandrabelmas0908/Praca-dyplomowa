"""Uploads bench/test.jpg to POST /catalog/products/{id}/image for products 1..N in parallel,
waits until catalog-worker has processed every upload, prints how long each step took, and then
deletes every image.

Uploads go out at once from one asyncio event loop, as in parallel_products.py. Upload server time
is catalog's duration_ms from its request log, upload client time is measured here. Processing is
the task's run time from Celery's "succeeded in" log line. Queue wait runs from the upload's request
log line to the start of its task; that line is written just after the task is queued, so a task
picked up at once can show a few ms below zero. All are matched by correlation ID. Rows are in the
order the tasks finished. At the end, also after an error or Ctrl-C, every file on the images volume is deleted and
products.image_paths is emptied.
Needs the stack running and LOG_LEVEL=INFO. Usage: make test-images IMAGES=20
"""

import argparse
import asyncio
import json
import math
import re
import resource
import statistics
import subprocess
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

# An IP address rather than localhost, so thousands of connections need no name lookups
HOST = "127.0.0.1"
PORT = 8001
PATH = "/catalog/products/{}/image"
TIMEOUT_SECONDS = 120
PROCESSING_TIMEOUT_SECONDS = 900
ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml", "--project-directory", "."]
BOUNDARY = uuid.uuid4().hex
SUCCEEDED = re.compile(r"\] succeeded in ([0-9.]+)s")
FAILED = "image processing failed, task moved to the dead-letter queue"
EMPTY_IMAGE_PATHS = (
    "WITH emptied AS (UPDATE products SET image_paths = '[]' WHERE image_paths <> '[]' "
    "RETURNING 1) SELECT count(*) FROM emptied"
)
NAN = float("nan")
IMAGE = ROOT / "bench" / "test.jpg"


def compose(*command: str) -> bytes:
    return subprocess.run([*COMPOSE, *command], cwd=ROOT, capture_output=True, check=True).stdout


def multipart(image: bytes) -> bytes:
    head = (
        f"--{BOUNDARY}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{IMAGE.name}"\r\n'
        "Content-Type: image/jpeg\r\n\r\n"
    )
    return head.encode() + image + f"\r\n--{BOUNDARY}--\r\n".encode()


async def upload(product_id: int, body: bytes, correlation_id: str) -> str:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    try:
        writer.write(
            f"POST {PATH.format(product_id)} HTTP/1.1\r\nHost: {HOST}:{PORT}\r\n"
            f"x-correlation-id: {correlation_id}\r\nConnection: close\r\n"
            f"Content-Type: multipart/form-data; boundary={BOUNDARY}\r\n"
            f"Content-Length: {len(body)}\r\n\r\n".encode()
        )
        writer.write(body)
        await writer.drain()
        status_line = await reader.readline()
        if not status_line:
            return "RemoteDisconnected"
        await reader.read()
        return status_line.split()[1].decode()
    finally:
        writer.close()


async def send(product_id: int, body: bytes, correlation_id: str) -> tuple[str, float]:
    started = time.perf_counter()
    try:
        status = await asyncio.wait_for(upload(product_id, body, correlation_id), TIMEOUT_SECONDS)
    except asyncio.TimeoutError:  # noqa: UP041 - not the builtin TimeoutError on the Mac's Python 3.9
        status = "Timeout"
    except OSError as error:
        status = type(error).__name__
    return status, (time.perf_counter() - started) * 1000


async def send_all(
    product_ids: list[int], body: bytes, correlation_ids: list[str]
) -> list[tuple[str, float]]:
    return await asyncio.gather(
        *(send(product_id, body, cid) for product_id, cid in zip(product_ids, correlation_ids))
    )


def log_entries(service: str, since: str) -> list[dict]:
    entries = []
    for line in compose("logs", "--no-log-prefix", "--since", since, service).splitlines():
        try:
            entries.append(json.loads(line))
        except ValueError:
            continue
    return entries


def epoch(entry: dict) -> float:
    timestamp = entry["timestamp"].replace("Z", "+00:00")
    return datetime.fromisoformat(timestamp).timestamp()


def upload_times(since: str, correlation_ids: set[str]) -> dict[str, tuple[float, float]]:
    """Server duration in ms and the time the response went out, by correlation ID."""
    times: dict[str, tuple[float, float]] = {}
    # A log line can reach Docker shortly after its response, so retry for a few seconds
    for _ in range(10):
        for entry in log_entries("catalog", since):
            if entry.get("event") == "request" and entry.get("correlation_id") in correlation_ids:
                times[entry["correlation_id"]] = (entry["duration_ms"], epoch(entry))
        if len(times) == len(correlation_ids):
            break
        time.sleep(0.5)
    return times


def task_times(since: str, correlation_ids: set[str]) -> dict[str, tuple[str, float, float]]:
    """Outcome, run time in ms and the time the task finished, by correlation ID."""
    tasks: dict[str, tuple[str, float, float]] = {}
    deadline = time.monotonic() + PROCESSING_TIMEOUT_SECONDS
    while len(tasks) < len(correlation_ids) and time.monotonic() < deadline:
        time.sleep(1)
        for entry in log_entries("catalog-worker", since):
            correlation_id = entry.get("correlation_id")
            if correlation_id not in correlation_ids:
                continue
            succeeded = SUCCEEDED.search(entry["event"])
            if succeeded:
                tasks[correlation_id] = ("processed", float(succeeded[1]) * 1000, epoch(entry))
            elif entry["event"] == FAILED:
                tasks[correlation_id] = ("failed", NAN, epoch(entry))
        print(f"\rprocessed {len(tasks)}/{len(correlation_ids)}", end="", flush=True)
    print()
    return tasks


def delete_images() -> None:
    files = compose(
        "exec",
        "-T",
        "catalog",
        "sh",
        "-c",
        "find /images -type f | wc -l && find /images -mindepth 1 -delete",
    )
    products = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        f'psql -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "{EMPTY_IMAGE_PATHS}"',
    )
    print(
        f"deleted {files.decode().strip()} files from the images volume, "
        f"emptied image_paths on {products.decode().strip()} products"
    )


def stats(values: list[float]) -> list[float]:
    values = [value for value in values if not math.isnan(value)]
    if not values:
        return [NAN] * 4
    p95 = (
        statistics.quantiles(values, n=100, method="inclusive")[94]
        if len(values) > 1
        else values[0]
    )
    return [min(values), statistics.median(values), p95, max(values)]


def cell(value: float, width: int) -> str:
    return f"{'-':>{width}}" if math.isnan(value) else f"{value:>{width}.1f}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--images", type=int, required=True, help="uploaded all at once")
    args = parser.parse_args()
    product_ids = list(range(1, args.images + 1))
    correlation_ids = [str(uuid.uuid4()) for _ in product_ids]

    image = IMAGE.read_bytes()
    body = multipart(image)

    # Every open connection is a file descriptor, and macOS terminals often start with 256
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < args.images + 100:
        resource.setrlimit(resource.RLIMIT_NOFILE, (args.images + 100, hard))

    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    try:
        started = time.perf_counter()
        results = asyncio.run(send_all(product_ids, body, correlation_ids))
        upload_wall = time.perf_counter() - started

        uploads = upload_times(since, set(correlation_ids))
        queued = {cid for cid, (status, _) in zip(correlation_ids, results) if status == "202"}
        tasks = task_times(since, queued)

        rows = []
        for product_id, cid, (status, client_ms) in zip(product_ids, correlation_ids, results):
            server_ms, responded_at = uploads.get(cid, (NAN, NAN))
            default = ("unfinished", NAN, math.inf) if cid in queued else ("-", NAN, math.inf)
            outcome, processing_ms, finished_at = tasks.get(cid, default)
            wait_ms = (finished_at - responded_at) * 1000 - processing_ms
            rows.append(
                (
                    finished_at,
                    product_id,
                    status,
                    server_ms,
                    client_ms,
                    wait_ms,
                    processing_ms,
                    outcome,
                )
            )
        rows.sort(key=lambda row: row[0])

        print(
            f"\n{args.images} parallel uploads of {IMAGE.name} ({len(image) / 1000:.0f} kB) "
            f"to http://{HOST}:{PORT}{PATH.format('{id}')}\n"
        )
        print(
            f"{'#':>5}  {'product':>8}  {'status':>18}  {'upload server ms':>17}  "
            f"{'upload client ms':>17}  {'queue wait ms':>14}  {'processing ms':>14}  outcome"
        )
        for number, row in enumerate(rows, start=1):
            _, product_id, status, server_ms, client_ms, wait_ms, processing_ms, outcome = row
            print(
                f"{number:>5}  {product_id:>8}  {status:>18}  {cell(server_ms, 17)}  "
                f"{cell(client_ms, 17)}  {cell(wait_ms, 14)}  {cell(processing_ms, 14)}  {outcome}"
            )

        print(f"\n{'':<18}" + "".join(f"{name:>10}" for name in ["min", "median", "p95", "max"]))
        names = ["upload server ms", "upload client ms", "queue wait ms", "processing ms"]
        for column, name in enumerate(names, start=3):
            values = stats([row[column] for row in rows])
            print(f"{name:<18}" + "".join(cell(value, 10) for value in values))

        first_upload = min(
            (responded_at - server_ms / 1000 for server_ms, responded_at in uploads.values()),
            default=NAN,
        )
        last_task = max((finished_at for _, _, finished_at in tasks.values()), default=NAN)
        drain = last_task - first_upload
        outcomes = dict(sorted(Counter(row[7] for row in rows).items()))
        statuses = dict(sorted(Counter(status for status, _ in results).items()))
        print(
            f"\nuploads took {upload_wall:.2f} s; {drain:.2f} s from the first upload to the last "
            f"finished task, {outcomes.get('processed', 0) / drain:.1f} images/s\n"
            f"statuses {statuses}, outcomes {outcomes}"
        )
        if outcomes.get("unfinished"):
            print(
                f"{outcomes['unfinished']} tasks were still unfinished after "
                f"{PROCESSING_TIMEOUT_SECONDS} s and may write images after the cleanup"
            )
    finally:
        delete_images()


if __name__ == "__main__":
    main()
