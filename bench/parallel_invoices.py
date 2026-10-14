"""Places N orders of L lines in parallel with POST /orders, waits until billing-worker has
generated every invoice, and prints how long each step took.

Orders go out at once from one asyncio event loop, as in parallel_products.py. Every order uses
products of its own with stock, so none is rejected unless stock ran out since the last make seed.
Order server time is orders' duration_ms from its request log, order client time is measured here.
Saga is from the order's response to billing's "invoice requested" line, so it covers the outbox,
inventory and payments. Queue wait runs from that line to the start of the task; the line is
written just before billing commits and queues the task, so it includes that commit and publish.
Rendering is the task's run time from Celery's "succeeded in" log line. End to end is from sending
the order to the task finishing. All are matched by correlation ID. Rows are in the order the
tasks finished. At the end, also after an error or Ctrl-C, the PDFs and invoice rows of this run's
orders are deleted, and make seed restores the stock the orders used up.
Needs the stack running and LOG_LEVEL=INFO. Usage: make test-invoices ORDERS=20 LINES=3
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
PORT = 8002
PATH = "/orders"
TIMEOUT_SECONDS = 120
PROCESSING_TIMEOUT_SECONDS = 900
ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml", "--project-directory", "."]
SUCCEEDED = re.compile(r"\] succeeded in ([0-9.]+)s")
REQUESTED = "invoice requested"
FAILED = "invoice generation failed, invoice marked failed"
NAN = float("nan")


def compose(*command: str) -> bytes:
    return subprocess.run([*COMPOSE, *command], cwd=ROOT, capture_output=True, check=True).stdout


def psql(query: str) -> list[str]:
    output = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        f'psql -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "{query}"',
    )
    return output.decode().split()


async def post(body: bytes, correlation_id: str) -> tuple[str, int]:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    try:
        writer.write(
            f"POST {PATH} HTTP/1.1\r\nHost: {HOST}:{PORT}\r\n"
            f"x-correlation-id: {correlation_id}\r\nConnection: close\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n".encode()
        )
        writer.write(body)
        await writer.drain()
        response = await reader.read()
        if not response:
            return "RemoteDisconnected", 0
        head, _, payload = response.partition(b"\r\n\r\n")
        status = head.split()[1].decode()
        return status, json.loads(payload)["id"] if status == "201" else 0
    finally:
        writer.close()


async def send(body: bytes, correlation_id: str) -> tuple[str, int, float, float]:
    """Status, order ID, client time in ms and the time the order was sent."""
    sent_at = time.time()
    started = time.perf_counter()
    try:
        status, order_id = await asyncio.wait_for(post(body, correlation_id), TIMEOUT_SECONDS)
    except asyncio.TimeoutError:  # noqa: UP041 - not the builtin TimeoutError on the Mac's Python 3.9
        status, order_id = "Timeout", 0
    except OSError as error:
        status, order_id = type(error).__name__, 0
    return status, order_id, (time.perf_counter() - started) * 1000, sent_at


async def send_all(
    bodies: list[bytes], correlation_ids: list[str]
) -> list[tuple[str, int, float, float]]:
    return await asyncio.gather(*(send(body, cid) for body, cid in zip(bodies, correlation_ids)))


def wait_for_invoices(order_ids: list[int]) -> dict[int, str]:
    """Final invoice status by order ID, or the order's status if it was rejected."""
    outcomes: dict[int, str] = {}
    ids = ",".join(map(str, order_ids))
    query = (
        "SELECT o.id || ':' || coalesce(i.status::text, o.status::text) FROM orders o "
        f"LEFT JOIN invoices i ON i.order_id = o.id WHERE o.id IN ({ids})"
    )
    deadline = time.monotonic() + PROCESSING_TIMEOUT_SECONDS
    while len(outcomes) < len(order_ids) and time.monotonic() < deadline:
        time.sleep(1)
        for row in psql(query):
            order_id, status = row.split(":")
            if status in ("completed", "failed", "rejected"):
                outcomes[int(order_id)] = status
        print(f"\rfinished {len(outcomes)}/{len(order_ids)}", end="", flush=True)
    print()
    return outcomes


def clean_up(order_ids: list[int]) -> None:
    if order_ids:
        compose(
            "exec",
            "-T",
            "billing-worker",
            "rm",
            "-f",
            *(f"/invoices/{order_id}.pdf" for order_id in order_ids),
        )
        ids = ",".join(map(str, order_ids))
        deleted = psql(
            f"WITH deleted AS (DELETE FROM invoices WHERE order_id IN ({ids}) RETURNING 1) "
            "SELECT count(*) FROM deleted"
        )
        print(f"deleted the PDFs of {len(order_ids)} orders and {deleted[0]} invoice rows")
    subprocess.run(["make", "seed"], cwd=ROOT, check=True)


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


def order_times(since: str, correlation_ids: set[str]) -> dict[str, tuple[float, float]]:
    """Server duration in ms and the time the response went out, by correlation ID."""
    times: dict[str, tuple[float, float]] = {}
    for entry in log_entries("orders", since):
        if entry.get("event") == "request" and entry.get("correlation_id") in correlation_ids:
            times[entry["correlation_id"]] = (entry["duration_ms"], epoch(entry))
    return times


def requested_times(since: str, correlation_ids: set[str]) -> dict[str, float]:
    """The time billing requested the invoice, by correlation ID."""
    return {
        entry["correlation_id"]: epoch(entry)
        for entry in log_entries("billing", since)
        if entry.get("event") == REQUESTED and entry.get("correlation_id") in correlation_ids
    }


def task_times(since: str, correlation_ids: set[str]) -> dict[str, tuple[float, float]]:
    """Run time in ms and the time the task finished, by correlation ID."""
    tasks: dict[str, tuple[float, float]] = {}
    for entry in log_entries("billing-worker", since):
        correlation_id = entry.get("correlation_id")
        if correlation_id not in correlation_ids:
            continue
        succeeded = SUCCEEDED.search(entry["event"])
        if succeeded:
            tasks[correlation_id] = (float(succeeded[1]) * 1000, epoch(entry))
        elif entry["event"] == FAILED:
            tasks[correlation_id] = (NAN, epoch(entry))
    return tasks


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
    parser.add_argument("--orders", type=int, required=True, help="placed all at once")
    parser.add_argument("--lines", type=int, required=True, help="products per order")
    args = parser.parse_args()

    needed = args.orders * args.lines
    product_ids = psql(
        f"SELECT id FROM products WHERE stock_quantity > 0 ORDER BY random() LIMIT {needed}"
    )
    if len(product_ids) < needed:
        raise SystemExit(f"only {len(product_ids)} products have stock, {needed} needed")
    bodies = [
        json.dumps(
            {
                "customer_id": 1,
                "lines": [
                    {"product_id": int(product_id), "quantity": 1}
                    for product_id in product_ids[i * args.lines : (i + 1) * args.lines]
                ],
            }
        ).encode()
        for i in range(args.orders)
    ]
    correlation_ids = [str(uuid.uuid4()) for _ in range(args.orders)]

    # Every open connection is a file descriptor, and macOS terminals often start with 256
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < args.orders + 100:
        resource.setrlimit(resource.RLIMIT_NOFILE, (args.orders + 100, hard))

    created: list[int] = []
    try:
        since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
        started = time.perf_counter()
        results = asyncio.run(send_all(bodies, correlation_ids))
        order_wall = time.perf_counter() - started

        created.extend(order_id for _, order_id, _, _ in results if order_id)
        outcomes = wait_for_invoices(created) if created else {}
        # A log line can reach Docker shortly after it is written
        time.sleep(1)
        ids = set(correlation_ids)
        orders = order_times(since, ids)
        requested = requested_times(since, ids)
        tasks = task_times(since, ids)

        rows = []
        for cid, (status, order_id, client_ms, sent_at) in zip(correlation_ids, results):
            server_ms, responded_at = orders.get(cid, (NAN, NAN))
            requested_at = requested.get(cid, NAN)
            rendering_ms, finished_at = tasks.get(cid, (NAN, math.inf))
            outcome = outcomes.get(order_id, "unfinished") if order_id else "-"
            rows.append(
                (
                    finished_at,
                    order_id,
                    status,
                    server_ms,
                    client_ms,
                    (requested_at - responded_at) * 1000,
                    (finished_at - requested_at) * 1000 - rendering_ms,
                    rendering_ms,
                    (finished_at - sent_at) * 1000,
                    outcome,
                )
            )
        rows.sort(key=lambda row: row[0])

        names = [
            "order server ms",
            "order client ms",
            "saga ms",
            "queue wait ms",
            "rendering ms",
            "end to end ms",
        ]
        print(
            f"\n{args.orders} parallel orders of {args.lines} lines to http://{HOST}:{PORT}{PATH}\n"
        )
        print(
            f"{'#':>5}  {'order':>8}  {'status':>18}"
            + "".join(f"  {n:>15}" for n in names)
            + "  outcome"
        )
        for number, row in enumerate(rows, start=1):
            _, order_id, status, *values, outcome = row
            print(
                f"{number:>5}  {order_id or '-':>8}  {status:>18}"
                + "".join(f"  {cell(value, 15)}" for value in values)
                + f"  {outcome}"
            )

        print(f"\n{'':<18}" + "".join(f"{name:>10}" for name in ["min", "median", "p95", "max"]))
        for column, name in enumerate(names, start=3):
            values = stats([row[column] for row in rows])
            print(f"{name:<18}" + "".join(cell(value, 10) for value in values))

        first_order = min(sent_at for _, _, _, sent_at in results)
        last_task = max((finished_at for _, finished_at in tasks.values()), default=NAN)
        drain = last_task - first_order
        counts = dict(sorted(Counter(row[-1] for row in rows).items()))
        statuses = dict(sorted(Counter(status for status, _, _, _ in results).items()))
        print(
            f"\norders took {order_wall:.2f} s; {drain:.2f} s from the first order to the last "
            f"finished task, {counts.get('completed', 0) / drain:.1f} invoices/s\n"
            f"statuses {statuses}, outcomes {counts}"
        )
    finally:
        clean_up(created)


if __name__ == "__main__":
    main()
