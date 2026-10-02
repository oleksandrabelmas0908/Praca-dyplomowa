"""Sends parallel GET /catalog/products?limit=N requests and prints how long each one took.

Server time is catalog's own duration_ms from its request log, matched by correlation ID. Client
time is measured here and also includes the wait for a connection and the transfer of the body.
Needs the stack running and LOG_LEVEL=INFO. Usage: make test REQUESTS=100 LIMIT=1000
"""

import argparse
import json
import statistics
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE_URL = "http://localhost:8001/catalog/products"
ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml", "--project-directory", "."]


def send(url: str, correlation_id: str) -> tuple[int, float]:
    request = urllib.request.Request(url, headers={"x-correlation-id": correlation_id})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        error.read()
        status = error.code
    return status, (time.perf_counter() - started) * 1000


def server_durations(since: str, correlation_ids: set[str]) -> dict[str, float]:
    durations: dict[str, float] = {}
    # A log line can reach Docker shortly after its response, so retry for a few seconds
    for _ in range(10):
        logs = subprocess.run(
            [*COMPOSE, "logs", "--no-log-prefix", "--since", since, "catalog"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for line in logs.splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get("event") == "request" and entry.get("correlation_id") in correlation_ids:
                durations[entry["correlation_id"]] = entry["duration_ms"]
        if len(durations) == len(correlation_ids):
            break
        time.sleep(0.5)
    return durations


def stats(values: list[float]) -> list[float]:
    if not values:
        return [float("nan")] * 4
    p95 = statistics.quantiles(values, n=100)[94] if len(values) > 1 else values[0]
    return [min(values), statistics.median(values), p95, max(values)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, required=True, help="sent all at once")
    parser.add_argument("--limit", type=int, required=True, help="the limit query parameter")
    args = parser.parse_args()
    url = f"{BASE_URL}?limit={args.limit}"
    correlation_ids = [str(uuid.uuid4()) for _ in range(args.requests)]
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.requests) as pool:
        results = list(pool.map(send, [url] * args.requests, correlation_ids))
    wall = time.perf_counter() - started

    durations = server_durations(since, set(correlation_ids))
    # By client time, which is roughly the order the responses came back in
    rows = sorted(zip(correlation_ids, results), key=lambda row: row[1][1])

    print(f"{args.requests} parallel requests to {url}\n")
    print(f"{'#':>4}  {'status':>6}  {'server ms':>10}  {'client ms':>10}")
    for number, (correlation_id, (status, client_ms)) in enumerate(rows, start=1):
        server_ms = durations.get(correlation_id)
        server = f"{server_ms:>10.1f}" if server_ms is not None else f"{'-':>10}"
        print(f"{number:>4}  {status:>6}  {server}  {client_ms:>10.1f}")

    server_values = list(durations.values())
    client_values = [client_ms for _, client_ms in results]
    statuses = sorted({status for status, _ in results})
    print(f"\n{'':<10}{'server ms':^40}{'client ms':^40}")
    print(f"{'':<10}" + "".join(f"{name:>10}" for name in ["min", "median", "p95", "max"] * 2))
    print(f"{'':<10}" + "".join(f"{v:>10.1f}" for v in stats(server_values) + stats(client_values)))
    print(
        f"\nwall time {wall:.2f} s, {args.requests / wall:.1f} requests/s, statuses {statuses}, "
        f"server time found for {len(durations)}/{args.requests}"
    )


if __name__ == "__main__":
    main()
