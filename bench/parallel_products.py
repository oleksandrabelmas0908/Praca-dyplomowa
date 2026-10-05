"""Sends parallel GET /catalog/products?limit=N requests and prints how long each one took.

All requests go out at once from one asyncio event loop, so thousands can be in flight. Server time
is catalog's own duration_ms from its request log, matched by correlation ID. Client time is
measured here and also includes connecting and the transfer of the body. A request that gets no
HTTP response is reported by its error name instead of a status code.
Needs the stack running and LOG_LEVEL=INFO. Usage: make test REQUESTS=100 LIMIT=1000
"""

import argparse
import asyncio
import json
import resource
import statistics
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path

# An IP address rather than localhost, so thousands of connections need no name lookups
HOST = "127.0.0.1"
PORT = 8001
PATH = "/catalog/products"
TIMEOUT_SECONDS = 120
ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml", "--project-directory", "."]


async def fetch(path: str, correlation_id: str) -> str:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: {HOST}:{PORT}\r\n"
            f"x-correlation-id: {correlation_id}\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        status_line = await reader.readline()
        if not status_line:
            return "RemoteDisconnected"
        length = None
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            if name.lower() == "content-length":
                length = int(value)
        # The body is read and dropped chunk by chunk, so large responses are never held at once
        received = 0
        while length is None or received < length:
            chunk = await reader.read(65536)
            if not chunk:
                break
            received += len(chunk)
        if length is not None and received < length:
            return "IncompleteRead"
        return status_line.split()[1].decode()
    finally:
        writer.close()


async def send(path: str, correlation_id: str) -> tuple[str, float]:
    started = time.perf_counter()
    try:
        status = await asyncio.wait_for(fetch(path, correlation_id), TIMEOUT_SECONDS)
    except asyncio.TimeoutError:  # noqa: UP041 - not the builtin TimeoutError on the Mac's Python 3.9
        status = "Timeout"
    except OSError as error:
        status = type(error).__name__
    return status, (time.perf_counter() - started) * 1000


async def send_all(path: str, correlation_ids: list[str]) -> list[tuple[str, float]]:
    return await asyncio.gather(*(send(path, correlation_id) for correlation_id in correlation_ids))


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
    path = f"{PATH}?limit={args.limit}"
    correlation_ids = [str(uuid.uuid4()) for _ in range(args.requests)]
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))

    # Every open connection is a file descriptor, and macOS terminals often start with 256
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < args.requests + 100:
        resource.setrlimit(resource.RLIMIT_NOFILE, (args.requests + 100, hard))

    started = time.perf_counter()
    results = asyncio.run(send_all(path, correlation_ids))
    wall = time.perf_counter() - started

    durations = server_durations(since, set(correlation_ids))
    # By client time, which is roughly the order the responses came back in
    rows = sorted(zip(correlation_ids, results), key=lambda row: row[1][1])

    print(f"{args.requests} parallel requests to http://{HOST}:{PORT}{path}\n")
    print(f"{'#':>5}  {'status':>18}  {'server ms':>10}  {'client ms':>10}")
    for number, (correlation_id, (status, client_ms)) in enumerate(rows, start=1):
        server_ms = durations.get(correlation_id)
        server = f"{server_ms:>10.1f}" if server_ms is not None else f"{'-':>10}"
        print(f"{number:>5}  {status:>18}  {server}  {client_ms:>10.1f}")

    server_values = list(durations.values())
    client_values = [client_ms for _, client_ms in results]
    statuses = dict(sorted(Counter(status for status, _ in results).items()))
    print(f"\n{'':<10}{'server ms':^40}{'client ms':^40}")
    print(f"{'':<10}" + "".join(f"{name:>10}" for name in ["min", "median", "p95", "max"] * 2))
    print(f"{'':<10}" + "".join(f"{v:>10.1f}" for v in stats(server_values) + stats(client_values)))
    print(
        f"\nwall time {wall:.2f} s, {args.requests / wall:.1f} requests/s, statuses {statuses}, "
        f"server time found for {len(durations)}/{args.requests}"
    )


if __name__ == "__main__":
    main()
