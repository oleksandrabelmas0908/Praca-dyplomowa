"""Boots every configuration in bench/matrix.yaml in turn and proves an order still flows through it.

Per entry: write its flags to .env, docker compose down -v, make up OBS=false (Postgres and Kafka,
migrations, topics with the entry's partition count, then the services), poll docker compose ps
until every container is healthy, make seed, check that each flag took effect in the running
containers, place one order that must reach paid and one that must be rejected, check the stock
totals, and tear down. Under kafka POST /orders answers pending and the order is polled until it
leaves pending; under http the answer must already carry the final status.

A failing entry is recorded with its reason, its docker compose logs are written to
bench/results/smoke/<entry>.log before teardown, and the run goes on. .env is restored at the end.
Exits 1 if any entry failed. The running stack and its volumes are gone afterwards: make up.
Usage: make smoke, or make smoke ENTRY=e3-http for one entry.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path

import yaml

MATRIX = Path("bench/matrix.yaml")
LOGS = Path("bench/results/smoke")
ENV_FILE = Path(".env")
APP = ["docker", "compose", "-f", "infra/docker-compose.yml", "--project-directory", "."]
# Both files, as make down, so monitoring containers left from before the run go too
ALL = [*APP[:4], "-f", "infra/docker-compose.obs.yml", *APP[4:]]
TOPICS = ["order.created", "inventory.reserved", "inventory.rejected", "payment.completed"]
HTTP_SERVICES = ["catalog", "orders", "inventory", "payments", "billing"]

COMMAND_TIMEOUT_SECONDS = 120
UP_TIMEOUT_SECONDS = 300
HEALTHY_TIMEOUT_SECONDS = 120
ORDER_TIMEOUT_SECONDS = 30
# Consumers join their group a few seconds after a container turns healthy
GROUP_JOIN_SECONDS = 15
POLL_SECONDS = 1

# Runs inside a container: every process with its parent, command line and whether psycopg2's
# C extension is mapped into it
PROCESSES = """
import json, os
processes = []
for pid in filter(str.isdigit, os.listdir("/proc")):
    try:
        cmdline = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\\0", b" ").decode()
        ppid = int(open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[1])
        psycopg = "_psycopg" in open(f"/proc/{pid}/maps").read()
    except OSError:
        continue
    processes.append({"pid": int(pid), "ppid": ppid, "cmdline": cmdline, "psycopg": psycopg})
print(json.dumps(processes))
"""

# Runs inside a container: the status of a POST with an empty body to the service itself
POST_STATUS = """
import sys, urllib.error, urllib.request
request = urllib.request.Request("http://localhost:8000" + sys.argv[1], b"{}",
                                 {"content-type": "application/json"})
try:
    print(urllib.request.urlopen(request, timeout=5).status)
except urllib.error.HTTPError as error:
    print(error.code)
"""


class Failed(Exception):
    pass


def read_env() -> dict[str, str]:
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def env_value(value: object) -> str:
    return str(value).lower() if isinstance(value, bool) else str(value)


def write_env(original: str, flags: dict[str, object]) -> None:
    text = original
    for key, value in flags.items():
        line = f"{key}={env_value(value)}"
        text, replaced = re.subn(rf"^{key}=.*$", line, text, flags=re.MULTILINE)
        if not replaced:
            text = text.rstrip("\n") + f"\n{line}\n"
    ENV_FILE.write_text(text)


# Without this, variables that make smoke exported from the original .env would take precedence
# over the rewritten file in every docker compose call, and its command line would reach make up
def child_environment() -> dict[str, str]:
    dropped = set(read_env()) | {"MAKEFLAGS", "MFLAGS", "MAKELEVEL"}
    return {key: value for key, value in os.environ.items() if key not in dropped}


CHILD_ENV = child_environment()


def run(args: list[str], timeout: float = COMMAND_TIMEOUT_SECONDS) -> str:
    try:
        result = subprocess.run(
            args,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            env=CHILD_ENV,
        )
    except subprocess.TimeoutExpired:
        raise Failed(f"{command(args)} timed out after {timeout:.0f} s") from None
    if result.returncode != 0:
        lines = [
            line
            for line in result.stdout.splitlines()
            if line.strip() and not line.startswith("make: ***")
        ]
        last = lines[-1].strip() if lines else "no output"
        raise Failed(f"{command(args)} exited {result.returncode}: {last}")
    return result.stdout


def command(args: list[str]) -> str:
    for prefix in (ALL, APP):
        if args[: len(prefix)] == prefix:
            args = ["docker compose", *args[len(prefix) :]]
    return " ".join(args)[:80]


def containers(service: str) -> list[str]:
    return sorted(run([*APP, "ps", "--format", "{{.Name}}", service]).split())


# docker compose ps lists only running containers, so a stopped service has none
def container(service: str) -> str:
    found = containers(service)
    if not found:
        raise Failed(f"no running {service} container")
    return found[0]


def psql(sql: str) -> str:
    env = read_env()
    return run(
        [*APP, "exec", "-T", "postgres", "psql", "-U", env["POSTGRES_USER"],
         "-d", env["POSTGRES_DB"], "-At", "-c", sql]
    ).strip()  # fmt: skip


def http(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data, {"content-type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {}
    except (urllib.error.URLError, OSError) as error:
        raise Failed(f"{method} {url}: {getattr(error, 'reason', error)}") from None


def wait_healthy() -> None:
    deadline = time.monotonic() + HEALTHY_TIMEOUT_SECONDS
    while True:
        output = run([*APP, "ps", "--all", "--format", "json"])
        rows = [json.loads(line) for line in output.splitlines() if line.strip()]
        exited = [row for row in rows if row["State"] != "running"]
        if exited:
            row = exited[0]
            raise Failed(f"{row['Name']} {row['State']} with code {row['ExitCode']}")
        waiting = [row for row in rows if row["Health"] != "healthy"]
        if rows and not waiting:
            return
        if time.monotonic() > deadline:
            names = ", ".join(f"{row['Name']} {row['Health'] or 'no health'}" for row in waiting)
            raise Failed(f"not healthy after {HEALTHY_TIMEOUT_SECONDS} s: {names}")
        time.sleep(POLL_SECONDS)


def seed() -> int:
    output = run([*APP, "exec", "-T", "catalog", "python", "-m", "catalog.seed"])
    for line in reversed(output.splitlines()):
        if '"stock_total"' in line:
            return int(json.loads(line)["stock_total"])
    raise Failed("make seed logged no stock_total")


def processes(container: str) -> list[dict]:
    return json.loads(run(["docker", "exec", container, "python", "-c", PROCESSES]))


# With --workers 1 Uvicorn serves from its own process, above that from spawned children
def uvicorn_workers(container: str) -> list[dict]:
    found = processes(container)
    children = [
        process
        for process in found
        if process["ppid"] == 1
        and "multiprocessing" in process["cmdline"]
        and "resource_tracker" not in process["cmdline"]
    ]
    return children or [process for process in found if process["pid"] == 1]


def kafka_tool(tool: str, *args: str) -> str:
    return run(
        ["docker", "exec", "-e", "KAFKA_HEAP_OPTS=-Xmx128m", container("kafka"),
         f"/opt/kafka/bin/{tool}", "--bootstrap-server", "localhost:9092", *args]
    )  # fmt: skip


def group_members() -> dict[str, int]:
    members: dict[str, int] = {}
    output = kafka_tool("kafka-consumer-groups.sh", "--describe", "--all-groups", "--members")
    for line in output.splitlines():
        fields = line.split()
        if fields and fields[0] != "GROUP" and not line.startswith("Consumer group"):
            members[fields[0]] = members.get(fields[0], 0) + 1
    return members


def check_db_driver(flags: dict[str, str]) -> str | None:
    observed = []
    for service in ("catalog", "orders"):
        for container in containers(service):
            for worker in uvicorn_workers(container):
                driver = "sync" if worker["psycopg"] else "async"
                if driver != flags["DB_DRIVER"]:
                    observed.append(f"{container} {driver}")
    return ", ".join(observed) or None


def check_uvicorn_workers(flags: dict[str, str]) -> str | None:
    observed = []
    for service in HTTP_SERVICES:
        for container in containers(service):
            count = len(uvicorn_workers(container))
            if count != int(flags["UVICORN_WORKERS"]):
                observed.append(f"{container} {count}")
    return ", ".join(observed) or None


def cache_ttl() -> int | None:
    catalog = f"http://localhost:{read_env()['CATALOG_PORT']}/catalog/products"
    status, page = http("GET", f"{catalog}?limit=1")
    if status != 200:
        raise Failed(f"GET /catalog/products answered {status}")
    product_id = page["items"][0]["id"]
    key = f"product:{product_id}"
    redis = ["docker", "exec", container("redis-cache"), "redis-cli"]
    run([*redis, "DEL", key])
    http("GET", f"{catalog}/{product_id}")
    ttl = int(run([*redis, "TTL", key]))
    # -2: no such key
    return None if ttl == -2 else ttl


def check_cache(flags: dict[str, str]) -> list[tuple[str, str]]:
    ttl = cache_ttl()
    if flags["CACHE_ENABLED"] == "false":
        return [] if ttl is None else [("CACHE_ENABLED", "product cached")]
    if ttl is None:
        return [("CACHE_ENABLED", "product not cached")]
    configured = int(flags["CACHE_TTL_SECONDS"])
    # The TTL counts down from when the entry was written, a second or two ago at most
    if not configured - 5 <= ttl <= configured:
        return [("CACHE_TTL_SECONDS", f"TTL {ttl}")]
    return []


def check_transport(flags: dict[str, str]) -> str | None:
    http_transport = flags["TRANSPORT"] == "http"
    observed = []
    for service, path in (("inventory", "/reservations"), ("payments", "/payments")):
        for container in containers(service):
            status = int(run(["docker", "exec", container, "python", "-c", POST_STATUS, path]))
            # An empty body fails validation (422) where the route exists, 404 where it does not
            if status != (422 if http_transport else 404):
                observed.append(f"{container} POST {path} {status}")
    deadline = time.monotonic() + GROUP_JOIN_SECONDS
    while True:
        members = group_members()
        consuming = {group: members.get(group, 0) for group in ("orders", "inventory", "payments")}
        wrong = [
            f"group {group} {count} members"
            for group, count in consuming.items()
            if (count > 0) == http_transport
        ]
        if not wrong or time.monotonic() > deadline:
            break
        time.sleep(2)
    return ", ".join(observed + wrong) or None


def check_partitions(flags: dict[str, str]) -> str | None:
    output = kafka_tool("kafka-topics.sh", "--describe")
    pattern = r"^Topic: (\S+)\s+TopicId: \S+\s+PartitionCount: (\d+)"
    counts = dict(re.findall(pattern, output, re.MULTILINE))
    wrong = [
        f"{topic} {counts.get(topic, 'missing')}"
        for topic in TOPICS
        if counts.get(topic) != flags["KAFKA_PARTITIONS"]
    ]
    return ", ".join(wrong) or None


def check_replicas(flags: dict[str, str], service: str) -> str | None:
    count = len(containers(service))
    return None if count == int(flags[f"{service.upper()}_REPLICAS"]) else f"{count} containers"


def check_celery_concurrency(flags: dict[str, str]) -> str | None:
    observed = []
    for container in containers("billing-worker"):
        count = sum(
            1
            for process in processes(container)
            if process["ppid"] == 1 and "celery" in process["cmdline"]
        )
        if count != int(flags["CELERY_CONCURRENCY"]):
            observed.append(f"{container} {count} pool children")
    return ", ".join(observed) or None


def check_flags(flags: dict[str, str]) -> None:
    mismatches = [
        ("DB_DRIVER", check_db_driver(flags)),
        ("UVICORN_WORKERS", check_uvicorn_workers(flags)),
        *check_cache(flags),
        ("TRANSPORT", check_transport(flags)),
        ("KAFKA_PARTITIONS", check_partitions(flags)),
        ("INVENTORY_REPLICAS", check_replicas(flags, "inventory")),
        ("PAYMENTS_REPLICAS", check_replicas(flags, "payments")),
        ("CELERY_CONCURRENCY", check_celery_concurrency(flags)),
    ]
    wrong = [f"{flag}={flags[flag]} but {observed}" for flag, observed in mismatches if observed]
    if wrong:
        raise Failed("flag not applied: " + "; ".join(wrong))


def check_order(flags: dict[str, str], product_id: str, expected: str) -> None:
    orders = f"http://localhost:{read_env()['ORDERS_PORT']}/orders"
    body = {"customer_id": 1, "lines": [{"product_id": int(product_id), "quantity": 1}]}
    status, order = http("POST", orders, body)
    if status != 201:
        raise Failed(f"POST /orders for product {product_id} answered {status}")
    if flags["TRANSPORT"] == "http":
        final = order["status"]
    else:
        if order["status"] != "pending":
            raise Failed(f"order {order['id']} answered {order['status']}, under kafka pending")
        deadline = time.monotonic() + ORDER_TIMEOUT_SECONDS
        while order["status"] == "pending":
            if time.monotonic() > deadline:
                raise Failed(f"order {order['id']} still pending after {ORDER_TIMEOUT_SECONDS} s")
            time.sleep(POLL_SECONDS / 5)
            _, order = http("GET", f"{orders}/{order['id']}")
        final = order["status"]
    if final != expected:
        raise Failed(f"order {order['id']} for product {product_id} {final}, expected {expected}")


def check_stock(stock_total: int) -> None:
    total = int(psql("SELECT sum(stock_quantity + stock_reserved) FROM products"))
    negative = int(
        psql("SELECT count(*) FROM products WHERE stock_quantity < 0 OR stock_reserved < 0")
    )
    if total != stock_total or negative:
        raise Failed(f"stock {total} units, seeded {stock_total}, {negative} products negative")


def run_entry(original_env: str, entry: dict) -> None:
    flags = {key: env_value(value) for key, value in entry["flags"].items()}
    write_env(original_env, flags)
    run([*ALL, "down", "-v", "--remove-orphans"])
    run(["make", "up", "OBS=false"], timeout=UP_TIMEOUT_SECONDS)
    wait_healthy()
    stock_total = seed()
    check_flags(flags)
    in_stock = psql("SELECT id FROM products WHERE stock_quantity >= 1 ORDER BY id LIMIT 1")
    out_of_stock = psql("SELECT id FROM products WHERE stock_quantity = 0 ORDER BY id LIMIT 1")
    check_order(flags, in_stock, "paid")
    check_order(flags, out_of_stock, "rejected")
    check_stock(stock_total)


def capture_logs(name: str, reason: str, details: str) -> Path:
    LOGS.mkdir(parents=True, exist_ok=True)
    path = LOGS / f"{name}.log"
    try:
        logs = run([*APP, "logs", "--no-color", "--timestamps"])
    except Failed as error:
        logs = f"docker compose logs failed: {error}\n"
    path.write_text(f"{reason}\n\n{details}\n{logs}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("entry", nargs="?", help="run only this entry of bench/matrix.yaml")
    args = parser.parse_args()

    entries = yaml.safe_load(MATRIX.read_text())["entries"]
    if args.entry:
        entries = [entry for entry in entries if entry["name"] == args.entry]
        if not entries:
            sys.exit(f"no entry {args.entry} in {MATRIX}")

    original_env = ENV_FILE.read_text()
    results = []
    try:
        for entry in entries:
            name = entry["name"]
            print(f"{name} ...", flush=True)
            started = time.monotonic()
            reason = ""
            try:
                run_entry(original_env, entry)
            except Exception as error:  # noqa: BLE001 - one broken entry must not stop the others
                reason = str(error) if isinstance(error, Failed) else repr(error)
                path = capture_logs(name, reason, traceback.format_exc())
                reason = f"{reason} (logs: {path})"
            finally:
                try:
                    run([*ALL, "down", "-v", "--remove-orphans"])
                except Failed as error:
                    reason = reason or f"teardown failed: {error}"
            duration = time.monotonic() - started
            results.append((name, "FAIL" if reason else "PASS", duration, reason))
            print(f"{name} {results[-1][1]} in {duration:.0f} s {reason}", flush=True)
    finally:
        ENV_FILE.write_text(original_env)

    width = max(len(name) for name, _, _, _ in results)
    print(f"\n{'entry':<{width}}  result  duration  reason")
    for name, result, duration, reason in results:
        print(f"{name:<{width}}  {result:<6}  {duration:>6.0f} s  {reason}")
    sys.exit(1 if any(result == "FAIL" for _, result, _, _ in results) else 0)


main()
