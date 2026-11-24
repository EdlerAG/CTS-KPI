"""Вузол репліки шарда з Prometheus-метриками, JSON-логами (trace_id), LWW і /health /version."""

import asyncio
from contextlib import asynccontextmanager
import contextvars
import json
import logging
import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from storage import LWWShardStorage

SHARD_ID = os.getenv("SHARD_ID", "shard-1")
REPLICA_ID = os.getenv("REPLICA_ID", "shard-1-leader")
ROLE = os.getenv("ROLE", "leader").lower()
REPLICA_URL = os.getenv("REPLICA_URL", "http://shard-1-leader:8001")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
WAL_URL = os.getenv("WAL_URL", "http://wal-service:8010")
DB_PATH = os.getenv("DB_PATH", "/data/shard.db")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "0.1"))
BUILD_VERSION = os.getenv("BUILD_VERSION", "1.4.0")
BUILD_SHA = os.getenv("BUILD_SHA", "lab4-local")

trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "level": record.levelname,
                "service": REPLICA_ID,
                "shard_id": SHARD_ID,
                "role": ROLE,
                "trace_id": trace_id_var.get(),
                "message": record.getMessage(),
            },
            ensure_ascii=False,
        )


logger = logging.getLogger(REPLICA_ID)
logger.setLevel(logging.INFO)
_h = logging.StreamHandler()
_h.setFormatter(JsonFormatter())
logger.handlers = [_h]

# Prometheus метрики репліки
REQ_COUNTER = Counter(
    "kv_http_requests_total",
    "Total HTTP requests on shard replica",
    ["service", "method", "path", "status"],
)
REQ_LATENCY = Histogram(
    "kv_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "path"],
    buckets=[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
KEYS_GAUGE = Gauge("kv_shard_keys_count", "Live keys stored on replica", ["shard_id", "replica_id", "role"])
LAG_OFFSETS_GAUGE = Gauge("kv_replication_lag_offsets", "Pending WAL offsets on replica", ["shard_id", "replica_id"])
LAG_SECONDS_GAUGE = Gauge("kv_replication_lag_seconds", "Replication lag in seconds", ["shard_id", "replica_id"])


class InternalWrite(BaseModel):
    table: str
    partition_key: str
    sort_key: str | None = None
    value: Any = None
    version_ts_ns: int | None = None
    node_id: str | None = None
    w: int = 2
    follower_urls: list[str] = []


class InternalMigrateCleanup(BaseModel):
    table: str
    partition_key: str
    sort_key: str | None = None


storage = LWWShardStorage(DB_PATH)
http_client: httpx.AsyncClient | None = None
_last_ts_ns = 0


def _next_monotonic_ns(requested_ts: int | None) -> int:
    global _last_ts_ns
    if requested_ts is not None:
        return requested_ts
    now = time.time_ns()
    if now <= _last_ts_ns:
        now = _last_ts_ns + 1
    _last_ts_ns = now
    return now


async def _register_loop(stop_event: asyncio.Event) -> None:
    assert http_client is not None
    while not stop_event.is_set():
        try:
            await http_client.post(
                f"{COORDINATOR_URL.rstrip('/')}/replicas/register",
                json={
                    "shard_id": SHARD_ID,
                    "replica_id": REPLICA_ID,
                    "role": ROLE,
                    "url": REPLICA_URL,
                },
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            continue


async def _replication_and_metrics_loop(stop_event: asyncio.Event) -> None:
    assert http_client is not None
    while not stop_event.is_set():
        try:
            next_offset = storage.get_last_offset() + 1
            resp = await http_client.get(
                f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
                params={"from_offset": next_offset},
            )
            if resp.status_code == 200:
                payload = resp.json()
                latest_offset = int(payload.get("latest_offset", 0))
                latest_ts_ns = int(payload.get("latest_ts_ns", 0))
                events = payload.get("events", [])
                if ROLE == "follower":
                    for ev in events:
                        storage.apply_lww(
                            op=ev["op"],
                            table=ev["table"],
                            partition_key=ev["partition_key"],
                            sort_key=ev.get("sort_key"),
                            value=ev.get("value"),
                            version_ts_ns=int(ev["version_ts_ns"]),
                            node_id=str(ev["node_id"]),
                            offset=int(ev["offset"]),
                        )
                cur_offset = storage.get_last_offset()
                lag_offsets = max(0, latest_offset - cur_offset)
                LAG_OFFSETS_GAUGE.labels(SHARD_ID, REPLICA_ID).set(lag_offsets)
                if lag_offsets > 0 and latest_ts_ns > 0:
                    LAG_SECONDS_GAUGE.labels(SHARD_ID, REPLICA_ID).set(max(0.0, (time.time_ns() - latest_ts_ns) / 1e9))
                else:
                    LAG_SECONDS_GAUGE.labels(SHARD_ID, REPLICA_ID).set(0.0)
            KEYS_GAUGE.labels(SHARD_ID, REPLICA_ID, ROLE).set(storage.count_keys())
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL_SEC)
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=3.0)
    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(_register_loop(stop_event)),
        asyncio.create_task(_replication_and_metrics_loop(stop_event)),
    ]
    yield
    stop_event.set()
    for t in tasks:
        await t
    await http_client.aclose()


app = FastAPI(title=f"KV Replica ({REPLICA_ID})", version=BUILD_VERSION, lifespan=lifespan)


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    tid = request.headers.get("X-Trace-Id", "-")
    token = trace_id_var.set(tid)
    t0 = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Trace-Id"] = tid
        return response
    finally:
        elapsed = time.perf_counter() - t0
        path = request.url.path
        if path != "/metrics":
            REQ_COUNTER.labels(REPLICA_ID, request.method, path, str(status_code)).inc()
            REQ_LATENCY.labels(REPLICA_ID, request.method, path).observe(elapsed)
            logger.info(f"{request.method} {path} -> {status_code} ({elapsed * 1000:.2f}ms)")
        trace_id_var.reset(token)


async def _wait_for_quorum(target_offset: int, w: int, follower_urls: list[str]) -> int:
    acks = 1
    if w <= 1:
        return acks
    assert http_client is not None
    needed = w - 1
    deadline = asyncio.get_running_loop().time() + 3.0
    acked: set[str] = set()
    headers = {"X-Trace-Id": trace_id_var.get()}
    while asyncio.get_running_loop().time() < deadline:
        for url in follower_urls:
            if url in acked:
                continue
            try:
                resp = await http_client.get(f"{url.rstrip('/')}/internal/status", headers=headers, timeout=0.5)
                if resp.status_code == 200 and resp.json().get("last_applied_offset", 0) >= target_offset:
                    acked.add(url)
            except Exception:
                pass
        if len(acked) >= needed:
            return acks + len(acked)
        await asyncio.sleep(0.05)
    raise HTTPException(status_code=503, detail=f"Write quorum W={w} not met")


@app.get("/metrics")
async def metrics() -> Response:
    KEYS_GAUGE.labels(SHARD_ID, REPLICA_ID, ROLE).set(storage.count_keys())
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
        "role": ROLE,
        "last_applied_offset": storage.get_last_offset(),
        "keys_count": storage.count_keys(),
    }


@app.get("/version")
async def version() -> dict[str, str]:
    return {"service": REPLICA_ID, "shard_id": SHARD_ID, "version": BUILD_VERSION, "build_sha": BUILD_SHA}


@app.get("/internal/status")
async def internal_status() -> dict[str, Any]:
    return {"shard_id": SHARD_ID, "replica_id": REPLICA_ID, "last_applied_offset": storage.get_last_offset()}


@app.post("/internal/records", status_code=201)
async def write_record(req: InternalWrite) -> dict[str, Any]:
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="Only shard leader accepts writes")
    assert http_client is not None

    ver_ts = _next_monotonic_ns(req.version_ts_ns)
    node_id = req.node_id or REPLICA_ID
    headers = {"X-Trace-Id": trace_id_var.get()}

    wal_resp = await http_client.post(
        f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
        headers=headers,
        json={
            "op": "put",
            "table": req.table,
            "partition_key": req.partition_key,
            "sort_key": req.sort_key,
            "value": req.value,
            "version_ts_ns": ver_ts,
            "node_id": node_id,
        },
    )
    if wal_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="WAL append failed")
    offset = int(wal_resp.json()["offset"])

    applied = storage.apply_lww(
        "put", req.table, req.partition_key, req.sort_key, req.value, ver_ts, node_id, offset=offset
    )
    acks = await _wait_for_quorum(offset, req.w, req.follower_urls)
    return {
        "status": "stored" if applied else "ignored_stale_lww",
        "table": req.table,
        "partition_key": req.partition_key,
        "sort_key": req.sort_key,
        "shard_id": SHARD_ID,
        "leader": REPLICA_ID,
        "version_ts_ns": ver_ts,
        "node_id": node_id,
        "offset": offset,
        "write_acks": acks,
    }


@app.delete("/internal/records")
async def delete_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
    version_ts_ns: int | None = Query(default=None),
    w: int = Query(default=2),
    follower_urls: list[str] = Query(default=[]),
) -> dict[str, Any]:
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="Only shard leader accepts deletes")
    assert http_client is not None

    exists_now, _, _ = storage.exists(table, partition_key, sort_key)
    if not exists_now:
        raise HTTPException(status_code=404, detail="Record not found")

    ver_ts = _next_monotonic_ns(version_ts_ns)
    headers = {"X-Trace-Id": trace_id_var.get()}
    wal_resp = await http_client.post(
        f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
        headers=headers,
        json={
            "op": "delete",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "value": None,
            "version_ts_ns": ver_ts,
            "node_id": REPLICA_ID,
        },
    )
    if wal_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="WAL append failed")
    offset = int(wal_resp.json()["offset"])

    storage.apply_lww("delete", table, partition_key, sort_key, None, ver_ts, REPLICA_ID, offset=offset)
    acks = await _wait_for_quorum(offset, w, follower_urls)
    return {
        "status": "deleted",
        "table": table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "shard_id": SHARD_ID,
        "version_ts_ns": ver_ts,
        "offset": offset,
        "write_acks": acks,
    }


@app.get("/internal/records/exists")
async def exists_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    found, ver_ts, node_id = storage.exists(table, partition_key, sort_key)
    return {
        "exists": found,
        "version_ts_ns": ver_ts,
        "node_id": node_id,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }


@app.get("/internal/records")
async def get_records(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    items, ver_ts, node_id = storage.get(table, partition_key, sort_key)
    return {
        "found": len(items) > 0,
        "version_ts_ns": ver_ts,
        "node_id": node_id,
        "items": items,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }


@app.get("/internal/records/scan")
async def scan_records() -> dict[str, Any]:
    return {"shard_id": SHARD_ID, "items": storage.scan_all_live()}


@app.post("/internal/records/cleanup")
async def cleanup_migrated(req: InternalMigrateCleanup) -> dict[str, str]:
    storage.hard_delete_migrated(req.table, req.partition_key, req.sort_key)
    return {"status": "cleaned"}
