"""Координатор Лаби 4: Prometheus метрики, JSON trace_id логи, LWW, /health /version, фонове ребалансування."""

import asyncio
from contextlib import asynccontextmanager
import contextvars
from dataclasses import dataclass
import json
import logging
import os
import time
from typing import Any
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

from hashing import ConsistentHashRing

BUILD_VERSION = os.getenv("BUILD_VERSION", "1.4.0")
BUILD_SHA = os.getenv("BUILD_SHA", "lab4-local")
DEFAULT_W = int(os.getenv("DEFAULT_W", "2"))
DEFAULT_R = int(os.getenv("DEFAULT_R", "2"))
VNODES_PER_SHARD = int(os.getenv("VNODES_PER_SHARD", "100"))
AUTO_REBALANCE = os.getenv("AUTO_REBALANCE", "true").lower() == "true"

trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "level": record.levelname,
                "service": "coordinator",
                "trace_id": trace_id_var.get(),
                "message": record.getMessage(),
            },
            ensure_ascii=False,
        )


logger = logging.getLogger("coordinator")
logger.setLevel(logging.INFO)
_h = logging.StreamHandler()
_h.setFormatter(JsonFormatter())
logger.handlers = [_h]

# Prometheus метрики координатора
REQ_COUNTER = Counter(
    "kv_http_requests_total",
    "Total HTTP requests on coordinator",
    ["service", "method", "path", "status"],
)
REQ_ERRORS = Counter(
    "kv_http_errors_total",
    "Total 4xx/5xx HTTP errors on coordinator",
    ["service", "method", "path", "status"],
)
REQ_LATENCY = Histogram(
    "kv_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "path"],
    buckets=[0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)
NODE_UP = Gauge("kv_node_up", "Health state of cluster replica (1=up, 0=down)", ["shard_id", "replica_id", "role"])
REBALANCE_MOVED_TOTAL = Counter("kv_rebalance_keys_moved_total", "Total keys migrated during background rebalance")


@dataclass
class ReplicaInfo:
    shard_id: str
    replica_id: str
    role: str
    url: str
    healthy: bool = True
    last_applied_offset: int = 0


class ReplicaRegistration(BaseModel):
    shard_id: str
    replica_id: str
    role: str
    url: str


class TableDefinition(BaseModel):
    name: str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_-]+$")
    partition_key: str = Field(..., min_length=1)
    sort_key: str | None = Field(default=None)


class RecordWriteRequest(BaseModel):
    partition_key: str = Field(..., min_length=1)
    sort_key: str | None = Field(default=None)
    value: Any
    version_ts_ns: int | None = Field(default=None, description="Опціональний timestamp для LWW-конфліктів")


replica_sets: dict[str, dict[str, ReplicaInfo]] = {}
read_cursors: dict[str, int] = {}
tables: dict[str, TableDefinition] = {}
ring = ConsistentHashRing(vnodes_per_shard=VNODES_PER_SHARD)
http_client: httpx.AsyncClient | None = None

rebalance_status: dict[str, Any] = {
    "state": "idle",
    "scanned": 0,
    "moved": 0,
    "errors": 0,
    "last_run_ts": None,
}
_rebalance_lock = asyncio.Lock()
_rebalanced_shards: set[str] = set()


async def _run_rebalance() -> None:
    """Фонове перенесення ключів на новий шард після зміни топології кільця."""
    if _rebalance_lock.locked():
        return
    async with _rebalance_lock:
        assert http_client is not None
        rebalance_status.update({"state": "running", "scanned": 0, "moved": 0, "errors": 0})
        # Коротка пауза, щоб фолловери нового шарда встигли зареєструватися перед міграцією
        await asyncio.sleep(1.0)
        tid = f"rebal-{uuid.uuid4().hex[:8]}"
        token = trace_id_var.set(tid)
        headers = {"X-Trace-Id": tid}
        try:
            leaders: dict[str, ReplicaInfo] = {}
            followers_map: dict[str, list[str]] = {}
            for sid, reps in list(replica_sets.items()):
                l_list = [r for r in reps.values() if r.role == "leader" and r.healthy]
                if l_list:
                    leaders[sid] = l_list[0]
                    followers_map[sid] = [r.url for r in reps.values() if r.role == "follower" and r.healthy]

            for src_shard, src_leader in leaders.items():
                try:
                    scan_resp = await http_client.get(f"{src_leader.url}/internal/records/scan", headers=headers)
                    if scan_resp.status_code != 200:
                        continue
                    items = scan_resp.json().get("items", [])
                    for item in items:
                        rebalance_status["scanned"] += 1
                        target_shard = ring.get_shard(item["partition_key"])
                        if target_shard == src_shard or target_shard not in leaders:
                            continue
                        dst_leader = leaders[target_shard]
                        w_count = min(DEFAULT_W, 1 + len(followers_map.get(target_shard, [])))
                        put_resp = await http_client.post(
                            f"{dst_leader.url}/internal/records",
                            headers=headers,
                            json={
                                "table": item["table"],
                                "partition_key": item["partition_key"],
                                "sort_key": item.get("sort_key"),
                                "value": item["value"],
                                "version_ts_ns": item["version_ts_ns"],
                                "node_id": item["node_id"],
                                "w": w_count,
                                "follower_urls": followers_map.get(target_shard, []),
                            },
                        )
                        if put_resp.status_code < 400:
                            await http_client.post(
                                f"{src_leader.url}/internal/records/cleanup",
                                headers=headers,
                                json={
                                    "table": item["table"],
                                    "partition_key": item["partition_key"],
                                    "sort_key": item.get("sort_key"),
                                },
                            )
                            rebalance_status["moved"] += 1
                            REBALANCE_MOVED_TOTAL.inc()
                        else:
                            rebalance_status["errors"] += 1
                except Exception as exc:
                    rebalance_status["errors"] += 1
                    logger.error(f"Rebalance error on {src_shard}: {exc}")
            logger.info(
                f"Background rebalance finished: scanned={rebalance_status['scanned']} moved={rebalance_status['moved']}"
            )
        finally:
            rebalance_status["state"] = "completed"
            rebalance_status["last_run_ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            trace_id_var.reset(token)


async def _health_probe_loop(stop_event: asyncio.Event) -> None:
    assert http_client is not None
    while not stop_event.is_set():
        for reps in list(replica_sets.values()):
            for rep in list(reps.values()):
                try:
                    resp = await http_client.get(f"{rep.url}/health", timeout=1.5)
                    if resp.status_code == 200:
                        rep.healthy = True
                        rep.last_applied_offset = int(resp.json().get("last_applied_offset", 0))
                        NODE_UP.labels(rep.shard_id, rep.replica_id, rep.role).set(1)
                    else:
                        rep.healthy = False
                        NODE_UP.labels(rep.shard_id, rep.replica_id, rep.role).set(0)
                except Exception:
                    rep.healthy = False
                    NODE_UP.labels(rep.shard_id, rep.replica_id, rep.role).set(0)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    stop_event = asyncio.Event()
    task = asyncio.create_task(_health_probe_loop(stop_event))
    yield
    stop_event.set()
    await task
    await http_client.aclose()


app = FastAPI(title="KV Store Coordinator (Lab 4 - Observability & MCP)", version=BUILD_VERSION, lifespan=lifespan)


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    tid = request.headers.get("X-Trace-Id") or uuid.uuid4().hex
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
            REQ_COUNTER.labels("coordinator", request.method, path, str(status_code)).inc()
            if status_code >= 400:
                REQ_ERRORS.labels("coordinator", request.method, path, str(status_code)).inc()
            REQ_LATENCY.labels("coordinator", request.method, path).observe(elapsed)
            logger.info(f"{request.method} {path} -> {status_code} ({elapsed * 1000:.2f}ms)")
        trace_id_var.reset(token)


def _select_shard_id(partition_key: str) -> str:
    if not replica_sets:
        raise HTTPException(status_code=503, detail="No shards registered")
    return ring.get_shard(partition_key)


def _get_leader_and_followers(shard_id: str) -> tuple[ReplicaInfo, list[ReplicaInfo]]:
    reps = list(replica_sets.get(shard_id, {}).values())
    leaders = [r for r in reps if r.role == "leader" and r.healthy]
    if not leaders:
        raise HTTPException(status_code=503, detail=f"No healthy leader for {shard_id}")
    followers = [r for r in reps if r.role == "follower" and r.healthy]
    return leaders[0], followers


def _pick_read_replicas(shard_id: str, r_count: int) -> list[ReplicaInfo]:
    healthy_reps = sorted(
        [r for r in replica_sets.get(shard_id, {}).values() if r.healthy],
        key=lambda x: x.replica_id,
    )
    if len(healthy_reps) < r_count:
        raise HTTPException(status_code=503, detail=f"Read quorum R={r_count} not met for {shard_id}")
    cursor = read_cursors.get(shard_id, 0) % len(healthy_reps)
    chosen = [healthy_reps[(cursor + i) % len(healthy_reps)] for i in range(r_count)]
    read_cursors[shard_id] = (cursor + 1) % len(healthy_reps)
    return chosen


def _validate_table_and_key(table_name: str, sort_key: str | None, require_sort_key: bool) -> TableDefinition:
    table = tables.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    if require_sort_key and table.sort_key and not sort_key:
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' requires sort_key '{table.sort_key}'")
    if not table.sort_key and sort_key is not None:
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' has no sort_key")
    return table


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "coordinator",
        "version": BUILD_VERSION,
        "shards_count": len(replica_sets),
    }


@app.get("/version")
async def version() -> dict[str, str]:
    return {"service": "coordinator", "version": BUILD_VERSION, "build_sha": BUILD_SHA}


@app.post("/replicas/register")
async def register_replica(reg: ReplicaRegistration) -> dict[str, Any]:
    shard_reps = replica_sets.setdefault(reg.shard_id, {})
    shard_reps[reg.replica_id] = ReplicaInfo(
        shard_id=reg.shard_id,
        replica_id=reg.replica_id,
        role=reg.role.lower(),
        url=reg.url.rstrip("/"),
        healthy=True,
    )
    NODE_UP.labels(reg.shard_id, reg.replica_id, reg.role.lower()).set(1)
    ring.add_shard(reg.shard_id)
    if reg.role.lower() == "leader":
        if AUTO_REBALANCE and len(replica_sets) >= 3 and reg.shard_id not in _rebalanced_shards:
            asyncio.create_task(_run_rebalance())
        _rebalanced_shards.add(reg.shard_id)
    return {"status": "registered", "shard_id": reg.shard_id, "replica_id": reg.replica_id}


@app.get("/shards")
async def list_shards() -> dict[str, Any]:
    return {
        "default_w": DEFAULT_W,
        "default_r": DEFAULT_R,
        "shards": {
            sid: [
                {
                    "replica_id": r.replica_id,
                    "role": r.role,
                    "healthy": r.healthy,
                    "offset": r.last_applied_offset,
                }
                for r in sorted(reps.values(), key=lambda x: x.replica_id)
            ]
            for sid, reps in sorted(replica_sets.items())
        },
    }


@app.post("/admin/rebalance")
async def trigger_rebalance() -> dict[str, Any]:
    asyncio.create_task(_run_rebalance())
    return {"status": "triggered", "rebalance": rebalance_status}


@app.get("/admin/rebalance/status")
async def get_rebalance_status() -> dict[str, Any]:
    return rebalance_status


@app.post("/tables", status_code=201)
async def create_table(table: TableDefinition) -> dict[str, Any]:
    if table.name in tables:
        raise HTTPException(status_code=409, detail=f"Table '{table.name}' already exists")
    tables[table.name] = table
    return {"status": "created", "table": table.model_dump()}


@app.get("/tables")
async def list_tables() -> dict[str, Any]:
    return {"tables": [t.model_dump() for t in tables.values()]}


@app.post("/tables/{table_name}/records", status_code=201)
async def create_record(
    table_name: str,
    req: RecordWriteRequest,
    w: int = Query(default=DEFAULT_W, ge=1, le=3),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, req.sort_key, require_sort_key=True)
    shard_id = _select_shard_id(req.partition_key)
    leader, followers = _get_leader_and_followers(shard_id)
    if 1 + len(followers) < w:
        raise HTTPException(
            status_code=503,
            detail=f"Write quorum W={w} not met for {shard_id} (healthy: {1 + len(followers)}/{w})",
        )
    assert http_client is not None

    headers = {"X-Trace-Id": trace_id_var.get()}
    try:
        resp = await http_client.post(
            f"{leader.url}/internal/records",
            headers=headers,
            json={
                "table": table_name,
                "partition_key": req.partition_key,
                "sort_key": req.sort_key,
                "value": req.value,
                "version_ts_ns": req.version_ts_ns,
                "w": w,
                "follower_urls": [f.url for f in followers],
            },
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Leader communication failed: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["trace_id"] = trace_id_var.get()
    return data


@app.get("/tables/{table_name}/records/{partition_key}/exists")
async def exists_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
    r: int = Query(default=DEFAULT_R, ge=1, le=3),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=False)
    shard_id = _select_shard_id(partition_key)
    targets = _pick_read_replicas(shard_id, r)
    assert http_client is not None

    params: dict[str, str] = {"table": table_name, "partition_key": partition_key}
    if sort_key is not None:
        params["sort_key"] = sort_key

    headers = {"X-Trace-Id": trace_id_var.get()}
    responses = await asyncio.gather(
        *[http_client.get(f"{rep.url}/internal/records/exists", params=params, headers=headers) for rep in targets],
        return_exceptions=True,
    )
    valid = [resp.json() for resp in responses if isinstance(resp, httpx.Response) and resp.status_code == 200]
    if len(valid) < r:
        raise HTTPException(status_code=503, detail=f"Read quorum R={r} failed")

    # Last-Write-Wins порівняння: (version_ts_ns, node_id)
    freshest = max(valid, key=lambda d: (int(d.get("version_ts_ns", 0)), str(d.get("node_id", ""))))
    return {
        "exists": freshest["exists"],
        "version_ts_ns": freshest["version_ts_ns"],
        "shard_id": shard_id,
        "queried_replicas": [d["replica_id"] for d in valid],
        "selected_replica": freshest["replica_id"],
        "trace_id": trace_id_var.get(),
    }


@app.get("/tables/{table_name}/records/{partition_key}")
async def read_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
    r: int = Query(default=DEFAULT_R, ge=1, le=3),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=False)
    shard_id = _select_shard_id(partition_key)
    targets = _pick_read_replicas(shard_id, r)
    assert http_client is not None

    params: dict[str, str] = {"table": table_name, "partition_key": partition_key}
    if sort_key is not None:
        params["sort_key"] = sort_key

    headers = {"X-Trace-Id": trace_id_var.get()}
    responses = await asyncio.gather(
        *[http_client.get(f"{rep.url}/internal/records", params=params, headers=headers) for rep in targets],
        return_exceptions=True,
    )
    valid = [resp.json() for resp in responses if isinstance(resp, httpx.Response) and resp.status_code == 200]
    if len(valid) < r:
        raise HTTPException(status_code=503, detail=f"Read quorum R={r} failed")

    freshest = max(valid, key=lambda d: (int(d.get("version_ts_ns", 0)), str(d.get("node_id", ""))))
    if not freshest.get("found"):
        raise HTTPException(status_code=404, detail="Record not found")

    result: dict[str, Any] = {
        "table": table_name,
        "shard_id": shard_id,
        "version_ts_ns": freshest["version_ts_ns"],
        "queried_replicas": [d["replica_id"] for d in valid],
        "selected_replica": freshest["replica_id"],
        "trace_id": trace_id_var.get(),
    }
    if sort_key is not None:
        result["item"] = freshest["items"][0]
    else:
        result["items"] = freshest["items"]
        result["count"] = len(freshest["items"])
    return result


@app.delete("/tables/{table_name}/records/{partition_key}")
async def delete_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
    version_ts_ns: int | None = Query(default=None),
    w: int = Query(default=DEFAULT_W, ge=1, le=3),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=True)
    shard_id = _select_shard_id(partition_key)
    leader, followers = _get_leader_and_followers(shard_id)
    if 1 + len(followers) < w:
        raise HTTPException(
            status_code=503,
            detail=f"Write quorum W={w} not met for {shard_id} (healthy: {1 + len(followers)}/{w})",
        )
    assert http_client is not None

    params: list[tuple[str, str]] = [
        ("table", table_name),
        ("partition_key", partition_key),
        ("w", str(w)),
    ]
    if sort_key is not None:
        params.append(("sort_key", sort_key))
    if version_ts_ns is not None:
        params.append(("version_ts_ns", str(version_ts_ns)))
    for f in followers:
        params.append(("follower_urls", f.url))

    headers = {"X-Trace-Id": trace_id_var.get()}
    resp = await http_client.delete(f"{leader.url}/internal/records", params=params, headers=headers)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Record not found")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["trace_id"] = trace_id_var.get()
    return data
