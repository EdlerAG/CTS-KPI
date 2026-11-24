"""Координатор із підтримкою реплікації (3 репліки/шард), балансування читань та кворумів R + W > N."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from hashing import ConsistentHashRing


@dataclass
class ReplicaInfo:
    shard_id: str
    replica_id: str
    role: str  # "leader" | "follower"
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


DEFAULT_W = int(os.getenv("DEFAULT_W", "2"))
DEFAULT_R = int(os.getenv("DEFAULT_R", "2"))
VNODES_PER_SHARD = int(os.getenv("VNODES_PER_SHARD", "100"))

replica_sets: dict[str, dict[str, ReplicaInfo]] = {}
read_cursors: dict[str, int] = {}
tables: dict[str, TableDefinition] = {}
ring = ConsistentHashRing(vnodes_per_shard=VNODES_PER_SHARD)
http_client: httpx.AsyncClient | None = None


async def _health_probe_loop(stop_event: asyncio.Event) -> None:
    """Періодично перевіряє здоров'я реплік, щоб не слати читання на впалі вузли."""
    assert http_client is not None
    while not stop_event.is_set():
        for reps in list(replica_sets.values()):
            for rep in list(reps.values()):
                try:
                    resp = await http_client.get(f"{rep.url}/health", timeout=1.5)
                    if resp.status_code == 200:
                        rep.healthy = True
                        rep.last_applied_offset = int(resp.json().get("last_applied_offset", 0))
                    else:
                        rep.healthy = False
                except Exception:
                    rep.healthy = False
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


app = FastAPI(title="KV Store Coordinator (Lab 3 - Replication)", version="1.0.0", lifespan=lifespan)


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
    """Обирає R живих реплік для читання з обертанням round-robin (не тільки лідер)."""
    healthy_reps = sorted(
        [r for r in replica_sets.get(shard_id, {}).values() if r.healthy],
        key=lambda x: x.replica_id,
    )
    if len(healthy_reps) < r_count:
        raise HTTPException(
            status_code=503,
            detail=f"Read quorum R={r_count} not met for {shard_id} (healthy: {len(healthy_reps)})",
        )
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


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
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
    ring.add_shard(reg.shard_id)
    return {"status": "registered", "shard_id": reg.shard_id, "replica_id": reg.replica_id}


@app.get("/shards")
async def list_shards() -> dict[str, Any]:
    return await health()


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

    try:
        resp = await http_client.post(
            f"{leader.url}/internal/records",
            json={
                "table": table_name,
                "partition_key": req.partition_key,
                "sort_key": req.sort_key,
                "value": req.value,
                "w": w,
                "follower_urls": [f.url for f in followers],
            },
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Leader communication failed: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


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

    responses = await asyncio.gather(
        *[http_client.get(f"{rep.url}/internal/records/exists", params=params) for rep in targets],
        return_exceptions=True,
    )
    valid = [resp.json() for resp in responses if isinstance(resp, httpx.Response) and resp.status_code == 200]
    if len(valid) < r:
        raise HTTPException(status_code=503, detail=f"Read quorum R={r} failed during request")

    freshest = max(valid, key=lambda d: int(d.get("version", 0)))
    return {
        "exists": freshest["exists"],
        "version": freshest["version"],
        "shard_id": shard_id,
        "queried_replicas": [d["replica_id"] for d in valid],
        "selected_replica": freshest["replica_id"],
        "r": r,
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

    responses = await asyncio.gather(
        *[http_client.get(f"{rep.url}/internal/records", params=params) for rep in targets],
        return_exceptions=True,
    )
    valid = [resp.json() for resp in responses if isinstance(resp, httpx.Response) and resp.status_code == 200]
    if len(valid) < r:
        raise HTTPException(status_code=503, detail=f"Read quorum R={r} failed during request")

    freshest = max(valid, key=lambda d: int(d.get("version", 0)))
    if not freshest.get("found"):
        raise HTTPException(status_code=404, detail="Record not found")

    result: dict[str, Any] = {
        "table": table_name,
        "shard_id": shard_id,
        "version": freshest["version"],
        "queried_replicas": [d["replica_id"] for d in valid],
        "selected_replica": freshest["replica_id"],
        "r": r,
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
    for f in followers:
        params.append(("follower_urls", f.url))

    resp = await http_client.delete(f"{leader.url}/internal/records", params=params)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Record not found")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()
