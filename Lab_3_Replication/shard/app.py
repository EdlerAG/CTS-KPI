"""Вузол репліки шарда (leader або follower) з WAL-реплікацією та кворумом."""

import asyncio
from contextlib import asynccontextmanager
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from storage import ReplicatedShardStorage


class InternalWrite(BaseModel):
    table: str
    partition_key: str
    sort_key: str | None = None
    value: Any = None
    w: int = 2
    follower_urls: list[str] = []


SHARD_ID = os.getenv("SHARD_ID", "shard-1")
REPLICA_ID = os.getenv("REPLICA_ID", "shard-1-leader")
ROLE = os.getenv("ROLE", "leader").lower()  # "leader" | "follower"
REPLICA_URL = os.getenv("REPLICA_URL", "http://shard-1-leader:8001")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
WAL_URL = os.getenv("WAL_URL", "http://wal-service:8010")
DB_PATH = os.getenv("DB_PATH", "/data/shard.db")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "0.1"))

storage = ReplicatedShardStorage(DB_PATH)
http_client: httpx.AsyncClient | None = None


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


async def _follower_sync_loop(stop_event: asyncio.Event) -> None:
    """Фолловер читає append-only лог починаючи з last_applied_offset + 1."""
    assert http_client is not None
    while not stop_event.is_set():
        try:
            next_offset = storage.get_last_offset() + 1
            resp = await http_client.get(
                f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
                params={"from_offset": next_offset},
            )
            if resp.status_code == 200:
                events = resp.json().get("events", [])
                for ev in events:
                    storage.apply_event(
                        offset=ev["offset"],
                        op=ev["op"],
                        table=ev["table"],
                        partition_key=ev["partition_key"],
                        sort_key=ev.get("sort_key"),
                        value=ev.get("value"),
                    )
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
    tasks = [asyncio.create_task(_register_loop(stop_event))]
    if ROLE == "follower":
        tasks.append(asyncio.create_task(_follower_sync_loop(stop_event)))
    yield
    stop_event.set()
    for t in tasks:
        await t
    await http_client.aclose()


app = FastAPI(title=f"KV Replica ({REPLICA_ID})", version="1.0.0", lifespan=lifespan)


async def _wait_for_quorum(target_offset: int, w: int, follower_urls: list[str]) -> int:
    """Чекає підтвердження застосування offset від (w - 1) фолловерів."""
    acks = 1  # Лідер уже застосував запис
    if w <= 1:
        return acks
    assert http_client is not None

    needed_followers = w - 1
    deadline = asyncio.get_running_loop().time() + 3.0
    acked_urls: set[str] = set()

    while asyncio.get_running_loop().time() < deadline:
        for url in follower_urls:
            if url in acked_urls:
                continue
            try:
                resp = await http_client.get(f"{url.rstrip('/')}/internal/status", timeout=0.5)
                if resp.status_code == 200 and resp.json().get("last_applied_offset", 0) >= target_offset:
                    acked_urls.add(url)
            except Exception:
                pass
        if len(acked_urls) >= needed_followers:
            return acks + len(acked_urls)
        await asyncio.sleep(0.05)

    raise HTTPException(
        status_code=503,
        detail=f"Write quorum W={w} not met (acked {1 + len(acked_urls)}/{w} replicas)",
    )


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


@app.get("/internal/status")
async def internal_status() -> dict[str, Any]:
    return {
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
        "role": ROLE,
        "last_applied_offset": storage.get_last_offset(),
    }


@app.post("/internal/records", status_code=201)
async def write_record(req: InternalWrite) -> dict[str, Any]:
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="Only shard leader accepts writes")
    assert http_client is not None

    wal_resp = await http_client.post(
        f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
        json={
            "op": "put",
            "table": req.table,
            "partition_key": req.partition_key,
            "sort_key": req.sort_key,
            "value": req.value,
        },
    )
    if wal_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="Failed to append to WAL")
    event = wal_resp.json()
    offset = int(event["offset"])

    storage.apply_event(offset, "put", req.table, req.partition_key, req.sort_key, req.value)

    acks = await _wait_for_quorum(offset, req.w, req.follower_urls)
    return {
        "status": "stored",
        "table": req.table,
        "partition_key": req.partition_key,
        "sort_key": req.sort_key,
        "shard_id": SHARD_ID,
        "leader": REPLICA_ID,
        "version": offset,
        "write_acks": acks,
        "w": req.w,
    }


@app.delete("/internal/records")
async def delete_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
    w: int = Query(default=2),
    follower_urls: list[str] = Query(default=[]),
) -> dict[str, Any]:
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="Only shard leader accepts deletes")
    assert http_client is not None

    exists_now, _ = storage.exists(table, partition_key, sort_key)
    if not exists_now:
        raise HTTPException(status_code=404, detail="Record not found")

    wal_resp = await http_client.post(
        f"{WAL_URL.rstrip('/')}/logs/{SHARD_ID}",
        json={
            "op": "delete",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "value": None,
        },
    )
    if wal_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="Failed to append delete to WAL")
    offset = int(wal_resp.json()["offset"])

    storage.apply_event(offset, "delete", table, partition_key, sort_key, None)
    acks = await _wait_for_quorum(offset, w, follower_urls)
    return {
        "status": "deleted",
        "table": table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "shard_id": SHARD_ID,
        "leader": REPLICA_ID,
        "version": offset,
        "write_acks": acks,
        "w": w,
    }


@app.get("/internal/records/exists")
async def exists_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    found, version = storage.exists(table, partition_key, sort_key)
    return {
        "exists": found,
        "version": version,
        "table": table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }


@app.get("/internal/records")
async def get_records(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    items, version = storage.get(table, partition_key, sort_key)
    if not items:
        return {
            "found": False,
            "version": version,
            "table": table,
            "items": [],
            "shard_id": SHARD_ID,
            "replica_id": REPLICA_ID,
        }
    return {
        "found": True,
        "version": version,
        "table": table,
        "items": items,
        "shard_id": SHARD_ID,
        "replica_id": REPLICA_ID,
    }
