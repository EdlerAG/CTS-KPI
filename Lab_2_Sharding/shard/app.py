"""Вузол-шард key-value сховища з автореєстрацією в координаторі."""

import asyncio
from contextlib import asynccontextmanager
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from storage import ShardStorage


class InternalWrite(BaseModel):
    table: str
    partition_key: str
    sort_key: str | None = None
    value: Any


SHARD_ID = os.getenv("SHARD_ID", "shard-1")
SHARD_URL = os.getenv("SHARD_URL", "http://shard-1:8001")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
DB_PATH = os.getenv("DB_PATH", "/data/shard.db")

storage = ShardStorage(DB_PATH)


async def _register_loop(stop_event: asyncio.Event) -> None:
    """Реєструє шард при старті та підтримує реєстрацію у разі рестарту координатора."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        while not stop_event.is_set():
            try:
                await client.post(
                    f"{COORDINATOR_URL.rstrip('/')}/shards/register",
                    json={"shard_id": SHARD_ID, "url": SHARD_URL},
                )
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    task = asyncio.create_task(_register_loop(stop_event))
    yield
    stop_event.set()
    await task


app = FastAPI(title=f"KV Shard ({SHARD_ID})", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "shard_id": SHARD_ID, "keys_count": storage.count_keys()}


@app.post("/internal/records", status_code=201)
async def put_record(req: InternalWrite) -> dict[str, Any]:
    storage.put(req.table, req.partition_key, req.sort_key, req.value)
    return {
        "status": "stored",
        "table": req.table,
        "partition_key": req.partition_key,
        "sort_key": req.sort_key,
        "shard_id": SHARD_ID,
    }


@app.get("/internal/records/exists")
async def exists_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    return {
        "exists": storage.exists(table, partition_key, sort_key),
        "table": table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "shard_id": SHARD_ID,
    }


@app.get("/internal/records")
async def get_records(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    items = storage.get(table, partition_key, sort_key)
    if not items:
        raise HTTPException(status_code=404, detail="Record not found")
    if sort_key is not None:
        return {"table": table, "item": items[0], "shard_id": SHARD_ID}
    return {"table": table, "items": items, "count": len(items), "shard_id": SHARD_ID}


@app.delete("/internal/records")
async def delete_record(
    table: str = Query(...),
    partition_key: str = Query(...),
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    deleted = storage.delete(table, partition_key, sort_key)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Record not found")
    return {
        "status": "deleted",
        "table": table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "shard_id": SHARD_ID,
    }
