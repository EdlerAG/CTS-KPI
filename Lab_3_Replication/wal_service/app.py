"""Append-only лог подій (WAL) на диску для реплікації шардів (Мілстоун 3)."""

import json
import os
import threading
from typing import Any

from fastapi import FastAPI, Query
from pydantic import BaseModel


class LogAppendRequest(BaseModel):
    op: str  # "put" | "delete"
    table: str
    partition_key: str
    sort_key: str | None = None
    value: Any = None


LOG_DIR = os.getenv("LOG_DIR", "/logs")
os.makedirs(LOG_DIR, exist_ok=True)

_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()


def _get_lock(shard_id: str) -> threading.Lock:
    with _global_lock:
        if shard_id not in _locks:
            _locks[shard_id] = threading.Lock()
        return _locks[shard_id]


def _log_path(shard_id: str) -> str:
    safe = "".join(c for c in shard_id if c.isalnum() or c in "-_")
    return os.path.join(LOG_DIR, f"{safe}.jsonl")


def _read_all(shard_id: str) -> list[dict[str, Any]]:
    path = _log_path(shard_id)
    if not os.path.exists(path):
        return []
    entries: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


app = FastAPI(title="KV Append-Only WAL Service (Lab 3)", version="1.0.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "wal-service"}


@app.post("/logs/{shard_id}", status_code=201)
async def append_event(shard_id: str, req: LogAppendRequest) -> dict[str, Any]:
    lock = _get_lock(shard_id)
    with lock:
        existing = _read_all(shard_id)
        next_offset = (existing[-1]["offset"] + 1) if existing else 1
        entry = {
            "offset": next_offset,
            "shard_id": shard_id,
            "op": req.op,
            "table": req.table,
            "partition_key": req.partition_key,
            "sort_key": req.sort_key,
            "value": req.value,
        }
        path = _log_path(shard_id)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    return entry


@app.get("/logs/{shard_id}")
async def read_events(shard_id: str, from_offset: int = Query(default=1, ge=1)) -> dict[str, Any]:
    lock = _get_lock(shard_id)
    with lock:
        entries = _read_all(shard_id)
    filtered = [e for e in entries if e["offset"] >= from_offset]
    latest_offset = entries[-1]["offset"] if entries else 0
    return {"shard_id": shard_id, "latest_offset": latest_offset, "events": filtered}


@app.get("/logs/{shard_id}/latest")
async def latest_offset(shard_id: str) -> dict[str, Any]:
    lock = _get_lock(shard_id)
    with lock:
        entries = _read_all(shard_id)
    return {"shard_id": shard_id, "latest_offset": entries[-1]["offset"] if entries else 0}
