"""Append-only WAL-лог подій із трасуванням (trace_id) та LWW-версіонуванням (Мілстоун 4)."""

import contextvars
import json
import logging
import os
import threading
import time
from typing import Any

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel

trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "service": "wal-service",
            "trace_id": trace_id_var.get(),
            "message": record.getMessage(),
        }
        return json.dumps(payload, ensure_ascii=False)


logger = logging.getLogger("wal")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(JsonFormatter())
logger.handlers = [_handler]


class LogAppendRequest(BaseModel):
    op: str
    table: str
    partition_key: str
    sort_key: str | None = None
    value: Any = None
    version_ts_ns: int
    node_id: str


LOG_DIR = os.getenv("LOG_DIR", "/logs")
BUILD_VERSION = os.getenv("BUILD_VERSION", "1.4.0")
BUILD_SHA = os.getenv("BUILD_SHA", "lab4-local")
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


app = FastAPI(title="KV Append-Only WAL Service (Lab 4)", version=BUILD_VERSION)


@app.middleware("http")
async def trace_middleware(request: Request, call_next):
    tid = request.headers.get("X-Trace-Id", "-")
    token = trace_id_var.set(tid)
    try:
        response = await call_next(request)
        response.headers["X-Trace-Id"] = tid
        return response
    finally:
        trace_id_var.reset(token)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "wal-service"}


@app.get("/version")
async def version() -> dict[str, str]:
    return {"service": "wal-service", "version": BUILD_VERSION, "build_sha": BUILD_SHA}


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
            "version_ts_ns": req.version_ts_ns,
            "node_id": req.node_id,
            "created_at_ns": time.time_ns(),
        }
        path = _log_path(shard_id)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    logger.info(f"Appended {req.op} offset={next_offset} shard={shard_id} key={req.partition_key}")
    return entry


@app.get("/logs/{shard_id}")
async def read_events(shard_id: str, from_offset: int = Query(default=1, ge=1)) -> dict[str, Any]:
    lock = _get_lock(shard_id)
    with lock:
        entries = _read_all(shard_id)
    filtered = [e for e in entries if e["offset"] >= from_offset]
    latest_offset = entries[-1]["offset"] if entries else 0
    latest_ts_ns = entries[-1].get("created_at_ns", 0) if entries else 0
    return {
        "shard_id": shard_id,
        "latest_offset": latest_offset,
        "latest_ts_ns": latest_ts_ns,
        "events": filtered,
    }


@app.get("/logs/{shard_id}/latest")
async def latest_offset(shard_id: str) -> dict[str, Any]:
    lock = _get_lock(shard_id)
    with lock:
        entries = _read_all(shard_id)
    return {
        "shard_id": shard_id,
        "latest_offset": entries[-1]["offset"] if entries else 0,
        "latest_ts_ns": entries[-1].get("created_at_ns", 0) if entries else 0,
    }
