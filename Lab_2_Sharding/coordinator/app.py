"""Сервіс-координатор розподіленого key-value сховища (Мілстоун 2)."""

from contextlib import asynccontextmanager
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from hashing import ConsistentHashRing, modulo_shard


class ShardRegistration(BaseModel):
    shard_id: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)


class TableDefinition(BaseModel):
    name: str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_-]+$")
    partition_key: str = Field(..., min_length=1)
    sort_key: str | None = Field(default=None)


class RecordWriteRequest(BaseModel):
    partition_key: str = Field(..., min_length=1)
    sort_key: str | None = Field(default=None)
    value: Any


SHARDING_STRATEGY = os.getenv("SHARDING_STRATEGY", "consistent").lower()
VNODES_PER_SHARD = int(os.getenv("VNODES_PER_SHARD", "100"))

shards: dict[str, str] = {}
tables: dict[str, TableDefinition] = {}
ring = ConsistentHashRing(vnodes_per_shard=VNODES_PER_SHARD)
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    yield
    await http_client.aclose()


app = FastAPI(
    title="KV Store Coordinator (Lab 2 - Sharding)",
    version="1.0.0",
    lifespan=lifespan,
)


def _select_shard(partition_key: str) -> tuple[str, str]:
    if not shards:
        raise HTTPException(status_code=503, detail="No shards registered")
    if SHARDING_STRATEGY == "modulo":
        shard_id = modulo_shard(partition_key, shards.keys())
    else:
        shard_id = ring.get_shard(partition_key)
    return shard_id, shards[shard_id]


def _validate_table_and_key(table_name: str, sort_key: str | None, require_sort_key: bool) -> TableDefinition:
    table = tables.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    if require_sort_key and table.sort_key and not sort_key:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' requires sort_key '{table.sort_key}'",
        )
    if not table.sort_key and sort_key is not None:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' does not define a sort_key",
        )
    return table


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "strategy": SHARDING_STRATEGY,
        "registered_shards": len(shards),
        "shards": sorted(shards.keys()),
    }


@app.post("/shards/register")
async def register_shard(reg: ShardRegistration) -> dict[str, Any]:
    shards[reg.shard_id] = reg.url.rstrip("/")
    ring.add_shard(reg.shard_id)
    return {"status": "registered", "shard_id": reg.shard_id, "total_shards": len(shards)}


@app.get("/shards")
async def list_shards() -> dict[str, Any]:
    return {
        "strategy": SHARDING_STRATEGY,
        "vnodes_per_shard": VNODES_PER_SHARD,
        "shards": [{"shard_id": sid, "url": url} for sid, url in sorted(shards.items())],
    }


@app.post("/tables", status_code=201)
async def create_table(table: TableDefinition) -> dict[str, Any]:
    if table.name in tables:
        raise HTTPException(status_code=409, detail=f"Table '{table.name}' already exists")
    tables[table.name] = table
    return {"status": "created", "table": table.model_dump()}


@app.get("/tables")
async def list_tables() -> dict[str, Any]:
    return {"tables": [t.model_dump() for t in tables.values()]}


@app.get("/tables/{table_name}")
async def get_table(table_name: str) -> dict[str, Any]:
    table = tables.get(table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return table.model_dump()


@app.post("/tables/{table_name}/records", status_code=201)
async def create_record(table_name: str, req: RecordWriteRequest) -> dict[str, Any]:
    _validate_table_and_key(table_name, req.sort_key, require_sort_key=True)
    shard_id, shard_url = _select_shard(req.partition_key)
    assert http_client is not None

    resp = await http_client.post(
        f"{shard_url}/internal/records",
        json={
            "table": table_name,
            "partition_key": req.partition_key,
            "sort_key": req.sort_key,
            "value": req.value,
        },
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["routed_to_shard"] = shard_id
    return data


@app.get("/tables/{table_name}/records/{partition_key}/exists")
async def exists_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=False)
    shard_id, shard_url = _select_shard(partition_key)
    assert http_client is not None

    params: dict[str, str] = {"table": table_name, "partition_key": partition_key}
    if sort_key is not None:
        params["sort_key"] = sort_key

    resp = await http_client.get(f"{shard_url}/internal/records/exists", params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["routed_to_shard"] = shard_id
    return data


@app.get("/tables/{table_name}/records/{partition_key}")
async def read_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=False)
    shard_id, shard_url = _select_shard(partition_key)
    assert http_client is not None

    params: dict[str, str] = {"table": table_name, "partition_key": partition_key}
    if sort_key is not None:
        params["sort_key"] = sort_key

    resp = await http_client.get(f"{shard_url}/internal/records", params=params)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Record not found")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["routed_to_shard"] = shard_id
    return data


@app.delete("/tables/{table_name}/records/{partition_key}")
async def delete_record(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
) -> dict[str, Any]:
    _validate_table_and_key(table_name, sort_key, require_sort_key=True)
    shard_id, shard_url = _select_shard(partition_key)
    assert http_client is not None

    params: dict[str, str] = {"table": table_name, "partition_key": partition_key}
    if sort_key is not None:
        params["sort_key"] = sort_key

    resp = await http_client.delete(f"{shard_url}/internal/records", params=params)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Record not found")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    data["routed_to_shard"] = shard_id
    return data
