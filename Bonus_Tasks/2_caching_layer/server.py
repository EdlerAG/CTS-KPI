"""Кешуючий проксі-шар перед координатором (порт 8002) із TTL та інвалідацією."""

from contextlib import asynccontextmanager
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response

from cache import TTLCache

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
CACHE_TTL_SEC = float(os.getenv("CACHE_TTL_SEC", "30.0"))
CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", "1000"))

cache = TTLCache(max_size=CACHE_MAX_SIZE, ttl_sec=CACHE_TTL_SEC)
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0)
    yield
    await http_client.aclose()


app = FastAPI(title="KV Store Caching Layer (Bonus 2)", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "coordinator_url": COORDINATOR_URL, "cache": cache.stats()}


@app.get("/cache/stats")
async def cache_stats() -> dict[str, Any]:
    return cache.stats()


@app.post("/cache/clear")
async def cache_clear() -> dict[str, str]:
    cache.clear()
    return {"status": "cleared"}


@app.get("/tables/{table_name}/records/{partition_key}/exists")
async def cached_exists(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
    response: Response = None,  # type: ignore[assignment]
) -> Any:
    cache_key = (table_name, partition_key, f"exists:{sort_key or ''}")
    cached = cache.get(cache_key)
    if cached is not None:
        response.headers["X-Cache"] = "HIT"
        return cached

    assert http_client is not None
    params = {"sort_key": sort_key} if sort_key is not None else {}
    resp = await http_client.get(f"{COORDINATOR_URL}/tables/{table_name}/records/{partition_key}/exists", params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    cache.set(cache_key, data)
    response.headers["X-Cache"] = "MISS"
    return data


@app.get("/tables/{table_name}/records/{partition_key}")
async def cached_read(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
    response: Response = None,  # type: ignore[assignment]
) -> Any:
    cache_key = (table_name, partition_key, f"read:{sort_key or ''}")
    cached = cache.get(cache_key)
    if cached is not None:
        response.headers["X-Cache"] = "HIT"
        return cached

    assert http_client is not None
    params = {"sort_key": sort_key} if sort_key is not None else {}
    resp = await http_client.get(f"{COORDINATOR_URL}/tables/{table_name}/records/{partition_key}", params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    cache.set(cache_key, data)
    response.headers["X-Cache"] = "MISS"
    return data


@app.post("/tables/{table_name}/records", status_code=201)
async def write_with_invalidation(table_name: str, request: Request) -> Any:
    assert http_client is not None
    payload = await request.json()
    resp = await http_client.post(f"{COORDINATOR_URL}/tables/{table_name}/records", json=payload)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    if "partition_key" in payload:
        cache.invalidate_partition(table_name, payload["partition_key"])
    return resp.json()


@app.delete("/tables/{table_name}/records/{partition_key}")
async def delete_with_invalidation(
    table_name: str,
    partition_key: str,
    sort_key: str | None = Query(default=None),
) -> Any:
    assert http_client is not None
    params = {"sort_key": sort_key} if sort_key is not None else {}
    resp = await http_client.delete(f"{COORDINATOR_URL}/tables/{table_name}/records/{partition_key}", params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    cache.invalidate_partition(table_name, partition_key)
    return resp.json()


@app.api_route("/{full_path:path}", methods=["GET", "POST", "DELETE", "PUT"])
async def passthrough(full_path: str, request: Request) -> Response:
    assert http_client is not None
    body = await request.body()
    resp = await http_client.request(
        method=request.method,
        url=f"{COORDINATOR_URL}/{full_path}",
        params=request.query_params,
        content=body if body else None,
        headers={"Content-Type": request.headers.get("content-type", "application/json")},
    )
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
