"""Серверний Throttling-шлюз (5 запитів/хв, понад квоту — HTTP 429) перед координатором."""

from contextlib import asynccontextmanager
import os
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from limiters import build_limiter

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
SERVER_LIMIT_RPM = int(os.getenv("SERVER_LIMIT_RPM", "5"))
WINDOW_SEC = float(os.getenv("WINDOW_SEC", "60.0"))
LIMITER_ALGO = os.getenv("LIMITER_ALGO", "sliding_window")

limiter = build_limiter(LIMITER_ALGO, max_requests=SERVER_LIMIT_RPM, window_sec=WINDOW_SEC)
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    yield
    await http_client.aclose()


app = FastAPI(title="KV Store Throttling Gateway (Bonus 1)", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def throttle_middleware(request: Request, call_next):
    if request.url.path in ("/health", "/throttle/info"):
        return await call_next(request)

    client_ip = request.client.host if request.client else "default"
    allowed, retry_after = limiter.allow(client_ip)
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "limit_rpm": SERVER_LIMIT_RPM,
                "algorithm": LIMITER_ALGO,
                "retry_after_sec": round(retry_after, 2),
            },
            headers={"Retry-After": str(max(1, int(retry_after) + 1))},
        )
    return await call_next(request)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "limit_rpm": SERVER_LIMIT_RPM, "window_sec": WINDOW_SEC, "algorithm": LIMITER_ALGO}


@app.get("/ping")
async def ping() -> dict[str, str]:
    """Легкий ендпоінт для автономної перевірки rate-limiting без координатора."""
    return {"status": "ok", "message": "allowed"}


@app.api_route("/{full_path:path}", methods=["GET", "POST", "DELETE", "PUT"])
async def proxy_to_coordinator(full_path: str, request: Request) -> Response:
    assert http_client is not None
    target_url = f"{COORDINATOR_URL}/{full_path}"
    body = await request.body()
    resp = await http_client.request(
        method=request.method,
        url=target_url,
        params=request.query_params,
        content=body if body else None,
        headers={"Content-Type": request.headers.get("content-type", "application/json")},
    )
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
