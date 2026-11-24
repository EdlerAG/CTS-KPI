"""Клієнтська черга запитів (4 запити/хв) + демо порівняння з некерованим клієнтом."""

import asyncio
from collections import deque
import time
from typing import Any

import httpx


class ThrottledClient:
    """Клієнт із локальним обмеженням (4 req/min): запити понад квоту стають у чергу."""

    def __init__(self, base_url: str, max_requests: int = 4, window_sec: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._timestamps: deque[float] = deque()
        self._queue: asyncio.Queue[tuple[str, str, dict | None, asyncio.Future]] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self._worker_task = asyncio.create_task(self._worker())
        return self

    async def __aexit__(self, *args):
        await self._queue.join()
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()

    async def _wait_slot(self) -> None:
        while True:
            now = time.monotonic()
            cutoff = now - self.window_sec
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) < self.max_requests:
                self._timestamps.append(now)
                return
            wait_for = (self._timestamps[0] + self.window_sec) - now + 0.02
            await asyncio.sleep(max(0.01, wait_for))

    async def _worker(self) -> None:
        assert self._client is not None
        while True:
            method, path, json_body, fut = await self._queue.get()
            try:
                await self._wait_slot()
                resp = await self._client.request(method, path, json=json_body)
                if not fut.done():
                    fut.set_result((resp.status_code, resp.json()))
            except Exception as exc:
                if not fut.done():
                    fut.set_exception(exc)
            finally:
                self._queue.task_done()

    async def request(self, method: str, path: str, json_body: dict[str, Any] | None = None) -> tuple[int, Any]:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        await self._queue.put((method, path, json_body, fut))
        return await fut


async def demo() -> None:
    base_url = "http://localhost:8001"
    print("1) Некерований клієнт шле 7 запитів підряд на сервер із лімітом 5 req/min:")
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as raw:
        for i in range(1, 8):
            r = await raw.get("/ping")
            print(f"   Запит #{i}: HTTP {r.status_code} -> {r.json()}")

    print("\n2) Клієнт із чергою (4 req/min): дивіться автотест pytest ..\\tests\\test_bonuses.py.")


if __name__ == "__main__":
    asyncio.run(demo())
