"""Навантажувальний тест для Лаби 3: порівняння latency і throughput
до (W=1, R=1) і після кворумної реплікації (W=2, R=2).
"""

import asyncio
import statistics
import time

import httpx


BASE_URL = "http://localhost:8000"


async def run_benchmark(label: str, w: int, r: int, num_ops: int = 100) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # Переконаємося, що таблиця існує
        await client.post("/tables", json={"name": "bench", "partition_key": "k"})

        write_latencies_ms: list[float] = []
        t0 = time.perf_counter()
        for i in range(num_ops):
            s = time.perf_counter()
            resp = await client.post(
                f"/tables/bench/records?w={w}",
                json={"partition_key": f"key-{i}", "value": {"idx": i}},
            )
            resp.raise_for_status()
            write_latencies_ms.append((time.perf_counter() - s) * 1000)
        write_total = time.perf_counter() - t0

        read_latencies_ms: list[float] = []
        t1 = time.perf_counter()
        for i in range(num_ops):
            s = time.perf_counter()
            resp = await client.get(f"/tables/bench/records/key-{i}?r={r}")
            resp.raise_for_status()
            read_latencies_ms.append((time.perf_counter() - s) * 1000)
        read_total = time.perf_counter() - t1

    def pct(data: list[float], p: float) -> float:
        ordered = sorted(data)
        idx = min(len(ordered) - 1, int(len(ordered) * p))
        return ordered[idx]

    print(f"\n=== {label} (W={w}, R={r}, ops={num_ops}) ===")
    print(
        f"WRITE -> throughput: {num_ops / write_total:.1f} ops/s | "
        f"p50: {statistics.median(write_latencies_ms):.2f} ms | "
        f"p95: {pct(write_latencies_ms, 0.95):.2f} ms | "
        f"p99: {pct(write_latencies_ms, 0.99):.2f} ms"
    )
    print(
        f"READ  -> throughput: {num_ops / read_total:.1f} ops/s | "
        f"p50: {statistics.median(read_latencies_ms):.2f} ms | "
        f"p95: {pct(read_latencies_ms, 0.95):.2f} ms | "
        f"p99: {pct(read_latencies_ms, 0.99):.2f} ms"
    )


async def main() -> None:
    await run_benchmark("Асинхронний запис / 1 репліка", w=1, r=1, num_ops=100)
    await run_benchmark("Синхронний кворум (R+W > N=3)", w=2, r=2, num_ops=100)


if __name__ == "__main__":
    asyncio.run(main())
