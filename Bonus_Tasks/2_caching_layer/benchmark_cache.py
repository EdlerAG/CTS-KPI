"""Бенчмарк кешу: вимірювання виграшу на hot-keys та деградації на write-heavy/unique-miss."""

import asyncio
import statistics
import time

import httpx

DIRECT_URL = "http://localhost:8000"
CACHE_URL = "http://localhost:8002"


async def measure(client: httpx.AsyncClient, urls: list[str]) -> tuple[float, float, float]:
    lats = []
    t0 = time.perf_counter()
    for u in urls:
        s = time.perf_counter()
        r = await client.get(u)
        r.raise_for_status()
        lats.append((time.perf_counter() - s) * 1000)
    total = time.perf_counter() - t0
    return len(urls) / total, statistics.median(lats), sorted(lats)[int(len(lats) * 0.99)]


async def main() -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        await client.post(f"{DIRECT_URL}/tables", json={"name": "cache_bench", "partition_key": "k"})
        for i in range(100):
            await client.post(f"{DIRECT_URL}/tables/cache_bench/records", json={"partition_key": f"k-{i}", "value": {"i": i}})
        await client.post(f"{CACHE_URL}/cache/clear")

        # 1. Read-heavy на 5 гарячих ключах (високий Hit Rate)
        hot_direct = [f"{DIRECT_URL}/tables/cache_bench/records/k-{i % 5}" for i in range(100)]
        hot_cached = [f"{CACHE_URL}/tables/cache_bench/records/k-{i % 5}" for i in range(100)]
        d_rps, d_p50, d_p99 = await measure(client, hot_direct)
        c_rps, c_p50, c_p99 = await measure(client, hot_cached)
        stats_hot = (await client.get(f"{CACHE_URL}/cache/stats")).json()
        print(f"[HOT READS 100 ops] Direct -> {d_rps:.1f} ops/s (p50={d_p50:.2f}ms, p99={d_p99:.2f}ms)")
        print(f"[HOT READS 100 ops] Cached -> {c_rps:.1f} ops/s (p50={c_p50:.2f}ms, p99={c_p99:.2f}ms) | hit_rate={stats_hot['hit_rate_pct']}%")

        # 2. Унікальні ключі (0% Hit Rate — коли кеш робить гірше!)
        await client.post(f"{CACHE_URL}/cache/clear")
        miss_direct = [f"{DIRECT_URL}/tables/cache_bench/records/k-{i}" for i in range(100)]
        miss_cached = [f"{CACHE_URL}/tables/cache_bench/records/k-{i}" for i in range(100)]
        md_rps, md_p50, _ = await measure(client, miss_direct)
        mc_rps, mc_p50, _ = await measure(client, miss_cached)
        stats_miss = (await client.get(f"{CACHE_URL}/cache/stats")).json()
        print(f"[100% MISSES 100 ops] Direct -> {md_rps:.1f} ops/s (p50={md_p50:.2f}ms)")
        print(f"[100% MISSES 100 ops] Cached -> {mc_rps:.1f} ops/s (p50={mc_p50:.2f}ms) | hit_rate={stats_miss['hit_rate_pct']}%")


if __name__ == "__main__":
    asyncio.run(main())
