"""Генератор тестового трафіку для наповнення дашбордів Prometheus/Grafana."""

import asyncio
import random

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=5.0) as client:
        await client.post("/tables", json={"name": "telemetry_demo", "partition_key": "k"})
        for i in range(60):
            key = f"device-{i}"
            await client.post(
                "/tables/telemetry_demo/records",
                json={"partition_key": key, "value": {"temp": random.randint(20, 45)}},
            )
            await client.get(f"/tables/telemetry_demo/records/{key}")
        # Кілька 404 для демонстрації error rate на графіку
        for j in range(5):
            await client.get(f"/tables/telemetry_demo/records/missing-{j}")
    print("Traffic generation complete (60 writes, 60 reads, 5 intentional 404s).")


if __name__ == "__main__":
    asyncio.run(main())
