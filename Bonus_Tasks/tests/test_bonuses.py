"""Юніт-тести для всіх 3 бонусних завдань: API throttling, Caching layer, MCP Chatbot."""

import asyncio
import importlib.util
import os
import time

from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load(name: str, rel_path: str):
    full = os.path.join(ROOT, rel_path)
    import sys
    parent = os.path.dirname(full)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    spec = importlib.util.spec_from_file_location(name, full)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


limiters_mod = _load("bonus_limiters", os.path.join("1_api_throttling", "limiters.py"))
throttle_srv = _load("bonus_throttle_srv", os.path.join("1_api_throttling", "server.py"))
client_queue_mod = _load("bonus_client_queue", os.path.join("1_api_throttling", "client_queue.py"))
cache_mod = _load("bonus_cache", os.path.join("2_caching_layer", "cache.py"))
chatbot_mod = _load("bonus_chatbot", os.path.join("3_chatbot_mcp", "chatbot.py"))


def test_all_four_throttling_algorithms_enforce_limit():
    for algo in ("fixed_window", "sliding_window", "token_bucket", "leaking_bucket"):
        lim = limiters_mod.build_limiter(algo, max_requests=5, window_sec=60.0)
        results = [lim.allow("c1")[0] for _ in range(7)]
        assert results[:5] == [True] * 5, f"{algo} should allow first 5"
        assert results[5:] == [False, False], f"{algo} should reject 6th and 7th"


def test_server_returns_429_after_5_requests():
    throttle_srv.limiter = limiters_mod.build_limiter("sliding_window", max_requests=5, window_sec=60.0)
    client = TestClient(throttle_srv.app)
    statuses = [client.get("/ping").status_code for _ in range(7)]
    assert statuses == [200, 200, 200, 200, 200, 429, 429]


def test_client_queue_paces_4_per_window_without_hitting_5_per_window_server():
    async def _run():
        q_client = client_queue_mod.ThrottledClient("http://test", max_requests=4, window_sec=0.3)
        srv_limiter = limiters_mod.build_limiter("sliding_window", max_requests=5, window_sec=0.3)

        # Імітуємо 6 послідовних запитів через _wait_slot клієнта (4/вікно) проти сервера (5/вікно)
        results = []
        for _ in range(6):
            await q_client._wait_slot()
            allowed, _ = srv_limiter.allow("c")
            results.append(allowed)
        assert all(results), "Queued client (4/window) must never trigger 429 on 5/window server"

    asyncio.run(_run())


def test_ttl_cache_hits_expiration_and_invalidation():
    c = cache_mod.TTLCache(max_size=2, ttl_sec=0.2)
    c.set(("t", "k1", "read:"), {"val": 1})
    assert c.get(("t", "k1", "read:")) == {"val": 1}
    assert c.hits == 1

    # Інвалідація після запису/видалення прибирає всі ключі партиції
    c.set(("t", "k1", "exists:"), {"exists": True})
    assert c.invalidate_partition("t", "k1") == 2
    assert c.get(("t", "k1", "read:")) is None

    # Перевірка TTL
    c.set(("t", "k2", "read:"), {"val": 2}, ttl_sec=0.05)
    time.sleep(0.08)
    assert c.get(("t", "k2", "read:")) is None


def test_chatbot_nlu_and_source_formatting():
    i1 = chatbot_mod._parse_intent_rule_based("Який стан кластера?", ["telemetry_demo"])
    assert i1["tool"] == "HEALTH"

    i2 = chatbot_mod._parse_intent_rule_based("Чи існує ключ device-5 у таблиці telemetry_demo?", ["telemetry_demo"])
    assert i2 == {"tool": "EXISTS", "table": "telemetry_demo", "partition_key": "device-5"}

    i3 = chatbot_mod._parse_intent_rule_based("Покажи дані для ключа alice з таблиці users", ["users"])
    assert i3 == {"tool": "READ", "table": "users", "partition_key": "alice"}

    i4 = chatbot_mod._parse_intent_rule_based('Додай запис ключ bot-key зі значенням {"from":"chatbot"} у таблицю telemetry_demo', ["telemetry_demo"])
    assert i4 == {"tool": "CREATE", "table": "telemetry_demo", "partition_key": "bot-key", "value": {"from": "chatbot"}}

    src = chatbot_mod._format_source("users", "alice", {"shard_id": "shard-1", "selected_replica": "shard-1-follower-1", "version_ts_ns": 2000})
    assert "таблиця='users'" in src and "ключ='alice'" in src and "шард='shard-1'" in src
