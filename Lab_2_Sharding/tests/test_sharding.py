"""Юніт та інтеграційні тести для Лаби 2 (Шардинг, Consistent Hashing, складений ключ)."""

import importlib.util
import os
import sys

from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "shard"))
sys.path.insert(0, os.path.join(ROOT, "coordinator"))

_spec = importlib.util.spec_from_file_location("coordinator_app", os.path.join(ROOT, "coordinator", "app.py"))
assert _spec and _spec.loader
coordinator_app_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(coordinator_app_mod)

from hashing import ConsistentHashRing, modulo_shard  # noqa: E402
from storage import ShardStorage  # noqa: E402


def test_consistent_hashing_moves_fewer_keys_than_modulo():
    keys = [f"k-{i}" for i in range(2000)]
    mod_before = {k: modulo_shard(k, ["shard-1", "shard-2"]) for k in keys}
    mod_after = {k: modulo_shard(k, ["shard-1", "shard-2", "shard-3"]) for k in keys}
    mod_moved = sum(1 for k in keys if mod_before[k] != mod_after[k])

    ring = ConsistentHashRing(vnodes_per_shard=100)
    ring.add_shard("shard-1")
    ring.add_shard("shard-2")
    ring_before = {k: ring.get_shard(k) for k in keys}
    ring.add_shard("shard-3")
    ring_after = {k: ring.get_shard(k) for k in keys}
    ring_moved = sum(1 for k in keys if ring_before[k] != ring_after[k])

    assert mod_moved > 1100  # ~66% для modulo
    assert ring_moved < 850   # ~33% для consistent hashing


def test_shard_storage_composite_key_and_crud(tmp_path):
    store = ShardStorage(str(tmp_path / "test.db"))
    try:
        store.put("orders", "user-1", "2026-11-25#002", {"sum": 200})
        store.put("orders", "user-1", "2026-11-25#001", {"sum": 100})

        assert store.exists("orders", "user-1") is True
        assert store.exists("orders", "user-1", "2026-11-25#001") is True

        # Без sort_key повертаються всі записи partition_key, відсортовані за sort_key
        all_items = store.get("orders", "user-1")
        assert [item["sort_key"] for item in all_items] == ["2026-11-25#001", "2026-11-25#002"]

        # Видалення конкретного sort_key
        assert store.delete("orders", "user-1", "2026-11-25#001") == 1
        assert store.exists("orders", "user-1", "2026-11-25#001") is False
        assert store.exists("orders", "user-1", "2026-11-25#002") is True
    finally:
        store.close()


def test_coordinator_routes_across_two_shards_in_memory(tmp_path):
    coordinator_app_mod.shards.clear()
    coordinator_app_mod.tables.clear()
    coordinator_app_mod.ring = ConsistentHashRing(vnodes_per_shard=100)

    s1 = ShardStorage(str(tmp_path / "s1.db"))
    s2 = ShardStorage(str(tmp_path / "s2.db"))
    try:
        stores = {"shard-1": s1, "shard-2": s2}

        client = TestClient(coordinator_app_mod.app)
        assert client.post("/shards/register", json={"shard_id": "shard-1", "url": "http://shard-1:8001"}).status_code == 200
        assert client.post("/shards/register", json={"shard_id": "shard-2", "url": "http://shard-2:8001"}).status_code == 200

        # Реєстрація таблиці
        r = client.post("/tables", json={"name": "users", "partition_key": "user_id"})
        assert r.status_code == 201

        # Перевіряємо маршрутизацію 30 ключів на обидва шарди через реальний кільцевий хеш
        routed = set()
        for i in range(30):
            pk = f"u-{i}"
            sid, _ = coordinator_app_mod._select_shard(pk)
            routed.add(sid)
            stores[sid].put("users", pk, None, {"i": i})

        assert routed == {"shard-1", "shard-2"}
        assert s1.count_keys() + s2.count_keys() == 30
    finally:
        s1.close()
        s2.close()
