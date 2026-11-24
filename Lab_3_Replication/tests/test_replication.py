"""Юніт-тести Лаби 3: append-only WAL, збереження offset після рестарту, round-robin та кворуми."""

import importlib.util
import os

from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_module(name: str, file_path: str, extra_sys_path: str):
    import sys
    if extra_sys_path not in sys.path:
        sys.path.insert(0, extra_sys_path)
    spec = importlib.util.spec_from_file_location(name, file_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_storage_mod = _load_module("lab3_storage", os.path.join(ROOT, "shard", "storage.py"), os.path.join(ROOT, "shard"))
ReplicatedShardStorage = _storage_mod.ReplicatedShardStorage
coord_mod = _load_module("lab3_coord_app", os.path.join(ROOT, "coordinator", "app.py"), os.path.join(ROOT, "coordinator"))


def test_follower_persists_offset_and_resumes_incrementally(tmp_path):
    db_path = str(tmp_path / "follower.db")
    store = ReplicatedShardStorage(db_path)
    try:
        assert store.get_last_offset() == 0
        assert store.apply_event(1, "put", "t", "k1", None, {"v": 1}) is True
        assert store.apply_event(2, "put", "t", "k2", None, {"v": 2}) is True
        assert store.apply_event(1, "put", "t", "k1", None, {"v": 999}) is False
        assert store.get_last_offset() == 2
    finally:
        store.close()

    restarted = ReplicatedShardStorage(db_path)
    try:
        assert restarted.get_last_offset() == 2
        items, ver = restarted.get("t", "k1")
        assert items[0]["value"] == {"v": 1}
        assert ver == 1

        assert restarted.apply_event(3, "delete", "t", "k1", None, None) is True
        assert restarted.get_last_offset() == 3
        exists, ver_after = restarted.exists("t", "k1")
        assert exists is False
        assert ver_after == 3
    finally:
        restarted.close()


def test_round_robin_read_distribution_and_quorum_pick():
    coord_mod.replica_sets.clear()
    coord_mod.read_cursors.clear()

    client = TestClient(coord_mod.app)
    for rid, role in [
        ("shard-1-leader", "leader"),
        ("shard-1-follower-1", "follower"),
        ("shard-1-follower-2", "follower"),
    ]:
        client.post(
            "/replicas/register",
            json={"shard_id": "shard-1", "replica_id": rid, "role": role, "url": f"http://{rid}:8001"},
        )

    picked_first = [coord_mod._pick_read_replicas("shard-1", 1)[0].replica_id for _ in range(3)]
    assert len(set(picked_first)) == 3

    coord_mod.replica_sets["shard-1"]["shard-1-follower-2"].healthy = False
    picked_two = coord_mod._pick_read_replicas("shard-1", 2)
    assert {r.replica_id for r in picked_two} == {"shard-1-leader", "shard-1-follower-1"}
