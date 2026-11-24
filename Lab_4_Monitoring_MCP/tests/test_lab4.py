"""Юніт-тести Лаби 4: LWW + tombstones, /health, /version, /metrics, X-Trace-Id."""

import importlib.util
import os
import sys

from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_module(name: str, file_path: str, extra_sys_path: str):
    if extra_sys_path not in sys.path:
        sys.path.insert(0, extra_sys_path)
    spec = importlib.util.spec_from_file_location(name, file_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_storage_mod = _load_module("lab4_storage", os.path.join(ROOT, "shard", "storage.py"), os.path.join(ROOT, "shard"))
LWWShardStorage = _storage_mod.LWWShardStorage
coord_mod = _load_module("lab4_coord_app", os.path.join(ROOT, "coordinator", "app.py"), os.path.join(ROOT, "coordinator"))


def test_lww_resolves_out_of_order_writes_and_tombstones(tmp_path):
    store = LWWShardStorage(str(tmp_path / "lww.db"))
    try:
        assert store.apply_lww("put", "t", "k1", None, {"v": "new"}, 2000, "node-a") is True
        assert store.apply_lww("put", "t", "k1", None, {"v": "old"}, 1000, "node-b") is False

        items, ver, _ = store.get("t", "k1")
        assert ver == 2000
        assert items[0]["value"] == {"v": "new"}

        assert store.apply_lww("put", "t", "k1", None, {"v": "tie-b"}, 2000, "node-b") is True
        items, _, nid = store.get("t", "k1")
        assert nid == "node-b"
        assert items[0]["value"] == {"v": "tie-b"}

        assert store.apply_lww("delete", "t", "k1", None, None, 3000, "node-a") is True
        assert store.apply_lww("put", "t", "k1", None, {"v": "zombie"}, 2500, "node-b") is False
        exists, _, _ = store.exists("t", "k1")
        assert exists is False
    finally:
        store.close()


def test_coordinator_health_version_metrics_and_trace_id():
    client = TestClient(coord_mod.app)

    h = client.get("/health", headers={"X-Trace-Id": "trace-demo-123"})
    assert h.status_code == 200
    assert h.headers.get("X-Trace-Id") == "trace-demo-123"
    assert h.json()["status"] == "ok"

    v = client.get("/version")
    assert v.status_code == 200
    assert "version" in v.json() and "build_sha" in v.json()

    m = client.get("/metrics")
    assert m.status_code == 200
    assert "kv_http_requests_total" in m.text
    assert "kv_http_request_duration_seconds" in m.text
