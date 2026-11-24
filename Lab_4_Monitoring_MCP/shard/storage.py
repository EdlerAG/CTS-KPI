"""Сховище репліки з LWW-версіонуванням (version_ts_ns, node_id), tombstones і ребалансуванням."""

import json
import os
import sqlite3
import threading
from typing import Any


class LWWShardStorage:
    """SQLite-сховище з Last-Write-Wins (LWW) розв'язанням конфліктів та offset фолловера."""

    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_records (
                table_name TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                sort_key TEXT NOT NULL DEFAULT '',
                value_json TEXT,
                version_ts_ns INTEGER NOT NULL DEFAULT 0,
                node_id TEXT NOT NULL DEFAULT '',
                deleted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (table_name, partition_key, sort_key)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS replication_state (
                key TEXT PRIMARY KEY,
                val INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    def get_last_offset(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT val FROM replication_state WHERE key='last_applied_offset'"
            ).fetchone()
            return int(row["val"]) if row else 0

    def apply_lww(
        self,
        op: str,
        table: str,
        partition_key: str,
        sort_key: str | None,
        value: Any,
        version_ts_ns: int,
        node_id: str,
        offset: int | None = None,
    ) -> bool:
        """Застосовує запис за правилом Last-Write-Wins: (version_ts_ns, node_id)."""
        sk = sort_key or ""
        deleted_flag = 1 if op == "delete" else 0
        payload = None if deleted_flag else json.dumps(value, ensure_ascii=False)

        with self._lock:
            if offset is not None:
                cur_row = self._conn.execute(
                    "SELECT val FROM replication_state WHERE key='last_applied_offset'"
                ).fetchone()
                current_offset = int(cur_row["val"]) if cur_row else 0
                if offset <= current_offset:
                    return False

            existing = self._conn.execute(
                "SELECT version_ts_ns, node_id FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=?",
                (table, partition_key, sk),
            ).fetchone()

            should_write = existing is None or (version_ts_ns, node_id) >= (
                int(existing["version_ts_ns"]),
                str(existing["node_id"]),
            )
            if should_write:
                self._conn.execute(
                    """
                    INSERT INTO kv_records (table_name, partition_key, sort_key, value_json, version_ts_ns, node_id, deleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(table_name, partition_key, sort_key)
                    DO UPDATE SET
                        value_json = excluded.value_json,
                        version_ts_ns = excluded.version_ts_ns,
                        node_id = excluded.node_id,
                        deleted = excluded.deleted
                    """,
                    (table, partition_key, sk, payload, version_ts_ns, node_id, deleted_flag),
                )

            if offset is not None:
                self._conn.execute(
                    """
                    INSERT INTO replication_state (key, val) VALUES ('last_applied_offset', ?)
                    ON CONFLICT(key) DO UPDATE SET val = excluded.val
                    """,
                    (offset,),
                )
            self._conn.commit()
            return should_write

    def exists(self, table: str, partition_key: str, sort_key: str | None = None) -> tuple[bool, int, str]:
        with self._lock:
            if sort_key is not None:
                row = self._conn.execute(
                    "SELECT deleted, version_ts_ns, node_id FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=?",
                    (table, partition_key, sort_key),
                ).fetchone()
                if not row:
                    return False, 0, ""
                return (row["deleted"] == 0), int(row["version_ts_ns"]), str(row["node_id"])

            rows = self._conn.execute(
                "SELECT deleted, version_ts_ns, node_id FROM kv_records WHERE table_name=? AND partition_key=?",
                (table, partition_key),
            ).fetchall()
            if not rows:
                return False, 0, ""
            freshest = max(rows, key=lambda r: (int(r["version_ts_ns"]), str(r["node_id"])))
            has_live = any(r["deleted"] == 0 for r in rows)
            return has_live, int(freshest["version_ts_ns"]), str(freshest["node_id"])

    def get(
        self, table: str, partition_key: str, sort_key: str | None = None
    ) -> tuple[list[dict[str, Any]], int, str]:
        with self._lock:
            if sort_key is not None:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json, version_ts_ns, node_id, deleted
                    FROM kv_records
                    WHERE table_name=? AND partition_key=? AND sort_key=?
                    """,
                    (table, partition_key, sort_key),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json, version_ts_ns, node_id, deleted
                    FROM kv_records
                    WHERE table_name=? AND partition_key=?
                    ORDER BY sort_key ASC
                    """,
                    (table, partition_key),
                ).fetchall()

        if not rows:
            return [], 0, ""
        freshest = max(rows, key=lambda r: (int(r["version_ts_ns"]), str(r["node_id"])))
        live_items = [
            {
                "partition_key": r["partition_key"],
                "sort_key": r["sort_key"] if r["sort_key"] != "" else None,
                "value": json.loads(r["value_json"]) if r["value_json"] is not None else None,
                "version_ts_ns": int(r["version_ts_ns"]),
                "node_id": str(r["node_id"]),
            }
            for r in rows
            if r["deleted"] == 0
        ]
        return live_items, int(freshest["version_ts_ns"]), str(freshest["node_id"])

    def scan_all_live(self) -> list[dict[str, Any]]:
        """Повертає всі живі записи шарда для фонового ребалансування."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT table_name, partition_key, sort_key, value_json, version_ts_ns, node_id
                FROM kv_records
                WHERE deleted = 0
                """
            ).fetchall()
        return [
            {
                "table": r["table_name"],
                "partition_key": r["partition_key"],
                "sort_key": r["sort_key"] if r["sort_key"] != "" else None,
                "value": json.loads(r["value_json"]) if r["value_json"] is not None else None,
                "version_ts_ns": int(r["version_ts_ns"]),
                "node_id": str(r["node_id"]),
            }
            for r in rows
        ]

    def hard_delete_migrated(self, table: str, partition_key: str, sort_key: str | None) -> None:
        """Видаляє перенесений на інший шард ключ (без створення tombstone, бо власник змінився)."""
        sk = sort_key or ""
        with self._lock:
            self._conn.execute(
                "DELETE FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=?",
                (table, partition_key, sk),
            )
            self._conn.commit()

    def count_keys(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS cnt FROM kv_records WHERE deleted=0").fetchone()
            return int(row["cnt"]) if row else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
