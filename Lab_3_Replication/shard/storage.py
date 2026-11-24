"""Сховище репліки з версіонуванням (offset) і персистентним станом фолловера."""

import json
import os
import sqlite3
import threading
from typing import Any


class ReplicatedShardStorage:
    """SQLite-сховище для лідера та фолловерів із збереженням last_applied_offset."""

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
                version INTEGER NOT NULL DEFAULT 0,
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

    def apply_event(
        self,
        offset: int,
        op: str,
        table: str,
        partition_key: str,
        sort_key: str | None,
        value: Any,
    ) -> bool:
        """Застосовує подію з WAL, якщо її offset ще не застосовано. Повертає True при застосуванні."""
        sk = sort_key or ""
        with self._lock:
            cur_row = self._conn.execute(
                "SELECT val FROM replication_state WHERE key='last_applied_offset'"
            ).fetchone()
            current_offset = int(cur_row["val"]) if cur_row else 0
            if offset <= current_offset:
                return False

            if op == "put":
                payload = json.dumps(value, ensure_ascii=False)
                self._conn.execute(
                    """
                    INSERT INTO kv_records (table_name, partition_key, sort_key, value_json, version, deleted)
                    VALUES (?, ?, ?, ?, ?, 0)
                    ON CONFLICT(table_name, partition_key, sort_key)
                    DO UPDATE SET value_json = excluded.value_json, version = excluded.version, deleted = 0
                    WHERE excluded.version >= kv_records.version
                    """,
                    (table, partition_key, sk, payload, offset),
                )
            elif op == "delete":
                self._conn.execute(
                    """
                    INSERT INTO kv_records (table_name, partition_key, sort_key, value_json, version, deleted)
                    VALUES (?, ?, ?, NULL, ?, 1)
                    ON CONFLICT(table_name, partition_key, sort_key)
                    DO UPDATE SET value_json = NULL, version = excluded.version, deleted = 1
                    WHERE excluded.version >= kv_records.version
                    """,
                    (table, partition_key, sk, offset),
                )

            self._conn.execute(
                """
                INSERT INTO replication_state (key, val) VALUES ('last_applied_offset', ?)
                ON CONFLICT(key) DO UPDATE SET val = excluded.val
                """,
                (offset,),
            )
            self._conn.commit()
            return True

    def exists(self, table: str, partition_key: str, sort_key: str | None = None) -> tuple[bool, int]:
        with self._lock:
            if sort_key is not None:
                row = self._conn.execute(
                    "SELECT deleted, version FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=?",
                    (table, partition_key, sort_key),
                ).fetchone()
                if not row:
                    return False, 0
                return (row["deleted"] == 0), int(row["version"])

            rows = self._conn.execute(
                "SELECT deleted, version FROM kv_records WHERE table_name=? AND partition_key=?",
                (table, partition_key),
            ).fetchall()
            if not rows:
                return False, 0
            max_ver = max(int(r["version"]) for r in rows)
            has_live = any(r["deleted"] == 0 for r in rows)
            return has_live, max_ver

    def get(self, table: str, partition_key: str, sort_key: str | None = None) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            if sort_key is not None:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json, version, deleted
                    FROM kv_records
                    WHERE table_name=? AND partition_key=? AND sort_key=?
                    """,
                    (table, partition_key, sort_key),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json, version, deleted
                    FROM kv_records
                    WHERE table_name=? AND partition_key=?
                    ORDER BY sort_key ASC
                    """,
                    (table, partition_key),
                ).fetchall()

        if not rows:
            return [], 0
        max_ver = max(int(r["version"]) for r in rows)
        live_items = [
            {
                "partition_key": r["partition_key"],
                "sort_key": r["sort_key"] if r["sort_key"] != "" else None,
                "value": json.loads(r["value_json"]) if r["value_json"] is not None else None,
                "version": int(r["version"]),
            }
            for r in rows
            if r["deleted"] == 0
        ]
        return live_items, max_ver

    def count_keys(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS cnt FROM kv_records WHERE deleted=0").fetchone()
            return int(row["cnt"]) if row else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
