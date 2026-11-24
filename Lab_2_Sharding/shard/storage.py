"""Персистентне локальне key-value сховище шарда на SQLite."""

import json
import os
import sqlite3
import threading
from typing import Any


class ShardStorage:
    """Thread-safe SQLite-сховище зі складеним ключем (partition_key + sort_key)."""

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
                value_json TEXT NOT NULL,
                PRIMARY KEY (table_name, partition_key, sort_key)
            )
            """
        )
        self._conn.commit()

    def put(self, table: str, partition_key: str, sort_key: str | None, value: Any) -> None:
        sk = sort_key or ""
        payload = json.dumps(value, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO kv_records (table_name, partition_key, sort_key, value_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(table_name, partition_key, sort_key)
                DO UPDATE SET value_json = excluded.value_json
                """,
                (table, partition_key, sk, payload),
            )
            self._conn.commit()

    def exists(self, table: str, partition_key: str, sort_key: str | None = None) -> bool:
        with self._lock:
            if sort_key is not None:
                row = self._conn.execute(
                    "SELECT 1 FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=? LIMIT 1",
                    (table, partition_key, sort_key),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT 1 FROM kv_records WHERE table_name=? AND partition_key=? LIMIT 1",
                    (table, partition_key),
                ).fetchone()
            return row is not None

    def get(self, table: str, partition_key: str, sort_key: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if sort_key is not None:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json
                    FROM kv_records
                    WHERE table_name=? AND partition_key=? AND sort_key=?
                    """,
                    (table, partition_key, sort_key),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT partition_key, sort_key, value_json
                    FROM kv_records
                    WHERE table_name=? AND partition_key=?
                    ORDER BY sort_key ASC
                    """,
                    (table, partition_key),
                ).fetchall()

        return [
            {
                "partition_key": r["partition_key"],
                "sort_key": r["sort_key"] if r["sort_key"] != "" else None,
                "value": json.loads(r["value_json"]),
            }
            for r in rows
        ]

    def delete(self, table: str, partition_key: str, sort_key: str | None = None) -> int:
        sk = sort_key or ""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM kv_records WHERE table_name=? AND partition_key=? AND sort_key=?",
                (table, partition_key, sk),
            )
            self._conn.commit()
            return cur.rowcount

    def count_keys(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS cnt FROM kv_records").fetchone()
            return int(row["cnt"]) if row else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
