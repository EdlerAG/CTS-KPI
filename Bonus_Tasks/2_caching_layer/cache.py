"""In-memory LRU+TTL кеш з інвалідацією по (table, partition_key) і статистикою."""

from collections import OrderedDict
import threading
import time
from typing import Any


class TTLCache:
    """Потокобезпечний кеш із підтримкою TTL, LRU-витіснення та інвалідації."""

    def __init__(self, max_size: int = 1000, ttl_sec: float = 30.0) -> None:
        self.max_size = max_size
        self.ttl_sec = ttl_sec
        self._lock = threading.Lock()
        self._store: OrderedDict[tuple[str, str, str], tuple[float, Any]] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.invalidations = 0

    def get(self, key: tuple[str, str, str]) -> Any | None:
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None
            expires_at, value = entry
            if now >= expires_at:
                del self._store[key]
                self.misses += 1
                return None
            self._store.move_to_end(key)
            self.hits += 1
            return value

    def set(self, key: tuple[str, str, str], value: Any, ttl_sec: float | None = None) -> None:
        ttl = self.ttl_sec if ttl_sec is None else ttl_sec
        expires_at = time.monotonic() + ttl
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (expires_at, value)
            while len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def invalidate_partition(self, table: str, partition_key: str) -> int:
        """Інвалідує всі кешовані записи (і READ, і EXISTS) для вказаного partition_key."""
        removed = 0
        with self._lock:
            to_delete = [k for k in self._store if k[0] == table and k[1] == partition_key]
            for k in to_delete:
                del self._store[k]
                removed += 1
            self.invalidations += removed
        return removed

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            self.hits = 0
            self.misses = 0
            self.invalidations = 0

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100.0) if total else 0.0
            return {
                "size": len(self._store),
                "max_size": self.max_size,
                "ttl_sec": self.ttl_sec,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate_pct": round(hit_rate, 2),
                "invalidations": self.invalidations,
            }
