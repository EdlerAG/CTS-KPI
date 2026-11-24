"""Consistent Hashing з віртуальними вузлами (vnodes) та класичний hash % N."""

from bisect import bisect_right
import hashlib
from typing import Iterable


def stable_hash(value: str) -> int:
    """Повертає стабільний 64-бітний хеш (SHA-256) для рядка.

    Використовуємо SHA-256 замість вбудованого Python hash(), оскільки
    вбудований hash() рандомізується через PYTHONHASHSEED між запусками.
    """
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


class ConsistentHashRing:
    """Кільце консистентного хешування з віртуальними вузлами."""

    def __init__(self, vnodes_per_shard: int = 100) -> None:
        if vnodes_per_shard < 1:
            raise ValueError("vnodes_per_shard must be >= 1")
        self.vnodes_per_shard = vnodes_per_shard
        self._shards: set[str] = set()
        self._positions: list[int] = []
        self._ring: dict[int, str] = {}

    def add_shard(self, shard_id: str) -> None:
        if shard_id in self._shards:
            return
        self._shards.add(shard_id)
        for idx in range(self.vnodes_per_shard):
            pos = stable_hash(f"{shard_id}#vnode-{idx}")
            # У разі вкрай малоймовірної колізії зсуваємо позицію на 1
            while pos in self._ring:
                pos = (pos + 1) & ((1 << 64) - 1)
            self._ring[pos] = shard_id
        self._positions = sorted(self._ring.keys())

    def remove_shard(self, shard_id: str) -> None:
        if shard_id not in self._shards:
            return
        self._shards.remove(shard_id)
        self._ring = {pos: sid for pos, sid in self._ring.items() if sid != shard_id}
        self._positions = sorted(self._ring.keys())

    def get_shard(self, partition_key: str) -> str:
        if not self._positions:
            raise RuntimeError("No shards registered in the ring")
        key_pos = stable_hash(partition_key)
        idx = bisect_right(self._positions, key_pos)
        if idx == len(self._positions):
            idx = 0
        return self._ring[self._positions[idx]]

    @property
    def shards(self) -> list[str]:
        return sorted(self._shards)


def modulo_shard(partition_key: str, shard_ids: Iterable[str]) -> str:
    """Класичний шардинг hash(key) % N для порівняння з consistent hashing."""
    ordered = sorted(shard_ids)
    if not ordered:
        raise RuntimeError("No shards registered")
    return ordered[stable_hash(partition_key) % len(ordered)]
