"""Consistent Hashing з віртуальними вузлами (vnodes)."""

from bisect import bisect_right
import hashlib


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


class ConsistentHashRing:
    def __init__(self, vnodes_per_shard: int = 100) -> None:
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
            while pos in self._ring:
                pos = (pos + 1) & ((1 << 64) - 1)
            self._ring[pos] = shard_id
        self._positions = sorted(self._ring.keys())

    def get_shard(self, partition_key: str) -> str:
        if not self._positions:
            raise RuntimeError("No shards registered in the ring")
        key_pos = stable_hash(partition_key)
        idx = bisect_right(self._positions, key_pos)
        if idx == len(self._positions):
            idx = 0
        return self._ring[self._positions[idx]]
