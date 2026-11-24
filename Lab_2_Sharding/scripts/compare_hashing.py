"""Експеримент для захисту Лаби 2: порівняння переїзду ключів при додаванні 3-го шарда
між класичним `hash % N` та `Consistent Hashing (vnodes)`.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "coordinator"))

from hashing import ConsistentHashRing, modulo_shard  # noqa: E402


def main() -> None:
    total_keys = 10_000
    keys = [f"user-{i}" for i in range(total_keys)]

    # 1) Класичний hash(key) % N: 2 шарди -> 3 шарди
    shards_2 = ["shard-1", "shard-2"]
    shards_3 = ["shard-1", "shard-2", "shard-3"]

    mod_before = {k: modulo_shard(k, shards_2) for k in keys}
    mod_after = {k: modulo_shard(k, shards_3) for k in keys}
    mod_moved = sum(1 for k in keys if mod_before[k] != mod_after[k])

    # 2) Consistent Hashing (100 vnodes на шард): 2 шарди -> 3 шарди
    ring = ConsistentHashRing(vnodes_per_shard=100)
    for sid in shards_2:
        ring.add_shard(sid)
    ring_before = {k: ring.get_shard(k) for k in keys}

    ring.add_shard("shard-3")
    ring_after = {k: ring.get_shard(k) for k in keys}
    ring_moved = sum(1 for k in keys if ring_before[k] != ring_after[k])

    dist = {sid: sum(1 for k in keys if ring_after[k] == sid) for sid in shards_3}

    print(f"Всього ключів: {total_keys}")
    print(f"hash(key) % N  (2 -> 3 шарди): переїхало {mod_moved} ключів ({mod_moved / total_keys * 100:.2f}%)")
    print(f"ConsistentHash (2 -> 3 шарди): переїхало {ring_moved} ключів ({ring_moved / total_keys * 100:.2f}%)")
    print(f"Розподіл по 3 шардах (ConsistentHash): {dist}")


if __name__ == "__main__":
    main()
