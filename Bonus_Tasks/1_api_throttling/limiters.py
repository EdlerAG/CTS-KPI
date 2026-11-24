"""Реалізація 4 алгоритмів обмеження частоти запитів (Rate Limiting)."""

from collections import deque
import threading
import time


class BaseLimiter:
    """Базовий інтерфейс лімітера: allow() -> (allowed, retry_after_sec)."""

    def allow(self, client_id: str = "default") -> tuple[bool, float]:
        raise NotImplementedError


class FixedWindowLimiter(BaseLimiter):
    """Фіксоване вікно: лічильник скидається на межі кожного інтервалу window_sec."""

    def __init__(self, max_requests: int = 5, window_sec: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._lock = threading.Lock()
        self._windows: dict[str, tuple[int, int]] = {}

    def allow(self, client_id: str = "default") -> tuple[bool, float]:
        now = time.monotonic()
        bucket = int(now // self.window_sec)
        with self._lock:
            cur_bucket, count = self._windows.get(client_id, (bucket, 0))
            if cur_bucket != bucket:
                cur_bucket, count = bucket, 0
            if count < self.max_requests:
                self._windows[client_id] = (cur_bucket, count + 1)
                return True, 0.0
            retry_after = (bucket + 1) * self.window_sec - now
            return False, max(0.01, retry_after)


class SlidingWindowLogLimiter(BaseLimiter):
    """Ковзне вікно (Sliding Window Log): зберігає точні мітки часу за останні window_sec."""

    def __init__(self, max_requests: int = 5, window_sec: float = 60.0) -> None:
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._lock = threading.Lock()
        self._logs: dict[str, deque[float]] = {}

    def allow(self, client_id: str = "default") -> tuple[bool, float]:
        now = time.monotonic()
        cutoff = now - self.window_sec
        with self._lock:
            q = self._logs.setdefault(client_id, deque())
            while q and q[0] <= cutoff:
                q.popleft()
            if len(q) < self.max_requests:
                q.append(now)
                return True, 0.0
            retry_after = (q[0] + self.window_sec) - now
            return False, max(0.01, retry_after)


class TokenBucketLimiter(BaseLimiter):
    """Token Bucket: поповнення токенів зі швидкістю rate_per_sec, ємність capacity."""

    def __init__(self, capacity: int = 5, window_sec: float = 60.0) -> None:
        self.capacity = float(capacity)
        self.refill_rate = capacity / window_sec
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, float]] = {}

    def allow(self, client_id: str = "default") -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            tokens, last_ts = self._buckets.get(client_id, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - last_ts) * self.refill_rate)
            if tokens >= 1.0:
                self._buckets[client_id] = (tokens - 1.0, now)
                return True, 0.0
            self._buckets[client_id] = (tokens, now)
            retry_after = (1.0 - tokens) / self.refill_rate
            return False, max(0.01, retry_after)


class LeakingBucketLimiter(BaseLimiter):
    """Leaking Bucket: рівень води у відрі витікає з постійною швидкістю leak_rate."""

    def __init__(self, capacity: int = 5, window_sec: float = 60.0) -> None:
        self.capacity = float(capacity)
        self.leak_rate = capacity / window_sec
        self._lock = threading.Lock()
        self._levels: dict[str, tuple[float, float]] = {}

    def allow(self, client_id: str = "default") -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            level, last_ts = self._levels.get(client_id, (0.0, now))
            level = max(0.0, level - (now - last_ts) * self.leak_rate)
            if level + 1.0 <= self.capacity:
                self._levels[client_id] = (level + 1.0, now)
                return True, 0.0
            self._levels[client_id] = (level, now)
            retry_after = (level + 1.0 - self.capacity) / self.leak_rate
            return False, max(0.01, retry_after)


LIMITER_CLASSES = {
    "fixed_window": FixedWindowLimiter,
    "sliding_window": SlidingWindowLogLimiter,
    "token_bucket": TokenBucketLimiter,
    "leaking_bucket": LeakingBucketLimiter,
}


def build_limiter(algo: str, max_requests: int = 5, window_sec: float = 60.0) -> BaseLimiter:
    cls = LIMITER_CLASSES.get(algo.lower())
    if not cls:
        raise ValueError(f"Unknown limiter algorithm: {algo}")
    return cls(max_requests, window_sec)
