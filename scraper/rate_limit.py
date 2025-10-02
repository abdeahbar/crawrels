from __future__ import annotations

import threading
import time


class RateLimiter:
    def __init__(self, rate_per_sec: float) -> None:
        self._lock = threading.Lock()
        self.update(rate_per_sec)
        self._last_ts = 0.0

    def update(self, rate_per_sec: float) -> None:
        self._rate = max(rate_per_sec, 0.0)
        self._interval = 1.0 / self._rate if self._rate > 0 else 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_ts
            wait_for = self._interval - elapsed
            if wait_for > 0:
                time.sleep(wait_for)
                now = time.monotonic()
            self._last_ts = now
