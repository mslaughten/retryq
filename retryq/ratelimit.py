"""Rate limiting support for retry processing."""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RateLimitExceeded(Exception):
    """Raised when the rate limit has been exceeded."""
    limit: int
    window: float

    def __str__(self) -> str:
        return f"Rate limit of {self.limit} per {self.window}s exceeded"


class RateLimiter:
    """Token-bucket style rate limiter for controlling retry throughput.

    Args:
        limit: Maximum number of operations allowed per window.
        window: Time window in seconds.
    """

    def __init__(self, limit: int, window: float = 1.0) -> None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        if window <= 0:
            raise ValueError("window must be a positive number")
        self._limit = limit
        self._window = window
        self._timestamps: list[float] = []

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window(self) -> float:
        return self._window

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self._window
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    def is_allowed(self, now: Optional[float] = None) -> bool:
        """Return True if an operation is allowed at the given time."""
        if now is None:
            now = time.monotonic()
        self._evict_expired(now)
        return len(self._timestamps) < self._limit

    def acquire(self, now: Optional[float] = None) -> None:
        """Acquire a slot, raising RateLimitExceeded if none are available."""
        if now is None:
            now = time.monotonic()
        if not self.is_allowed(now):
            raise RateLimitExceeded(self._limit, self._window)
        self._timestamps.append(now)

    def remaining(self, now: Optional[float] = None) -> int:
        """Return the number of remaining slots in the current window."""
        if now is None:
            now = time.monotonic()
        self._evict_expired(now)
        return max(0, self._limit - len(self._timestamps))

    def reset(self) -> None:
        """Clear all recorded timestamps."""
        self._timestamps.clear()
