"""Sliding window statistics for retry queues."""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, List


class WindowError(ValueError):
    def __init__(self, msg: str) -> None:
        self._msg = msg

    def __str__(self) -> str:
        return self._msg


@dataclass
class WindowStats:
    """Aggregated stats over a time window."""
    attempts: int = 0
    successes: int = 0
    failures: int = 0

    @property
    def success_rate(self) -> float | None:
        if self.attempts == 0:
            return None
        return self.successes / self.attempts

    @property
    def failure_rate(self) -> float | None:
        if self.attempts == 0:
            return None
        return self.failures / self.attempts


@dataclass
class _Event:
    ts: float
    success: bool


class SlidingWindow:
    """Tracks retry outcomes within a rolling time window."""

    def __init__(
        self,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if window_seconds <= 0:
            raise WindowError("window_seconds must be positive")
        self._window = window_seconds
        self._clock = clock
        self._events: Deque[_Event] = deque()

    @property
    def window_seconds(self) -> float:
        return self._window

    def record(self, *, success: bool) -> None:
        """Record a single retry attempt outcome."""
        self._events.append(_Event(ts=self._clock(), success=success))
        self._evict()

    def stats(self) -> WindowStats:
        """Return aggregated stats for events within the current window."""
        self._evict()
        s = WindowStats()
        for ev in self._events:
            s.attempts += 1
            if ev.success:
                s.successes += 1
            else:
                s.failures += 1
        return s

    def reset(self) -> None:
        """Clear all recorded events."""
        self._events.clear()

    def _evict(self) -> None:
        cutoff = self._clock() - self._window
        while self._events and self._events[0].ts < cutoff:
            self._events.popleft()
