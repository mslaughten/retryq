"""Token-bucket throttle for controlling retry throughput."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable


class ThrottleExceeded(Exception):
    """Raised when the token bucket is empty and a token cannot be acquired."""

    def __init__(self, available: float, requested: float) -> None:
        self.available = available
        self.requested = requested

    def __str__(self) -> str:
        return (
            f"ThrottleExceeded: requested {self.requested} token(s), "
            f"only {self.available:.2f} available"
        )


@dataclass
class TokenBucket:
    """Token-bucket implementation with configurable capacity and refill rate."""

    capacity: float
    refill_rate: float  # tokens per second
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _clock: Callable[[], float] = field(default=time.monotonic, repr=False)

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.refill_rate <= 0:
            raise ValueError("refill_rate must be positive")
        self._tokens = self.capacity
        self._last_refill = self._clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens

    def consume(self, tokens: float = 1.0) -> None:
        """Consume *tokens* from the bucket or raise ThrottleExceeded."""
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        self._refill()
        if self._tokens < tokens:
            raise ThrottleExceeded(available=self._tokens, requested=tokens)
        self._tokens -= tokens

    def try_consume(self, tokens: float = 1.0) -> bool:
        """Return True and consume if possible, False otherwise."""
        try:
            self.consume(tokens)
            return True
        except ThrottleExceeded:
            return False
