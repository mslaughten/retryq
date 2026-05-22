"""Pluggable backoff strategy implementations for retryq."""

import random
from abc import ABC, abstractmethod


class BackoffStrategy(ABC):
    """Abstract base class for backoff strategies."""

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """Return delay in seconds for the given attempt number (1-indexed)."""
        ...


class ConstantBackoff(BackoffStrategy):
    """Returns a fixed delay regardless of attempt number."""

    def __init__(self, delay: float = 5.0):
        if delay < 0:
            raise ValueError("delay must be non-negative")
        self.delay = delay

    def get_delay(self, attempt: int) -> float:
        return self.delay


class LinearBackoff(BackoffStrategy):
    """Increases delay linearly with each attempt."""

    def __init__(self, base: float = 5.0, max_delay: float = 300.0):
        if base < 0:
            raise ValueError("base must be non-negative")
        self.base = base
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        return min(self.base * attempt, self.max_delay)


class ExponentialBackoff(BackoffStrategy):
    """Doubles the delay with each attempt, with optional jitter."""

    def __init__(
        self,
        base: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 300.0,
        jitter: bool = True,
    ):
        if base < 0:
            raise ValueError("base must be non-negative")
        if multiplier <= 1:
            raise ValueError("multiplier must be greater than 1")
        self.base = base
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        delay = min(self.base * (self.multiplier ** (attempt - 1)), self.max_delay)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay
