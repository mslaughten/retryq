"""Priority queue support for RetryQ — messages can be assigned a priority
level so that higher-priority items are processed before lower-priority ones."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Callable, Optional

from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import BackoffStrategy, ConstantBackoff


class InvalidPriorityError(ValueError):
    """Raised when a priority value is outside the allowed range."""

    def __str__(self) -> str:
        return f"Priority must be an integer between 0 and 10: {self.args[0]!r}"


PRIORITY_MIN = 0
PRIORITY_MAX = 10


@dataclass(order=True)
class _PrioritizedEntry:
    """Heap entry that sorts by (inverted priority, insertion order)."""

    sort_key: tuple = field(compare=True)
    message: RetryMessage = field(compare=False)


class PriorityRetryQueue:
    """A RetryQueue variant that processes messages in priority order.

    Priority 10 is the highest; priority 0 is the lowest.
    Messages with equal priority are processed FIFO.
    """

    def __init__(
        self,
        handler: Callable[[RetryMessage], bool],
        backoff: Optional[BackoffStrategy] = None,
        max_attempts: int = 3,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._handler = handler
        self._backoff: BackoffStrategy = backoff or ConstantBackoff()
        self._max_attempts = max_attempts
        self._heap: list[_PrioritizedEntry] = []
        self._counter = 0  # tie-breaker for equal priorities
        self._dead_letters: list[RetryMessage] = []

    def enqueue(self, message: RetryMessage, priority: int = 5) -> None:
        """Add *message* to the queue with the given *priority* (0–10)."""
        if not (PRIORITY_MIN <= priority <= PRIORITY_MAX):
            raise InvalidPriorityError(priority)
        # Invert priority so that higher values come first in the min-heap.
        entry = _PrioritizedEntry(
            sort_key=(-priority, self._counter),
            message=message,
        )
        heapq.heappush(self._heap, entry)
        self._counter += 1

    def process_next(self) -> bool:
        """Process the highest-priority message. Returns True if one was found."""
        if not self._heap:
            return False
        entry = heapq.heappop(self._heap)
        msg = entry.message
        success = self._handler(msg)
        if not success:
            msg.attempts += 1
            if msg.attempts >= self._max_attempts:
                self._dead_letters.append(msg)
            else:
                # Re-enqueue with the same priority (stored in sort_key[0] negated).
                original_priority = -entry.sort_key[0]
                self.enqueue(msg, priority=original_priority)
        return True

    @property
    def dead_letters(self) -> list[RetryMessage]:
        return list(self._dead_letters)

    def __len__(self) -> int:
        return len(self._heap)
