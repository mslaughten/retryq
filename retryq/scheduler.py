"""Scheduler module for retryq — manages timed retry scheduling."""

import time
from typing import Callable, Optional
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import BackoffStrategy


class ScheduledRetry:
    """Represents a message scheduled for retry at a specific time."""

    def __init__(self, message: RetryMessage, retry_at: float):
        self.message = message
        self.retry_at = retry_at

    def is_due(self, now: Optional[float] = None) -> bool:
        """Return True if the retry time has been reached."""
        current = now if now is not None else time.monotonic()
        return current >= self.retry_at

    def __repr__(self) -> str:
        return (
            f"ScheduledRetry(id={self.message.id!r}, "
            f"retry_at={self.retry_at:.3f}, "
            f"attempts={self.message.attempts})"
        )


class RetryScheduler:
    """Schedules messages for delayed retry using a backoff strategy."""

    def __init__(self, queue: RetryQueue, backoff: BackoffStrategy,
                 clock: Callable[[], float] = time.monotonic):
        self._queue = queue
        self._backoff = backoff
        self._clock = clock
        self._scheduled: list[ScheduledRetry] = []

    def schedule(self, message: RetryMessage) -> ScheduledRetry:
        """Schedule a message for retry based on its current attempt count."""
        delay = self._backoff.get_delay(message.attempts)
        retry_at = self._clock() + delay
        entry = ScheduledRetry(message=message, retry_at=retry_at)
        self._scheduled.append(entry)
        return entry

    def flush_due(self) -> int:
        """Enqueue all messages whose retry time has arrived. Returns count."""
        now = self._clock()
        due = [s for s in self._scheduled if s.is_due(now)]
        for entry in due:
            self._scheduled.remove(entry)
            self._queue.enqueue(entry.message)
        return len(due)

    @property
    def pending_count(self) -> int:
        """Number of messages waiting to be retried."""
        return len(self._scheduled)
