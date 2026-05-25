"""Queue inspection utilities for retryq."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from retryq.queue import RetryMessage, RetryQueue


@dataclass
class QueueSummary:
    """Snapshot summary of a RetryQueue's current state."""

    total: int = 0
    exhausted: int = 0
    pending: int = 0
    attempt_histogram: dict = field(default_factory=dict)

    @property
    def healthy(self) -> bool:
        """True when no messages are exhausted."""
        return self.exhausted == 0


def inspect_queue(queue: RetryQueue) -> QueueSummary:
    """Return a QueueSummary for *queue* without mutating it."""
    messages: List[RetryMessage] = list(queue._queue.queue)  # type: ignore[attr-defined]

    total = len(messages)
    exhausted_count = sum(1 for m in messages if m.exhausted())
    pending_count = total - exhausted_count

    histogram: dict = {}
    for msg in messages:
        bucket = msg.attempts
        histogram[bucket] = histogram.get(bucket, 0) + 1

    return QueueSummary(
        total=total,
        exhausted=exhausted_count,
        pending=pending_count,
        attempt_histogram=histogram,
    )


def find_message(queue: RetryQueue, message_id: str) -> Optional[RetryMessage]:
    """Return the first message in *queue* whose id matches *message_id*, or None."""
    for msg in list(queue._queue.queue):  # type: ignore[attr-defined]
        if msg.id == message_id:
            return msg
    return None


def filter_exhausted(queue: RetryQueue) -> List[RetryMessage]:
    """Return all exhausted messages in *queue* without removing them."""
    return [m for m in list(queue._queue.queue) if m.exhausted()]  # type: ignore[attr-defined]
