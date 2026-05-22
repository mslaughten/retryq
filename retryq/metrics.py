"""Metrics collection for RetryQueue operations."""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class RetryMetrics:
    """Tracks statistics for a RetryQueue instance."""

    total_enqueued: int = 0
    total_retried: int = 0
    total_succeeded: int = 0
    total_dead_lettered: int = 0
    attempts_histogram: Dict[int, int] = field(default_factory=dict)

    def record_enqueue(self) -> None:
        """Record a new message being enqueued."""
        self.total_enqueued += 1

    def record_retry(self) -> None:
        """Record a retry attempt."""
        self.total_retried += 1

    def record_success(self, attempts: int) -> None:
        """Record a successful message processing."""
        self.total_succeeded += 1
        self.attempts_histogram[attempts] = self.attempts_histogram.get(attempts, 0) + 1

    def record_dead_letter(self, attempts: int) -> None:
        """Record a message being sent to the dead-letter queue."""
        self.total_dead_lettered += 1
        self.attempts_histogram[attempts] = self.attempts_histogram.get(attempts, 0) + 1

    @property
    def success_rate(self) -> Optional[float]:
        """Return the success rate as a float between 0 and 1, or None if no messages processed."""
        total_processed = self.total_succeeded + self.total_dead_lettered
        if total_processed == 0:
            return None
        return self.total_succeeded / total_processed

    def summary(self) -> Dict[str, object]:
        """Return a dictionary summary of all metrics."""
        return {
            "total_enqueued": self.total_enqueued,
            "total_retried": self.total_retried,
            "total_succeeded": self.total_succeeded,
            "total_dead_lettered": self.total_dead_lettered,
            "success_rate": self.success_rate,
            "attempts_histogram": dict(self.attempts_histogram),
        }

    def reset(self) -> None:
        """Reset all metrics to their initial state."""
        self.total_enqueued = 0
        self.total_retried = 0
        self.total_succeeded = 0
        self.total_dead_lettered = 0
        self.attempts_histogram.clear()
