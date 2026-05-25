"""Batch processing support for RetryQueue."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from retryq.queue import RetryMessage, RetryQueue
from retryq.metrics import RetryMetrics


@dataclass
class BatchResult:
    """Summary of a batch processing run."""
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    dead_lettered: int = 0

    @property
    def total(self) -> int:
        return self.processed

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"BatchResult(processed={self.processed}, succeeded={self.succeeded}, "
            f"failed={self.failed}, dead_lettered={self.dead_lettered})"
        )


class BatchProcessor:
    """Process multiple messages from a RetryQueue in a single call."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], bool],
        *,
        max_batch_size: int = 10,
        dead_letter_callback: Optional[Callable[[RetryMessage], None]] = None,
    ) -> None:
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be at least 1")
        self._queue = queue
        self._handler = handler
        self._max_batch_size = max_batch_size
        self._dead_letter_callback = dead_letter_callback
        self._metrics = RetryMetrics()

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def max_batch_size(self) -> int:
        return self._max_batch_size

    def process_batch(self, limit: Optional[int] = None) -> BatchResult:
        """Process up to *limit* (or max_batch_size) messages and return a BatchResult."""
        cap = min(limit, self._max_batch_size) if limit is not None else self._max_batch_size
        result = BatchResult()

        for _ in range(cap):
            msg = self._queue.dequeue()
            if msg is None:
                break

            result.processed += 1
            success = False
            try:
                success = bool(self._handler(msg))
            except Exception:
                success = False

            if success:
                result.succeeded += 1
                self._metrics.record_success()
            else:
                msg.attempts += 1
                if msg.exhausted:
                    result.dead_lettered += 1
                    self._metrics.record_dead_letter()
                    if self._dead_letter_callback:
                        self._dead_letter_callback(msg)
                else:
                    result.failed += 1
                    self._metrics.record_retry()
                    self._queue.enqueue(msg)

        return result
