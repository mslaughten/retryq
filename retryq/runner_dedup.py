"""RetryRunner variant with built-in deduplication."""

from __future__ import annotations

from typing import Callable, Optional

from retryq.backoff import BackoffStrategy, ExponentialBackoff
from retryq.dedup import DedupWindow, DuplicateMessageError
from retryq.hooks import HookRegistry
from retryq.metrics import RetryMetrics
from retryq.queue import RetryMessage, RetryQueue


class DedupRetryRunner:
    """A RetryRunner that silently drops (or raises on) duplicate messages."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], None],
        *,
        backoff: Optional[BackoffStrategy] = None,
        dedup_window: Optional[DedupWindow] = None,
        raise_on_duplicate: bool = False,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._backoff = backoff or ExponentialBackoff()
        self._dedup = dedup_window or DedupWindow()
        self._raise_on_duplicate = raise_on_duplicate
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()
        self._duplicates_dropped: int = 0

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    @property
    def duplicates_dropped(self) -> int:
        return self._duplicates_dropped

    def enqueue(self, msg: RetryMessage) -> None:
        """Enqueue a message, deduplicating before insertion."""
        is_dup = self._dedup.check_and_record(msg, raise_on_duplicate=self._raise_on_duplicate)
        if is_dup:
            self._duplicates_dropped += 1
            return
        self._queue.enqueue(msg)
        self._metrics.record_enqueue()

    def process_next(self, dead_letter_callback: Optional[Callable[[RetryMessage], None]] = None) -> bool:
        """Process the next message. Returns False if the queue is empty."""
        msg = self._queue.dequeue()
        if msg is None:
            return False

        try:
            self._handler(msg)
            self._metrics.record_success()
            self._hooks.fire("success", msg)
        except Exception as exc:  # noqa: BLE001
            msg.attempts += 1
            self._hooks.fire("failure", msg)
            if msg.exhausted:
                self._metrics.record_dead_letter()
                self._hooks.fire("dead_letter", msg)
                if dead_letter_callback:
                    dead_letter_callback(msg)
            else:
                delay = self._backoff.get_delay(msg.attempts)
                self._metrics.record_retry()
                self._queue.enqueue(msg)
        return True
