"""RetryRunner variant that emits lifecycle events via RetryObserver."""

from __future__ import annotations

from typing import Callable, Optional

from retryq.backoff import BackoffStrategy, ExponentialBackoff
from retryq.metrics import RetryMetrics
from retryq.observer import RetryObserver
from retryq.queue import RetryMessage, RetryQueue


class ObservableRetryRunner:
    """Wraps RetryQueue processing and fires observer events at each lifecycle stage."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], bool],
        *,
        backoff: Optional[BackoffStrategy] = None,
        observer: Optional[RetryObserver] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._backoff = backoff or ExponentialBackoff()
        self._observer = observer or RetryObserver()
        self._metrics = RetryMetrics()

    @property
    def observer(self) -> RetryObserver:
        return self._observer

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    def enqueue(self, message: RetryMessage) -> None:
        self._queue.enqueue(message)
        self._metrics.record_enqueue()
        self._observer.notify("enqueue", message)

    def process_next(self) -> bool:
        """Process one message from the queue.  Returns False when queue is empty."""
        message = self._queue.dequeue()
        if message is None:
            return False

        success = self._handler(message)

        if success:
            self._metrics.record_success()
            self._observer.notify("success", message)
            return True

        message.attempts += 1

        if message.exhausted():
            self._metrics.record_dead_letter()
            self._observer.notify("dead_letter", message)
            return True

        delay = self._backoff.get_delay(message.attempts)
        self._metrics.record_retry()
        self._observer.notify("retry", message, delay=delay)
        self._queue.enqueue(message)
        return True
