"""RetryRunner — wires RetryQueue, MiddlewareChain, and RetryMetrics together."""

from typing import Callable, Optional

from retryq.metrics import RetryMetrics
from retryq.middleware import MiddlewareChain
from retryq.queue import RetryMessage, RetryQueue


class RetryRunner:
    """High-level runner that processes a RetryQueue with middleware and metrics."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], None],
        middleware: Optional[MiddlewareChain] = None,
        metrics: Optional[RetryMetrics] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._middleware = middleware or MiddlewareChain()
        self._metrics = metrics or RetryMetrics()

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    def enqueue(self, message: RetryMessage) -> None:
        """Add a message to the underlying queue and record the enqueue event."""
        self._queue.enqueue(message)
        self._metrics.record_enqueue()

    def process_next(self) -> bool:
        """Process the next available message. Returns True if a message was processed."""
        message = self._queue.next_ready()
        if message is None:
            return False

        message = self._middleware.run_before(message)
        error: Optional[Exception] = None
        success = False

        try:
            self._handler(message)
            success = True
            self._metrics.record_success()
        except Exception as exc:  # noqa: BLE001
            error = exc
            self._queue.requeue(message)
            self._metrics.record_retry()

        self._middleware.run_after(message, success, error)
        return True

    def process_all(self) -> int:
        """Drain the queue until no ready messages remain. Returns count processed."""
        processed = 0
        while self.process_next():
            processed += 1
        return processed
