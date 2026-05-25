"""RetryRunner variant that gates process_next behind a token-bucket throttle."""

from __future__ import annotations

from typing import Callable, Optional

from retryq.hooks import HookRegistry
from retryq.metrics import RetryMetrics
from retryq.queue import RetryMessage, RetryQueue
from retryq.throttle import ThrottleExceeded, TokenBucket


class ThrottledRetryRunner:
    """Wraps a RetryQueue and enforces a token-bucket throttle on processing."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], bool],
        throttle: TokenBucket,
        dead_letter: Optional[Callable[[RetryMessage], None]] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._throttle = throttle
        self._dead_letter = dead_letter
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()
        self._throttle_rejections: int = 0

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    @property
    def throttle_rejections(self) -> int:
        return self._throttle_rejections

    def enqueue(self, message: RetryMessage) -> None:
        self._queue.enqueue(message)
        self._metrics.record_enqueue()

    def process_next(self) -> bool:
        """Process the next message if a token is available; return False if
        the queue is empty or the throttle rejects the attempt."""
        if len(self._queue) == 0:
            return False

        if not self._throttle.try_consume():
            self._throttle_rejections += 1
            return False

        message = self._queue.dequeue()
        if message is None:
            return False

        success = self._handler(message)
        if success:
            self._metrics.record_success()
        else:
            message.attempts += 1
            if message.exhausted():
                self._metrics.record_dead_letter()
                if self._dead_letter:
                    self._dead_letter(message)
            else:
                self._metrics.record_retry()
                self._queue.enqueue(message)
        return True
