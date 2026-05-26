"""RetryRunner variant with high/low watermark depth alerting."""
from typing import Callable, Optional

from retryq.backoff import BackoffStrategy, ExponentialBackoff
from retryq.hooks import HookRegistry
from retryq.metrics import RetryMetrics
from retryq.queue import RetryMessage, RetryQueue
from retryq.watermark import WatermarkPolicy


class WatermarkRetryRunner:
    """Wraps RetryRunner with watermark-based depth alerting."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable,
        backoff: Optional[BackoffStrategy] = None,
        dead_letter: Optional[Callable] = None,
        watermark: Optional[WatermarkPolicy] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._backoff = backoff or ExponentialBackoff()
        self._dead_letter = dead_letter
        self._watermark = watermark
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    @property
    def watermark(self) -> Optional[WatermarkPolicy]:
        return self._watermark

    def enqueue(self, message: RetryMessage) -> None:
        self._queue.enqueue(message)
        self._metrics.record_enqueue()
        if self._watermark is not None:
            self._watermark.check(len(self._queue))

    def process_next(self) -> bool:
        message = self._queue.dequeue()
        if message is None:
            return False

        try:
            self._handler(message)
            self._metrics.record_success()
        except Exception:
            message.attempts += 1
            if message.exhausted():
                self._metrics.record_dead_letter()
                if self._dead_letter is not None:
                    self._dead_letter(message)
            else:
                delay = self._backoff.get_delay(message.attempts)
                message.metadata["next_delay"] = delay
                self._queue.enqueue(message)
                self._metrics.record_retry()

        if self._watermark is not None:
            self._watermark.check(len(self._queue))

        return True
