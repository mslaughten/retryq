"""RetryRunner orchestrates queue processing with metrics, middleware, and hooks."""

from typing import Callable, Optional

from retryq.queue import RetryQueue, RetryMessage
from retryq.metrics import RetryMetrics
from retryq.middleware import MiddlewareChain
from retryq.hooks import HookRegistry, HookEvent


class RetryRunner:
    """High-level runner that ties together queue, metrics, middleware, and hooks."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], bool],
        middleware: Optional[MiddlewareChain] = None,
        hooks: Optional[HookRegistry] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._metrics = RetryMetrics()
        self._middleware = middleware or MiddlewareChain()
        self._hooks = hooks or HookRegistry()

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    def enqueue(self, message: RetryMessage) -> None:
        self._queue.enqueue(message)
        self._metrics.record_enqueue()
        self._hooks.fire(HookEvent.ON_ENQUEUE, message)

    def process_next(self) -> bool:
        """Process the next message in the queue. Returns True if a message was processed."""
        message = self._queue.dequeue()
        if message is None:
            return False

        message = self._middleware.run(message)

        try:
            success = self._handler(message)
        except Exception:
            self._metrics.record_retry()
            self._hooks.fire(HookEvent.ON_ERROR, message)
            if not message.exhausted():
                self._queue.enqueue(message)
            else:
                self._metrics.record_dead_letter()
                self._hooks.fire(HookEvent.ON_DEAD_LETTER, message)
            return True

        if success:
            self._metrics.record_success()
            self._hooks.fire(HookEvent.ON_SUCCESS, message)
        else:
            self._metrics.record_retry()
            self._hooks.fire(HookEvent.ON_RETRY, message)
            if not message.exhausted():
                self._queue.enqueue(message)
            else:
                self._metrics.record_dead_letter()
                self._hooks.fire(HookEvent.ON_DEAD_LETTER, message)

        return True
