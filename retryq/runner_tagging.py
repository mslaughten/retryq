"""RetryRunner variant that integrates TagRegistry lifecycle management."""
from __future__ import annotations

from typing import Callable, Iterable

from retryq.backoff import BackoffStrategy
from retryq.hooks import HookEvent, HookRegistry
from retryq.metrics import RetryMetrics
from retryq.queue import RetryMessage, RetryQueue
from retryq.tagging import TagRegistry


class TaggingRetryRunner:
    """Wraps RetryRunner with automatic tag lifecycle management.

    Tags are purged from the registry when a message is dead-lettered,
    preventing unbounded memory growth.
    """

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], None],
        backoff: BackoffStrategy,
        *,
        tag_registry: TagRegistry | None = None,
        dead_letter_callback: Callable[[RetryMessage], None] | None = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._backoff = backoff
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()
        self.tag_registry: TagRegistry = tag_registry or TagRegistry()
        self._dead_letter_callback = dead_letter_callback

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    def enqueue(self, message: RetryMessage, *initial_tags: str) -> None:
        """Enqueue a message, optionally tagging it immediately."""
        self._queue.enqueue(message)
        self._metrics.record_enqueue()
        if initial_tags:
            self.tag_registry.tag(message, *initial_tags)
        self._hooks.fire(HookEvent.ENQUEUED, message)

    def process_next(self) -> bool:
        """Process the next message in the queue.

        Returns True if a message was processed, False if the queue was empty.
        """
        message = self._queue.dequeue()
        if message is None:
            return False

        try:
            self._handler(message)
            self._metrics.record_success()
            self._hooks.fire(HookEvent.SUCCESS, message)
            self.tag_registry.purge(message)
        except Exception:
            if message.exhausted():
                self._metrics.record_dead_letter()
                self._hooks.fire(HookEvent.DEAD_LETTERED, message)
                if self._dead_letter_callback:
                    self._dead_letter_callback(message)
                self.tag_registry.purge(message)
            else:
                delay = self._backoff.get_delay(message.attempts)
                message.attempts += 1
                self._queue.enqueue(message)
                self._metrics.record_retry()
                self._hooks.fire(HookEvent.RETRIED, message)

        return True
