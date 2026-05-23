"""PriorityRetryRunner — a RetryRunner that dispatches messages in priority order.

Wraps PriorityRetryQueue and exposes the same metrics / hooks surface as
the standard RetryRunner so it can be used as a drop-in replacement.
"""

from __future__ import annotations

from typing import Callable, Optional

from retryq.backoff import BackoffStrategy
from retryq.hooks import HookEvent, HookRegistry
from retryq.metrics import RetryMetrics
from retryq.priority import InvalidPriorityError, PriorityRetryQueue
from retryq.queue import RetryMessage


class PriorityRetryRunner:
    """High-level runner backed by a PriorityRetryQueue.

    Parameters
    ----------
    handler:
        Callable that processes a message; returns True on success.
    backoff:
        Optional backoff strategy (forwarded to the underlying queue).
    max_attempts:
        Maximum delivery attempts before a message is dead-lettered.
    default_priority:
        Priority assigned by :meth:`enqueue` when none is specified (0–10).
    """

    def __init__(
        self,
        handler: Callable[[RetryMessage], bool],
        backoff: Optional[BackoffStrategy] = None,
        max_attempts: int = 3,
        default_priority: int = 5,
    ) -> None:
        self._queue = PriorityRetryQueue(
            handler=self._tracked_handler,
            backoff=backoff,
            max_attempts=max_attempts,
        )
        self._raw_handler = handler
        self._default_priority = default_priority
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    def enqueue(self, message: RetryMessage, priority: Optional[int] = None) -> None:
        """Enqueue *message* with *priority* (defaults to ``default_priority``)."""
        p = priority if priority is not None else self._default_priority
        self._queue.enqueue(message, priority=p)
        self._metrics.record_enqueue(message)
        self._hooks.fire(HookEvent.ENQUEUE, message)

    def process_next(self) -> bool:
        """Process the highest-priority pending message."""
        return self._queue.process_next()

    def drain(self) -> int:
        """Process all pending messages. Returns the number processed."""
        count = 0
        while self.process_next():
            count += 1
        return count

    @property
    def dead_letters(self) -> list[RetryMessage]:
        return self._queue.dead_letters

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _tracked_handler(self, message: RetryMessage) -> bool:
        success = self._raw_handler(message)
        if success:
            self._metrics.record_success(message)
            self._hooks.fire(HookEvent.SUCCESS, message)
        else:
            self._metrics.record_retry(message)
            self._hooks.fire(HookEvent.RETRY, message)
        return success
