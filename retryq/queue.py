"""Dead-letter queue retry logic core module."""

import time
import logging
from typing import Callable, Optional, Any
from dataclasses import dataclass, field
from retryq.backoff import BackoffStrategy, ExponentialBackoff

logger = logging.getLogger(__name__)


@dataclass
class RetryMessage:
    """Represents a message in the retry queue."""

    payload: Any
    attempt: int = 0
    max_attempts: int = 5
    last_error: Optional[Exception] = None
    metadata: dict = field(default_factory=dict)

    @property
    def exhausted(self) -> bool:
        """Return True if all retry attempts have been used."""
        return self.attempt >= self.max_attempts


class RetryQueue:
    """Manages retry logic for failed messages using a configurable backoff strategy."""

    def __init__(
        self,
        handler: Callable[[Any], None],
        backoff: Optional[BackoffStrategy] = None,
        max_attempts: int = 5,
        on_exhausted: Optional[Callable[[RetryMessage], None]] = None,
    ):
        self.handler = handler
        self.backoff = backoff or ExponentialBackoff()
        self.max_attempts = max_attempts
        self.on_exhausted = on_exhausted
        self._queue: list[RetryMessage] = []

    def enqueue(self, payload: Any, metadata: Optional[dict] = None) -> RetryMessage:
        """Add a new message to the retry queue."""
        msg = RetryMessage(
            payload=payload,
            max_attempts=self.max_attempts,
            metadata=metadata or {},
        )
        self._queue.append(msg)
        logger.debug("Enqueued message: %s", payload)
        return msg

    def process_next(self, dry_run: bool = False) -> Optional[RetryMessage]:
        """Process the next message in the queue. Returns the message or None."""
        if not self._queue:
            return None

        msg = self._queue.pop(0)
        if msg.exhausted:
            logger.warning("Message exhausted after %d attempts.", msg.attempt)
            if self.on_exhausted:
                self.on_exhausted(msg)
            return msg

        delay = self.backoff.get_delay(msg.attempt)
        logger.debug("Attempt %d for message, waiting %.2fs", msg.attempt + 1, delay)

        if not dry_run and delay > 0:
            time.sleep(delay)

        try:
            self.handler(msg.payload)
            logger.info("Message processed successfully on attempt %d.", msg.attempt + 1)
        except Exception as exc:  # noqa: BLE001
            msg.attempt += 1
            msg.last_error = exc
            logger.warning("Handler failed (attempt %d): %s", msg.attempt, exc)
            if not msg.exhausted:
                self._queue.append(msg)
            elif self.on_exhausted:
                self.on_exhausted(msg)

        return msg

    def drain(self, dry_run: bool = False) -> list[RetryMessage]:
        """Process all messages in the queue. Returns list of processed messages."""
        processed = []
        while self._queue:
            msg = self.process_next(dry_run=dry_run)
            if msg:
                processed.append(msg)
        return processed

    @property
    def size(self) -> int:
        """Return the current number of messages in the queue."""
        return len(self._queue)
