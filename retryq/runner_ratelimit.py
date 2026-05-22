"""A RetryRunner wrapper that enforces a rate limit on message processing."""

from typing import Optional

from retryq.runner import RetryRunner
from retryq.ratelimit import RateLimiter, RateLimitExceeded
from retryq.queue import RetryMessage


class RateLimitedRetryRunner:
    """Wraps a RetryRunner and gates process_next behind a RateLimiter.

    If the rate limit is exceeded, process_next returns False without
    consuming a message from the queue.

    Args:
        runner: The underlying RetryRunner to delegate to.
        rate_limiter: The RateLimiter instance to enforce.
    """

    def __init__(self, runner: RetryRunner, rate_limiter: RateLimiter) -> None:
        self._runner = runner
        self._rate_limiter = rate_limiter

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._rate_limiter

    @property
    def metrics(self):
        return self._runner.metrics

    def enqueue(self, message: RetryMessage) -> None:
        """Enqueue a message on the underlying runner."""
        self._runner.enqueue(message)

    def process_next(self) -> bool:
        """Process the next message if the rate limit allows it.

        Returns:
            True if a message was processed, False otherwise (queue empty
            or rate limit exceeded).
        """
        try:
            self._rate_limiter.acquire()
        except RateLimitExceeded:
            return False
        return self._runner.process_next()

    def process_all(self) -> int:
        """Process messages until the queue is empty or rate limit blocks.

        Returns:
            Number of messages successfully processed.
        """
        processed = 0
        while self.process_next():
            processed += 1
        return processed
