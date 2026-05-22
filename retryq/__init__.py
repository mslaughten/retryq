"""retryq — Simple library for dead-letter queue retry logic with pluggable backoff strategies."""

from retryq.backoff import (
    BackoffStrategy,
    ConstantBackoff,
    ExponentialBackoff,
    LinearBackoff,
    get_delay,
)
from retryq.queue import RetryQueue, RetryMessage

__all__ = [
    "BackoffStrategy",
    "ConstantBackoff",
    "ExponentialBackoff",
    "LinearBackoff",
    "get_delay",
    "RetryQueue",
    "RetryMessage",
]

__version__ = "0.1.0"
