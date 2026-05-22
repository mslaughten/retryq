"""retryq — Simple dead-letter queue retry logic with pluggable backoff strategies."""

from retryq.backoff import (
    BackoffStrategy,
    ConstantBackoff,
    ExponentialBackoff,
    LinearBackoff,
)

__all__ = [
    "BackoffStrategy",
    "ConstantBackoff",
    "LinearBackoff",
    "ExponentialBackoff",
]

__version__ = "0.1.0"
