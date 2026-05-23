"""Partition support for routing messages to named retry queues."""

from __future__ import annotations

from typing import Callable, Dict, Optional

from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import BackoffStrategy, ExponentialBackoff


PartitionKey = str
KeyFn = Callable[[RetryMessage], PartitionKey]


class PartitionedRetryQueue:
    """Routes messages to isolated RetryQueue instances based on a partition key."""

    def __init__(
        self,
        key_fn: KeyFn,
        backoff: Optional[BackoffStrategy] = None,
        max_attempts: int = 3,
    ) -> None:
        if not callable(key_fn):
            raise TypeError("key_fn must be callable")
        self._key_fn = key_fn
        self._backoff = backoff or ExponentialBackoff()
        self._max_attempts = max_attempts
        self._partitions: Dict[PartitionKey, RetryQueue] = {}

    @property
    def partition_keys(self) -> list[PartitionKey]:
        """Return the currently active partition keys."""
        return list(self._partitions.keys())

    def _get_or_create(self, key: PartitionKey) -> RetryQueue:
        if key not in self._partitions:
            self._partitions[key] = RetryQueue(
                backoff=self._backoff,
                max_attempts=self._max_attempts,
            )
        return self._partitions[key]

    def enqueue(self, message: RetryMessage) -> PartitionKey:
        """Enqueue a message into the appropriate partition. Returns the key."""
        key = self._key_fn(message)
        queue = self._get_or_create(key)
        queue.enqueue(message)
        return key

    def process_next(self, key: PartitionKey, handler: Callable[[RetryMessage], bool]) -> bool:
        """Process the next message in the named partition. Returns False if empty."""
        queue = self._partitions.get(key)
        if queue is None:
            return False
        return queue.process_next(handler)

    def queue_for(self, key: PartitionKey) -> Optional[RetryQueue]:
        """Return the RetryQueue for a given key, or None if it doesn't exist."""
        return self._partitions.get(key)

    def size(self, key: PartitionKey) -> int:
        """Return the number of pending messages in a partition."""
        queue = self._partitions.get(key)
        return len(queue) if queue is not None else 0

    def total_size(self) -> int:
        """Return the total number of pending messages across all partitions."""
        return sum(len(q) for q in self._partitions.values())
