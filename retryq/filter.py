"""Filter module for selectively processing retry messages based on predicates."""

from typing import Callable, List, Optional
from retryq.queue import RetryMessage


FilterPredicate = Callable[[RetryMessage], bool]


class MessageFilter:
    """Applies one or more predicate functions to filter retry messages."""

    def __init__(self) -> None:
        self._predicates: List[FilterPredicate] = []

    def add(self, predicate: FilterPredicate) -> "MessageFilter":
        """Register a predicate. Returns self for chaining."""
        self._predicates.append(predicate)
        return self

    def matches(self, message: RetryMessage) -> bool:
        """Return True only if all predicates accept the message."""
        return all(p(message) for p in self._predicates)

    def apply(self, messages: List[RetryMessage]) -> List[RetryMessage]:
        """Return the subset of messages that match all predicates."""
        return [m for m in messages if self.matches(m)]

    @property
    def predicate_count(self) -> int:
        return len(self._predicates)


def by_topic(topic: str) -> FilterPredicate:
    """Predicate: message payload contains a matching 'topic' key."""
    def _check(message: RetryMessage) -> bool:
        return message.payload.get("topic") == topic
    return _check


def by_max_attempts(max_attempts: int) -> FilterPredicate:
    """Predicate: message has not yet exceeded max_attempts retries."""
    def _check(message: RetryMessage) -> bool:
        return message.attempts < max_attempts
    return _check


def by_metadata_key(key: str, value: object) -> FilterPredicate:
    """Predicate: message metadata contains key with the given value."""
    def _check(message: RetryMessage) -> bool:
        return message.metadata.get(key) == value
    return _check
