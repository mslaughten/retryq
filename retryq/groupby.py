"""Group messages in a RetryQueue by a key function for bulk inspection."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from retryq.queue import RetryMessage, RetryQueue


KeyFn = Callable[[RetryMessage], str]


class InvalidKeyFnError(Exception):
    def __init__(self, reason: str) -> None:
        self._reason = reason

    def __str__(self) -> str:
        return f"InvalidKeyFnError: {self._reason}"


@dataclass
class MessageGroup:
    key: str
    messages: List[RetryMessage] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.messages)

    @property
    def exhausted_count(self) -> int:
        return sum(1 for m in self.messages if m.exhausted())

    def __repr__(self) -> str:  # pragma: no cover
        return f"MessageGroup(key={self.key!r}, count={self.count})"


@dataclass
class GroupByResult:
    groups: Dict[str, MessageGroup] = field(default_factory=dict)

    @property
    def keys(self) -> List[str]:
        return list(self.groups.keys())

    @property
    def total(self) -> int:
        return sum(g.count for g in self.groups.values())

    def get(self, key: str) -> Optional[MessageGroup]:
        return self.groups.get(key)


def group_messages(queue: RetryQueue, key_fn: KeyFn) -> GroupByResult:
    """Group all pending messages in *queue* by the string returned by *key_fn*."""
    if not callable(key_fn):
        raise InvalidKeyFnError("key_fn must be callable")

    buckets: Dict[str, List[RetryMessage]] = defaultdict(list)

    for message in queue.messages():
        k = key_fn(message)
        if not isinstance(k, str):
            raise InvalidKeyFnError(
                f"key_fn must return str, got {type(k).__name__}"
            )
        buckets[k].append(message)

    result = GroupByResult()
    for k, msgs in buckets.items():
        result.groups[k] = MessageGroup(key=k, messages=msgs)
    return result
