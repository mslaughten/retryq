"""Tag-based message classification and filtering for RetryQueue."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Set

from retryq.queue import RetryMessage


class InvalidTagError(Exception):
    def __init__(self, tag: str) -> None:
        self.tag = tag

    def __str__(self) -> str:
        return f"Invalid tag: {self.tag!r}. Tags must be non-empty strings."


def _validate_tag(tag: str) -> None:
    if not isinstance(tag, str) or not tag.strip():
        raise InvalidTagError(tag)


@dataclass
class TagRegistry:
    """Attach and query tags on RetryMessage instances."""

    _tags: dict[str, Set[str]] = field(default_factory=dict)

    def tag(self, message: RetryMessage, *tags: str) -> None:
        """Associate one or more tags with a message."""
        for t in tags:
            _validate_tag(t)
        bucket = self._tags.setdefault(message.id, set())
        bucket.update(tags)

    def untag(self, message: RetryMessage, *tags: str) -> None:
        """Remove tags from a message (silently ignores missing tags)."""
        bucket = self._tags.get(message.id, set())
        for t in tags:
            bucket.discard(t)

    def get_tags(self, message: RetryMessage) -> Set[str]:
        """Return the set of tags for a message."""
        return frozenset(self._tags.get(message.id, set()))

    def has_tag(self, message: RetryMessage, tag: str) -> bool:
        """Return True if the message carries the given tag."""
        return tag in self._tags.get(message.id, set())

    def has_all(self, message: RetryMessage, tags: Iterable[str]) -> bool:
        """Return True if the message carries every tag in *tags*."""
        bucket = self._tags.get(message.id, set())
        return all(t in bucket for t in tags)

    def has_any(self, message: RetryMessage, tags: Iterable[str]) -> bool:
        """Return True if the message carries at least one tag from *tags*."""
        bucket = self._tags.get(message.id, set())
        return any(t in bucket for t in tags)

    def predicate(self, *required_tags: str) -> Callable[[RetryMessage], bool]:
        """Return a callable suitable for use with MessageFilter.add()."""
        for t in required_tags:
            _validate_tag(t)

        def _check(msg: RetryMessage) -> bool:
            return self.has_all(msg, required_tags)

        return _check

    def purge(self, message: RetryMessage) -> None:
        """Remove all tag data for a message (e.g. after it is dead-lettered)."""
        self._tags.pop(message.id, None)
