"""Deduplication support for RetryQueue messages."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Optional

from retryq.queue import RetryMessage


class DuplicateMessageError(Exception):
    """Raised when a duplicate message is detected."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id

    def __str__(self) -> str:
        return f"Duplicate message detected: {self.message_id}"


def _fingerprint(msg: RetryMessage) -> str:
    """Compute a stable fingerprint from message id and payload."""
    raw = json.dumps({"id": msg.id, "payload": msg.payload}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class DedupWindow:
    """Tracks seen message fingerprints within a rolling time window."""

    ttl_seconds: float = 300.0
    _seen: dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [k for k, ts in self._seen.items() if now - ts >= self.ttl_seconds]
        for k in expired:
            del self._seen[k]

    def is_duplicate(self, msg: RetryMessage) -> bool:
        """Return True if the message has been seen within the TTL window."""
        self._evict_expired()
        fp = _fingerprint(msg)
        return fp in self._seen

    def record(self, msg: RetryMessage) -> None:
        """Record a message as seen."""
        self._evict_expired()
        fp = _fingerprint(msg)
        self._seen[fp] = time.monotonic()

    def check_and_record(self, msg: RetryMessage, *, raise_on_duplicate: bool = False) -> bool:
        """Check for duplicate and record. Returns True if duplicate.

        If raise_on_duplicate is True, raises DuplicateMessageError instead.
        """
        if self.is_duplicate(msg):
            if raise_on_duplicate:
                raise DuplicateMessageError(msg.id)
            return True
        self.record(msg)
        return False

    @property
    def size(self) -> int:
        """Number of fingerprints currently tracked."""
        self._evict_expired()
        return len(self._seen)
