"""TTL (time-to-live) support for retry messages."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from retryq.queue import RetryMessage


class TTLExpiredError(Exception):
    """Raised when a message has exceeded its time-to-live."""

    def __init__(self, message_id: str, ttl: float, age: float) -> None:
        self.message_id = message_id
        self.ttl = ttl
        self.age = age

    def __str__(self) -> str:
        return (
            f"Message '{self.message_id}' expired: "
            f"age={self.age:.2f}s exceeds ttl={self.ttl:.2f}s"
        )


@dataclass
class TTLPolicy:
    """Defines TTL behaviour for retry messages."""

    max_age_seconds: float
    clock: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if self.clock is None:
            self.clock = time

    def age_of(self, message: RetryMessage) -> float:
        """Return the age of a message in seconds."""
        created_at: Optional[float] = message.metadata.get("created_at")
        if created_at is None:
            return 0.0
        return self.clock.time() - created_at

    def is_expired(self, message: RetryMessage) -> bool:
        """Return True if the message has exceeded its TTL."""
        return self.age_of(message) > self.max_age_seconds

    def stamp(self, message: RetryMessage) -> RetryMessage:
        """Attach a creation timestamp to the message metadata if absent."""
        if "created_at" not in message.metadata:
            message.metadata["created_at"] = self.clock.time()
        return message

    def check(self, message: RetryMessage) -> None:
        """Raise TTLExpiredError if the message has expired."""
        age = self.age_of(message)
        if age > self.max_age_seconds:
            raise TTLExpiredError(message.id, self.max_age_seconds, age)
