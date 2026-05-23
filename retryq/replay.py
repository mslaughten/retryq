"""Replay support for retryq — allows replaying dead-lettered messages back into a queue."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from retryq.queue import RetryMessage, RetryQueue


class ReplayError(Exception):
    """Raised when a replay operation fails."""

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __str__(self) -> str:
        return f"ReplayError: {self.reason}"


@dataclass
class ReplayResult:
    """Summary of a replay operation."""

    replayed: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.replayed + self.skipped


class MessageReplayer:
    """Replays a collection of dead-lettered messages into a target RetryQueue."""

    def __init__(
        self,
        target: RetryQueue,
        *,
        reset_attempts: bool = True,
        predicate: Optional[Callable[[RetryMessage], bool]] = None,
    ) -> None:
        if not isinstance(target, RetryQueue):
            raise TypeError("target must be a RetryQueue instance")
        self._target = target
        self._reset_attempts = reset_attempts
        self._predicate = predicate or (lambda _: True)

    @property
    def target(self) -> RetryQueue:
        return self._target

    def replay(self, messages: List[RetryMessage]) -> ReplayResult:
        """Replay *messages* into the target queue and return a ReplayResult."""
        if not isinstance(messages, list):
            raise ReplayError("messages must be a list")

        result = ReplayResult()

        for msg in messages:
            if not isinstance(msg, RetryMessage):
                result.errors.append(f"skipped non-RetryMessage value: {msg!r}")
                result.skipped += 1
                continue

            if not self._predicate(msg):
                result.skipped += 1
                continue

            if self._reset_attempts:
                msg = RetryMessage(
                    id=msg.id,
                    payload=msg.payload,
                    max_attempts=msg.max_attempts,
                    metadata=dict(msg.metadata),
                )

            self._target.enqueue(msg)
            result.replayed += 1

        return result
