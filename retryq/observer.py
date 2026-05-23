"""Observer pattern for retry lifecycle events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List

from retryq.queue import RetryMessage


@dataclass
class RetryEvent:
    """Represents a single lifecycle event emitted during retry processing."""

    event_type: str
    message: RetryMessage
    extra: Dict = field(default_factory=dict)

    def __repr__(self) -> str:  # pragma: no cover
        return f"RetryEvent(type={self.event_type!r}, id={self.message.id!r})"


ObserverCallback = Callable[[RetryEvent], None]


class RetryObserver:
    """Subscribes to retry lifecycle events and dispatches them to registered callbacks."""

    VALID_EVENTS = frozenset(
        {"enqueue", "retry", "success", "dead_letter", "skip"}
    )

    def __init__(self) -> None:
        self._listeners: Dict[str, List[ObserverCallback]] = {
            ev: [] for ev in self.VALID_EVENTS
        }

    def subscribe(self, event_type: str, callback: ObserverCallback) -> None:
        """Register *callback* to be called whenever *event_type* is emitted."""
        if event_type not in self.VALID_EVENTS:
            raise ValueError(
                f"Unknown event type {event_type!r}. "
                f"Valid types: {sorted(self.VALID_EVENTS)}"
            )
        self._listeners[event_type].append(callback)

    def notify(self, event_type: str, message: RetryMessage, **extra) -> None:
        """Emit *event_type* carrying *message* to all subscribed callbacks."""
        if event_type not in self.VALID_EVENTS:
            raise ValueError(f"Unknown event type {event_type!r}")
        event = RetryEvent(event_type=event_type, message=message, extra=extra)
        for cb in self._listeners[event_type]:
            cb(event)

    def listener_count(self, event_type: str) -> int:
        """Return the number of listeners registered for *event_type*."""
        return len(self._listeners.get(event_type, []))
