"""Event hooks for RetryQueue lifecycle events."""

from typing import Callable, Dict, List, Optional
from retryq.queue import RetryMessage


HookFn = Callable[[RetryMessage], None]


class HookEvent:
    ON_ENQUEUE = "on_enqueue"
    ON_RETRY = "on_retry"
    ON_SUCCESS = "on_success"
    ON_DEAD_LETTER = "on_dead_letter"
    ON_ERROR = "on_error"

    ALL = [ON_ENQUEUE, ON_RETRY, ON_SUCCESS, ON_DEAD_LETTER, ON_ERROR]


class HookRegistry:
    """Registry for lifecycle event hooks."""

    def __init__(self) -> None:
        self._hooks: Dict[str, List[HookFn]] = {event: [] for event in HookEvent.ALL}

    def register(self, event: str, fn: HookFn) -> None:
        """Register a hook function for a given event."""
        if event not in HookEvent.ALL:
            raise ValueError(f"Unknown hook event: {event!r}. Must be one of {HookEvent.ALL}")
        self._hooks[event].append(fn)

    def fire(self, event: str, message: RetryMessage) -> None:
        """Fire all hooks registered for the given event."""
        for fn in self._hooks.get(event, []):
            fn(message)

    def clear(self, event: Optional[str] = None) -> None:
        """Clear hooks for a specific event, or all events if none specified."""
        if event is not None:
            if event not in HookEvent.ALL:
                raise ValueError(f"Unknown hook event: {event!r}")
            self._hooks[event] = []
        else:
            self._hooks = {e: [] for e in HookEvent.ALL}

    def count(self, event: str) -> int:
        """Return the number of hooks registered for an event."""
        return len(self._hooks.get(event, []))
