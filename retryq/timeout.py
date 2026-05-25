"""Per-message processing timeout enforcement for retry handlers."""

from __future__ import annotations

import signal
from dataclasses import dataclass, field
from typing import Callable, Any

from retryq.queue import RetryMessage


class TimeoutError(Exception):  # noqa: A001
    """Raised when a handler exceeds its allotted processing time."""

    def __init__(self, message_id: str, timeout_seconds: float) -> None:
        self.message_id = message_id
        self.timeout_seconds = timeout_seconds

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"Message '{self.message_id}' timed out after "
            f"{self.timeout_seconds}s"
        )


@dataclass
class TimeoutPolicy:
    """Defines how long a handler may run before being cancelled."""

    seconds: float
    raise_on_timeout: bool = True

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("timeout seconds must be positive")

    def enforce(self, message: RetryMessage, handler: Callable[[Any], bool]) -> bool:
        """Run *handler* with the message, raising TimeoutError if it stalls.

        Uses SIGALRM on POSIX systems.  Returns the handler's bool result on
        success, or False when raise_on_timeout is False and the deadline fires.
        """
        timed_out: list[bool] = [False]

        def _alarm_handler(signum: int, frame: Any) -> None:  # noqa: ARG001
            timed_out[0] = True

        old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, self.seconds)
        try:
            result = handler(message)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

        if timed_out[0]:
            if self.raise_on_timeout:
                raise TimeoutError(message.id, self.seconds)
            return False

        return result
