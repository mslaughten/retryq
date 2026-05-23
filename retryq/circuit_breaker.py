"""Circuit breaker for retryq — prevents retrying when a handler is consistently failing."""

from dataclasses import dataclass, field
from enum import Enum, auto
from time import monotonic
from typing import Optional


class CircuitState(Enum):
    CLOSED = auto()    # Normal operation
    OPEN = auto()      # Blocking calls
    HALF_OPEN = auto() # Testing recovery


class CircuitBreakerOpen(Exception):
    """Raised when an operation is attempted while the circuit is open."""

    def __init__(self, name: str, reset_at: float):
        self.name = name
        self.reset_at = reset_at

    def __str__(self) -> str:
        remaining = max(0.0, self.reset_at - monotonic())
        return f"Circuit '{self.name}' is OPEN — resets in {remaining:.1f}s"


@dataclass
class CircuitBreaker:
    """Tracks failure counts and opens the circuit after a threshold is exceeded."""

    name: str
    failure_threshold: int = 3
    recovery_timeout: float = 30.0

    _failures: int = field(default=0, init=False, repr=False)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False, repr=False)
    _opened_at: Optional[float] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if self._opened_at is not None and monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def allow_request(self) -> bool:
        """Return True if the circuit allows the request to proceed."""
        return self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    def record_success(self) -> None:
        """Reset the circuit after a successful call."""
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._opened_at = None

    def record_failure(self) -> None:
        """Increment failure count and open the circuit if threshold is reached."""
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = monotonic()

    def check(self) -> None:
        """Raise CircuitBreakerOpen if requests are not allowed."""
        if not self.allow_request():
            assert self._opened_at is not None
            raise CircuitBreakerOpen(self.name, self._opened_at + self.recovery_timeout)

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._opened_at = None
