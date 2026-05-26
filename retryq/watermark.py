"""High/low watermark alerting for RetryQueue depth monitoring."""
from dataclasses import dataclass, field
from typing import Callable, Optional


class WatermarkError(Exception):
    def __init__(self, message: str) -> None:
        self._message = message

    def __str__(self) -> str:
        return self._message


@dataclass
class WatermarkPolicy:
    """Triggers callbacks when queue depth crosses high or low watermarks."""

    high: int
    low: int
    on_high: Optional[Callable[[int], None]] = field(default=None)
    on_low: Optional[Callable[[int], None]] = field(default=None)

    def __post_init__(self) -> None:
        if self.high <= 0:
            raise WatermarkError("high watermark must be a positive integer")
        if self.low < 0:
            raise WatermarkError("low watermark must be a non-negative integer")
        if self.low >= self.high:
            raise WatermarkError("low watermark must be less than high watermark")
        self._above_high: bool = False

    def check(self, depth: int) -> str:
        """Evaluate depth against watermarks; fire callbacks as needed.

        Returns a string status: 'high', 'low', or 'normal'.
        """
        if depth >= self.high:
            if not self._above_high:
                self._above_high = True
                if self.on_high is not None:
                    self.on_high(depth)
            return "high"
        elif depth <= self.low:
            if self._above_high:
                self._above_high = False
                if self.on_low is not None:
                    self.on_low(depth)
            return "low"
        return "normal"

    @property
    def triggered_high(self) -> bool:
        """True if the queue is currently in a high-watermark state."""
        return self._above_high
