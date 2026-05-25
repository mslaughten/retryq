"""Tests for retryq.timeout — TimeoutPolicy and TimeoutError."""

from __future__ import annotations

import time
import uuid
import pytest

from retryq.queue import RetryMessage
from retryq.timeout import TimeoutError, TimeoutPolicy


def make_message(payload: dict | None = None) -> RetryMessage:
    return RetryMessage(
        id=str(uuid.uuid4()),
        payload=payload or {"task": "noop"},
        max_attempts=3,
    )


class TestTimeoutPolicy:
    def test_invalid_seconds_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutPolicy(seconds=0)

    def test_negative_seconds_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutPolicy(seconds=-1.0)

    def test_fast_handler_returns_true(self) -> None:
        policy = TimeoutPolicy(seconds=2.0)
        msg = make_message()
        result = policy.enforce(msg, lambda m: True)
        assert result is True

    def test_fast_handler_returning_false_propagates(self) -> None:
        policy = TimeoutPolicy(seconds=2.0)
        msg = make_message()
        result = policy.enforce(msg, lambda m: False)
        assert result is False

    def test_slow_handler_raises_timeout_error(self) -> None:
        policy = TimeoutPolicy(seconds=0.05)
        msg = make_message()

        def slow_handler(m: RetryMessage) -> bool:
            time.sleep(1)
            return True  # pragma: no cover

        with pytest.raises(TimeoutError) as exc_info:
            policy.enforce(msg, slow_handler)

        assert exc_info.value.message_id == msg.id
        assert exc_info.value.timeout_seconds == pytest.approx(0.05)

    def test_slow_handler_returns_false_when_no_raise(self) -> None:
        policy = TimeoutPolicy(seconds=0.05, raise_on_timeout=False)
        msg = make_message()

        def slow_handler(m: RetryMessage) -> bool:
            time.sleep(1)
            return True  # pragma: no cover

        result = policy.enforce(msg, slow_handler)
        assert result is False

    def test_timeout_error_str_contains_id(self) -> None:
        err = TimeoutError("abc-123", 1.5)
        assert "abc-123" in str(err)
        assert "1.5" in str(err)

    def test_default_raise_on_timeout_is_true(self) -> None:
        policy = TimeoutPolicy(seconds=1.0)
        assert policy.raise_on_timeout is True
