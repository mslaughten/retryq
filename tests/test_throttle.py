"""Unit tests for retryq.throttle (TokenBucket and ThrottleExceeded)."""

import pytest

from retryq.throttle import ThrottleExceeded, TokenBucket


def make_clock(start: float = 0.0):
    """Returns a mutable clock callable."""
    state = {"t": start}

    def clock() -> float:
        return state["t"]

    clock.advance = lambda dt: state.update({"t": state["t"] + dt})  # type: ignore[attr-defined]
    return clock


class TestTokenBucket:
    def test_invalid_capacity_raises(self):
        with pytest.raises(ValueError, match="capacity"):
            TokenBucket(capacity=0, refill_rate=1.0)

    def test_invalid_refill_rate_raises(self):
        with pytest.raises(ValueError, match="refill_rate"):
            TokenBucket(capacity=5, refill_rate=0)

    def test_starts_full(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=10, refill_rate=1.0, _clock=clock)
        assert bucket.available == pytest.approx(10.0)

    def test_consume_reduces_tokens(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=5, refill_rate=1.0, _clock=clock)
        bucket.consume(2)
        assert bucket.available == pytest.approx(3.0)

    def test_consume_raises_when_empty(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=1, refill_rate=1.0, _clock=clock)
        bucket.consume(1)
        with pytest.raises(ThrottleExceeded):
            bucket.consume(1)

    def test_try_consume_returns_false_when_empty(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=1, refill_rate=1.0, _clock=clock)
        assert bucket.try_consume() is True
        assert bucket.try_consume() is False

    def test_refill_over_time(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=10, refill_rate=2.0, _clock=clock)
        bucket.consume(10)
        clock.advance(3.0)
        assert bucket.available == pytest.approx(6.0)

    def test_refill_does_not_exceed_capacity(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=5, refill_rate=10.0, _clock=clock)
        clock.advance(100.0)
        assert bucket.available == pytest.approx(5.0)

    def test_consume_zero_raises(self):
        clock = make_clock()
        bucket = TokenBucket(capacity=5, refill_rate=1.0, _clock=clock)
        with pytest.raises(ValueError):
            bucket.consume(0)


class TestThrottleExceeded:
    def test_str_contains_info(self):
        exc = ThrottleExceeded(available=0.5, requested=1.0)
        msg = str(exc)
        assert "0.50" in msg or "0.5" in msg
        assert "1.0" in msg
