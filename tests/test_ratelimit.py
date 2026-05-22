"""Tests for RateLimiter and RateLimitedRetryRunner."""

import pytest
from unittest.mock import MagicMock

from retryq.ratelimit import RateLimiter, RateLimitExceeded
from retryq.runner_ratelimit import RateLimitedRetryRunner
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff
from retryq.runner import RetryRunner


def make_message(payload="hello"):
    return RetryMessage(payload=payload)


def make_runner(handler=None):
    if handler is None:
        handler = lambda msg: None
    queue = RetryQueue(backoff=ConstantBackoff(0))
    return RetryRunner(queue=queue, handler=handler)


class TestRateLimiter:
    def test_invalid_limit_raises(self):
        with pytest.raises(ValueError):
            RateLimiter(limit=0)

    def test_invalid_window_raises(self):
        with pytest.raises(ValueError):
            RateLimiter(limit=1, window=0)

    def test_initial_remaining_equals_limit(self):
        rl = RateLimiter(limit=5, window=1.0)
        assert rl.remaining(now=0.0) == 5

    def test_acquire_decrements_remaining(self):
        rl = RateLimiter(limit=3, window=1.0)
        rl.acquire(now=0.0)
        assert rl.remaining(now=0.0) == 2

    def test_acquire_raises_when_limit_reached(self):
        rl = RateLimiter(limit=2, window=1.0)
        rl.acquire(now=0.0)
        rl.acquire(now=0.1)
        with pytest.raises(RateLimitExceeded) as exc_info:
            rl.acquire(now=0.2)
        assert exc_info.value.limit == 2

    def test_is_allowed_false_when_full(self):
        rl = RateLimiter(limit=1, window=1.0)
        rl.acquire(now=0.0)
        assert rl.is_allowed(now=0.5) is False

    def test_slots_free_after_window_expires(self):
        rl = RateLimiter(limit=1, window=1.0)
        rl.acquire(now=0.0)
        assert rl.is_allowed(now=1.1) is True

    def test_reset_clears_all_timestamps(self):
        rl = RateLimiter(limit=2, window=1.0)
        rl.acquire(now=0.0)
        rl.acquire(now=0.1)
        rl.reset()
        assert rl.remaining(now=0.2) == 2

    def test_rate_limit_exceeded_str(self):
        exc = RateLimitExceeded(limit=5, window=2.0)
        assert "5" in str(exc) and "2.0" in str(exc)


class TestRateLimitedRetryRunner:
    def test_process_next_blocked_when_rate_limit_exceeded(self):
        runner = make_runner()
        rl = RateLimiter(limit=1, window=10.0)
        rl.acquire()  # exhaust the single slot
        wrapped = RateLimitedRetryRunner(runner=runner, rate_limiter=rl)
        runner.enqueue(make_message())
        result = wrapped.process_next()
        assert result is False

    def test_process_next_succeeds_when_slot_available(self):
        runner = make_runner()
        rl = RateLimiter(limit=5, window=10.0)
        wrapped = RateLimitedRetryRunner(runner=runner, rate_limiter=rl)
        wrapped.enqueue(make_message())
        result = wrapped.process_next()
        assert result is True

    def test_process_all_returns_count(self):
        runner = make_runner()
        rl = RateLimiter(limit=10, window=60.0)
        wrapped = RateLimitedRetryRunner(runner=runner, rate_limiter=rl)
        for _ in range(3):
            wrapped.enqueue(make_message())
        count = wrapped.process_all()
        assert count == 3

    def test_metrics_delegated_to_runner(self):
        runner = make_runner()
        rl = RateLimiter(limit=5, window=1.0)
        wrapped = RateLimitedRetryRunner(runner=runner, rate_limiter=rl)
        assert wrapped.metrics is runner.metrics
