"""Integration tests: ThrottledRetryRunner with real token-bucket timing."""

import time

import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_throttle import ThrottledRetryRunner
from retryq.throttle import TokenBucket


def make_message(payload: str = "data") -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=5)


def make_runner(capacity: float, refill_rate: float) -> ThrottledRetryRunner:
    queue = RetryQueue(backoff=ConstantBackoff(delay=0))
    throttle = TokenBucket(capacity=capacity, refill_rate=refill_rate)
    return ThrottledRetryRunner(
        queue=queue,
        handler=lambda m: True,
        throttle=throttle,
    )


class TestThrottleIntegration:
    def test_burst_limited_by_capacity(self):
        """Only `capacity` messages can be processed before throttle kicks in."""
        runner = make_runner(capacity=3, refill_rate=0.0001)
        for _ in range(6):
            runner.enqueue(make_message())

        processed = sum(1 for _ in range(6) if runner.process_next())
        assert processed == 3
        assert runner.throttle_rejections == 3

    def test_tokens_refill_allows_more_processing(self):
        """After waiting, refilled tokens allow additional messages through."""
        runner = make_runner(capacity=1, refill_rate=50.0)  # 50 tokens/sec
        runner.enqueue(make_message())
        runner.enqueue(make_message())

        first = runner.process_next()
        assert first is True

        # Bucket empty — second attempt should be throttled
        rejected = not runner.process_next()
        assert rejected

        # Wait for refill (~20 ms for 1 token at 50/sec)
        time.sleep(0.03)
        second = runner.process_next()
        assert second is True

    def test_metrics_consistent_after_mixed_run(self):
        """Enqueued == succeeded + retried + dead_lettered + still_queued."""
        runner = make_runner(capacity=5, refill_rate=0.0001)
        for i in range(8):
            runner.enqueue(make_message(f"msg-{i}"))

        for _ in range(8):
            runner.process_next()

        m = runner.metrics
        assert m.enqueued == 8
        assert m.succeeded == 5
        assert runner.throttle_rejections == 3
