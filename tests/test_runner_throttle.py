"""Unit tests for ThrottledRetryRunner."""

import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_throttle import ThrottledRetryRunner
from retryq.throttle import TokenBucket


def make_message(payload: str = "hello", max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=max_attempts)


def make_runner(
    capacity: float = 10.0,
    refill_rate: float = 100.0,
    max_attempts: int = 3,
):
    queue = RetryQueue(backoff=ConstantBackoff(delay=0))
    throttle = TokenBucket(capacity=capacity, refill_rate=refill_rate)
    handler = lambda msg: True  # noqa: E731
    return ThrottledRetryRunner(queue=queue, handler=handler, throttle=throttle)


class TestThrottledRetryRunner:
    def test_process_next_returns_false_when_empty(self):
        runner = make_runner()
        assert runner.process_next() is False

    def test_enqueue_increments_metrics(self):
        runner = make_runner()
        runner.enqueue(make_message())
        assert runner.metrics.enqueued == 1

    def test_successful_process_records_success(self):
        runner = make_runner()
        runner.enqueue(make_message())
        result = runner.process_next()
        assert result is True
        assert runner.metrics.succeeded == 1

    def test_throttle_rejection_increments_counter(self):
        queue = RetryQueue(backoff=ConstantBackoff(delay=0))
        # Bucket with zero capacity — always empty after init workaround:
        # use capacity=1 and drain it first
        throttle = TokenBucket(capacity=1, refill_rate=0.0001)
        throttle.consume(1)  # drain
        runner = ThrottledRetryRunner(
            queue=queue,
            handler=lambda m: True,
            throttle=throttle,
        )
        runner.enqueue(make_message())
        result = runner.process_next()
        assert result is False
        assert runner.throttle_rejections == 1

    def test_dead_letter_called_when_exhausted(self):
        dead_letters = []
        queue = RetryQueue(backoff=ConstantBackoff(delay=0))
        throttle = TokenBucket(capacity=100, refill_rate=100)
        runner = ThrottledRetryRunner(
            queue=queue,
            handler=lambda m: False,
            throttle=throttle,
            dead_letter=dead_letters.append,
        )
        msg = make_message(max_attempts=1)
        runner.enqueue(msg)
        runner.process_next()
        assert len(dead_letters) == 1
        assert runner.metrics.dead_lettered == 1

    def test_failed_message_requeued_if_not_exhausted(self):
        queue = RetryQueue(backoff=ConstantBackoff(delay=0))
        throttle = TokenBucket(capacity=100, refill_rate=100)
        runner = ThrottledRetryRunner(
            queue=queue,
            handler=lambda m: False,
            throttle=throttle,
        )
        runner.enqueue(make_message(max_attempts=3))
        runner.process_next()
        assert len(queue) == 1
        assert runner.metrics.retried == 1
