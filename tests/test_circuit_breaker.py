"""Tests for CircuitBreaker and CircuitBreakerRetryRunner."""

import pytest
from unittest.mock import MagicMock

from retryq.backoff import ConstantBackoff
from retryq.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, CircuitState
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_circuit import CircuitBreakerRetryRunner


def make_message(payload: str = "test") -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=3)


def make_runner(failure_threshold: int = 2, handler=None) -> CircuitBreakerRetryRunner:
    queue = RetryQueue(backoff=ConstantBackoff(delay=0))
    cb = CircuitBreaker(name="test", failure_threshold=failure_threshold, recovery_timeout=60.0)
    h = handler or (lambda msg: False)
    return CircuitBreakerRetryRunner(queue=queue, handler=h, circuit_breaker=cb)


class TestCircuitBreaker:
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker(name="svc")
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(name="svc", failure_threshold=2)
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_check_raises_when_open(self):
        cb = CircuitBreaker(name="svc", failure_threshold=1)
        cb.record_failure()
        with pytest.raises(CircuitBreakerOpen):
            cb.check()

    def test_success_resets_to_closed(self):
        cb = CircuitBreaker(name="svc", failure_threshold=1)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_manual_reset(self):
        cb = CircuitBreaker(name="svc", failure_threshold=1)
        cb.record_failure()
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_invalid_threshold_raises(self):
        with pytest.raises(ValueError):
            CircuitBreaker(name="svc", failure_threshold=0)

    def test_invalid_timeout_raises(self):
        with pytest.raises(ValueError):
            CircuitBreaker(name="svc", recovery_timeout=0)

    def test_circuit_breaker_open_str(self):
        cb = CircuitBreaker(name="svc", failure_threshold=1, recovery_timeout=10.0)
        cb.record_failure()
        try:
            cb.check()
        except CircuitBreakerOpen as exc:
            assert "svc" in str(exc)
            assert "OPEN" in str(exc)


class TestCircuitBreakerRetryRunner:
    def test_process_next_returns_false_when_empty(self):
        runner = make_runner()
        assert runner.process_next() is False

    def test_success_increments_metrics(self):
        runner = make_runner(handler=lambda msg: True)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.successes == 1

    def test_failure_opens_circuit_after_threshold(self):
        runner = make_runner(failure_threshold=2, handler=lambda msg: False)
        for _ in range(4):
            runner.enqueue(make_message())
        runner.process_next()
        runner.process_next()
        assert runner.circuit.state == CircuitState.OPEN

    def test_open_circuit_blocks_processing(self):
        runner = make_runner(failure_threshold=1, handler=lambda msg: False)
        runner.enqueue(make_message())
        runner.process_next()
        runner.enqueue(make_message())
        with pytest.raises(CircuitBreakerOpen):
            runner.process_next()

    def test_dead_letter_callback_called_on_exhaustion(self):
        dead_letters = []
        runner = make_runner(failure_threshold=99, handler=lambda msg: False)
        runner._dead_letter_callback = dead_letters.append
        msg = RetryMessage(payload="x", max_attempts=1)
        runner.enqueue(msg)
        runner.process_next()
        assert len(dead_letters) == 1
        assert dead_letters[0].payload == "x"
