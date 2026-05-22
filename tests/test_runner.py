"""Tests for retryq.runner.RetryRunner."""

from unittest.mock import MagicMock, call

import pytest

from retryq.backoff import ConstantBackoff
from retryq.metrics import RetryMetrics
from retryq.middleware import MiddlewareChain, LoggingMiddleware
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner import RetryRunner


def make_queue() -> RetryQueue:
    return RetryQueue(backoff=ConstantBackoff(delay=0))


def make_message(msg_id: str = "m1", max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(id=msg_id, payload={"data": 1}, max_attempts=max_attempts)


class TestRetryRunnerBasics:
    def test_enqueue_increments_metrics(self):
        metrics = RetryMetrics()
        runner = RetryRunner(make_queue(), handler=MagicMock(), metrics=metrics)
        runner.enqueue(make_message())
        assert metrics.total_enqueued == 1

    def test_process_next_returns_false_when_empty(self):
        runner = RetryRunner(make_queue(), handler=MagicMock())
        assert runner.process_next() is False

    def test_process_next_calls_handler(self):
        handler = MagicMock()
        runner = RetryRunner(make_queue(), handler=handler)
        runner.enqueue(make_message())
        runner.process_next()
        handler.assert_called_once()

    def test_successful_processing_records_success(self):
        metrics = RetryMetrics()
        runner = RetryRunner(make_queue(), handler=MagicMock(), metrics=metrics)
        runner.enqueue(make_message())
        runner.process_next()
        assert metrics.total_succeeded == 1

    def test_failed_handler_records_retry(self):
        metrics = RetryMetrics()
        runner = RetryRunner(
            make_queue(), handler=MagicMock(side_effect=RuntimeError("fail")), metrics=metrics
        )
        runner.enqueue(make_message())
        runner.process_next()
        assert metrics.total_retried == 1


class TestRetryRunnerProcessAll:
    def test_process_all_returns_count(self):
        handler = MagicMock()
        runner = RetryRunner(make_queue(), handler=handler)
        for i in range(3):
            runner.enqueue(make_message(msg_id=str(i)))
        count = runner.process_all()
        assert count == 3

    def test_process_all_empty_queue_returns_zero(self):
        runner = RetryRunner(make_queue(), handler=MagicMock())
        assert runner.process_all() == 0


class TestRetryRunnerMiddleware:
    def test_middleware_before_is_called(self):
        chain = MiddlewareChain()
        spy = MagicMock(side_effect=lambda msg: msg)
        chain.run_before = spy
        runner = RetryRunner(make_queue(), handler=MagicMock(), middleware=chain)
        runner.enqueue(make_message())
        runner.process_next()
        spy.assert_called_once()

    def test_middleware_after_called_on_success(self):
        chain = MiddlewareChain()
        after_spy = MagicMock()
        chain.run_after = after_spy
        runner = RetryRunner(make_queue(), handler=MagicMock(), middleware=chain)
        runner.enqueue(make_message())
        runner.process_next()
        after_spy.assert_called_once()
        _, success, error = after_spy.call_args[0]
        assert success is True
        assert error is None

    def test_middleware_after_called_on_failure(self):
        chain = MiddlewareChain()
        after_spy = MagicMock()
        chain.run_after = after_spy
        err = ValueError("bad")
        runner = RetryRunner(
            make_queue(), handler=MagicMock(side_effect=err), middleware=chain
        )
        runner.enqueue(make_message())
        runner.process_next()
        _, success, error = after_spy.call_args[0]
        assert success is False
        assert error is err
