"""Tests for RetryRunner basics (queue, metrics, middleware integration)."""

import pytest
from retryq.queue import RetryQueue, RetryMessage
from retryq.backoff import ConstantBackoff
from retryq.runner import RetryRunner


def make_queue(max_attempts: int = 3) -> RetryQueue:
    return RetryQueue(backoff=ConstantBackoff(delay=0), max_attempts=max_attempts)


def make_message(payload: dict = None, attempts: int = 0) -> RetryMessage:
    msg = RetryMessage(payload=payload or {"x": 1})
    msg.attempts = attempts
    return msg


class TestRetryRunnerBasics:
    def test_enqueue_increments_metrics(self):
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        runner.enqueue(make_message())
        assert runner.metrics.enqueued == 1

    def test_process_next_returns_false_when_empty(self):
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        assert runner.process_next() is False

    def test_process_next_returns_true_when_message_present(self):
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        runner.enqueue(make_message())
        assert runner.process_next() is True

    def test_successful_handler_records_success(self):
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.succeeded == 1

    def test_failed_handler_requeues_message(self):
        runner = RetryRunner(make_queue(max_attempts=3), handler=lambda m: False)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.retried == 1

    def test_exhausted_message_goes_to_dead_letter(self):
        runner = RetryRunner(make_queue(max_attempts=1), handler=lambda m: False)
        runner.enqueue(make_message(attempts=0))
        runner.process_next()
        assert runner.metrics.dead_lettered == 1

    def test_exception_in_handler_triggers_error_path(self):
        def bad_handler(m):
            raise RuntimeError("boom")

        runner = RetryRunner(make_queue(max_attempts=3), handler=bad_handler)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.retried == 1

    def test_hooks_property_returns_registry(self):
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        from retryq.hooks import HookRegistry
        assert isinstance(runner.hooks, HookRegistry)

    def test_on_enqueue_hook_fires(self):
        fired = []
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        from retryq.hooks import HookEvent
        runner.hooks.register(HookEvent.ON_ENQUEUE, lambda m: fired.append(m))
        msg = make_message()
        runner.enqueue(msg)
        assert fired == [msg]

    def test_on_success_hook_fires(self):
        fired = []
        runner = RetryRunner(make_queue(), handler=lambda m: True)
        from retryq.hooks import HookEvent
        runner.hooks.register(HookEvent.ON_SUCCESS, lambda m: fired.append(True))
        runner.enqueue(make_message())
        runner.process_next()
        assert fired == [True]
