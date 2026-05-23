"""Tests for PriorityRetryRunner."""

from __future__ import annotations

import pytest

from retryq.hooks import HookEvent
from retryq.priority import InvalidPriorityError
from retryq.queue import RetryMessage
from retryq.runner_priority import PriorityRetryRunner


def make_message(data: str = "msg") -> RetryMessage:
    return RetryMessage(payload={"data": data})


def make_runner(handler=None, **kwargs) -> PriorityRetryRunner:
    if handler is None:
        handler = lambda m: True  # noqa: E731
    return PriorityRetryRunner(handler, **kwargs)


class TestPriorityRetryRunnerBasics:
    def test_enqueue_increments_metrics(self):
        runner = make_runner()
        runner.enqueue(make_message())
        assert runner.metrics.enqueued == 1

    def test_process_next_returns_false_when_empty(self):
        runner = make_runner()
        assert runner.process_next() is False

    def test_successful_handler_records_success(self):
        runner = make_runner(handler=lambda m: True)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.succeeded == 1

    def test_failing_handler_records_retry(self):
        runner = make_runner(handler=lambda m: False, max_attempts=5)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.retried == 1

    def test_invalid_priority_raises(self):
        runner = make_runner()
        with pytest.raises(InvalidPriorityError):
            runner.enqueue(make_message(), priority=99)

    def test_drain_processes_all_messages(self):
        processed: list[str] = []

        def handler(msg: RetryMessage) -> bool:
            processed.append(msg.payload["data"])
            return True

        runner = make_runner(handler=handler)
        for label in ["a", "b", "c"]:
            runner.enqueue(make_message(label))

        count = runner.drain()
        assert count == 3
        assert len(processed) == 3

    def test_priority_order_respected(self):
        order: list[str] = []

        def handler(msg: RetryMessage) -> bool:
            order.append(msg.payload["data"])
            return True

        runner = make_runner(handler=handler)
        runner.enqueue(make_message("low"), priority=2)
        runner.enqueue(make_message("high"), priority=8)
        runner.drain()

        assert order == ["high", "low"]

    def test_dead_letters_after_exhaustion(self):
        runner = make_runner(handler=lambda m: False, max_attempts=2)
        runner.enqueue(make_message())
        runner.drain()
        assert len(runner.dead_letters) == 1

    def test_hooks_fire_on_enqueue(self):
        fired: list[RetryMessage] = []
        runner = make_runner()
        runner.hooks.register(HookEvent.ENQUEUE, lambda m: fired.append(m))
        runner.enqueue(make_message())
        assert len(fired) == 1

    def test_hooks_fire_on_success(self):
        fired: list[RetryMessage] = []
        runner = make_runner(handler=lambda m: True)
        runner.hooks.register(HookEvent.SUCCESS, lambda m: fired.append(m))
        runner.enqueue(make_message())
        runner.process_next()
        assert len(fired) == 1

    def test_default_priority_used_when_none_specified(self):
        order: list[str] = []

        def handler(msg: RetryMessage) -> bool:
            order.append(msg.payload["data"])
            return True

        runner = make_runner(handler=handler, default_priority=9)
        runner.enqueue(make_message("default"))          # priority=9
        runner.enqueue(make_message("explicit"), priority=1)
        runner.drain()

        assert order[0] == "default"
