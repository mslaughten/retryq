"""Tests for FilteredRetryRunner integration with MessageFilter."""

import pytest
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff
from retryq.filter import MessageFilter, by_topic, by_max_attempts
from retryq.runner_filter import FilteredRetryRunner


def make_message(topic: str = "default", attempts: int = 0) -> RetryMessage:
    return RetryMessage(
        payload={"topic": topic},
        attempts=attempts,
    )


def make_runner(message_filter=None) -> FilteredRetryRunner:
    queue = RetryQueue(backoff=ConstantBackoff(delay=0), max_attempts=5)
    results = []

    def handler(msg: RetryMessage) -> None:
        results.append(msg)

    runner = FilteredRetryRunner(
        queue=queue,
        handler=handler,
        message_filter=message_filter,
    )
    runner._results = results
    return runner


class TestFilteredRetryRunner:
    def test_default_filter_processes_all_messages(self):
        runner = make_runner()
        runner.enqueue(make_message("a"))
        runner.enqueue(make_message("b"))
        processed = runner.process_all_matching()
        assert processed == 2

    def test_filter_skips_non_matching_messages(self):
        f = MessageFilter().add(by_topic("payments"))
        runner = make_runner(message_filter=f)
        runner.enqueue(make_message("payments"))
        runner.enqueue(make_message("orders"))
        processed = runner.process_all_matching()
        assert processed == 1

    def test_skipped_messages_remain_in_queue(self):
        f = MessageFilter().add(by_topic("payments"))
        runner = make_runner(message_filter=f)
        runner.enqueue(make_message("orders"))
        runner.process_all_matching()
        # The skipped message should be back in the queue
        assert runner._queue.size() == 1

    def test_process_next_skips_and_returns_false(self):
        f = MessageFilter().add(by_topic("payments"))
        runner = make_runner(message_filter=f)
        runner.enqueue(make_message("orders"))
        result = runner.process_next()
        assert result is False

    def test_process_next_returns_true_for_matching(self):
        f = MessageFilter().add(by_topic("payments"))
        runner = make_runner(message_filter=f)
        runner.enqueue(make_message("payments"))
        result = runner.process_next()
        assert result is True

    def test_process_next_returns_false_when_empty(self):
        runner = make_runner()
        assert runner.process_next() is False

    def test_set_filter_replaces_existing_filter(self):
        runner = make_runner(message_filter=MessageFilter().add(by_topic("a")))
        new_filter = MessageFilter().add(by_topic("b"))
        runner.set_filter(new_filter)
        assert runner.message_filter is new_filter

    def test_process_all_matching_returns_count(self):
        f = MessageFilter().add(by_max_attempts(3))
        runner = make_runner(message_filter=f)
        runner.enqueue(make_message(attempts=1))
        runner.enqueue(make_message(attempts=5))
        runner.enqueue(make_message(attempts=2))
        count = runner.process_all_matching()
        assert count == 2

    def test_message_filter_property_accessible(self):
        f = MessageFilter()
        runner = make_runner(message_filter=f)
        assert runner.message_filter is f
