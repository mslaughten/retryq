"""Unit tests for retryq.runner_tagging.TaggingRetryRunner."""
import uuid
import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.tagging import TagRegistry
from retryq.runner_tagging import TaggingRetryRunner


def make_message(max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(
        id=str(uuid.uuid4()),
        payload={"data": "test"},
        max_attempts=max_attempts,
    )


def make_runner(
    handler=None,
    tag_registry=None,
    dead_letter_callback=None,
) -> TaggingRetryRunner:
    if handler is None:
        handler = lambda msg: None  # noqa: E731
    return TaggingRetryRunner(
        queue=RetryQueue(),
        handler=handler,
        backoff=ConstantBackoff(delay=0),
        tag_registry=tag_registry,
        dead_letter_callback=dead_letter_callback,
    )


class TestTaggingRetryRunner:
    def test_enqueue_with_initial_tags(self):
        runner = make_runner()
        msg = make_message()
        runner.enqueue(msg, "priority", "batch")
        assert runner.tag_registry.has_tag(msg, "priority")
        assert runner.tag_registry.has_tag(msg, "batch")

    def test_enqueue_without_tags(self):
        runner = make_runner()
        msg = make_message()
        runner.enqueue(msg)
        assert runner.tag_registry.get_tags(msg) == frozenset()

    def test_success_purges_tags(self):
        runner = make_runner(handler=lambda m: None)
        msg = make_message()
        runner.enqueue(msg, "temp")
        runner.process_next()
        assert runner.tag_registry.get_tags(msg) == frozenset()

    def test_dead_letter_purges_tags(self):
        dead_lettered = []
        runner = make_runner(
            handler=lambda m: (_ for _ in ()).throw(RuntimeError("fail")),
            dead_letter_callback=dead_lettered.append,
        )
        msg = make_message(max_attempts=1)
        runner.enqueue(msg, "critical")
        runner.process_next()
        assert runner.tag_registry.get_tags(msg) == frozenset()
        assert dead_lettered == [msg]

    def test_metrics_incremented_on_enqueue(self):
        runner = make_runner()
        runner.enqueue(make_message())
        assert runner.metrics.total_enqueued == 1

    def test_metrics_incremented_on_success(self):
        runner = make_runner(handler=lambda m: None)
        runner.enqueue(make_message())
        runner.process_next()
        assert runner.metrics.total_succeeded == 1

    def test_process_next_returns_false_on_empty_queue(self):
        runner = make_runner()
        assert runner.process_next() is False

    def test_custom_tag_registry_is_used(self):
        registry = TagRegistry()
        runner = make_runner(tag_registry=registry)
        msg = make_message()
        runner.enqueue(msg, "shared")
        assert registry.has_tag(msg, "shared")
