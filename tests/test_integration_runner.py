"""Integration tests for RetryRunner with real middleware and metrics."""

import pytest

from retryq.backoff import ExponentialBackoff
from retryq.metrics import RetryMetrics
from retryq.middleware import LoggingMiddleware, MetadataEnrichmentMiddleware, MiddlewareChain
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner import RetryRunner


def make_runner(handler, max_attempts=3, delay=0):
    queue = RetryQueue(backoff=ExponentialBackoff(base=delay, multiplier=1))
    metrics = RetryMetrics()
    chain = MiddlewareChain([
        LoggingMiddleware(logger=lambda _: None),
        MetadataEnrichmentMiddleware(),
    ])
    runner = RetryRunner(queue, handler=handler, middleware=chain, metrics=metrics)
    return runner


class TestRunnerIntegration:
    def test_success_on_first_attempt_updates_all_metrics(self):
        runner = make_runner(handler=lambda msg: None)
        msg = RetryMessage(id="ok", payload={}, max_attempts=3)
        runner.enqueue(msg)
        runner.process_all()
        assert runner.metrics.total_enqueued == 1
        assert runner.metrics.total_succeeded == 1
        assert runner.metrics.total_retried == 0

    def test_metadata_enriched_after_successful_attempt(self):
        runner = make_runner(handler=lambda msg: None)
        msg = RetryMessage(id="meta", payload={}, max_attempts=3)
        runner.enqueue(msg)
        runner.process_next()
        assert msg.metadata.get("last_attempt_success") is True
        assert msg.metadata.get("last_attempt_number") == 1

    def test_metadata_enriched_after_failed_attempt(self):
        runner = make_runner(handler=MagicMock(side_effect=RuntimeError("err")))
        msg = RetryMessage(id="fail", payload={}, max_attempts=3)
        runner.enqueue(msg)
        runner.process_next()
        assert msg.metadata.get("last_attempt_success") is False
        assert msg.metadata.get("last_error") == "err"

    def test_full_cycle_eventually_dead_letters(self):
        dead_letters = []
        queue = RetryQueue(
            backoff=ExponentialBackoff(base=0, multiplier=1),
            dead_letter_callback=lambda msg: dead_letters.append(msg),
        )
        metrics = RetryMetrics()
        runner = RetryRunner(
            queue,
            handler=lambda msg: (_ for _ in ()).throw(RuntimeError("always fails")),
            metrics=metrics,
        )
        msg = RetryMessage(id="dlq", payload={"x": 1}, max_attempts=2)
        runner.enqueue(msg)
        # Exhaust all attempts
        for _ in range(10):
            runner.process_next()
        assert len(dead_letters) == 1
        assert dead_letters[0].id == "dlq"


from unittest.mock import MagicMock
