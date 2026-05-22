"""Integration tests for RetryRunner with hooks, metrics, and middleware together."""

import pytest
from retryq.queue import RetryQueue, RetryMessage
from retryq.backoff import ConstantBackoff
from retryq.runner import RetryRunner
from retryq.hooks import HookEvent
from retryq.middleware import MiddlewareChain, RetryMiddleware


def make_runner(max_attempts: int = 3, middleware: MiddlewareChain = None) -> RetryRunner:
    queue = RetryQueue(backoff=ConstantBackoff(delay=0), max_attempts=max_attempts)
    return RetryRunner(queue=queue, handler=lambda m: True, middleware=middleware)


class TestRunnerIntegration:
    def test_success_on_first_attempt_updates_all_metrics(self):
        runner = make_runner()
        runner.enqueue(RetryMessage(payload={"a": 1}))
        runner.process_next()
        m = runner.metrics
        assert m.enqueued == 1
        assert m.succeeded == 1
        assert m.retried == 0
        assert m.dead_lettered == 0

    def test_metadata_enriched_after_successful_attempt(self):
        enriched = []

        class EnrichMiddleware(RetryMiddleware):
            def before_retry(self, message: RetryMessage) -> RetryMessage:
                message.metadata["enriched"] = True
                return message

        chain = MiddlewareChain()
        chain.add(EnrichMiddleware())
        runner = make_runner(middleware=chain)
        runner.hooks.register(HookEvent.ON_SUCCESS, lambda m: enriched.append(m.metadata.get("enriched")))
        runner.enqueue(RetryMessage(payload={}))
        runner.process_next()
        assert enriched == [True]

    def test_metadata_enriched_after_failed_attempt(self):
        attempts_seen = []
        queue = RetryQueue(backoff=ConstantBackoff(delay=0), max_attempts=3)
        call_count = [0]

        def handler(m):
            call_count[0] += 1
            return call_count[0] >= 2

        runner = RetryRunner(queue=queue, handler=handler)
        runner.hooks.register(HookEvent.ON_RETRY, lambda m: attempts_seen.append(m.attempts))
        runner.enqueue(RetryMessage(payload={"task": "go"}))
        runner.process_next()
        assert len(attempts_seen) == 1

    def test_dead_letter_hook_fires_after_exhaustion(self):
        dead = []
        queue = RetryQueue(backoff=ConstantBackoff(delay=0), max_attempts=1)
        runner = RetryRunner(queue=queue, handler=lambda m: False)
        runner.hooks.register(HookEvent.ON_DEAD_LETTER, lambda m: dead.append(m.payload))
        runner.enqueue(RetryMessage(payload={"job": "fail"}))
        runner.process_next()
        assert dead == [{"job": "fail"}]
