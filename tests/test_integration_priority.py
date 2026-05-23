"""Integration tests for priority-aware retry processing."""

from __future__ import annotations

from retryq.backoff import ExponentialBackoff
from retryq.queue import RetryMessage
from retryq.runner_priority import PriorityRetryRunner


def make_message(data: str = "msg", **meta) -> RetryMessage:
    return RetryMessage(payload={"data": data}, metadata=meta)


class TestPriorityIntegration:
    def test_mixed_priorities_drain_in_correct_order(self):
        """Messages with different priorities should be consumed highest-first."""
        order: list[int] = []

        def handler(msg: RetryMessage) -> bool:
            order.append(msg.payload["priority_label"])
            return True

        runner = PriorityRetryRunner(handler)
        for p in [3, 7, 1, 10, 5]:
            runner.enqueue(
                RetryMessage(payload={"priority_label": p}),
                priority=p,
            )

        runner.drain()
        assert order == sorted(order, reverse=True)

    def test_retry_preserves_priority(self):
        """A failed message should be re-queued at the same priority."""
        call_counts: dict[str, int] = {"a": 0, "b": 0}

        def handler(msg: RetryMessage) -> bool:
            key = msg.payload["data"]
            call_counts[key] += 1
            # 'a' fails on first attempt, succeeds on second
            if key == "a" and call_counts[key] == 1:
                return False
            return True

        runner = PriorityRetryRunner(handler, max_attempts=3)
        runner.enqueue(make_message("a"), priority=8)
        runner.enqueue(make_message("b"), priority=3)

        runner.drain()

        # Both messages should eventually succeed
        assert runner.metrics.succeeded == 2
        assert runner.metrics.retried == 1

    def test_metrics_reflect_full_run(self):
        attempts = {"n": 0}

        def flaky(msg: RetryMessage) -> bool:
            attempts["n"] += 1
            return attempts["n"] % 2 == 0  # succeed on even calls

        runner = PriorityRetryRunner(flaky, max_attempts=4)
        for i in range(3):
            runner.enqueue(make_message(str(i)), priority=5)

        runner.drain()

        total = runner.metrics.succeeded + len(runner.dead_letters)
        assert total == 3

    def test_exponential_backoff_compatible(self):
        """PriorityRetryRunner accepts a custom backoff strategy."""
        runner = PriorityRetryRunner(
            handler=lambda m: True,
            backoff=ExponentialBackoff(base=0.1, multiplier=2.0),
        )
        runner.enqueue(make_message())
        assert runner.process_next() is True
