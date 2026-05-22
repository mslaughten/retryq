"""Integration tests for RetryScheduler with runner and backoff."""

import pytest
from retryq.scheduler import RetryScheduler
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ExponentialBackoff
from retryq.runner import RetryRunner


def make_message(payload=None):
    return RetryMessage(payload=payload or {"op": "sync"})


class TestSchedulerRunnerIntegration:
    def setup_method(self):
        self.clock_value = 0.0
        self.clock = lambda: self.clock_value
        self.queue = RetryQueue(max_attempts=4)
        self.backoff = ExponentialBackoff(base=2.0, multiplier=1.0)
        self.scheduler = RetryScheduler(
            queue=self.queue,
            backoff=self.backoff,
            clock=self.clock,
        )

    def test_scheduled_message_processed_after_flush(self):
        results = []
        runner = RetryRunner(queue=self.queue)

        msg = make_message()
        self.clock_value = 0.0
        self.scheduler.schedule(msg)

        # Not yet due — queue should be empty
        assert self.queue.size() == 0

        self.clock_value = 10.0
        self.scheduler.flush_due()

        assert self.queue.size() == 1
        runner.process_next(handler=lambda m: results.append(m.payload))
        assert len(results) == 1
        assert results[0] == {"op": "sync"}

    def test_exponential_delays_grow_with_attempts(self):
        delays = []
        for attempt in range(4):
            msg = make_message()
            msg.attempts = attempt
            self.clock_value = 0.0
            entry = self.scheduler.schedule(msg)
            delays.append(entry.retry_at)
            self.scheduler._scheduled.clear()

        # Each delay should be greater than the previous
        for i in range(1, len(delays)):
            assert delays[i] > delays[i - 1]

    def test_pending_count_decreases_on_flush(self):
        for _ in range(3):
            self.scheduler.schedule(make_message())

        assert self.scheduler.pending_count == 3
        self.clock_value = 100.0
        self.scheduler.flush_due()
        assert self.scheduler.pending_count == 0
