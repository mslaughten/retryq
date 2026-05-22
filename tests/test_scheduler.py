"""Tests for retryq.scheduler module."""

import pytest
from unittest.mock import MagicMock
from retryq.scheduler import RetryScheduler, ScheduledRetry
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff, ExponentialBackoff


def make_message(payload=None, attempts=0):
    msg = RetryMessage(payload=payload or {"task": "send_email"})
    msg.attempts = attempts
    return msg


def make_queue():
    return RetryQueue(max_attempts=5)


class TestScheduledRetry:
    def test_is_due_when_time_has_passed(self):
        msg = make_message()
        entry = ScheduledRetry(message=msg, retry_at=100.0)
        assert entry.is_due(now=101.0) is True

    def test_is_not_due_before_time(self):
        msg = make_message()
        entry = ScheduledRetry(message=msg, retry_at=200.0)
        assert entry.is_due(now=150.0) is False

    def test_is_due_exactly_at_time(self):
        msg = make_message()
        entry = ScheduledRetry(message=msg, retry_at=50.0)
        assert entry.is_due(now=50.0) is True

    def test_repr_contains_message_id(self):
        msg = make_message()
        entry = ScheduledRetry(message=msg, retry_at=10.0)
        assert msg.id in repr(entry)


class TestRetryScheduler:
    def setup_method(self):
        self.clock_value = 0.0
        self.clock = lambda: self.clock_value
        self.queue = make_queue()
        self.backoff = ConstantBackoff(delay=5.0)
        self.scheduler = RetryScheduler(
            queue=self.queue,
            backoff=self.backoff,
            clock=self.clock,
        )

    def test_schedule_adds_to_pending(self):
        msg = make_message()
        self.scheduler.schedule(msg)
        assert self.scheduler.pending_count == 1

    def test_schedule_returns_scheduled_retry(self):
        msg = make_message()
        entry = self.scheduler.schedule(msg)
        assert isinstance(entry, ScheduledRetry)
        assert entry.message is msg

    def test_scheduled_retry_at_uses_backoff_delay(self):
        self.clock_value = 10.0
        msg = make_message()
        entry = self.scheduler.schedule(msg)
        assert entry.retry_at == pytest.approx(15.0)

    def test_flush_due_enqueues_ready_messages(self):
        msg = make_message()
        self.clock_value = 0.0
        self.scheduler.schedule(msg)
        self.clock_value = 10.0
        flushed = self.scheduler.flush_due()
        assert flushed == 1
        assert self.scheduler.pending_count == 0

    def test_flush_due_skips_not_ready(self):
        msg = make_message()
        self.clock_value = 0.0
        self.scheduler.schedule(msg)
        self.clock_value = 2.0  # delay is 5.0, not ready yet
        flushed = self.scheduler.flush_due()
        assert flushed == 0
        assert self.scheduler.pending_count == 1

    def test_flush_due_places_message_in_queue(self):
        msg = make_message()
        self.clock_value = 0.0
        self.scheduler.schedule(msg)
        self.clock_value = 10.0
        self.scheduler.flush_due()
        assert self.queue.size() == 1

    def test_multiple_messages_only_due_flushed(self):
        msg1 = make_message()
        msg2 = make_message()
        self.clock_value = 0.0
        self.scheduler.schedule(msg1)  # retry_at = 5.0
        self.clock_value = 3.0
        self.scheduler.schedule(msg2)  # retry_at = 8.0
        self.clock_value = 6.0
        flushed = self.scheduler.flush_due()
        assert flushed == 1
        assert self.scheduler.pending_count == 1
