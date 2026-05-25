"""Tests for retryq.inspector."""
import uuid
import pytest

from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff
from retryq.inspector import (
    QueueSummary,
    inspect_queue,
    find_message,
    filter_exhausted,
)


def make_message(payload: str = "hello", max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(
        id=str(uuid.uuid4()),
        payload=payload,
        max_attempts=max_attempts,
    )


def make_queue() -> RetryQueue:
    return RetryQueue(backoff=ConstantBackoff(delay=0))


class TestQueueSummary:
    def test_healthy_when_no_exhausted(self):
        summary = QueueSummary(total=3, exhausted=0, pending=3)
        assert summary.healthy is True

    def test_not_healthy_when_exhausted_present(self):
        summary = QueueSummary(total=3, exhausted=1, pending=2)
        assert summary.healthy is False

    def test_defaults_are_zero(self):
        summary = QueueSummary()
        assert summary.total == 0
        assert summary.exhausted == 0
        assert summary.pending == 0
        assert summary.attempt_histogram == {}


class TestInspectQueue:
    def test_empty_queue_returns_zero_totals(self):
        q = make_queue()
        summary = inspect_queue(q)
        assert summary.total == 0
        assert summary.exhausted == 0
        assert summary.pending == 0

    def test_counts_pending_messages(self):
        q = make_queue()
        q.enqueue(make_message())
        q.enqueue(make_message())
        summary = inspect_queue(q)
        assert summary.total == 2
        assert summary.pending == 2
        assert summary.exhausted == 0

    def test_counts_exhausted_messages(self):
        q = make_queue()
        msg = make_message(max_attempts=1)
        msg.attempts = 1  # mark as exhausted
        q.enqueue(msg)
        summary = inspect_queue(q)
        assert summary.exhausted == 1
        assert summary.pending == 0

    def test_attempt_histogram_groups_by_attempt_count(self):
        q = make_queue()
        m1 = make_message()
        m2 = make_message()
        m3 = make_message()
        m2.attempts = 1
        m3.attempts = 1
        q.enqueue(m1)
        q.enqueue(m2)
        q.enqueue(m3)
        summary = inspect_queue(q)
        assert summary.attempt_histogram[0] == 1
        assert summary.attempt_histogram[1] == 2

    def test_inspect_does_not_mutate_queue(self):
        q = make_queue()
        q.enqueue(make_message())
        inspect_queue(q)
        assert len(q) == 1


class TestFindMessage:
    def test_returns_none_for_unknown_id(self):
        q = make_queue()
        assert find_message(q, "nonexistent") is None

    def test_finds_message_by_id(self):
        q = make_queue()
        msg = make_message()
        q.enqueue(msg)
        found = find_message(q, msg.id)
        assert found is msg

    def test_returns_none_when_queue_empty(self):
        q = make_queue()
        assert find_message(q, str(uuid.uuid4())) is None


class TestFilterExhausted:
    def test_empty_result_when_no_exhausted(self):
        q = make_queue()
        q.enqueue(make_message())
        assert filter_exhausted(q) == []

    def test_returns_only_exhausted_messages(self):
        q = make_queue()
        fresh = make_message()
        dead = make_message(max_attempts=1)
        dead.attempts = 1
        q.enqueue(fresh)
        q.enqueue(dead)
        result = filter_exhausted(q)
        assert len(result) == 1
        assert result[0] is dead
