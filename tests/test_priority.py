"""Unit tests for retryq.priority — PriorityRetryQueue."""

import pytest

from retryq.queue import RetryMessage
from retryq.priority import (
    InvalidPriorityError,
    PriorityRetryQueue,
    PRIORITY_MIN,
    PRIORITY_MAX,
)


def make_message(payload: str = "test") -> RetryMessage:
    return RetryMessage(payload={"data": payload})


def always_succeed(msg: RetryMessage) -> bool:
    return True


def always_fail(msg: RetryMessage) -> bool:
    return False


class TestPriorityRetryQueue:
    def test_enqueue_and_len(self):
        q = PriorityRetryQueue(always_succeed)
        q.enqueue(make_message(), priority=5)
        assert len(q) == 1

    def test_invalid_priority_too_high_raises(self):
        q = PriorityRetryQueue(always_succeed)
        with pytest.raises(InvalidPriorityError):
            q.enqueue(make_message(), priority=11)

    def test_invalid_priority_negative_raises(self):
        q = PriorityRetryQueue(always_succeed)
        with pytest.raises(InvalidPriorityError):
            q.enqueue(make_message(), priority=-1)

    def test_boundary_priorities_accepted(self):
        q = PriorityRetryQueue(always_succeed)
        q.enqueue(make_message(), priority=PRIORITY_MIN)
        q.enqueue(make_message(), priority=PRIORITY_MAX)
        assert len(q) == 2

    def test_higher_priority_processed_first(self):
        order: list[str] = []

        def handler(msg: RetryMessage) -> bool:
            order.append(msg.payload["data"])
            return True

        q = PriorityRetryQueue(handler)
        q.enqueue(make_message("low"), priority=1)
        q.enqueue(make_message("high"), priority=9)
        q.enqueue(make_message("mid"), priority=5)

        q.process_next()
        q.process_next()
        q.process_next()

        assert order == ["high", "mid", "low"]

    def test_equal_priority_fifo(self):
        order: list[str] = []

        def handler(msg: RetryMessage) -> bool:
            order.append(msg.payload["data"])
            return True

        q = PriorityRetryQueue(handler)
        for label in ["a", "b", "c"]:
            q.enqueue(make_message(label), priority=5)

        while len(q):
            q.process_next()

        assert order == ["a", "b", "c"]

    def test_process_next_returns_false_when_empty(self):
        q = PriorityRetryQueue(always_succeed)
        assert q.process_next() is False

    def test_failed_message_requeued_until_exhausted(self):
        q = PriorityRetryQueue(always_fail, max_attempts=2)
        q.enqueue(make_message(), priority=5)
        q.process_next()  # attempt 1 — requeued
        assert len(q) == 1
        q.process_next()  # attempt 2 — dead-lettered
        assert len(q) == 0
        assert len(q.dead_letters) == 1

    def test_dead_letters_property_is_copy(self):
        q = PriorityRetryQueue(always_fail, max_attempts=1)
        q.enqueue(make_message())
        q.process_next()
        dl = q.dead_letters
        dl.clear()
        assert len(q.dead_letters) == 1

    def test_invalid_max_attempts_raises(self):
        with pytest.raises(ValueError):
            PriorityRetryQueue(always_succeed, max_attempts=0)
