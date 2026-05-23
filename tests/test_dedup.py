"""Tests for retryq.dedup and DedupRetryRunner."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from retryq.backoff import ConstantBackoff
from retryq.dedup import DedupWindow, DuplicateMessageError, _fingerprint
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_dedup import DedupRetryRunner


def make_message(payload: dict | None = None, *, msg_id: str = "msg-1") -> RetryMessage:
    return RetryMessage(id=msg_id, payload=payload or {"key": "value"}, max_attempts=3)


def make_runner(raise_on_dup: bool = False) -> tuple[DedupRetryRunner, RetryQueue]:
    queue = RetryQueue()
    handler = MagicMock()
    runner = DedupRetryRunner(
        queue,
        handler,
        backoff=ConstantBackoff(delay=0),
        raise_on_duplicate=raise_on_dup,
    )
    return runner, queue


class TestDedupWindow:
    def test_invalid_ttl_raises(self):
        with pytest.raises(ValueError):
            DedupWindow(ttl_seconds=0)

    def test_negative_ttl_raises(self):
        with pytest.raises(ValueError):
            DedupWindow(ttl_seconds=-5)

    def test_new_message_is_not_duplicate(self):
        dw = DedupWindow()
        msg = make_message()
        assert dw.is_duplicate(msg) is False

    def test_recorded_message_is_duplicate(self):
        dw = DedupWindow()
        msg = make_message()
        dw.record(msg)
        assert dw.is_duplicate(msg) is True

    def test_different_ids_not_duplicate(self):
        dw = DedupWindow()
        msg1 = make_message(msg_id="a")
        msg2 = make_message(msg_id="b")
        dw.record(msg1)
        assert dw.is_duplicate(msg2) is False

    def test_size_reflects_recorded_messages(self):
        dw = DedupWindow()
        assert dw.size == 0
        dw.record(make_message(msg_id="x"))
        dw.record(make_message(msg_id="y"))
        assert dw.size == 2

    def test_check_and_record_returns_false_for_new(self):
        dw = DedupWindow()
        msg = make_message()
        assert dw.check_and_record(msg) is False

    def test_check_and_record_returns_true_for_duplicate(self):
        dw = DedupWindow()
        msg = make_message()
        dw.check_and_record(msg)
        assert dw.check_and_record(msg) is True

    def test_raises_on_duplicate_when_flag_set(self):
        dw = DedupWindow()
        msg = make_message()
        dw.record(msg)
        with pytest.raises(DuplicateMessageError) as exc_info:
            dw.check_and_record(msg, raise_on_duplicate=True)
        assert msg.id in str(exc_info.value)


class TestDedupRetryRunner:
    def test_enqueue_unique_increments_metrics(self):
        runner, _ = make_runner()
        runner.enqueue(make_message(msg_id="u1"))
        assert runner.metrics.total_enqueued == 1

    def test_enqueue_duplicate_drops_silently(self):
        runner, _ = make_runner()
        msg = make_message()
        runner.enqueue(msg)
        runner.enqueue(msg)
        assert runner.metrics.total_enqueued == 1
        assert runner.duplicates_dropped == 1

    def test_enqueue_duplicate_raises_when_configured(self):
        runner, _ = make_runner(raise_on_dup=True)
        msg = make_message()
        runner.enqueue(msg)
        with pytest.raises(DuplicateMessageError):
            runner.enqueue(msg)

    def test_process_next_returns_false_on_empty_queue(self):
        runner, _ = make_runner()
        assert runner.process_next() is False

    def test_successful_handler_records_success(self):
        runner, _ = make_runner()
        runner.enqueue(make_message(msg_id="s1"))
        runner.process_next()
        assert runner.metrics.total_successes == 1

    def test_dead_letter_callback_called_after_exhaustion(self):
        queue = RetryQueue()
        handler = MagicMock(side_effect=RuntimeError("boom"))
        runner = DedupRetryRunner(queue, handler, backoff=ConstantBackoff(delay=0))
        dlq = []
        msg = RetryMessage(id="dl-1", payload={}, max_attempts=1)
        runner.enqueue(msg)
        runner.process_next(dead_letter_callback=dlq.append)
        assert len(dlq) == 1
        assert runner.metrics.total_dead_letters == 1
