"""Tests for retryq.queue module."""

import pytest
from unittest.mock import MagicMock, call
from retryq.queue import RetryQueue, RetryMessage
from retryq.backoff import ConstantBackoff


class TestRetryMessage:
    def test_not_exhausted_initially(self):
        msg = RetryMessage(payload="hello", max_attempts=3)
        assert not msg.exhausted

    def test_exhausted_when_attempts_reach_max(self):
        msg = RetryMessage(payload="hello", max_attempts=3, attempt=3)
        assert msg.exhausted

    def test_default_max_attempts(self):
        msg = RetryMessage(payload="data")
        assert msg.max_attempts == 5

    def test_metadata_defaults_to_empty_dict(self):
        msg = RetryMessage(payload="data")
        assert msg.metadata == {}


class TestRetryQueue:
    def _make_queue(self, handler=None, max_attempts=3):
        handler = handler or MagicMock()
        return RetryQueue(
            handler=handler,
            backoff=ConstantBackoff(delay=0),
            max_attempts=max_attempts,
        ), handler

    def test_enqueue_increases_size(self):
        q, _ = self._make_queue()
        q.enqueue("msg1")
        q.enqueue("msg2")
        assert q.size == 2

    def test_process_next_calls_handler(self):
        q, handler = self._make_queue()
        q.enqueue("payload")
        q.process_next(dry_run=True)
        handler.assert_called_once_with("payload")

    def test_process_next_returns_none_when_empty(self):
        q, _ = self._make_queue()
        result = q.process_next()
        assert result is None

    def test_failed_message_requeued_until_exhausted(self):
        handler = MagicMock(side_effect=ValueError("oops"))
        q = RetryQueue(
            handler=handler,
            backoff=ConstantBackoff(delay=0),
            max_attempts=3,
        )
        q.enqueue("bad")
        q.drain(dry_run=True)
        assert handler.call_count == 3
        assert q.size == 0

    def test_on_exhausted_called_when_attempts_used_up(self):
        on_exhausted = MagicMock()
        handler = MagicMock(side_effect=RuntimeError("fail"))
        q = RetryQueue(
            handler=handler,
            backoff=ConstantBackoff(delay=0),
            max_attempts=2,
            on_exhausted=on_exhausted,
        )
        q.enqueue("data")
        q.drain(dry_run=True)
        on_exhausted.assert_called_once()
        exhausted_msg = on_exhausted.call_args[0][0]
        assert isinstance(exhausted_msg, RetryMessage)
        assert exhausted_msg.attempt == 2

    def test_drain_processes_all_messages(self):
        q, handler = self._make_queue()
        q.enqueue("a")
        q.enqueue("b")
        q.enqueue("c")
        processed = q.drain(dry_run=True)
        assert q.size == 0
        assert handler.call_count == 3

    def test_enqueue_stores_metadata(self):
        q, _ = self._make_queue()
        msg = q.enqueue("payload", metadata={"source": "kafka"})
        assert msg.metadata["source"] == "kafka"

    def test_successful_message_not_requeued(self):
        q, handler = self._make_queue()
        q.enqueue("ok")
        q.process_next(dry_run=True)
        assert q.size == 0
        handler.assert_called_once()
