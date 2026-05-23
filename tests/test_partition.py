"""Tests for PartitionedRetryQueue."""

import pytest

from retryq.partition import PartitionedRetryQueue
from retryq.queue import RetryMessage
from retryq.backoff import ConstantBackoff


def make_message(payload: dict, metadata: dict | None = None) -> RetryMessage:
    return RetryMessage(payload=payload, metadata=metadata or {})


def key_by_type(msg: RetryMessage) -> str:
    return msg.payload.get("type", "default")


class TestPartitionedRetryQueue:
    def setup_method(self):
        self.pq = PartitionedRetryQueue(
            key_fn=key_by_type,
            backoff=ConstantBackoff(delay=0),
            max_attempts=3,
        )

    def test_invalid_key_fn_raises(self):
        with pytest.raises(TypeError):
            PartitionedRetryQueue(key_fn="not_callable")

    def test_enqueue_returns_partition_key(self):
        msg = make_message({"type": "email"})
        key = self.pq.enqueue(msg)
        assert key == "email"

    def test_enqueue_creates_partition(self):
        msg = make_message({"type": "sms"})
        self.pq.enqueue(msg)
        assert "sms" in self.pq.partition_keys

    def test_multiple_partitions_are_isolated(self):
        self.pq.enqueue(make_message({"type": "email"}))
        self.pq.enqueue(make_message({"type": "push"}))
        assert self.pq.size("email") == 1
        assert self.pq.size("push") == 1

    def test_size_returns_zero_for_unknown_key(self):
        assert self.pq.size("nonexistent") == 0

    def test_total_size_sums_all_partitions(self):
        self.pq.enqueue(make_message({"type": "email"}))
        self.pq.enqueue(make_message({"type": "email"}))
        self.pq.enqueue(make_message({"type": "sms"}))
        assert self.pq.total_size() == 3

    def test_process_next_returns_false_when_empty(self):
        result = self.pq.process_next("email", lambda m: True)
        assert result is False

    def test_process_next_returns_false_for_unknown_key(self):
        result = self.pq.process_next("ghost", lambda m: True)
        assert result is False

    def test_process_next_invokes_handler(self):
        msg = make_message({"type": "email", "body": "hello"})
        self.pq.enqueue(msg)
        received = []
        self.pq.process_next("email", lambda m: received.append(m) or True)
        assert len(received) == 1
        assert received[0].payload["body"] == "hello"

    def test_queue_for_returns_none_for_missing_key(self):
        assert self.pq.queue_for("missing") is None

    def test_queue_for_returns_queue_after_enqueue(self):
        self.pq.enqueue(make_message({"type": "email"}))
        q = self.pq.queue_for("email")
        assert q is not None

    def test_default_partition_key_used_when_type_missing(self):
        msg = make_message({"content": "no type field"})
        key = self.pq.enqueue(msg)
        assert key == "default"
        assert self.pq.size("default") == 1
