"""Tests for retryq.snapshot module."""

import json
import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.snapshot import SnapshotError, load_snapshot, save_snapshot


def make_message(payload: str = "hello", attempts: int = 0, max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(
        id="msg-1",
        payload=payload,
        attempts=attempts,
        max_attempts=max_attempts,
        metadata={"source": "test"},
    )


def make_queue() -> RetryQueue:
    return RetryQueue(backoff=ConstantBackoff(delay=1.0), handler=lambda m: None)


class TestSaveSnapshot:
    def test_empty_queue_returns_empty_array(self):
        q = make_queue()
        result = save_snapshot(q)
        assert json.loads(result) == []

    def test_single_message_snapshot(self):
        q = make_queue()
        msg = make_message()
        q.enqueue(msg)
        result = save_snapshot(q)
        entries = json.loads(result)
        assert len(entries) == 1

    def test_snapshot_preserves_payload(self):
        q = make_queue()
        msg = make_message(payload="important-data")
        q.enqueue(msg)
        result = save_snapshot(q)
        entries = json.loads(result)
        inner = json.loads(entries[0])
        assert inner["payload"] == "important-data"

    def test_snapshot_preserves_attempts(self):
        q = make_queue()
        msg = make_message(attempts=2)
        q.enqueue(msg)
        result = save_snapshot(q)
        entries = json.loads(result)
        inner = json.loads(entries[0])
        assert inner["attempts"] == 2

    def test_multiple_messages_snapshot(self):
        q = make_queue()
        for i in range(3):
            q.enqueue(RetryMessage(id=f"msg-{i}", payload=f"p{i}"))
        result = save_snapshot(q)
        entries = json.loads(result)
        assert len(entries) == 3


class TestLoadSnapshot:
    def test_loads_messages_into_queue(self):
        src = make_queue()
        src.enqueue(make_message())
        snapshot = save_snapshot(src)

        dst = make_queue()
        count = load_snapshot(dst, snapshot)
        assert count == 1

    def test_loaded_message_is_processable(self):
        src = make_queue()
        src.enqueue(make_message(payload="restore-me"))
        snapshot = save_snapshot(src)

        received = []
        dst = RetryQueue(backoff=ConstantBackoff(delay=0), handler=lambda m: received.append(m.payload))
        load_snapshot(dst, snapshot)
        dst.process_next()
        assert received == ["restore-me"]

    def test_empty_snapshot_loads_zero(self):
        dst = make_queue()
        count = load_snapshot(dst, json.dumps([]))
        assert count == 0

    def test_invalid_json_raises_snapshot_error(self):
        dst = make_queue()
        with pytest.raises(SnapshotError):
            load_snapshot(dst, "not-json")

    def test_empty_string_raises_snapshot_error(self):
        dst = make_queue()
        with pytest.raises(SnapshotError):
            load_snapshot(dst, "")

    def test_non_array_root_raises_snapshot_error(self):
        dst = make_queue()
        with pytest.raises(SnapshotError):
            load_snapshot(dst, json.dumps({"key": "value"}))

    def test_round_trip_preserves_metadata(self):
        src = make_queue()
        src.enqueue(RetryMessage(id="abc", payload="x", metadata={"tag": "v1"}))
        snapshot = save_snapshot(src)

        dst = make_queue()
        load_snapshot(dst, snapshot)
        msg = dst._queue[0]
        assert msg.metadata.get("tag") == "v1"
