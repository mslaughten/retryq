"""Tests for retryq.runner_snapshot.SnapshotRetryRunner."""

import json
import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_snapshot import SnapshotRetryRunner
from retryq.snapshot import SnapshotError


def make_message(payload: str = "data", msg_id: str = "m1") -> RetryMessage:
    return RetryMessage(id=msg_id, payload=payload)


def make_runner(handler=None) -> SnapshotRetryRunner:
    if handler is None:
        handler = lambda m: None
    queue = RetryQueue(backoff=ConstantBackoff(delay=0), handler=handler)
    return SnapshotRetryRunner(queue=queue)


class TestSnapshotRetryRunner:
    def test_save_returns_json_string(self):
        runner = make_runner()
        result = runner.save()
        assert isinstance(result, str)
        assert json.loads(result) == []

    def test_last_snapshot_none_before_save(self):
        runner = make_runner()
        assert runner.last_snapshot is None

    def test_last_snapshot_updated_after_save(self):
        runner = make_runner()
        runner.enqueue(make_message())
        snap = runner.save()
        assert runner.last_snapshot == snap

    def test_restore_returns_count(self):
        src = make_runner()
        src.enqueue(make_message())
        snapshot = src.save()

        dst = make_runner()
        count = dst.restore(snapshot)
        assert count == 1

    def test_restore_makes_messages_processable(self):
        received = []
        src = make_runner()
        src.enqueue(make_message(payload="snap-payload"))
        snapshot = src.save()

        dst = make_runner(handler=lambda m: received.append(m.payload))
        dst.restore(snapshot)
        dst.process_next()
        assert received == ["snap-payload"]

    def test_restore_updates_last_snapshot(self):
        src = make_runner()
        src.enqueue(make_message())
        snapshot = src.save()

        dst = make_runner()
        dst.restore(snapshot)
        assert dst.last_snapshot == snapshot

    def test_restore_invalid_raises_snapshot_error(self):
        runner = make_runner()
        with pytest.raises(SnapshotError):
            runner.restore("invalid-json!!!")

    def test_checkpoint_alias_works(self):
        runner = make_runner()
        runner.enqueue(make_message())
        snap1 = runner.checkpoint()
        snap2 = runner.save()
        assert snap1 == snap2

    def test_round_trip_multiple_messages(self):
        src = make_runner()
        for i in range(4):
            src.enqueue(make_message(payload=f"p{i}", msg_id=f"id-{i}"))
        snapshot = src.save()

        dst = make_runner()
        count = dst.restore(snapshot)
        assert count == 4

    def test_metrics_still_track_after_restore(self):
        src = make_runner()
        src.enqueue(make_message())
        snapshot = src.save()

        results = []
        dst = make_runner(handler=lambda m: results.append(m))
        dst.restore(snapshot)
        dst.process_next()
        assert dst.metrics.total_successes == 1
