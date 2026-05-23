"""Tests for retryq.replay."""

import pytest

from retryq.backoff import ConstantBackoff
from retryq.queue import RetryMessage, RetryQueue
from retryq.replay import MessageReplayer, ReplayError, ReplayResult


def make_message(payload: str = "hello", attempts: int = 0, max_attempts: int = 3) -> RetryMessage:
    msg = RetryMessage(payload=payload, max_attempts=max_attempts)
    msg.attempts = attempts
    return msg


def make_queue() -> RetryQueue:
    return RetryQueue(handler=lambda m: None, backoff=ConstantBackoff(0))


class TestReplayResult:
    def test_total_is_sum_of_replayed_and_skipped(self):
        r = ReplayResult(replayed=3, skipped=2)
        assert r.total == 5

    def test_defaults_are_zero(self):
        r = ReplayResult()
        assert r.replayed == 0
        assert r.skipped == 0
        assert r.errors == []


class TestMessageReplayer:
    def test_invalid_target_raises(self):
        with pytest.raises(TypeError):
            MessageReplayer(target="not-a-queue")

    def test_replay_empty_list_returns_zero_counts(self):
        replayer = MessageReplayer(target=make_queue())
        result = replayer.replay([])
        assert result.replayed == 0
        assert result.skipped == 0

    def test_replay_non_list_raises(self):
        replayer = MessageReplayer(target=make_queue())
        with pytest.raises(ReplayError):
            replayer.replay("bad")

    def test_replay_adds_messages_to_queue(self):
        queue = make_queue()
        replayer = MessageReplayer(target=queue)
        msgs = [make_message("a"), make_message("b")]
        result = replayer.replay(msgs)
        assert result.replayed == 2
        assert len(queue) == 2

    def test_reset_attempts_resets_to_zero(self):
        queue = make_queue()
        replayer = MessageReplayer(target=queue, reset_attempts=True)
        msg = make_message(attempts=3)
        replayer.replay([msg])
        queued = queue._queue[0]
        assert queued.attempts == 0

    def test_no_reset_preserves_attempts(self):
        queue = make_queue()
        replayer = MessageReplayer(target=queue, reset_attempts=False)
        msg = make_message(attempts=2)
        replayer.replay([msg])
        queued = queue._queue[0]
        assert queued.attempts == 2

    def test_predicate_filters_messages(self):
        queue = make_queue()
        replayer = MessageReplayer(
            target=queue,
            predicate=lambda m: m.payload == "keep",
        )
        msgs = [make_message("keep"), make_message("drop")]
        result = replayer.replay(msgs)
        assert result.replayed == 1
        assert result.skipped == 1
        assert len(queue) == 1

    def test_non_message_in_list_is_skipped_with_error(self):
        queue = make_queue()
        replayer = MessageReplayer(target=queue)
        result = replayer.replay([make_message("ok"), "bad-value"])
        assert result.replayed == 1
        assert result.skipped == 1
        assert len(result.errors) == 1

    def test_target_property_returns_queue(self):
        queue = make_queue()
        replayer = MessageReplayer(target=queue)
        assert replayer.target is queue
