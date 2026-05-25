"""Tests for retryq.groupby."""
import pytest

from retryq.backoff import ConstantBackoff
from retryq.groupby import (
    GroupByResult,
    InvalidKeyFnError,
    MessageGroup,
    group_messages,
)
from retryq.queue import RetryMessage, RetryQueue


def make_message(msg_type: str = "order", attempts: int = 0, max_attempts: int = 3) -> RetryMessage:
    m = RetryMessage(payload={"type": msg_type}, max_attempts=max_attempts)
    m.attempts = attempts
    return m


def make_queue(*messages: RetryMessage) -> RetryQueue:
    q = RetryQueue(backoff=ConstantBackoff(0))
    for m in messages:
        q.enqueue(m)
    return q


class TestMessageGroup:
    def test_count_reflects_messages(self):
        msgs = [make_message() for _ in range(3)]
        g = MessageGroup(key="x", messages=msgs)
        assert g.count == 3

    def test_exhausted_count(self):
        msgs = [make_message(attempts=3), make_message(attempts=1), make_message(attempts=3)]
        g = MessageGroup(key="x", messages=msgs)
        assert g.exhausted_count == 2


class TestGroupByResult:
    def test_keys_returns_all_group_keys(self):
        r = GroupByResult()
        r.groups["a"] = MessageGroup(key="a", messages=[make_message()])
        r.groups["b"] = MessageGroup(key="b", messages=[make_message(), make_message()])
        assert set(r.keys) == {"a", "b"}

    def test_total_sums_all_groups(self):
        r = GroupByResult()
        r.groups["a"] = MessageGroup(key="a", messages=[make_message()])
        r.groups["b"] = MessageGroup(key="b", messages=[make_message(), make_message()])
        assert r.total == 3

    def test_get_returns_none_for_missing_key(self):
        r = GroupByResult()
        assert r.get("missing") is None


class TestGroupMessages:
    def test_empty_queue_returns_empty_result(self):
        q = make_queue()
        result = group_messages(q, lambda m: m.payload["type"])
        assert result.total == 0
        assert result.keys == []

    def test_groups_by_payload_type(self):
        q = make_queue(
            make_message("order"),
            make_message("order"),
            make_message("refund"),
        )
        result = group_messages(q, lambda m: m.payload["type"])
        assert set(result.keys) == {"order", "refund"}
        assert result.get("order").count == 2
        assert result.get("refund").count == 1

    def test_non_callable_key_fn_raises(self):
        q = make_queue(make_message())
        with pytest.raises(InvalidKeyFnError):
            group_messages(q, "not_a_function")  # type: ignore[arg-type]

    def test_non_string_key_raises(self):
        q = make_queue(make_message())
        with pytest.raises(InvalidKeyFnError):
            group_messages(q, lambda m: 42)  # type: ignore[return-value]

    def test_single_group_when_all_same_key(self):
        q = make_queue(make_message("ping"), make_message("ping"), make_message("ping"))
        result = group_messages(q, lambda m: m.payload["type"])
        assert len(result.keys) == 1
        assert result.get("ping").count == 3

    def test_total_matches_enqueued_count(self):
        msgs = [make_message("a"), make_message("b"), make_message("a"), make_message("c")]
        q = make_queue(*msgs)
        result = group_messages(q, lambda m: m.payload["type"])
        assert result.total == 4
