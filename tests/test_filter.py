"""Tests for retryq.filter module."""

import pytest
from retryq.queue import RetryMessage
from retryq.filter import (
    MessageFilter,
    by_topic,
    by_max_attempts,
    by_metadata_key,
)


def make_message(
    payload: dict = None,
    attempts: int = 0,
    metadata: dict = None,
) -> RetryMessage:
    return RetryMessage(
        payload=payload or {},
        attempts=attempts,
        metadata=metadata or {},
    )


class TestMessageFilter:
    def test_empty_filter_matches_everything(self):
        f = MessageFilter()
        msg = make_message()
        assert f.matches(msg) is True

    def test_add_returns_self_for_chaining(self):
        f = MessageFilter()
        result = f.add(lambda m: True)
        assert result is f

    def test_predicate_count_increments(self):
        f = MessageFilter()
        assert f.predicate_count == 0
        f.add(lambda m: True)
        assert f.predicate_count == 1

    def test_single_predicate_false_rejects_message(self):
        f = MessageFilter().add(lambda m: False)
        assert f.matches(make_message()) is False

    def test_all_predicates_must_pass(self):
        f = MessageFilter().add(lambda m: True).add(lambda m: False)
        assert f.matches(make_message()) is False

    def test_apply_filters_list(self):
        f = MessageFilter().add(lambda m: m.attempts > 0)
        msgs = [make_message(attempts=0), make_message(attempts=1), make_message(attempts=2)]
        result = f.apply(msgs)
        assert len(result) == 2

    def test_apply_empty_list_returns_empty(self):
        f = MessageFilter().add(lambda m: True)
        assert f.apply([]) == []


class TestBuiltinPredicates:
    def test_by_topic_matches_correct_topic(self):
        pred = by_topic("payments")
        msg = make_message(payload={"topic": "payments"})
        assert pred(msg) is True

    def test_by_topic_rejects_wrong_topic(self):
        pred = by_topic("payments")
        msg = make_message(payload={"topic": "orders"})
        assert pred(msg) is False

    def test_by_topic_missing_key_returns_false(self):
        pred = by_topic("payments")
        assert pred(make_message()) is False

    def test_by_max_attempts_allows_under_limit(self):
        pred = by_max_attempts(3)
        assert pred(make_message(attempts=2)) is True

    def test_by_max_attempts_rejects_at_limit(self):
        pred = by_max_attempts(3)
        assert pred(make_message(attempts=3)) is False

    def test_by_metadata_key_matches(self):
        pred = by_metadata_key("env", "prod")
        msg = make_message(metadata={"env": "prod"})
        assert pred(msg) is True

    def test_by_metadata_key_rejects_wrong_value(self):
        pred = by_metadata_key("env", "prod")
        msg = make_message(metadata={"env": "staging"})
        assert pred(msg) is False

    def test_combined_predicates_in_filter(self):
        f = (
            MessageFilter()
            .add(by_topic("alerts"))
            .add(by_max_attempts(5))
            .add(by_metadata_key("priority", "high"))
        )
        matching = make_message(
            payload={"topic": "alerts"},
            attempts=2,
            metadata={"priority": "high"},
        )
        non_matching = make_message(
            payload={"topic": "alerts"},
            attempts=6,
            metadata={"priority": "high"},
        )
        assert f.matches(matching) is True
        assert f.matches(non_matching) is False
