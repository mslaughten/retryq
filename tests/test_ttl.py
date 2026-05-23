"""Tests for retryq.ttl — TTL policy for retry messages."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from retryq.queue import RetryMessage
from retryq.ttl import TTLExpiredError, TTLPolicy


def make_message(**metadata) -> RetryMessage:
    return RetryMessage(
        id=str(uuid.uuid4()),
        payload={"event": "test"},
        metadata=dict(metadata),
    )


def make_clock(current: float) -> MagicMock:
    clock = MagicMock()
    clock.time.return_value = current
    return clock


class TestTTLPolicy:
    def test_invalid_max_age_raises(self):
        with pytest.raises(ValueError, match="max_age_seconds must be positive"):
            TTLPolicy(max_age_seconds=0)

    def test_negative_max_age_raises(self):
        with pytest.raises(ValueError):
            TTLPolicy(max_age_seconds=-5.0)

    def test_stamp_adds_created_at(self):
        clock = make_clock(1000.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message()
        policy.stamp(msg)
        assert msg.metadata["created_at"] == 1000.0

    def test_stamp_does_not_overwrite_existing(self):
        clock = make_clock(2000.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message(created_at=500.0)
        policy.stamp(msg)
        assert msg.metadata["created_at"] == 500.0

    def test_age_of_returns_elapsed_seconds(self):
        clock = make_clock(1060.0)
        policy = TTLPolicy(max_age_seconds=120.0, clock=clock)
        msg = make_message(created_at=1000.0)
        assert policy.age_of(msg) == pytest.approx(60.0)

    def test_age_of_returns_zero_when_no_timestamp(self):
        clock = make_clock(9999.0)
        policy = TTLPolicy(max_age_seconds=30.0, clock=clock)
        msg = make_message()
        assert policy.age_of(msg) == 0.0

    def test_is_expired_false_within_ttl(self):
        clock = make_clock(1010.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message(created_at=1000.0)
        assert policy.is_expired(msg) is False

    def test_is_expired_true_past_ttl(self):
        clock = make_clock(1070.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message(created_at=1000.0)
        assert policy.is_expired(msg) is True

    def test_check_raises_when_expired(self):
        clock = make_clock(1100.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message(created_at=1000.0)
        with pytest.raises(TTLExpiredError) as exc_info:
            policy.check(msg)
        err = exc_info.value
        assert err.ttl == 60.0
        assert err.age == pytest.approx(100.0)
        assert msg.id in str(err)

    def test_check_does_not_raise_within_ttl(self):
        clock = make_clock(1030.0)
        policy = TTLPolicy(max_age_seconds=60.0, clock=clock)
        msg = make_message(created_at=1000.0)
        policy.check(msg)  # should not raise

    def test_ttl_expired_error_str(self):
        err = TTLExpiredError("abc-123", 30.0, 45.5)
        assert "abc-123" in str(err)
        assert "30.00" in str(err)
        assert "45.50" in str(err)
