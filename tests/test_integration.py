"""Integration tests for retryq — queue + backoff working together."""

import pytest
from unittest.mock import MagicMock
from retryq import RetryQueue, RetryMessage, ExponentialBackoff, ConstantBackoff


class TestRetryQueueIntegration:
    def test_exponential_backoff_delays_increase(self):
        """Verify ExponentialBackoff produces increasing delays per attempt."""
        backoff = ExponentialBackoff(base=2.0, multiplier=1.0)
        delays = [backoff.get_delay(i) for i in range(5)]
        for i in range(1, len(delays)):
            assert delays[i] >= delays[i - 1], (
                f"Expected delay at attempt {i} >= attempt {i-1}"
            )

    def test_full_retry_cycle_success_on_third_attempt(self):
        """Handler fails twice then succeeds; message should not be requeued after success."""
        call_count = {"n": 0}

        def flaky_handler(payload):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ValueError("not yet")

        q = RetryQueue(
            handler=flaky_handler,
            backoff=ConstantBackoff(delay=0),
            max_attempts=5,
        )
        q.enqueue("task")
        q.drain(dry_run=True)

        assert call_count["n"] == 3
        assert q.size == 0

    def test_dead_letter_callback_receives_correct_payload(self):
        dead_letters = []

        def on_exhausted(msg: RetryMessage):
            dead_letters.append(msg.payload)

        q = RetryQueue(
            handler=MagicMock(side_effect=Exception("always fails")),
            backoff=ConstantBackoff(delay=0),
            max_attempts=2,
            on_exhausted=on_exhausted,
        )
        q.enqueue("important-task")
        q.drain(dry_run=True)

        assert dead_letters == ["important-task"]

    def test_dead_letter_callback_receives_attempt_count(self):
        """Verify the exhausted message carries the correct attempt count when dead-lettered."""
        exhausted_msgs = []

        def on_exhausted(msg: RetryMessage):
            exhausted_msgs.append(msg)

        q = RetryQueue(
            handler=MagicMock(side_effect=Exception("always fails")),
            backoff=ConstantBackoff(delay=0),
            max_attempts=3,
            on_exhausted=on_exhausted,
        )
        q.enqueue("tracked-task")
        q.drain(dry_run=True)

        assert len(exhausted_msgs) == 1
        assert exhausted_msgs[0].payload == "tracked-task"
        assert exhausted_msgs[0].attempts == 3

    def test_multiple_messages_independent_retry_counts(self):
        results = []
        fail_payloads = {"fail-me"}

        def handler(payload):
            if payload in fail_payloads:
                raise RuntimeError("forced failure")
            results.append(payload)

        q = RetryQueue(
            handler=handler,
            backoff=ConstantBackoff(delay=0),
            max_attempts=3,
        )
        q.enqueue("ok-msg")
        q.enqueue("fail-me")
        q.drain(dry_run=True)

        assert "ok-msg" in results
        assert "fail-me" not in results
        assert q.size == 0
