"""Integration tests for PartitionedRetryQueue with real backoff and handlers."""

import pytest

from retryq.partition import PartitionedRetryQueue
from retryq.queue import RetryMessage
from retryq.backoff import ConstantBackoff


def make_message(msg_type: str, body: str) -> RetryMessage:
    return RetryMessage(payload={"type": msg_type, "body": body})


def make_pq(max_attempts: int = 3) -> PartitionedRetryQueue:
    return PartitionedRetryQueue(
        key_fn=lambda m: m.payload.get("type", "default"),
        backoff=ConstantBackoff(delay=0),
        max_attempts=max_attempts,
    )


class TestPartitionIntegration:
    def test_successful_handler_drains_partition(self):
        pq = make_pq()
        pq.enqueue(make_message("email", "msg1"))
        pq.enqueue(make_message("email", "msg2"))

        processed = []
        while pq.process_next("email", lambda m: processed.append(m.payload["body"]) or True):
            pass

        assert processed == ["msg1", "msg2"]

    def test_partitions_do_not_interfere(self):
        pq = make_pq()
        pq.enqueue(make_message("email", "e1"))
        pq.enqueue(make_message("sms", "s1"))
        pq.enqueue(make_message("sms", "s2"))

        email_seen = []
        sms_seen = []

        while pq.process_next("email", lambda m: email_seen.append(m.payload["body"]) or True):
            pass
        while pq.process_next("sms", lambda m: sms_seen.append(m.payload["body"]) or True):
            pass

        assert email_seen == ["e1"]
        assert sms_seen == ["s1", "s2"]

    def test_failed_handler_retries_up_to_max_attempts(self):
        pq = make_pq(max_attempts=3)
        pq.enqueue(make_message("push", "retry-me"))

        call_count = [0]

        def always_fail(msg: RetryMessage) -> bool:
            call_count[0] += 1
            return False

        for _ in range(10):
            pq.process_next("push", always_fail)

        # Should have been attempted max_attempts times then dead-lettered
        assert call_count[0] == 3

    def test_total_size_decreases_as_messages_processed(self):
        pq = make_pq()
        pq.enqueue(make_message("a", "1"))
        pq.enqueue(make_message("b", "2"))
        assert pq.total_size() == 2

        pq.process_next("a", lambda m: True)
        assert pq.total_size() == 1

        pq.process_next("b", lambda m: True)
        assert pq.total_size() == 0
