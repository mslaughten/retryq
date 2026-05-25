"""Integration tests: group_messages with a live RetryQueue and RetryRunner."""
from retryq.backoff import ExponentialBackoff
from retryq.groupby import group_messages
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner import RetryRunner


def make_message(msg_type: str, max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(payload={"type": msg_type}, max_attempts=max_attempts)


def make_runner() -> RetryRunner:
    q = RetryQueue(backoff=ExponentialBackoff(base=0.0, factor=1.0))
    return RetryRunner(queue=q)


class TestGroupByIntegration:
    def setup_method(self):
        self.runner = make_runner()

    def test_groups_reflect_live_queue_state(self):
        for t in ["order", "order", "refund", "ping"]:
            self.runner.enqueue(make_message(t))

        result = group_messages(self.runner.queue, lambda m: m.payload["type"])
        assert result.total == 4
        assert result.get("order").count == 2
        assert result.get("refund").count == 1
        assert result.get("ping").count == 1

    def test_exhausted_messages_counted_correctly(self):
        exhausted = make_message("job", max_attempts=1)
        exhausted.attempts = 1  # mark as exhausted
        fresh = make_message("job", max_attempts=3)

        self.runner.enqueue(exhausted)
        self.runner.enqueue(fresh)

        result = group_messages(self.runner.queue, lambda m: m.payload["type"])
        group = result.get("job")
        assert group is not None
        assert group.count == 2
        assert group.exhausted_count == 1

    def test_group_by_metadata_key(self):
        m1 = RetryMessage(payload={"v": 1}, max_attempts=3, metadata={"region": "us"})
        m2 = RetryMessage(payload={"v": 2}, max_attempts=3, metadata={"region": "eu"})
        m3 = RetryMessage(payload={"v": 3}, max_attempts=3, metadata={"region": "us"})

        for m in [m1, m2, m3]:
            self.runner.enqueue(m)

        result = group_messages(
            self.runner.queue,
            lambda m: m.metadata.get("region", "unknown"),
        )
        assert result.get("us").count == 2
        assert result.get("eu").count == 1

    def test_empty_runner_queue_yields_no_groups(self):
        result = group_messages(self.runner.queue, lambda m: "any")
        assert result.total == 0
        assert result.keys == []
