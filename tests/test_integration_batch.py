"""Integration tests for BatchProcessor with realistic retry cycles."""
from retryq.batch import BatchProcessor
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff, ExponentialBackoff


def make_queue(backoff=None) -> RetryQueue:
    return RetryQueue(backoff=backoff or ConstantBackoff(0))


def make_message(payload="data", max_attempts=3) -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=max_attempts)


class TestBatchIntegration:
    def test_all_messages_eventually_succeed(self):
        queue = make_queue()
        for i in range(5):
            queue.enqueue(make_message(payload=f"msg-{i}"))

        proc = BatchProcessor(queue, lambda m: True, max_batch_size=10)
        result = proc.process_batch()

        assert result.succeeded == 5
        assert result.failed == 0
        assert len(queue) == 0

    def test_retry_until_exhausted_drains_queue(self):
        dead: list = []
        queue = make_queue()
        for _ in range(3):
            queue.enqueue(make_message(max_attempts=2))

        proc = BatchProcessor(
            queue,
            lambda m: False,
            max_batch_size=20,
            dead_letter_callback=dead.append,
        )

        # Each message needs 2 failed attempts before dead-lettering
        for _ in range(5):
            proc.process_batch()

        assert len(dead) == 3
        assert len(queue) == 0

    def test_mixed_success_and_failure(self):
        dead: list = []
        queue = make_queue()
        succeed_ids = {"a", "b"}

        queue.enqueue(make_message(payload="a", max_attempts=1))
        queue.enqueue(make_message(payload="b", max_attempts=1))
        queue.enqueue(make_message(payload="c", max_attempts=1))

        proc = BatchProcessor(
            queue,
            lambda m: m.payload in succeed_ids,
            max_batch_size=10,
            dead_letter_callback=dead.append,
        )

        proc.process_batch()
        proc.process_batch()  # second pass picks up re-queued failures

        assert proc.metrics.successes == 2
        assert len(dead) == 1
        assert dead[0].payload == "c"

    def test_batch_size_limits_per_call(self):
        queue = make_queue()
        for _ in range(10):
            queue.enqueue(make_message())

        proc = BatchProcessor(queue, lambda m: True, max_batch_size=3)
        r1 = proc.process_batch()
        r2 = proc.process_batch()
        r3 = proc.process_batch()
        r4 = proc.process_batch()  # last batch, only 1 remaining

        assert r1.processed == 3
        assert r2.processed == 3
        assert r3.processed == 3
        assert r4.processed == 1
        assert len(queue) == 0
