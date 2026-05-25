"""Tests for retryq.batch — BatchProcessor and BatchResult."""
import pytest

from retryq.batch import BatchProcessor, BatchResult
from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff


def make_message(payload: str = "hello", max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=max_attempts)


def make_queue() -> RetryQueue:
    return RetryQueue(backoff=ConstantBackoff(0))


def always_succeed(msg: RetryMessage) -> bool:
    return True


def always_fail(msg: RetryMessage) -> bool:
    return False


class TestBatchResult:
    def test_total_equals_processed(self):
        r = BatchResult(processed=5, succeeded=3, failed=2)
        assert r.total == 5

    def test_defaults_are_zero(self):
        r = BatchResult()
        assert r.processed == 0
        assert r.succeeded == 0
        assert r.failed == 0
        assert r.dead_lettered == 0


class TestBatchProcessor:
    def setup_method(self):
        self.queue = make_queue()
        self.dead_letters: list = []

    def _make_processor(self, handler, max_batch_size=10):
        return BatchProcessor(
            self.queue,
            handler,
            max_batch_size=max_batch_size,
            dead_letter_callback=self.dead_letters.append,
        )

    def test_invalid_batch_size_raises(self):
        with pytest.raises(ValueError):
            BatchProcessor(self.queue, always_succeed, max_batch_size=0)

    def test_empty_queue_returns_zero_processed(self):
        proc = self._make_processor(always_succeed)
        result = proc.process_batch()
        assert result.processed == 0

    def test_successful_messages_counted(self):
        for _ in range(3):
            self.queue.enqueue(make_message())
        proc = self._make_processor(always_succeed)
        result = proc.process_batch()
        assert result.processed == 3
        assert result.succeeded == 3
        assert result.failed == 0

    def test_failed_message_requeued_when_not_exhausted(self):
        self.queue.enqueue(make_message(max_attempts=3))
        proc = self._make_processor(always_fail)
        result = proc.process_batch(limit=1)
        assert result.failed == 1
        assert len(self.queue) == 1  # re-enqueued

    def test_exhausted_message_sent_to_dead_letter(self):
        msg = make_message(max_attempts=1)
        self.queue.enqueue(msg)
        proc = self._make_processor(always_fail)
        result = proc.process_batch(limit=1)
        assert result.dead_lettered == 1
        assert len(self.dead_letters) == 1
        assert len(self.queue) == 0

    def test_limit_caps_processing(self):
        for _ in range(10):
            self.queue.enqueue(make_message())
        proc = self._make_processor(always_succeed, max_batch_size=10)
        result = proc.process_batch(limit=4)
        assert result.processed == 4

    def test_max_batch_size_respected(self):
        for _ in range(20):
            self.queue.enqueue(make_message())
        proc = self._make_processor(always_succeed, max_batch_size=5)
        result = proc.process_batch()
        assert result.processed == 5

    def test_metrics_record_success(self):
        self.queue.enqueue(make_message())
        proc = self._make_processor(always_succeed)
        proc.process_batch()
        assert proc.metrics.successes == 1

    def test_metrics_record_dead_letter(self):
        msg = make_message(max_attempts=1)
        self.queue.enqueue(msg)
        proc = self._make_processor(always_fail)
        proc.process_batch()
        assert proc.metrics.dead_letters == 1

    def test_handler_exception_treated_as_failure(self):
        def boom(msg):
            raise RuntimeError("oops")

        self.queue.enqueue(make_message(max_attempts=5))
        proc = self._make_processor(boom)
        result = proc.process_batch(limit=1)
        assert result.failed == 1
