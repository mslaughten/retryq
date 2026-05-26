"""Tests for WatermarkPolicy and WatermarkRetryRunner."""
import pytest

from retryq.queue import RetryMessage, RetryQueue
from retryq.backoff import ConstantBackoff
from retryq.watermark import WatermarkError, WatermarkPolicy
from retryq.runner_watermark import WatermarkRetryRunner


def make_message(payload: str = "test") -> RetryMessage:
    return RetryMessage(id=payload, payload={"data": payload}, max_attempts=3)


def make_runner(high: int = 5, low: int = 1):
    queue = RetryQueue()
    high_calls = []
    low_calls = []
    wm = WatermarkPolicy(
        high=high,
        low=low,
        on_high=lambda d: high_calls.append(d),
        on_low=lambda d: low_calls.append(d),
    )
    runner = WatermarkRetryRunner(
        queue=queue,
        handler=lambda msg: None,
        backoff=ConstantBackoff(delay=0),
        watermark=wm,
    )
    return runner, high_calls, low_calls


class TestWatermarkPolicy:
    def test_invalid_high_raises(self):
        with pytest.raises(WatermarkError):
            WatermarkPolicy(high=0, low=0)

    def test_negative_low_raises(self):
        with pytest.raises(WatermarkError):
            WatermarkPolicy(high=5, low=-1)

    def test_low_gte_high_raises(self):
        with pytest.raises(WatermarkError):
            WatermarkPolicy(high=3, low=3)

    def test_normal_status_between_marks(self):
        wm = WatermarkPolicy(high=10, low=2)
        assert wm.check(5) == "normal"

    def test_high_status_at_threshold(self):
        wm = WatermarkPolicy(high=5, low=1)
        assert wm.check(5) == "high"

    def test_low_status_at_threshold(self):
        wm = WatermarkPolicy(high=5, low=1)
        wm.check(5)  # trigger high first
        assert wm.check(1) == "low"

    def test_on_high_callback_fires_once(self):
        calls = []
        wm = WatermarkPolicy(high=3, low=0, on_high=lambda d: calls.append(d))
        wm.check(3)
        wm.check(4)
        assert len(calls) == 1
        assert calls[0] == 3

    def test_on_low_callback_fires_after_high(self):
        calls = []
        wm = WatermarkPolicy(high=3, low=1, on_low=lambda d: calls.append(d))
        wm.check(3)  # go high
        wm.check(1)  # come back down
        assert len(calls) == 1

    def test_triggered_high_property(self):
        wm = WatermarkPolicy(high=3, low=1)
        assert not wm.triggered_high
        wm.check(3)
        assert wm.triggered_high
        wm.check(1)
        assert not wm.triggered_high


class TestWatermarkRetryRunner:
    def test_enqueue_fires_high_watermark(self):
        runner, high_calls, _ = make_runner(high=2, low=0)
        runner.enqueue(make_message("a"))
        runner.enqueue(make_message("b"))
        assert len(high_calls) == 1

    def test_process_next_fires_low_watermark(self):
        runner, _, low_calls = make_runner(high=2, low=0)
        runner.enqueue(make_message("a"))
        runner.enqueue(make_message("b"))
        runner.process_next()
        runner.process_next()
        assert len(low_calls) == 1

    def test_metrics_updated_on_success(self):
        runner, _, _ = make_runner()
        runner.enqueue(make_message("x"))
        runner.process_next()
        assert runner.metrics.total_successes == 1

    def test_no_watermark_runs_without_error(self):
        queue = RetryQueue()
        runner = WatermarkRetryRunner(queue=queue, handler=lambda m: None)
        runner.enqueue(make_message("z"))
        assert runner.process_next() is True
