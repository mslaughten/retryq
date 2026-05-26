"""Tests for retryq.window — SlidingWindow and WindowStats."""
import pytest
from retryq.window import SlidingWindow, WindowStats, WindowError


def make_clock(start: float = 0.0):
    """Returns a mutable clock and an advance callable."""
    t = [start]

    def clock() -> float:
        return t[0]

    def advance(seconds: float) -> None:
        t[0] += seconds

    return clock, advance


class TestWindowStats:
    def test_success_rate_none_when_no_attempts(self):
        s = WindowStats()
        assert s.success_rate is None

    def test_failure_rate_none_when_no_attempts(self):
        s = WindowStats()
        assert s.failure_rate is None

    def test_success_rate_calculation(self):
        s = WindowStats(attempts=4, successes=3, failures=1)
        assert s.success_rate == pytest.approx(0.75)

    def test_failure_rate_calculation(self):
        s = WindowStats(attempts=4, successes=3, failures=1)
        assert s.failure_rate == pytest.approx(0.25)


class TestSlidingWindow:
    def test_invalid_window_raises(self):
        with pytest.raises(WindowError):
            SlidingWindow(window_seconds=0)

    def test_negative_window_raises(self):
        with pytest.raises(WindowError):
            SlidingWindow(window_seconds=-5)

    def test_empty_stats_initially(self):
        clock, _ = make_clock()
        sw = SlidingWindow(window_seconds=10, clock=clock)
        s = sw.stats()
        assert s.attempts == 0
        assert s.successes == 0
        assert s.failures == 0

    def test_records_success(self):
        clock, _ = make_clock()
        sw = SlidingWindow(window_seconds=10, clock=clock)
        sw.record(success=True)
        s = sw.stats()
        assert s.attempts == 1
        assert s.successes == 1
        assert s.failures == 0

    def test_records_failure(self):
        clock, _ = make_clock()
        sw = SlidingWindow(window_seconds=10, clock=clock)
        sw.record(success=False)
        s = sw.stats()
        assert s.attempts == 1
        assert s.successes == 0
        assert s.failures == 1

    def test_evicts_old_events(self):
        clock, advance = make_clock()
        sw = SlidingWindow(window_seconds=5, clock=clock)
        sw.record(success=True)
        sw.record(success=False)
        advance(6)  # move past the window
        sw.record(success=True)  # only this should remain
        s = sw.stats()
        assert s.attempts == 1
        assert s.successes == 1

    def test_mixed_events_within_window(self):
        clock, advance = make_clock()
        sw = SlidingWindow(window_seconds=10, clock=clock)
        for _ in range(3):
            sw.record(success=True)
        for _ in range(2):
            sw.record(success=False)
        s = sw.stats()
        assert s.attempts == 5
        assert s.successes == 3
        assert s.failures == 2

    def test_reset_clears_all_events(self):
        clock, _ = make_clock()
        sw = SlidingWindow(window_seconds=10, clock=clock)
        sw.record(success=True)
        sw.record(success=False)
        sw.reset()
        s = sw.stats()
        assert s.attempts == 0

    def test_window_seconds_property(self):
        sw = SlidingWindow(window_seconds=30)
        assert sw.window_seconds == 30
