"""Tests for retryq backoff strategies."""

import pytest

from retryq.backoff import (
    ConstantBackoff,
    ExponentialBackoff,
    LinearBackoff,
)


class TestConstantBackoff:
    def test_returns_fixed_delay(self):
        strategy = ConstantBackoff(delay=10.0)
        assert strategy.get_delay(1) == 10.0
        assert strategy.get_delay(5) == 10.0
        assert strategy.get_delay(100) == 10.0

    def test_default_delay(self):
        strategy = ConstantBackoff()
        assert strategy.get_delay(1) == 5.0

    def test_negative_delay_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            ConstantBackoff(delay=-1.0)

    def test_zero_delay(self):
        strategy = ConstantBackoff(delay=0.0)
        assert strategy.get_delay(1) == 0.0


class TestLinearBackoff:
    def test_scales_linearly(self):
        strategy = LinearBackoff(base=5.0, max_delay=1000.0)
        assert strategy.get_delay(1) == 5.0
        assert strategy.get_delay(2) == 10.0
        assert strategy.get_delay(3) == 15.0

    def test_respects_max_delay(self):
        strategy = LinearBackoff(base=10.0, max_delay=25.0)
        assert strategy.get_delay(3) == 25.0
        assert strategy.get_delay(10) == 25.0

    def test_negative_base_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            LinearBackoff(base=-1.0)


class TestExponentialBackoff:
    def test_exponential_growth(self):
        strategy = ExponentialBackoff(base=1.0, multiplier=2.0, max_delay=1000.0, jitter=False)
        assert strategy.get_delay(1) == 1.0
        assert strategy.get_delay(2) == 2.0
        assert strategy.get_delay(3) == 4.0
        assert strategy.get_delay(4) == 8.0

    def test_respects_max_delay(self):
        strategy = ExponentialBackoff(base=1.0, multiplier=2.0, max_delay=10.0, jitter=False)
        assert strategy.get_delay(10) == 10.0

    def test_jitter_within_bounds(self):
        strategy = ExponentialBackoff(base=1.0, multiplier=2.0, max_delay=100.0, jitter=True)
        for attempt in range(1, 10):
            delay = strategy.get_delay(attempt)
            assert 0.0 <= delay <= 100.0

    def test_invalid_multiplier_raises(self):
        with pytest.raises(ValueError, match="multiplier"):
            ExponentialBackoff(multiplier=1.0)

    def test_negative_base_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            ExponentialBackoff(base=-0.5)
