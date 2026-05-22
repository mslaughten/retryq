"""Tests for RetryMetrics."""

import pytest
from retryq.metrics import RetryMetrics


class TestRetryMetrics:
    def setup_method(self):
        self.metrics = RetryMetrics()

    def test_initial_state_is_zero(self):
        assert self.metrics.total_enqueued == 0
        assert self.metrics.total_retried == 0
        assert self.metrics.total_succeeded == 0
        assert self.metrics.total_dead_lettered == 0

    def test_success_rate_none_when_no_messages_processed(self):
        assert self.metrics.success_rate is None

    def test_record_enqueue_increments_counter(self):
        self.metrics.record_enqueue()
        self.metrics.record_enqueue()
        assert self.metrics.total_enqueued == 2

    def test_record_retry_increments_counter(self):
        self.metrics.record_retry()
        assert self.metrics.total_retried == 1

    def test_record_success_increments_counter_and_histogram(self):
        self.metrics.record_success(attempts=2)
        assert self.metrics.total_succeeded == 1
        assert self.metrics.attempts_histogram[2] == 1

    def test_record_dead_letter_increments_counter_and_histogram(self):
        self.metrics.record_dead_letter(attempts=5)
        assert self.metrics.total_dead_lettered == 1
        assert self.metrics.attempts_histogram[5] == 1

    def test_success_rate_all_success(self):
        self.metrics.record_success(attempts=1)
        self.metrics.record_success(attempts=2)
        assert self.metrics.success_rate == 1.0

    def test_success_rate_all_dead_lettered(self):
        self.metrics.record_dead_letter(attempts=3)
        assert self.metrics.success_rate == 0.0

    def test_success_rate_mixed(self):
        self.metrics.record_success(attempts=1)
        self.metrics.record_dead_letter(attempts=3)
        assert self.metrics.success_rate == 0.5

    def test_attempts_histogram_accumulates(self):
        self.metrics.record_success(attempts=2)
        self.metrics.record_success(attempts=2)
        self.metrics.record_dead_letter(attempts=2)
        assert self.metrics.attempts_histogram[2] == 3

    def test_summary_returns_all_fields(self):
        self.metrics.record_enqueue()
        self.metrics.record_retry()
        self.metrics.record_success(attempts=1)
        summary = self.metrics.summary()
        assert summary["total_enqueued"] == 1
        assert summary["total_retried"] == 1
        assert summary["total_succeeded"] == 1
        assert summary["total_dead_lettered"] == 0
        assert summary["success_rate"] == 1.0
        assert summary["attempts_histogram"] == {1: 1}

    def test_reset_clears_all_counters(self):
        self.metrics.record_enqueue()
        self.metrics.record_success(attempts=1)
        self.metrics.reset()
        assert self.metrics.total_enqueued == 0
        assert self.metrics.total_succeeded == 0
        assert self.metrics.attempts_histogram == {}
        assert self.metrics.success_rate is None
