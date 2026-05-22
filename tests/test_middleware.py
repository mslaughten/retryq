"""Tests for retryq.middleware."""

from unittest.mock import MagicMock

import pytest

from retryq.middleware import (
    LoggingMiddleware,
    MetadataEnrichmentMiddleware,
    MiddlewareChain,
)
from retryq.queue import RetryMessage


def make_message(**kwargs) -> RetryMessage:
    defaults = {"id": "msg-1", "payload": {"key": "value"}, "max_attempts": 3}
    defaults.update(kwargs)
    return RetryMessage(**defaults)


class TestMiddlewareChain:
    def test_empty_chain_returns_message_unchanged(self):
        chain = MiddlewareChain()
        msg = make_message()
        result = chain.run_before(msg)
        assert result is msg

    def test_add_and_run_single_middleware(self):
        calls = []

        class Spy(LoggingMiddleware):
            def before_retry(self, message):
                calls.append("before")
                return message

        chain = MiddlewareChain()
        chain.add(Spy())
        chain.run_before(make_message())
        assert calls == ["before"]

    def test_run_after_calls_in_reverse_order(self):
        order = []

        class First(LoggingMiddleware):
            def after_retry(self, msg, success, error):
                order.append("first")

        class Second(LoggingMiddleware):
            def after_retry(self, msg, success, error):
                order.append("second")

        chain = MiddlewareChain([First(), Second()])
        chain.run_after(make_message(), True)
        assert order == ["second", "first"]


class TestLoggingMiddleware:
    def test_before_retry_logs_message(self):
        log = MagicMock()
        mw = LoggingMiddleware(logger=log)
        msg = make_message(id="abc")
        mw.before_retry(msg)
        log.assert_called_once()
        assert "abc" in log.call_args[0][0]

    def test_after_retry_logs_success(self):
        log = MagicMock()
        mw = LoggingMiddleware(logger=log)
        mw.after_retry(make_message(), success=True, error=None)
        assert "succeeded" in log.call_args[0][0]

    def test_after_retry_logs_failure(self):
        log = MagicMock()
        mw = LoggingMiddleware(logger=log)
        mw.after_retry(make_message(), success=False, error=ValueError("oops"))
        assert "failed" in log.call_args[0][0]


class TestMetadataEnrichmentMiddleware:
    def test_before_retry_stamps_attempt_number(self):
        mw = MetadataEnrichmentMiddleware()
        msg = make_message()
        mw.before_retry(msg)
        assert msg.metadata["last_attempt_number"] == 1

    def test_after_retry_stamps_success_flag(self):
        mw = MetadataEnrichmentMiddleware()
        msg = make_message()
        mw.after_retry(msg, success=True, error=None)
        assert msg.metadata["last_attempt_success"] is True

    def test_after_retry_stamps_error_string(self):
        mw = MetadataEnrichmentMiddleware()
        msg = make_message()
        mw.after_retry(msg, success=False, error=RuntimeError("boom"))
        assert msg.metadata["last_error"] == "boom"

    def test_after_retry_no_error_key_when_none(self):
        mw = MetadataEnrichmentMiddleware()
        msg = make_message()
        mw.after_retry(msg, success=True, error=None)
        assert "last_error" not in msg.metadata
