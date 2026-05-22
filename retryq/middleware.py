"""Middleware support for RetryQueue — allows pre/post processing hooks on retry cycles."""

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional

from retryq.queue import RetryMessage


class RetryMiddleware(ABC):
    """Base class for retry middleware."""

    @abstractmethod
    def before_retry(self, message: RetryMessage) -> RetryMessage:
        """Called before a retry attempt. May modify or replace the message."""
        ...

    @abstractmethod
    def after_retry(
        self, message: RetryMessage, success: bool, error: Optional[Exception]
    ) -> None:
        """Called after a retry attempt with the outcome."""
        ...


class MiddlewareChain:
    """Executes a sequence of middleware around retry operations."""

    def __init__(self, middlewares: Optional[List[RetryMiddleware]] = None) -> None:
        self._middlewares: List[RetryMiddleware] = middlewares or []

    def add(self, middleware: RetryMiddleware) -> None:
        """Append a middleware to the chain."""
        self._middlewares.append(middleware)

    def run_before(self, message: RetryMessage) -> RetryMessage:
        """Run all before_retry hooks in order."""
        for mw in self._middlewares:
            message = mw.before_retry(message)
        return message

    def run_after(
        self, message: RetryMessage, success: bool, error: Optional[Exception] = None
    ) -> None:
        """Run all after_retry hooks in reverse order."""
        for mw in reversed(self._middlewares):
            mw.after_retry(message, success, error)


class LoggingMiddleware(RetryMiddleware):
    """Simple middleware that records retry events via a callable logger."""

    def __init__(self, logger: Callable[[str], None] = print) -> None:
        self._log = logger

    def before_retry(self, message: RetryMessage) -> RetryMessage:
        self._log(
            f"[retryq] Retrying message id={message.id} "
            f"attempt={message.attempts + 1}/{message.max_attempts}"
        )
        return message

    def after_retry(
        self, message: RetryMessage, success: bool, error: Optional[Exception]
    ) -> None:
        status = "succeeded" if success else f"failed ({error})"
        self._log(f"[retryq] Message id={message.id} attempt {status}")


class MetadataEnrichmentMiddleware(RetryMiddleware):
    """Middleware that stamps retry metadata onto the message before each attempt."""

    def before_retry(self, message: RetryMessage) -> RetryMessage:
        message.metadata["last_attempt_number"] = message.attempts + 1
        return message

    def after_retry(
        self, message: RetryMessage, success: bool, error: Optional[Exception]
    ) -> None:
        message.metadata["last_attempt_success"] = success
        if error is not None:
            message.metadata["last_error"] = str(error)
