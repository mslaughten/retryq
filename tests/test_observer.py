"""Tests for RetryObserver and ObservableRetryRunner."""

from __future__ import annotations

from typing import List

import pytest

from retryq.backoff import ConstantBackoff
from retryq.observer import RetryEvent, RetryObserver
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner_observer import ObservableRetryRunner


def make_message(payload: str = "hello", max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(payload=payload, max_attempts=max_attempts)


def make_runner(
    handler,
    max_attempts: int = 3,
    observer: RetryObserver | None = None,
) -> ObservableRetryRunner:
    queue = RetryQueue()
    msg_template = make_message(max_attempts=max_attempts)
    return ObservableRetryRunner(
        queue,
        handler,
        backoff=ConstantBackoff(delay=0),
        observer=observer,
    )


class TestRetryObserver:
    def setup_method(self):
        self.observer = RetryObserver()
        self.received: List[RetryEvent] = []

    def _capture(self, event: RetryEvent) -> None:
        self.received.append(event)

    def test_initial_listener_count_is_zero(self):
        assert self.observer.listener_count("enqueue") == 0

    def test_subscribe_increments_listener_count(self):
        self.observer.subscribe("enqueue", self._capture)
        assert self.observer.listener_count("enqueue") == 1

    def test_notify_dispatches_to_callback(self):
        self.observer.subscribe("success", self._capture)
        msg = make_message()
        self.observer.notify("success", msg)
        assert len(self.received) == 1
        assert self.received[0].event_type == "success"
        assert self.received[0].message is msg

    def test_extra_kwargs_stored_on_event(self):
        self.observer.subscribe("retry", self._capture)
        msg = make_message()
        self.observer.notify("retry", msg, delay=2.5)
        assert self.received[0].extra["delay"] == 2.5

    def test_subscribe_unknown_event_raises(self):
        with pytest.raises(ValueError, match="Unknown event type"):
            self.observer.subscribe("unknown_event", self._capture)

    def test_notify_unknown_event_raises(self):
        msg = make_message()
        with pytest.raises(ValueError, match="Unknown event type"):
            self.observer.notify("bogus", msg)

    def test_multiple_listeners_all_called(self):
        second: List[RetryEvent] = []
        self.observer.subscribe("dead_letter", self._capture)
        self.observer.subscribe("dead_letter", lambda e: second.append(e))
        msg = make_message()
        self.observer.notify("dead_letter", msg)
        assert len(self.received) == 1
        assert len(second) == 1


class TestObservableRetryRunner:
    def setup_method(self):
        self.observer = RetryObserver()
        self.events: List[RetryEvent] = []
        for ev in RetryObserver.VALID_EVENTS:
            self.observer.subscribe(ev, self.events.append)

    def _make_runner(self, handler, max_attempts=3):
        queue = RetryQueue()
        return ObservableRetryRunner(
            queue,
            handler,
            backoff=ConstantBackoff(delay=0),
            observer=self.observer,
        )

    def test_enqueue_fires_enqueue_event(self):
        runner = self._make_runner(lambda m: True)
        runner.enqueue(make_message())
        types = [e.event_type for e in self.events]
        assert "enqueue" in types

    def test_success_fires_success_event(self):
        runner = self._make_runner(lambda m: True)
        runner.enqueue(make_message())
        runner.process_next()
        types = [e.event_type for e in self.events]
        assert "success" in types

    def test_failure_then_retry_fires_retry_event(self):
        calls = {"n": 0}

        def handler(m):
            calls["n"] += 1
            return False

        runner = self._make_runner(handler, max_attempts=3)
        runner.enqueue(make_message(max_attempts=3))
        runner.process_next()
        types = [e.event_type for e in self.events]
        assert "retry" in types

    def test_exhausted_message_fires_dead_letter_event(self):
        runner = self._make_runner(lambda m: False, max_attempts=1)
        runner.enqueue(make_message(max_attempts=1))
        runner.process_next()
        types = [e.event_type for e in self.events]
        assert "dead_letter" in types

    def test_process_next_returns_false_when_empty(self):
        runner = self._make_runner(lambda m: True)
        assert runner.process_next() is False
