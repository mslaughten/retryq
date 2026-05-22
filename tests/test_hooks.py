"""Tests for retryq.hooks HookRegistry."""

import pytest
from retryq.hooks import HookRegistry, HookEvent
from retryq.queue import RetryMessage


def make_message(payload: dict = None) -> RetryMessage:
    return RetryMessage(payload=payload or {"key": "value"})


class TestHookRegistry:
    def setup_method(self):
        self.registry = HookRegistry()

    def test_initial_count_is_zero(self):
        for event in HookEvent.ALL:
            assert self.registry.count(event) == 0

    def test_register_increments_count(self):
        self.registry.register(HookEvent.ON_ENQUEUE, lambda m: None)
        assert self.registry.count(HookEvent.ON_ENQUEUE) == 1

    def test_register_unknown_event_raises(self):
        with pytest.raises(ValueError, match="Unknown hook event"):
            self.registry.register("on_unknown", lambda m: None)

    def test_fire_calls_registered_hook(self):
        called_with = []
        self.registry.register(HookEvent.ON_SUCCESS, lambda m: called_with.append(m))
        msg = make_message()
        self.registry.fire(HookEvent.ON_SUCCESS, msg)
        assert called_with == [msg]

    def test_fire_calls_multiple_hooks_in_order(self):
        order = []
        self.registry.register(HookEvent.ON_RETRY, lambda m: order.append(1))
        self.registry.register(HookEvent.ON_RETRY, lambda m: order.append(2))
        self.registry.fire(HookEvent.ON_RETRY, make_message())
        assert order == [1, 2]

    def test_fire_unknown_event_does_not_raise(self):
        # fire with unknown event should silently do nothing
        self.registry.fire("nonexistent", make_message())

    def test_clear_specific_event(self):
        self.registry.register(HookEvent.ON_ENQUEUE, lambda m: None)
        self.registry.register(HookEvent.ON_SUCCESS, lambda m: None)
        self.registry.clear(HookEvent.ON_ENQUEUE)
        assert self.registry.count(HookEvent.ON_ENQUEUE) == 0
        assert self.registry.count(HookEvent.ON_SUCCESS) == 1

    def test_clear_all_events(self):
        for event in HookEvent.ALL:
            self.registry.register(event, lambda m: None)
        self.registry.clear()
        for event in HookEvent.ALL:
            assert self.registry.count(event) == 0

    def test_clear_unknown_event_raises(self):
        with pytest.raises(ValueError, match="Unknown hook event"):
            self.registry.clear("bad_event")

    def test_dead_letter_hook_fires(self):
        dead = []
        self.registry.register(HookEvent.ON_DEAD_LETTER, lambda m: dead.append(m.payload))
        msg = make_message({"id": 99})
        self.registry.fire(HookEvent.ON_DEAD_LETTER, msg)
        assert dead == [{"id": 99}]
