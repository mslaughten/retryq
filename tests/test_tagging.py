"""Unit tests for retryq.tagging.TagRegistry."""
import uuid
import pytest

from retryq.queue import RetryMessage
from retryq.tagging import InvalidTagError, TagRegistry


def make_message(payload: dict | None = None) -> RetryMessage:
    return RetryMessage(id=str(uuid.uuid4()), payload=payload or {"k": "v"})


class TestTagRegistry:
    def setup_method(self):
        self.registry = TagRegistry()
        self.msg = make_message()

    def test_new_message_has_no_tags(self):
        assert self.registry.get_tags(self.msg) == frozenset()

    def test_tag_single(self):
        self.registry.tag(self.msg, "urgent")
        assert self.registry.has_tag(self.msg, "urgent")

    def test_tag_multiple_at_once(self):
        self.registry.tag(self.msg, "a", "b", "c")
        assert self.registry.get_tags(self.msg) == {"a", "b", "c"}

    def test_untag_removes_specific_tag(self):
        self.registry.tag(self.msg, "x", "y")
        self.registry.untag(self.msg, "x")
        assert not self.registry.has_tag(self.msg, "x")
        assert self.registry.has_tag(self.msg, "y")

    def test_untag_missing_tag_is_silent(self):
        self.registry.untag(self.msg, "nonexistent")  # should not raise

    def test_has_all_true(self):
        self.registry.tag(self.msg, "a", "b")
        assert self.registry.has_all(self.msg, ["a", "b"])

    def test_has_all_false_when_one_missing(self):
        self.registry.tag(self.msg, "a")
        assert not self.registry.has_all(self.msg, ["a", "b"])

    def test_has_any_true(self):
        self.registry.tag(self.msg, "a")
        assert self.registry.has_any(self.msg, ["a", "z"])

    def test_has_any_false(self):
        assert not self.registry.has_any(self.msg, ["x", "y"])

    def test_predicate_returns_callable(self):
        pred = self.registry.predicate("important")
        assert callable(pred)

    def test_predicate_matches_tagged_message(self):
        self.registry.tag(self.msg, "important")
        pred = self.registry.predicate("important")
        assert pred(self.msg)

    def test_predicate_does_not_match_untagged(self):
        pred = self.registry.predicate("important")
        assert not pred(self.msg)

    def test_purge_removes_all_tags(self):
        self.registry.tag(self.msg, "a", "b")
        self.registry.purge(self.msg)
        assert self.registry.get_tags(self.msg) == frozenset()

    def test_purge_unknown_message_is_silent(self):
        other = make_message()
        self.registry.purge(other)  # should not raise

    def test_invalid_tag_empty_string_raises(self):
        with pytest.raises(InvalidTagError):
            self.registry.tag(self.msg, "")

    def test_invalid_tag_whitespace_raises(self):
        with pytest.raises(InvalidTagError):
            self.registry.tag(self.msg, "   ")

    def test_invalid_tag_non_string_raises(self):
        with pytest.raises(InvalidTagError):
            self.registry.tag(self.msg, 42)  # type: ignore
