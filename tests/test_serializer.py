"""Tests for retryq.serializer."""

import json
import pytest

from retryq.queue import RetryMessage
from retryq.serializer import SerializationError, deserialize, serialize


def make_message(**kwargs) -> RetryMessage:
    defaults = dict(id="msg-1", payload={"key": "value"}, max_attempts=3)
    defaults.update(kwargs)
    return RetryMessage(**defaults)


class TestSerialize:
    def test_returns_valid_json_string(self):
        msg = make_message()
        result = serialize(msg)
        data = json.loads(result)
        assert isinstance(data, dict)

    def test_contains_all_required_fields(self):
        msg = make_message()
        data = json.loads(serialize(msg))
        for field in ("id", "payload", "attempts", "max_attempts", "metadata"):
            assert field in data

    def test_preserves_id_and_payload(self):
        msg = make_message(id="abc-123", payload={"x": 42})
        data = json.loads(serialize(msg))
        assert data["id"] == "abc-123"
        assert data["payload"] == {"x": 42}

    def test_preserves_attempts_count(self):
        msg = make_message()
        msg.attempts = 2
        data = json.loads(serialize(msg))
        assert data["attempts"] == 2

    def test_preserves_metadata(self):
        msg = make_message(metadata={"source": "queue-a"})
        data = json.loads(serialize(msg))
        assert data["metadata"] == {"source": "queue-a"}

    def test_non_serializable_payload_raises(self):
        msg = make_message(payload=object())
        with pytest.raises(SerializationError):
            serialize(msg)


class TestDeserialize:
    def test_roundtrip_produces_equivalent_message(self):
        original = make_message(id="rt-1", payload={"n": 7}, max_attempts=5)
        original.attempts = 3
        restored = deserialize(serialize(original))
        assert restored.id == original.id
        assert restored.payload == original.payload
        assert restored.max_attempts == original.max_attempts
        assert restored.attempts == original.attempts
        assert restored.metadata == original.metadata

    def test_invalid_json_raises(self):
        with pytest.raises(SerializationError, match="Invalid JSON"):
            deserialize("not-json")

    def test_missing_field_raises(self):
        data = json.dumps({"id": "x", "payload": {}})
        with pytest.raises(SerializationError, match="Missing required fields"):
            deserialize(data)

    def test_exhausted_state_preserved(self):
        msg = make_message(max_attempts=2)
        msg.attempts = 2
        assert msg.exhausted()
        restored = deserialize(serialize(msg))
        assert restored.exhausted()

    def test_default_metadata_roundtrips(self):
        msg = make_message()
        restored = deserialize(serialize(msg))
        assert restored.metadata == {}
