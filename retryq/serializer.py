"""Serialization and deserialization utilities for RetryMessage."""

import json
from typing import Any, Dict

from retryq.queue import RetryMessage


class SerializationError(Exception):
    """Raised when a message cannot be serialized or deserialized."""


def serialize(message: RetryMessage) -> str:
    """Serialize a RetryMessage to a JSON string.

    Args:
        message: The RetryMessage instance to serialize.

    Returns:
        A JSON string representation of the message.

    Raises:
        SerializationError: If the message payload is not JSON-serializable.
    """
    try:
        data: Dict[str, Any] = {
            "id": message.id,
            "payload": message.payload,
            "attempts": message.attempts,
            "max_attempts": message.max_attempts,
            "metadata": message.metadata,
        }
        return json.dumps(data)
    except (TypeError, ValueError) as exc:
        raise SerializationError(
            f"Failed to serialize message {message.id!r}: {exc}"
        ) from exc


def deserialize(raw: str) -> RetryMessage:
    """Deserialize a JSON string back into a RetryMessage.

    Args:
        raw: A JSON string previously produced by :func:`serialize`.

    Returns:
        A reconstructed RetryMessage instance.

    Raises:
        SerializationError: If the string is not valid JSON or is missing
            required fields.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SerializationError(f"Invalid JSON: {exc}") from exc

    required = {"id", "payload", "attempts", "max_attempts", "metadata"}
    missing = required - data.keys()
    if missing:
        raise SerializationError(
            f"Missing required fields: {', '.join(sorted(missing))}"
        )

    msg = RetryMessage(
        id=data["id"],
        payload=data["payload"],
        max_attempts=data["max_attempts"],
        metadata=data["metadata"],
    )
    msg.attempts = data["attempts"]
    return msg
