"""Snapshot support for persisting and restoring RetryQueue state."""

import json
from typing import Any, Dict, List

from retryq.queue import RetryMessage, RetryQueue
from retryq.serializer import SerializationError, deserialize, serialize


class SnapshotError(Exception):
    """Raised when snapshot save or load fails."""

    def __str__(self) -> str:
        return f"SnapshotError: {self.args[0]}"


def save_snapshot(queue: RetryQueue) -> str:
    """Serialize the current queue contents to a JSON string.

    Returns a JSON string representing all pending messages.
    Raises SnapshotError if serialization fails.
    """
    try:
        entries: List[str] = []
        # Peek at internal deque without consuming
        for msg in list(queue._queue):
            entries.append(serialize(msg))
        return json.dumps(entries)
    except Exception as exc:
        raise SnapshotError(f"Failed to save snapshot: {exc}") from exc


def load_snapshot(queue: RetryQueue, snapshot: str) -> int:
    """Deserialize messages from a JSON snapshot string into the queue.

    Returns the number of messages loaded.
    Raises SnapshotError if deserialization fails.
    """
    if not isinstance(snapshot, str) or not snapshot.strip():
        raise SnapshotError("Snapshot must be a non-empty string")
    try:
        raw_entries: List[str] = json.loads(snapshot)
    except json.JSONDecodeError as exc:
        raise SnapshotError(f"Invalid snapshot JSON: {exc}") from exc

    if not isinstance(raw_entries, list):
        raise SnapshotError("Snapshot root must be a JSON array")

    count = 0
    for entry in raw_entries:
        try:
            msg = deserialize(entry)
            queue.enqueue(msg)
            count += 1
        except (SerializationError, Exception) as exc:
            raise SnapshotError(f"Failed to load message: {exc}") from exc

    return count
