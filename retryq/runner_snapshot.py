"""SnapshotRetryRunner — RetryRunner with save/restore snapshot support."""

from typing import Optional

from retryq.backoff import BackoffStrategy
from retryq.hooks import HookRegistry
from retryq.metrics import RetryMetrics
from retryq.middleware import MiddlewareChain
from retryq.queue import RetryMessage, RetryQueue
from retryq.runner import RetryRunner
from retryq.snapshot import SnapshotError, load_snapshot, save_snapshot


class SnapshotRetryRunner(RetryRunner):
    """RetryRunner extended with snapshot persistence capabilities."""

    def __init__(
        self,
        queue: RetryQueue,
        middleware: Optional[MiddlewareChain] = None,
        hooks: Optional[HookRegistry] = None,
    ) -> None:
        super().__init__(queue=queue, middleware=middleware, hooks=hooks)
        self._last_snapshot: Optional[str] = None

    def save(self) -> str:
        """Capture current queue state as a JSON snapshot string.

        Stores the result internally and returns it.
        """
        self._last_snapshot = save_snapshot(self._queue)
        return self._last_snapshot

    def restore(self, snapshot: str) -> int:
        """Load messages from a snapshot string into the runner's queue.

        Returns the number of messages restored.
        Raises SnapshotError on invalid input.
        """
        count = load_snapshot(self._queue, snapshot)
        self._last_snapshot = snapshot
        return count

    @property
    def last_snapshot(self) -> Optional[str]:
        """Return the most recently saved or restored snapshot, or None."""
        return self._last_snapshot

    def checkpoint(self) -> str:
        """Alias for save(); semantically represents a checkpoint."""
        return self.save()
