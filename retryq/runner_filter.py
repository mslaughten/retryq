"""Extension of RetryRunner that supports message filtering before processing."""

from typing import Optional
from retryq.runner import RetryRunner
from retryq.filter import MessageFilter
from retryq.queue import RetryMessage


class FilteredRetryRunner(RetryRunner):
    """RetryRunner variant that skips messages not matching the active filter."""

    def __init__(self, *args, message_filter: Optional[MessageFilter] = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._filter: MessageFilter = message_filter or MessageFilter()

    @property
    def message_filter(self) -> MessageFilter:
        return self._filter

    def set_filter(self, message_filter: MessageFilter) -> None:
        """Replace the active filter."""
        self._filter = message_filter

    def process_next(self) -> bool:
        """Process the next message only if it passes the filter.

        Skipped messages are re-enqueued so they are not lost.
        Returns True if a message was processed, False if queue is empty.
        """
        message = self._queue.dequeue()
        if message is None:
            return False

        if not self._filter.matches(message):
            # Put it back so it can be processed later or by another runner
            self._queue.enqueue(message)
            return False

        self._dispatch(message)
        return True

    def process_all_matching(self) -> int:
        """Drain the queue, processing only messages that match the filter.

        Returns the count of messages actually processed (not skipped).
        """
        processed = 0
        skipped: list[RetryMessage] = []

        while True:
            message = self._queue.dequeue()
            if message is None:
                break
            if self._filter.matches(message):
                self._dispatch(message)
                processed += 1
            else:
                skipped.append(message)

        for msg in skipped:
            self._queue.enqueue(msg)

        return processed
