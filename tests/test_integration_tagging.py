"""Integration tests combining TagRegistry with MessageFilter and RetryRunner."""
import uuid

from retryq.backoff import ConstantBackoff
from retryq.filter import MessageFilter
from retryq.queue import RetryMessage, RetryQueue
from retryq.tagging import TagRegistry
from retryq.runner_tagging import TaggingRetryRunner


def make_message(payload: dict | None = None, max_attempts: int = 3) -> RetryMessage:
    return RetryMessage(
        id=str(uuid.uuid4()),
        payload=payload or {"value": 1},
        max_attempts=max_attempts,
    )


class TestTaggingIntegration:
    def setup_method(self):
        self.registry = TagRegistry()
        self.processed: list[RetryMessage] = []
        self.dead_letters: list[RetryMessage] = []

        self.runner = TaggingRetryRunner(
            queue=RetryQueue(),
            handler=self.processed.append,
            backoff=ConstantBackoff(delay=0),
            tag_registry=self.registry,
            dead_letter_callback=self.dead_letters.append,
        )

    def test_filter_by_tag_predicate(self):
        """MessageFilter can use a tag predicate to skip untagged messages."""
        msg_a = make_message()
        msg_b = make_message()
        self.runner.enqueue(msg_a, "vip")
        self.runner.enqueue(msg_b)  # no tags

        pred = self.registry.predicate("vip")
        filt = MessageFilter().add(pred)

        queue_snapshot = []
        while True:
            msg = self.runner._queue.dequeue()
            if msg is None:
                break
            if filt.matches(msg):
                queue_snapshot.append(msg)

        assert queue_snapshot == [msg_a]

    def test_tags_cleared_after_full_cycle(self):
        msg = make_message()
        self.runner.enqueue(msg, "transient")
        assert self.registry.has_tag(msg, "transient")
        self.runner.process_next()
        assert not self.registry.has_tag(msg, "transient")

    def test_multiple_messages_independent_tags(self):
        m1 = make_message()
        m2 = make_message()
        self.runner.enqueue(m1, "alpha")
        self.runner.enqueue(m2, "beta")

        assert self.registry.has_tag(m1, "alpha")
        assert not self.registry.has_tag(m1, "beta")
        assert self.registry.has_tag(m2, "beta")
        assert not self.registry.has_tag(m2, "alpha")

    def test_dead_lettered_message_tags_purged(self):
        failing_runner = TaggingRetryRunner(
            queue=RetryQueue(),
            handler=lambda m: (_ for _ in ()).throw(ValueError("boom")),
            backoff=ConstantBackoff(delay=0),
            tag_registry=self.registry,
            dead_letter_callback=self.dead_letters.append,
        )
        msg = make_message(max_attempts=1)
        failing_runner.enqueue(msg, "doomed")
        failing_runner.process_next()

        assert self.registry.get_tags(msg) == frozenset()
        assert msg in self.dead_letters
