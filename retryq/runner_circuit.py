"""RetryRunner variant with circuit-breaker protection per queue."""

from typing import Callable, Optional

from retryq.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from retryq.hooks import HookEvent, HookRegistry
from retryq.metrics import RetryMetrics
from retryq.queue import RetryMessage, RetryQueue


class CircuitBreakerRetryRunner:
    """Wraps a RetryQueue with a CircuitBreaker that halts processing on repeated failures."""

    def __init__(
        self,
        queue: RetryQueue,
        handler: Callable[[RetryMessage], bool],
        circuit_breaker: Optional[CircuitBreaker] = None,
        dead_letter_callback: Optional[Callable[[RetryMessage], None]] = None,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._circuit = circuit_breaker or CircuitBreaker(name="default")
        self._dead_letter_callback = dead_letter_callback
        self._metrics = RetryMetrics()
        self._hooks = HookRegistry()

    @property
    def metrics(self) -> RetryMetrics:
        return self._metrics

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    @property
    def circuit(self) -> CircuitBreaker:
        return self._circuit

    def enqueue(self, message: RetryMessage) -> None:
        self._queue.enqueue(message)
        self._metrics.record_enqueue()
        self._hooks.fire(HookEvent.ENQUEUED, message)

    def process_next(self) -> bool:
        """Process the next message, respecting the circuit breaker state.

        Raises CircuitBreakerOpen if the circuit is open.
        Returns False when the queue is empty.
        """
        self._circuit.check()

        message = self._queue.dequeue()
        if message is None:
            return False

        self._hooks.fire(HookEvent.RETRYING, message)
        success = self._handler(message)

        if success:
            self._circuit.record_success()
            self._metrics.record_success()
            self._hooks.fire(HookEvent.SUCCESS, message)
        else:
            self._circuit.record_failure()
            message.attempts += 1
            if message.exhausted():
                self._metrics.record_dead_letter()
                self._hooks.fire(HookEvent.DEAD_LETTERED, message)
                if self._dead_letter_callback:
                    self._dead_letter_callback(message)
            else:
                self._metrics.record_retry()
                self._queue.enqueue(message)

        return True
