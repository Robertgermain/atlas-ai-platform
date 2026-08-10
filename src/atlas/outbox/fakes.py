"""In-memory event producer for Slice 13B relay tests (no Kafka)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from sqlalchemy.orm import Session

from atlas.eventing.contracts import DomainEvent
from atlas.outbox.clock import ControllableClock


class FakeEventProducer:
    """Records published envelopes and can be configured to fail."""

    def __init__(
        self,
        *,
        fail_on_event_ids: set[object] | None = None,
        fail_next: int = 0,
        failure_exc: Exception | None = None,
        on_before_publish: Callable[[DomainEvent], None] | None = None,
    ) -> None:
        self.published: list[DomainEvent] = []
        self.attempts: list[DomainEvent] = []
        self._fail_on_event_ids = set(fail_on_event_ids or ())
        self._fail_next = fail_next
        self._failure_exc = failure_exc or RuntimeError("FakeProducerFailure")
        self._on_before_publish = on_before_publish

    def publish(self, event: DomainEvent) -> None:
        self.attempts.append(event)
        if self._on_before_publish is not None:
            self._on_before_publish(event)
        if self._fail_next > 0:
            self._fail_next -= 1
            raise self._failure_exc
        if event.event_id in self._fail_on_event_ids:
            raise self._failure_exc
        self.published.append(event)


class ClockAdvancingProducer:
    """Advance a controllable clock during publish to simulate slow producer I/O."""

    def __init__(
        self,
        *,
        clock: ControllableClock,
        advance_by: timedelta,
        inner: FakeEventProducer | None = None,
        fail_on_event_ids: set[object] | None = None,
    ) -> None:
        self._clock = clock
        self._advance_by = advance_by
        self._inner = inner or FakeEventProducer(fail_on_event_ids=fail_on_event_ids)

    @property
    def published(self) -> list[DomainEvent]:
        return self._inner.published

    @property
    def attempts(self) -> list[DomainEvent]:
        return self._inner.attempts

    def publish(self, event: DomainEvent) -> None:
        self._clock.advance(self._advance_by)
        self._inner.publish(event)


class RecordingOutbox:
    """Unit-test outbox that records enqueue calls without persistence."""

    def __init__(self, *, fail_enqueue: bool = False) -> None:
        self.events: list[DomainEvent] = []
        self.fail_enqueue = fail_enqueue

    def enqueue(self, session: Session, event: DomainEvent) -> None:
        del session
        if self.fail_enqueue:
            from atlas.outbox.errors import OutboxEnqueueError

            raise OutboxEnqueueError("ForcedEnqueueFailure")
        self.events.append(event)
