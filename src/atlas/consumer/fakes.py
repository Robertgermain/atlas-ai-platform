"""Test-only fakes for the business Kafka consumer.

Never imported by production application code. ``FakeKafkaMessage`` and
``build_kafka_message_for_event`` reproduce exactly the header/value wire
format ``KafkaEventProducer`` writes so decode/runner tests exercise the
real encoding without a broker. ``InMemoryInboxRepository`` is a genuine
(not scripted) implementation of the dedup contract so tests exercise real
branching behavior, not a canned return value.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from sqlalchemy.orm import Session

from atlas.consumer.ports import ApplyEffect, InboxOutcome
from atlas.eventing.contracts import DomainEvent
from atlas.eventing.serialization import serialize_domain_event


class FakeKafkaMessage:
    """A ``confluent_kafka.Message``-shaped double for network-free tests."""

    def __init__(
        self,
        *,
        value: bytes | None,
        headers: list[tuple[str, bytes]] | None,
        partition: int = 0,
        offset: int = 0,
        error: object | None = None,
    ) -> None:
        self.raw_value = value
        self.raw_headers = headers
        self.raw_partition = partition
        self.raw_offset = offset
        self.raw_error = error

    def value(self) -> bytes | None:
        return self.raw_value

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self.raw_headers

    def partition(self) -> int:
        return self.raw_partition

    def offset(self) -> int:
        return self.raw_offset

    def error(self) -> object | None:
        return self.raw_error


def build_kafka_message_for_event(
    event: DomainEvent,
    *,
    partition: int = 0,
    offset: int = 0,
) -> FakeKafkaMessage:
    """Build a message reproducing exactly what ``KafkaEventProducer`` publishes."""
    value = serialize_domain_event(event).encode("utf-8")
    headers = [
        ("event_type", event.event_type.encode("utf-8")),
        ("event_version", str(int(event.event_version)).encode("utf-8")),
        ("aggregate_type", event.aggregate_type.encode("utf-8")),
    ]
    return FakeKafkaMessage(
        value=value, headers=headers, partition=partition, offset=offset
    )


class FakeKafkaConsumer:
    """Returns queued messages from ``poll()`` and records commits."""

    def __init__(self, messages: list[FakeKafkaMessage] | None = None) -> None:
        self._messages = list(messages or [])
        self.poll_calls = 0
        self.committed: list[FakeKafkaMessage] = []
        self.raise_on_poll: Exception | None = None
        self.raise_on_commit: Exception | None = None

    def poll(self, timeout_seconds: float) -> FakeKafkaMessage | None:
        del timeout_seconds
        self.poll_calls += 1
        if self.raise_on_poll is not None:
            raise self.raise_on_poll
        if not self._messages:
            return None
        return self._messages.pop(0)

    def commit_message(self, message: FakeKafkaMessage) -> None:
        if self.raise_on_commit is not None:
            raise self.raise_on_commit
        self.committed.append(message)


class InMemoryInboxRepository:
    """Real (not scripted) in-memory dedup boundary for runner unit tests."""

    def __init__(self) -> None:
        self._seen: set[tuple[str, object]] = set()
        self.applied_effects: list[DomainEvent] = []

    def record_and_apply(
        self,
        session: Session,
        *,
        consumer_id: str,
        event: DomainEvent,
        kafka_partition: int,
        kafka_offset: int,
        at: datetime,
        apply_effect: ApplyEffect,
    ) -> InboxOutcome:
        del kafka_partition, kafka_offset, at
        key = (consumer_id, event.event_id)
        if key in self._seen:
            return InboxOutcome.DUPLICATE
        apply_effect(session, event)
        self.applied_effects.append(event)
        self._seen.add(key)
        return InboxOutcome.APPLIED


class RecordingProjection:
    """Records every applied event without touching the database."""

    def __init__(self, *, raise_on_apply: Exception | None = None) -> None:
        self.applied: list[DomainEvent] = []
        self._raise_on_apply = raise_on_apply

    def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
        del session, at
        if self._raise_on_apply is not None:
            raise self._raise_on_apply
        self.applied.append(event)


#: Type alias documenting the on-before-poll test hook shape some tests use.
BeforePoll = Callable[[], None]
