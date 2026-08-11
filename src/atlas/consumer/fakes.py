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
from uuid import UUID, uuid4

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from atlas.consumer.ports import ApplyEffect, InboxOutcome
from atlas.consumer.retention import DeadLetterRetention
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
    """Returns queued messages from ``poll()`` and records commits.

    ``raise_on_commit`` always raises on every ``commit_message()`` call
    (for "commit never succeeds" tests). ``raise_on_commit_before_success``
    instead consumes one exception per call from a queue, then falls back to
    recording a successful commit -- for "commit fails N times then
    succeeds" retry tests. The two are mutually exclusive.
    """

    def __init__(self, messages: list[FakeKafkaMessage] | None = None) -> None:
        self._messages = list(messages or [])
        self.poll_calls = 0
        self.committed: list[FakeKafkaMessage] = []
        self.commit_calls = 0
        self.raise_on_poll: Exception | None = None
        self.raise_on_commit: Exception | None = None
        self.raise_on_commit_before_success: list[Exception] = []

    def poll(self, timeout_seconds: float) -> FakeKafkaMessage | None:
        del timeout_seconds
        self.poll_calls += 1
        if self.raise_on_poll is not None:
            raise self.raise_on_poll
        if not self._messages:
            return None
        return self._messages.pop(0)

    def commit_message(self, message: FakeKafkaMessage) -> None:
        self.commit_calls += 1
        if self.raise_on_commit is not None:
            raise self.raise_on_commit
        if self.raise_on_commit_before_success:
            raise self.raise_on_commit_before_success.pop(0)
        self.committed.append(message)


class InMemoryInboxRepository:
    """Real (not scripted) in-memory dedup boundary for runner unit tests.

    ``raise_before_success`` lets retry tests inject a queue of exceptions
    (e.g. a synthetic transient ``DBAPIError``) consumed one per call before
    falling back to genuine dedup/apply behavior -- this fake never invents
    its own retry logic, it only controls what the caller (``ConsumerRunner``)
    observes.
    """

    def __init__(self, *, raise_before_success: list[Exception] | None = None) -> None:
        self._seen: set[tuple[str, object]] = set()
        self.applied_effects: list[DomainEvent] = []
        self._raise_queue: list[Exception] = list(raise_before_success or [])
        self.call_count = 0

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
        self.call_count += 1
        if self._raise_queue:
            raise self._raise_queue.pop(0)
        key = (consumer_id, event.event_id)
        if key in self._seen:
            return InboxOutcome.DUPLICATE
        apply_effect(session, event)
        self.applied_effects.append(event)
        self._seen.add(key)
        return InboxOutcome.APPLIED


class RecordingProjection:
    """Records every applied event without touching the database.

    ``raise_on_apply`` may be a single exception (raised on every call) or a
    list consumed one exception per call, then falling back to recording --
    useful for "fails N times then succeeds" retry tests.
    """

    def __init__(
        self,
        *,
        raise_on_apply: Exception | list[Exception] | None = None,
    ) -> None:
        self.applied: list[DomainEvent] = []
        self._raise_queue: list[Exception] = (
            list(raise_on_apply)
            if isinstance(raise_on_apply, list)
            else ([raise_on_apply] * 10_000 if raise_on_apply is not None else [])
        )
        self._single = not isinstance(raise_on_apply, list)

    def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
        del session, at
        if self._raise_queue:
            raise self._raise_queue.pop(0)
        self.applied.append(event)


class InMemoryDeadLetterRepository:
    """Real (not scripted) in-memory dead-letter store for runner unit tests."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, int, int], dict[str, object]] = {}

    def upsert(
        self,
        session: Session,
        *,
        consumer_id: str,
        kafka_partition: int,
        kafka_offset: int,
        failure_code: str,
        processing_attempt_count: int,
        at: datetime,
        retention: DeadLetterRetention,
    ) -> UUID:
        del session
        key = (consumer_id, kafka_partition, kafka_offset)
        existing = self.rows.get(key)
        if existing is not None:
            previous_count = existing["dead_letter_delivery_count"]
            assert isinstance(previous_count, int)
            existing["dead_letter_delivery_count"] = previous_count + 1
            existing["last_failed_at"] = at
            existing_id = existing["id"]
            assert isinstance(existing_id, UUID)
            return existing_id
        row_id = uuid4()
        self.rows[key] = {
            "id": row_id,
            "failure_code": failure_code,
            "processing_attempt_count": processing_attempt_count,
            "dead_letter_delivery_count": 1,
            "first_failed_at": at,
            "last_failed_at": at,
            "retention": retention,
        }
        return row_id


class _FakeDriverError(Exception):
    """A network-free stand-in for a psycopg3 driver exception's ``sqlstate``."""

    def __init__(self, *, sqlstate: str | None) -> None:
        super().__init__("synthetic-driver-error")
        self.sqlstate = sqlstate


def build_dbapi_error(
    *, sqlstate: str | None = None, connection_invalidated: bool = False
) -> DBAPIError:
    """Build a network-free, accurately-shaped ``DBAPIError`` for classifier tests.

    Mirrors psycopg3's convention of exposing SQLSTATE via ``orig.sqlstate``
    (see ``atlas.consumer.db_classify.classify_database_error``). Never a
    real driver exception -- used only to exercise classification/retry
    logic without a real PostgreSQL error.
    """
    return DBAPIError(
        "SELECT 1",
        {},
        _FakeDriverError(sqlstate=sqlstate),
        connection_invalidated=connection_invalidated,
    )


#: Type alias documenting the on-before-poll test hook shape some tests use.
BeforePoll = Callable[[], None]
