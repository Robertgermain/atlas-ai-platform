"""Poll-decode-apply-commit orchestration for the business Kafka consumer."""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Protocol

from confluent_kafka import Message
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.deserialize import decode_message
from atlas.consumer.errors import ConsumerError
from atlas.consumer.ports import InboxOutcome, InboxRepository, ProjectionPort
from atlas.eventing.contracts import DomainEvent
from atlas.outbox.clock import Clock, utc_now
from atlas.persistence.db import session_scope


class ProcessOutcome(StrEnum):
    """Result of one ``ConsumerRunner.run_once()`` call."""

    NO_MESSAGE = "no_message"
    APPLIED = "applied"
    DUPLICATE = "duplicate"


class _ConsumerLike(Protocol):
    """The ``KafkaEventConsumer`` surface this orchestrator depends on.

    Typed against ``confluent_kafka.Message`` (not a duck-typed
    stand-in) so ``KafkaEventConsumer`` satisfies this structurally without
    a cast. Unit tests inject a fake and accept a ``# type: ignore[arg-type]``
    at the injection site instead of ever constructing a real ``Message``.
    """

    def poll(self, timeout_seconds: float) -> Message | None: ...
    def commit_message(self, message: Message) -> None: ...


def _require_int(value: int | None, *, context: str) -> int:
    """Narrow an optional Kafka position field, failing closed if absent.

    ``confluent_kafka``'s stub types ``Message.partition()``/``.offset()``
    as ``Optional[int]`` to cover error-carrying messages; this runner
    already rejects those via ``message.error() is not None`` before this
    is called, so ``None`` here would indicate an unexpected message shape
    rather than an ordinary condition.
    """
    if value is None:
        raise ConsumerError(context)
    return value


class ConsumerRunner:
    """Drives exactly one poll-decode-apply-commit cycle per ``run_once()`` call.

    Offset acknowledgment happens strictly after the PostgreSQL transaction
    (inbox record + business effect) commits -- ``commit_message`` is only
    ever reached once ``session_scope`` has returned normally. Any
    exception raised before that point (decode failure, lifecycle
    violation, database error) propagates out of ``run_once()`` with the
    Kafka offset never committed, so the record is redelivered on restart.
    """

    def __init__(
        self,
        *,
        consumer: _ConsumerLike,
        session_factory: sessionmaker[Session],
        inbox: InboxRepository,
        projection: ProjectionPort,
        consumer_id: str,
        poll_timeout_seconds: float,
        clock: Clock = utc_now,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._inbox = inbox
        self._projection = projection
        self._consumer_id = consumer_id
        self._poll_timeout_seconds = poll_timeout_seconds
        self._clock = clock

    def run_once(self) -> ProcessOutcome:
        message = self._consumer.poll(self._poll_timeout_seconds)
        if message is None:
            return ProcessOutcome.NO_MESSAGE
        if message.error() is not None:
            raise ConsumerError("PollReturnedBrokerError")

        event = decode_message(message)
        at = self._clock()
        partition = _require_int(message.partition(), context="MissingPartition")
        offset = _require_int(message.offset(), context="MissingOffset")

        def _apply_effect(session: Session, decoded_event: DomainEvent) -> None:
            self._projection.apply(session, decoded_event, at=at)

        with session_scope(self._session_factory) as session:
            outcome = self._inbox.record_and_apply(
                session,
                consumer_id=self._consumer_id,
                event=event,
                kafka_partition=partition,
                kafka_offset=offset,
                at=at,
                apply_effect=_apply_effect,
            )

        self._consumer.commit_message(message)
        return (
            ProcessOutcome.APPLIED
            if outcome is InboxOutcome.APPLIED
            else ProcessOutcome.DUPLICATE
        )


#: Type alias documenting the shutdown predicate ``python -m atlas.consumer`` polls.
ShutdownRequested = Callable[[], bool]
