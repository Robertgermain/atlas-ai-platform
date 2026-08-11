"""Application ports for the consumer inbox and business-effect application."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from sqlalchemy.orm import Session

from atlas.consumer.retention import DeadLetterRetention
from atlas.eventing.contracts import DomainEvent


class InboxOutcome(StrEnum):
    """Result of one ``InboxRepository.record_and_apply()`` call."""

    APPLIED = "applied"
    DUPLICATE = "duplicate"


#: Applies one event's business effect inside the same transaction as the
#: inbox dedup record. Must raise on failure -- never partially apply.
ApplyEffect = Callable[[Session, DomainEvent], None]


class InboxRepository(Protocol):
    """Durable deduplication boundary for exactly one consumer identity.

    ``record_and_apply`` is the sole write path: it durably records that
    ``(consumer_id, event.event_id)`` has been handled and invokes
    ``apply_effect`` in the same PostgreSQL transaction, so both commit or
    neither does. Callers must not call ``apply_effect`` themselves.
    """

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
        """Record dedup + apply the effect atomically, or detect a duplicate."""


class ProjectionPort(Protocol):
    """A durable, non-authoritative read model updated by one business consumer."""

    def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
        """Apply one event's effect. Raises on an inconsistent lifecycle transition."""


class DeadLetterRepository(Protocol):
    """Durable, permanent-poison-only dead-letter storage (Slice 13C2B).

    ``upsert`` is the sole write path used by ``ConsumerRunner``: the
    ``(consumer_id, kafka_partition, kafka_offset)`` uniqueness boundary
    means a redelivery of an already-dead-lettered record increments only
    ``dead_letter_delivery_count`` -- ``processing_attempt_count`` and every
    retained field keep their original first-insert values.
    """

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
        """Insert a new dead-letter row, or bump delivery count on conflict.

        Returns the row's id (existing or newly created).
        """
