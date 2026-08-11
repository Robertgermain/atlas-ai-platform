"""Application ports for the consumer inbox and business-effect application."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from sqlalchemy.orm import Session

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
