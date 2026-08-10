"""Application ports for the PostgreSQL transactional outbox and producers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy.orm import Session

from atlas.eventing.contracts import DomainEvent


@dataclass(frozen=True, slots=True)
class ClaimedOutboxRecord:
    """Typed claimed outbox row ready for publication outside the claim TX."""

    event_id: UUID
    outbox_position: int
    event: DomainEvent
    publish_claim_token: str
    publish_lease_expires_at: datetime
    publish_attempts: int


class OutboxEnqueuer(Protocol):
    """Minimal producer-facing port: durable insert in the caller's transaction."""

    def enqueue(self, session: Session, event: DomainEvent) -> None:
        """Insert a typed domain event into the caller's transaction."""


class OutboxRepository(OutboxEnqueuer, Protocol):
    """Durable outbox persistence used by producers and the relay."""

    def claim_batch(
        self,
        session: Session,
        *,
        claimant_token: str,
        at: datetime,
        lease_expires_at: datetime,
        batch_size: int,
    ) -> list[ClaimedOutboxRecord]:
        """Claim a contiguous head-of-line batch (global ``outbox_position`` order)."""

    def mark_published(
        self,
        session: Session,
        *,
        event_id: UUID,
        claimant_token: str,
        at: datetime,
    ) -> bool:
        """Conditionally mark a claimed row published. False = ownership lost."""

    def release_failed_claim(
        self,
        session: Session,
        *,
        event_id: UUID,
        claimant_token: str,
        at: datetime,
        error_class: str,
    ) -> bool:
        """Conditionally release an owned claim and store a sanitized error class."""


class EventProducer(Protocol):
    """Delivery port for claimed envelopes. Kafka is deferred to Slice 13C."""

    def publish(self, event: DomainEvent) -> None:
        """Deliver one typed envelope. Raises on failure."""
