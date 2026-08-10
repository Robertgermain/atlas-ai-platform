"""PostgreSQL repository for the transactional outbox."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, select, update
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import Session

from atlas.eventing.contracts import DomainEvent, parse_domain_event
from atlas.eventing.errors import DomainEventError
from atlas.eventing.serialization import serialize_payload
from atlas.outbox.errors import OutboxEnqueueError
from atlas.outbox.ports import ClaimedOutboxRecord
from atlas.persistence.models.outbox import OutboxEventModel


def _rebuild_event(model: OutboxEventModel) -> DomainEvent:
    return parse_domain_event(
        {
            "event_id": model.event_id,
            "event_version": model.event_version,
            "event_type": model.event_type,
            "aggregate_type": model.aggregate_type,
            "aggregate_id": model.aggregate_id,
            "occurred_at": model.occurred_at,
            "payload": model.payload,
        }
    )


class SqlAlchemyOutboxRepository:
    """Persist and claim typed domain events. Callers own the transaction."""

    def enqueue(self, session: Session, event: DomainEvent) -> None:
        """Insert a typed domain event; never accepts a raw public dict."""
        try:
            payload = serialize_payload(event)
            row = OutboxEventModel(
                event_id=event.event_id,
                event_type=event.event_type,
                event_version=int(event.event_version),
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                occurred_at=event.occurred_at,
                payload=payload,
                created_at=event.occurred_at,
                published_at=None,
                publish_claim_token=None,
                publish_lease_expires_at=None,
                publish_attempts=0,
                last_publish_error_class=None,
            )
            session.add(row)
            session.flush()
        except DomainEventError as exc:
            raise OutboxEnqueueError(exc.__class__.__name__) from None
        except (IntegrityError, StatementError) as exc:
            raise OutboxEnqueueError(exc.__class__.__name__) from None

    def claim_batch(
        self,
        session: Session,
        *,
        claimant_token: str,
        at: datetime,
        lease_expires_at: datetime,
        batch_size: int,
    ) -> list[ClaimedOutboxRecord]:
        """Claim a contiguous head-of-line batch for global ``outbox_position`` order.

        Ordering is **global** across the reserved topic
        (``atlas.research-job-events.v1``): the earliest unpublished row is a
        head-of-line barrier. If it is locked by another transaction or holds an
        unexpired publish lease, this returns an empty list and must not claim
        later positions. When the head is claimable, claims a bounded contiguous
        run of eligible unpublished rows in ascending ``outbox_position`` without
        skipping unavailable positions.

        ``publish_attempts`` counts claim attempts (incremented once per row
        successfully claimed here), not producer I/O calls.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if lease_expires_at <= at:
            raise ValueError("lease_expires_at must be later than at")

        head_peek = session.scalars(
            select(OutboxEventModel)
            .where(OutboxEventModel.published_at.is_(None))
            .order_by(OutboxEventModel.outbox_position.asc())
            .limit(1)
        ).first()
        if head_peek is None:
            return []

        locked_head = session.scalars(
            select(OutboxEventModel)
            .where(OutboxEventModel.event_id == head_peek.event_id)
            .with_for_update(skip_locked=True)
        ).first()
        if locked_head is None:
            # Head exists but is locked by another transaction — do not leapfrog.
            return []
        if locked_head.published_at is not None:
            return []
        if not self._is_claimable(locked_head, at=at):
            # Unexpired lease on the global head — HOL barrier.
            return []

        to_claim: list[OutboxEventModel] = [locked_head]
        while len(to_claim) < batch_size:
            after_position = int(to_claim[-1].outbox_position)
            next_peek = session.scalars(
                select(OutboxEventModel)
                .where(
                    OutboxEventModel.published_at.is_(None),
                    OutboxEventModel.outbox_position > after_position,
                )
                .order_by(OutboxEventModel.outbox_position.asc())
                .limit(1)
            ).first()
            if next_peek is None:
                break

            locked_next = session.scalars(
                select(OutboxEventModel)
                .where(OutboxEventModel.event_id == next_peek.event_id)
                .with_for_update(skip_locked=True)
            ).first()
            if locked_next is None:
                # Next unpublished position is locked — stop; do not skip it.
                break
            if locked_next.published_at is not None:
                break
            if not self._is_claimable(locked_next, at=at):
                # Contiguous barrier: do not skip a leased unpublished position.
                break
            to_claim.append(locked_next)

        claimed: list[ClaimedOutboxRecord] = []
        for row in to_claim:
            # ``publish_attempts`` counts claim attempts for publication, not
            # producer I/O calls. Incremented once per successful claim.
            row.publish_claim_token = claimant_token
            row.publish_lease_expires_at = lease_expires_at
            row.publish_attempts = int(row.publish_attempts) + 1
            claimed.append(
                ClaimedOutboxRecord(
                    event_id=row.event_id,
                    outbox_position=int(row.outbox_position),
                    event=_rebuild_event(row),
                    publish_claim_token=claimant_token,
                    publish_lease_expires_at=lease_expires_at,
                    publish_attempts=int(row.publish_attempts),
                )
            )
        if claimed:
            session.flush()
        return claimed

    @staticmethod
    def _is_claimable(row: OutboxEventModel, *, at: datetime) -> bool:
        if row.published_at is not None:
            return False
        if row.publish_claim_token is None:
            return True
        if row.publish_lease_expires_at is None:
            return True
        return row.publish_lease_expires_at <= at

    def mark_published(
        self,
        session: Session,
        *,
        event_id: UUID,
        claimant_token: str,
        at: datetime,
    ) -> bool:
        result = session.execute(
            update(OutboxEventModel)
            .where(
                and_(
                    OutboxEventModel.event_id == event_id,
                    OutboxEventModel.publish_claim_token == claimant_token,
                    OutboxEventModel.published_at.is_(None),
                    OutboxEventModel.publish_lease_expires_at.is_not(None),
                    OutboxEventModel.publish_lease_expires_at > at,
                )
            )
            .values(
                published_at=at,
                publish_claim_token=None,
                publish_lease_expires_at=None,
                last_publish_error_class=None,
            )
        )
        session.flush()
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def release_failed_claim(
        self,
        session: Session,
        *,
        event_id: UUID,
        claimant_token: str,
        at: datetime,
        error_class: str,
    ) -> bool:
        sanitized = error_class.strip()
        if not sanitized or any(ch.isspace() for ch in sanitized):
            sanitized = "PublishError"
        sanitized = sanitized[:128]
        result = session.execute(
            update(OutboxEventModel)
            .where(
                and_(
                    OutboxEventModel.event_id == event_id,
                    OutboxEventModel.publish_claim_token == claimant_token,
                    OutboxEventModel.published_at.is_(None),
                    OutboxEventModel.publish_lease_expires_at.is_not(None),
                    OutboxEventModel.publish_lease_expires_at > at,
                )
            )
            .values(
                publish_claim_token=None,
                publish_lease_expires_at=None,
                last_publish_error_class=sanitized,
            )
        )
        session.flush()
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def get_by_event_id(
        self, session: Session, event_id: UUID
    ) -> OutboxEventModel | None:
        """Test/helper load by primary key."""
        return session.get(OutboxEventModel, event_id)

    def list_for_aggregate(
        self,
        session: Session,
        *,
        aggregate_type: str,
        aggregate_id: str,
    ) -> list[OutboxEventModel]:
        """Test/helper ordered aggregate history."""
        statement = (
            select(OutboxEventModel)
            .where(
                OutboxEventModel.aggregate_type == aggregate_type,
                OutboxEventModel.aggregate_id == aggregate_id,
            )
            .order_by(OutboxEventModel.outbox_position.asc())
        )
        return list(session.scalars(statement).all())


def payload_as_mapping(payload: Any) -> dict[str, Any]:
    """Narrow JSONB payload for assertions (adapter-internal only)."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a mapping")
    return payload
