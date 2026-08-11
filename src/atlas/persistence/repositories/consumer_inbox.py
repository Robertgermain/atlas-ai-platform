"""PostgreSQL-backed durable dedup boundary for a single business consumer."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas.consumer.ports import ApplyEffect, InboxOutcome
from atlas.eventing.contracts import DomainEvent
from atlas.persistence.models.consumer import ConsumerInboxModel


class ConsumerInboxConflictError(RuntimeError):
    """Raised when a concurrent write violates the inbox's uniqueness boundary.

    Not expected in normal operation: Kafka's consumer-group protocol
    guarantees a single active reader of the topic's one partition. This
    surfaces the anomaly loudly instead of masking it as an ordinary
    duplicate.
    """


class SqlAlchemyInboxRepository:
    """Records ``(consumer_id, event_id)`` and applies the effect atomically.

    ``record_and_apply`` checks for a prior record first (safe under a
    single active consumer instance per Kafka's own consumer-group
    guarantee) and relies on the ``(consumer_id, event_id)`` primary key as
    the actual correctness boundary: a concurrent duplicate insert -- e.g.
    a brief rebalance overlap -- raises ``IntegrityError``, which is
    surfaced as a typed failure rather than silently ignored, so it is
    never mistaken for an ordinary duplicate.
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
        existing = session.get(ConsumerInboxModel, (consumer_id, event.event_id))
        if existing is not None:
            return InboxOutcome.DUPLICATE

        apply_effect(session, event)

        row = ConsumerInboxModel(
            consumer_id=consumer_id,
            event_id=event.event_id,
            event_type=event.event_type,
            received_at=at,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
        )
        session.add(row)
        try:
            session.flush()
        except IntegrityError as exc:
            raise ConsumerInboxConflictError("UnexpectedConcurrentInboxWrite") from exc
        return InboxOutcome.APPLIED
