"""PostgreSQL-backed research-job lifecycle projection (Slice 13C2A).

The first business consumer: a durable, non-authoritative read model of
each research job's last-known lifecycle event, built entirely from the
reserved Kafka topic rather than by reading ``research_jobs`` directly.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from atlas.consumer.errors import LifecycleOrderViolationError
from atlas.eventing.contracts import DomainEvent
from atlas.persistence.models.consumer import ResearchJobEventProjectionModel

#: Once one of these is recorded for a research_job_id, the domain
#: guarantees no further event for that job is ever produced.
TERMINAL_EVENT_TYPES: frozenset[str] = frozenset(
    {"research_job.completed", "research_job.failed"}
)


class SqlAlchemyResearchJobProjectionRepository:
    """Upserts the last-known lifecycle event per ``research_job_id``."""

    def apply(self, session: Session, event: DomainEvent, *, at: datetime) -> None:
        """Apply one event's effect. Raises on an inconsistent lifecycle transition.

        The inbox's ``(consumer_id, event_id)`` dedup check always runs
        before this is called, so this is never invoked twice for the same
        ``event_id``. Therefore, if a projection row already records a
        terminal event, any event reaching this method is necessarily a
        *different* event -- exactly the ordering violation this method
        must fail closed on rather than silently overwrite.
        """
        research_job_id = event.aggregate_id
        existing = session.get(ResearchJobEventProjectionModel, research_job_id)

        if existing is not None and existing.last_event_type in TERMINAL_EVENT_TYPES:
            raise LifecycleOrderViolationError("TerminalProjectionAlreadyRecorded")

        if existing is None:
            session.add(
                ResearchJobEventProjectionModel(
                    research_job_id=research_job_id,
                    last_event_id=event.event_id,
                    last_event_type=event.event_type,
                    last_event_at=event.occurred_at,
                    updated_at=at,
                )
            )
        else:
            existing.last_event_id = event.event_id
            existing.last_event_type = event.event_type
            existing.last_event_at = event.occurred_at
            existing.updated_at = at
        session.flush()

    def get(
        self, session: Session, research_job_id: str
    ) -> ResearchJobEventProjectionModel | None:
        """Test/helper load by primary key."""
        return session.get(ResearchJobEventProjectionModel, research_job_id)
