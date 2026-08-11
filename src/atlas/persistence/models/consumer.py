"""ORM mappings for the consumer inbox and research-job lifecycle projection."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base

_SUPPORTED_EVENT_TYPES_SQL = (
    "'research_job.created', "
    "'research_job.completed', "
    "'research_job.failed', "
    "'research_job.awaiting_review', "
    "'research_job.retry_scheduled'"
)

# A single fixed value today (atlas.consumer.identity.
# RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1). Extend to an IN-list via a
# future migration when a second business consumer is added, mirroring how
# outbox_events.event_type is extended when a new event type is added.
_ALLOWED_CONSUMER_ID_SQL = "'atlas.research-job-projection.v1'"


class ConsumerInboxModel(Base):
    """Durable per-consumer dedup record: one row per handled ``event_id``."""

    __tablename__ = "consumer_inbox"
    __table_args__ = (
        CheckConstraint(
            f"consumer_id = {_ALLOWED_CONSUMER_ID_SQL}",
            name="ck_consumer_inbox_consumer_id",
        ),
        CheckConstraint(
            f"event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_consumer_inbox_event_type",
        ),
        CheckConstraint(
            "kafka_partition >= 0",
            name="ck_consumer_inbox_kafka_partition_nonneg",
        ),
        CheckConstraint(
            "kafka_offset >= 0",
            name="ck_consumer_inbox_kafka_offset_nonneg",
        ),
    )

    consumer_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(Text(), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    kafka_partition: Mapped[int] = mapped_column(Integer(), nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger(), nullable=False)


class ResearchJobEventProjectionModel(Base):
    """Durable, non-authoritative last-known research-job lifecycle state.

    Populated exclusively from Kafka events (never read by the
    authoritative ``research_jobs`` write path); demonstrates a real,
    independent read model built from the reserved event topic.
    """

    __tablename__ = "research_job_event_projection"
    __table_args__ = (
        CheckConstraint(
            f"last_event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_research_job_event_projection_event_type",
        ),
    )

    research_job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    last_event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    last_event_type: Mapped[str] = mapped_column(Text(), nullable=False)
    last_event_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
