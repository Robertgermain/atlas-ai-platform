"""ORM mapping for the PostgreSQL transactional outbox."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
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


class OutboxEventModel(Base):
    """Durable outbox row. ``outbox_position`` is the authoritative publish order."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "event_version = 1",
            name="ck_outbox_events_event_version",
        ),
        CheckConstraint(
            "publish_attempts >= 0",
            name="ck_outbox_events_publish_attempts_nonneg",
        ),
        CheckConstraint(
            "("
            "publish_claim_token IS NULL AND publish_lease_expires_at IS NULL"
            ") OR ("
            "publish_claim_token IS NOT NULL AND publish_lease_expires_at IS NOT NULL"
            ")",
            name="ck_outbox_events_claim_pair",
        ),
        CheckConstraint(
            "published_at IS NULL OR ("
            "publish_claim_token IS NULL AND publish_lease_expires_at IS NULL"
            ")",
            name="ck_outbox_events_published_clears_claim",
        ),
        CheckConstraint(
            f"event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_outbox_events_event_type",
        ),
        CheckConstraint(
            "aggregate_type = 'research_job'",
            name="ck_outbox_events_aggregate_type",
        ),
        CheckConstraint(
            "pg_column_size(payload) <= 16384",
            name="ck_outbox_events_payload_size",
        ),
        CheckConstraint(
            "length(trim(aggregate_id)) > 0",
            name="ck_outbox_events_aggregate_id_nonempty",
        ),
        CheckConstraint(
            "traceparent IS NULL OR ("
            "traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' AND "
            "traceparent !~ '^00-0{32}-[0-9a-f]{16}-[0-9a-f]{2}$' AND "
            "traceparent !~ '^00-[0-9a-f]{32}-0{16}-[0-9a-f]{2}$'"
            ")",
            name="ck_outbox_events_traceparent_format",
        ),
        UniqueConstraint(
            "outbox_position",
            name="uq_outbox_events_outbox_position",
        ),
        Index(
            "ix_outbox_events_claimable_position",
            "outbox_position",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "ix_outbox_events_aggregate_history",
            "aggregate_type",
            "aggregate_id",
            "outbox_position",
        ),
        Index(
            "ix_outbox_events_occurred_at",
            "occurred_at",
        ),
    )

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    outbox_position: Mapped[int] = mapped_column(
        BigInteger(),
        Identity(always=True),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text(), nullable=False)
    event_version: Mapped[int] = mapped_column(Integer(), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(Text(), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB(none_as_null=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    publish_claim_token: Mapped[str | None] = mapped_column(Text(), nullable=True)
    publish_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    publish_attempts: Mapped[int] = mapped_column(
        Integer(), nullable=False, default=0, server_default="0"
    )
    last_publish_error_class: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    #: The W3C ``traceparent`` active at insert time (Slice 15A3). Persistence-
    #: only: never exposed through any public API or domain model. The relay
    #: reads this once per row to start an ``outbox.publish`` child span; it
    #: never forwards this value unchanged into Kafka headers -- see
    #: ``atlas.outbox.relay``.
    traceparent: Mapped[str | None] = mapped_column(String(55), nullable=True)
