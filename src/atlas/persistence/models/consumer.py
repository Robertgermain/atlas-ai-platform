"""ORM mappings for the consumer inbox, lifecycle projection, and dead letters."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
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


# Mirrors atlas.consumer.errors.ALLOWED_FAILURE_CODES exactly (Slice 13C2B).
_ALLOWED_FAILURE_CODES_SQL = (
    "'missing_headers', "
    "'unexpected_headers_shape', "
    "'unexpected_header_key_type', "
    "'duplicate_header_key', "
    "'null_header_value', "
    "'undecodable_header_value', "
    "'unexpected_header_value_type', "
    "'unexpected_header_keys', "
    "'event_type_header_mismatch', "
    "'event_version_header_mismatch', "
    "'aggregate_type_header_mismatch', "
    "'missing_value', "
    "'value_too_large', "
    "'undecodable_value', "
    "'invalid_json', "
    "'value_not_an_object', "
    "'schema_validation_failed', "
    "'lifecycle_order_violation'"
)

_REPLAY_STATES_SQL = (
    "'PENDING', 'REPLAYING', 'REPLAY_FAILED', 'REPLAYED_APPLIED', 'REPLAYED_DUPLICATE'"
)

_REPLAY_ATTEMPT_STATUSES_SQL = (
    "'IN_PROGRESS', 'APPLIED', 'DUPLICATE', 'FAILED', 'LOST_OWNERSHIP'"
)


class ConsumerDeadLetterModel(Base):
    """Durable, permanent-poison-only dead-letter record (Slice 13C2B).

    Two payload-retention tiers (see ``atlas.consumer.retention``):
    Tier A (``replay_eligible=true``, currently only
    ``lifecycle_order_violation``) retains a bounded canonical
    reserialization plus the three validated fixed headers; Tier B (every
    other failure_code) retains only a SHA-256 hash and byte length of the
    untrusted original value -- never raw bytes.
    """

    __tablename__ = "consumer_dead_letters"
    __table_args__ = (
        CheckConstraint(
            "consumer_id = 'atlas.research-job-projection.v1'",
            name="ck_consumer_dead_letters_consumer_id",
        ),
        CheckConstraint(
            "kafka_partition >= 0",
            name="ck_consumer_dead_letters_kafka_partition_nonneg",
        ),
        CheckConstraint(
            "kafka_offset >= 0",
            name="ck_consumer_dead_letters_kafka_offset_nonneg",
        ),
        CheckConstraint(
            f"event_type IS NULL OR event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_consumer_dead_letters_event_type",
        ),
        CheckConstraint(
            f"failure_code IN ({_ALLOWED_FAILURE_CODES_SQL})",
            name="ck_consumer_dead_letters_failure_code",
        ),
        CheckConstraint(
            "processing_attempt_count >= 1",
            name="ck_consumer_dead_letters_processing_attempt_count_positive",
        ),
        CheckConstraint(
            "dead_letter_delivery_count >= 1",
            name="ck_consumer_dead_letters_dead_letter_delivery_count_positive",
        ),
        CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letters_payload_sha256_format",
        ),
        CheckConstraint(
            "payload_byte_length >= 0",
            name="ck_consumer_dead_letters_payload_byte_length_nonneg",
        ),
        CheckConstraint(
            "(event_id IS NULL AND event_type IS NULL AND event_version IS NULL "
            "AND aggregate_type IS NULL AND aggregate_id IS NULL) OR "
            "(event_id IS NOT NULL AND event_type IS NOT NULL AND "
            "event_version IS NOT NULL AND aggregate_type IS NOT NULL AND "
            "aggregate_id IS NOT NULL)",
            name="ck_consumer_dead_letters_event_fields_pair",
        ),
        CheckConstraint(
            "(replay_eligible = false AND retained_canonical_value IS NULL AND "
            "retained_header_event_type IS NULL AND "
            "retained_header_event_version IS NULL AND "
            "retained_header_aggregate_type IS NULL) OR "
            "(replay_eligible = true AND retained_canonical_value IS NOT NULL AND "
            "retained_header_event_type IS NOT NULL AND "
            "retained_header_event_version IS NOT NULL AND "
            "retained_header_aggregate_type IS NOT NULL AND event_id IS NOT NULL)",
            name="ck_consumer_dead_letters_replay_eligible_tier",
        ),
        CheckConstraint(
            # Kept numerically aligned to
            # atlas.consumer.deserialize.MAX_MESSAGE_VALUE_BYTES and
            # atlas.consumer.retention.MAX_RETAINED_CANONICAL_VALUE_BYTES --
            # see that module's docstring for why these measure different
            # things (raw Kafka record bytes vs. the re-serialized
            # canonical value) despite sharing the same 32 KiB bound.
            "octet_length(retained_canonical_value) <= 32768",
            name="ck_consumer_dead_letters_retained_value_bound",
        ),
        CheckConstraint(
            f"replay_state IN ({_REPLAY_STATES_SQL})",
            name="ck_consumer_dead_letters_replay_state",
        ),
        CheckConstraint(
            "(replay_claim_token IS NULL AND replay_lease_expires_at IS NULL) OR "
            "(replay_claim_token IS NOT NULL AND replay_lease_expires_at IS NOT NULL)",
            name="ck_consumer_dead_letters_replay_claim_pair",
        ),
        CheckConstraint(
            "(replay_state = 'REPLAYING' AND replay_claim_token IS NOT NULL) OR "
            "(replay_state != 'REPLAYING' AND replay_claim_token IS NULL)",
            name="ck_consumer_dead_letters_replay_state_claim_consistency",
        ),
        CheckConstraint(
            "replay_claim_token IS NULL OR replay_claim_token ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letters_replay_claim_token_format",
        ),
        CheckConstraint(
            "first_failed_at <= last_failed_at",
            name="ck_consumer_dead_letters_failed_at_order",
        ),
        CheckConstraint(
            "created_at <= updated_at",
            name="ck_consumer_dead_letters_created_updated_order",
        ),
        UniqueConstraint(
            "consumer_id",
            "kafka_partition",
            "kafka_offset",
            name="uq_consumer_dead_letters_identity",
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    consumer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer(), nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger(), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    event_type: Mapped[str | None] = mapped_column(Text(), nullable=True)
    event_version: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    aggregate_type: Mapped[str | None] = mapped_column(Text(), nullable=True)
    aggregate_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    failure_code: Mapped[str] = mapped_column(Text(), nullable=False)
    processing_attempt_count: Mapped[int] = mapped_column(Integer(), nullable=False)
    dead_letter_delivery_count: Mapped[int] = mapped_column(
        Integer(), nullable=False, default=1
    )
    first_failed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_failed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_byte_length: Mapped[int] = mapped_column(Integer(), nullable=False)
    retained_canonical_value: Mapped[str | None] = mapped_column(Text(), nullable=True)
    retained_header_event_type: Mapped[str | None] = mapped_column(
        Text(), nullable=True
    )
    retained_header_event_version: Mapped[str | None] = mapped_column(
        Text(), nullable=True
    )
    retained_header_aggregate_type: Mapped[str | None] = mapped_column(
        Text(), nullable=True
    )
    replay_eligible: Mapped[bool] = mapped_column(
        Boolean(), nullable=False, default=False
    )
    replay_state: Mapped[str] = mapped_column(Text(), nullable=False, default="PENDING")
    replay_claim_token: Mapped[str | None] = mapped_column(Text(), nullable=True)
    replay_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ConsumerDeadLetterReplayAttemptModel(Base):
    """Append-in-spirit operator replay attempt record (Slice 13C2B).

    Every substantive field except ``status``/``finished_at`` is immutable
    after insert -- see ``atlas.persistence.repositories.consumer_dead_letter``
    for why a reclaim always creates a *new* row rather than mutating an
    existing one's ownership.
    """

    __tablename__ = "consumer_dead_letter_replay_attempts"
    __table_args__ = (
        CheckConstraint(
            f"status IN ({_REPLAY_ATTEMPT_STATUSES_SQL})",
            name="ck_consumer_dead_letter_replay_attempts_status",
        ),
        CheckConstraint(
            "(status = 'IN_PROGRESS' AND finished_at IS NULL) OR "
            "(status != 'IN_PROGRESS' AND finished_at IS NOT NULL)",
            name="ck_consumer_dead_letter_replay_attempts_status_finished_pair",
        ),
        CheckConstraint(
            "length(trim(actor_id)) > 0",
            name="ck_consumer_dead_letter_replay_attempts_actor_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(operator_reason)) > 0 AND length(operator_reason) <= 512",
            name="ck_consumer_dead_letter_replay_attempts_operator_reason_bound",
        ),
        CheckConstraint(
            "length(trim(idempotency_key)) > 0 AND length(idempotency_key) <= 256",
            name="ck_consumer_dead_letter_replay_attempts_idempotency_key_bound",
        ),
        CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letter_replay_attempts_fingerprint_format",
        ),
        CheckConstraint(
            "ownership_token ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letter_replay_attempts_token_format",
        ),
        CheckConstraint(
            "finished_at IS NULL OR created_at <= finished_at",
            name="ck_consumer_dead_letter_replay_attempts_created_finished_order",
        ),
        UniqueConstraint(
            "dead_letter_id",
            "idempotency_key",
            name="uq_consumer_dead_letter_replay_attempts_key",
        ),
        Index(
            "ix_consumer_dead_letter_replay_attempts_ownership_token",
            "dead_letter_id",
            "ownership_token",
            "status",
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    dead_letter_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "consumer_dead_letters.id",
            name="fk_consumer_dead_letter_replay_attempts_dead_letter_id",
        ),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(Text(), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_reason: Mapped[str] = mapped_column(Text(), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    ownership_token: Mapped[str] = mapped_column(Text(), nullable=False)
    status: Mapped[str] = mapped_column(Text(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
