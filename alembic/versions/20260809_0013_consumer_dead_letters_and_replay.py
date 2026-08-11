"""Add consumer dead-letter storage and operator replay attempts (Slice 13C2B).

Revision ID: 20260809_0013
Revises: 20260809_0012
Create Date: 2026-08-11 03:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0013"
down_revision: str | None = "20260809_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SUPPORTED_EVENT_TYPES_SQL = (
    "'research_job.created', "
    "'research_job.completed', "
    "'research_job.failed', "
    "'research_job.awaiting_review', "
    "'research_job.retry_scheduled'"
)

# Mirrors atlas.consumer.errors.ALLOWED_FAILURE_CODES exactly. Keep both in
# sync: a new PoisonEventError subclass requires both a new failure_code
# entry in that module's mapping and a new value here.
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


def upgrade() -> None:
    op.create_table(
        "consumer_dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("consumer_id", sa.String(length=128), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=True),
        sa.Column("event_version", sa.Integer(), nullable=True),
        sa.Column("aggregate_type", sa.Text(), nullable=True),
        sa.Column("aggregate_id", sa.String(length=128), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=False),
        sa.Column("processing_attempt_count", sa.Integer(), nullable=False),
        sa.Column(
            "dead_letter_delivery_count",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_byte_length", sa.Integer(), nullable=False),
        sa.Column("retained_canonical_value", sa.Text(), nullable=True),
        sa.Column("retained_header_event_type", sa.Text(), nullable=True),
        sa.Column("retained_header_event_version", sa.Text(), nullable=True),
        sa.Column("retained_header_aggregate_type", sa.Text(), nullable=True),
        sa.Column(
            "replay_eligible",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "replay_state",
            sa.Text(),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("replay_claim_token", sa.Text(), nullable=True),
        sa.Column("replay_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "consumer_id = 'atlas.research-job-projection.v1'",
            name="ck_consumer_dead_letters_consumer_id",
        ),
        sa.CheckConstraint(
            "kafka_partition >= 0",
            name="ck_consumer_dead_letters_kafka_partition_nonneg",
        ),
        sa.CheckConstraint(
            "kafka_offset >= 0",
            name="ck_consumer_dead_letters_kafka_offset_nonneg",
        ),
        sa.CheckConstraint(
            f"event_type IS NULL OR event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_consumer_dead_letters_event_type",
        ),
        sa.CheckConstraint(
            f"failure_code IN ({_ALLOWED_FAILURE_CODES_SQL})",
            name="ck_consumer_dead_letters_failure_code",
        ),
        sa.CheckConstraint(
            "processing_attempt_count >= 1",
            name="ck_consumer_dead_letters_processing_attempt_count_positive",
        ),
        sa.CheckConstraint(
            "dead_letter_delivery_count >= 1",
            name="ck_consumer_dead_letters_dead_letter_delivery_count_positive",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letters_payload_sha256_format",
        ),
        sa.CheckConstraint(
            "payload_byte_length >= 0",
            name="ck_consumer_dead_letters_payload_byte_length_nonneg",
        ),
        sa.CheckConstraint(
            "(event_id IS NULL AND event_type IS NULL AND event_version IS NULL "
            "AND aggregate_type IS NULL AND aggregate_id IS NULL) OR "
            "(event_id IS NOT NULL AND event_type IS NOT NULL AND "
            "event_version IS NOT NULL AND aggregate_type IS NOT NULL AND "
            "aggregate_id IS NOT NULL)",
            name="ck_consumer_dead_letters_event_fields_pair",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint(
            # 32 KiB: kept numerically aligned to
            # atlas.consumer.deserialize.MAX_MESSAGE_VALUE_BYTES and
            # atlas.consumer.retention.MAX_RETAINED_CANONICAL_VALUE_BYTES --
            # these measure different things (raw Kafka record bytes vs. the
            # re-serialized canonical value), see that module's docstring.
            "octet_length(retained_canonical_value) <= 32768",
            name="ck_consumer_dead_letters_retained_value_bound",
        ),
        sa.CheckConstraint(
            f"replay_state IN ({_REPLAY_STATES_SQL})",
            name="ck_consumer_dead_letters_replay_state",
        ),
        sa.CheckConstraint(
            "(replay_claim_token IS NULL AND replay_lease_expires_at IS NULL) OR "
            "(replay_claim_token IS NOT NULL AND replay_lease_expires_at IS NOT NULL)",
            name="ck_consumer_dead_letters_replay_claim_pair",
        ),
        sa.CheckConstraint(
            "(replay_state = 'REPLAYING' AND replay_claim_token IS NOT NULL) OR "
            "(replay_state != 'REPLAYING' AND replay_claim_token IS NULL)",
            name="ck_consumer_dead_letters_replay_state_claim_consistency",
        ),
        sa.CheckConstraint(
            "replay_claim_token IS NULL OR replay_claim_token ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letters_replay_claim_token_format",
        ),
        sa.CheckConstraint(
            "first_failed_at <= last_failed_at",
            name="ck_consumer_dead_letters_failed_at_order",
        ),
        sa.CheckConstraint(
            "created_at <= updated_at",
            name="ck_consumer_dead_letters_created_updated_order",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_consumer_dead_letters"),
        sa.UniqueConstraint(
            "consumer_id",
            "kafka_partition",
            "kafka_offset",
            name="uq_consumer_dead_letters_identity",
        ),
    )
    op.create_index(
        "ix_consumer_dead_letters_replayable",
        "consumer_dead_letters",
        ["replay_state"],
        unique=False,
        postgresql_where=sa.text("replay_eligible = true"),
    )

    op.create_table(
        "consumer_dead_letter_replay_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dead_letter_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.String(length=128), nullable=False),
        sa.Column("operator_reason", sa.Text(), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("ownership_token", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            f"status IN ({_REPLAY_ATTEMPT_STATUSES_SQL})",
            name="ck_consumer_dead_letter_replay_attempts_status",
        ),
        sa.CheckConstraint(
            "(status = 'IN_PROGRESS' AND finished_at IS NULL) OR "
            "(status != 'IN_PROGRESS' AND finished_at IS NOT NULL)",
            name="ck_consumer_dead_letter_replay_attempts_status_finished_pair",
        ),
        sa.CheckConstraint(
            "length(trim(actor_id)) > 0",
            name="ck_consumer_dead_letter_replay_attempts_actor_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(operator_reason)) > 0 AND length(operator_reason) <= 512",
            name="ck_consumer_dead_letter_replay_attempts_operator_reason_bound",
        ),
        sa.CheckConstraint(
            "length(trim(idempotency_key)) > 0 AND length(idempotency_key) <= 256",
            name="ck_consumer_dead_letter_replay_attempts_idempotency_key_bound",
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letter_replay_attempts_fingerprint_format",
        ),
        sa.CheckConstraint(
            "ownership_token ~ '^[0-9a-f]{64}$'",
            name="ck_consumer_dead_letter_replay_attempts_token_format",
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR created_at <= finished_at",
            name="ck_consumer_dead_letter_replay_attempts_created_finished_order",
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_id"],
            ["consumer_dead_letters.id"],
            name="fk_consumer_dead_letter_replay_attempts_dead_letter_id",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_consumer_dead_letter_replay_attempts"),
        sa.UniqueConstraint(
            "dead_letter_id",
            "idempotency_key",
            name="uq_consumer_dead_letter_replay_attempts_key",
        ),
    )
    op.create_index(
        "ix_consumer_dead_letter_replay_attempts_ownership_token",
        "consumer_dead_letter_replay_attempts",
        ["dead_letter_id", "ownership_token", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_consumer_dead_letter_replay_attempts_ownership_token",
        table_name="consumer_dead_letter_replay_attempts",
    )
    op.drop_table("consumer_dead_letter_replay_attempts")
    op.drop_index(
        "ix_consumer_dead_letters_replayable", table_name="consumer_dead_letters"
    )
    op.drop_table("consumer_dead_letters")
