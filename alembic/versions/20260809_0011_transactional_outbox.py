"""Add PostgreSQL transactional outbox for research-job domain events.

Revision ID: 20260809_0011
Revises: 20260809_0010
Create Date: 2026-08-10 16:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0011"
down_revision: str | None = "20260809_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "outbox_position",
            sa.BigInteger(),
            sa.Identity(always=True),
            nullable=False,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.String(length=128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_claim_token", sa.Text(), nullable=True),
        sa.Column(
            "publish_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "publish_attempts",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "last_publish_error_class",
            sa.String(length=128),
            nullable=True,
        ),
        sa.CheckConstraint(
            "event_version = 1",
            name="ck_outbox_events_event_version",
        ),
        sa.CheckConstraint(
            "publish_attempts >= 0",
            name="ck_outbox_events_publish_attempts_nonneg",
        ),
        sa.CheckConstraint(
            "("
            "publish_claim_token IS NULL AND publish_lease_expires_at IS NULL"
            ") OR ("
            "publish_claim_token IS NOT NULL AND publish_lease_expires_at IS NOT NULL"
            ")",
            name="ck_outbox_events_claim_pair",
        ),
        sa.CheckConstraint(
            "published_at IS NULL OR ("
            "publish_claim_token IS NULL AND publish_lease_expires_at IS NULL"
            ")",
            name="ck_outbox_events_published_clears_claim",
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'research_job.created', "
            "'research_job.completed', "
            "'research_job.failed', "
            "'research_job.awaiting_review', "
            "'research_job.retry_scheduled'"
            ")",
            name="ck_outbox_events_event_type",
        ),
        sa.CheckConstraint(
            "aggregate_type = 'research_job'",
            name="ck_outbox_events_aggregate_type",
        ),
        sa.CheckConstraint(
            "pg_column_size(payload) <= 16384",
            name="ck_outbox_events_payload_size",
        ),
        sa.CheckConstraint(
            "length(trim(aggregate_id)) > 0",
            name="ck_outbox_events_aggregate_id_nonempty",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_outbox_events"),
        sa.UniqueConstraint(
            "outbox_position",
            name="uq_outbox_events_outbox_position",
        ),
    )
    op.create_index(
        "ix_outbox_events_claimable_position",
        "outbox_events",
        ["outbox_position"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_events_aggregate_history",
        "outbox_events",
        ["aggregate_type", "aggregate_id", "outbox_position"],
        unique=False,
    )
    op.create_index(
        "ix_outbox_events_occurred_at",
        "outbox_events",
        ["occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_occurred_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_aggregate_history", table_name="outbox_events")
    op.drop_index(
        "ix_outbox_events_claimable_position",
        table_name="outbox_events",
    )
    op.drop_table("outbox_events")
