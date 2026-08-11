"""Add consumer inbox and research-job lifecycle projection (Slice 13C2A).

Revision ID: 20260809_0012
Revises: 20260809_0011
Create Date: 2026-08-10 22:30:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0012"
down_revision: str | None = "20260809_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SUPPORTED_EVENT_TYPES_SQL = (
    "'research_job.created', "
    "'research_job.completed', "
    "'research_job.failed', "
    "'research_job.awaiting_review', "
    "'research_job.retry_scheduled'"
)


def upgrade() -> None:
    op.create_table(
        "consumer_inbox",
        sa.Column("consumer_id", sa.String(length=128), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "consumer_id = 'atlas.research-job-projection.v1'",
            name="ck_consumer_inbox_consumer_id",
        ),
        sa.CheckConstraint(
            f"event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_consumer_inbox_event_type",
        ),
        sa.CheckConstraint(
            "kafka_partition >= 0",
            name="ck_consumer_inbox_kafka_partition_nonneg",
        ),
        sa.CheckConstraint(
            "kafka_offset >= 0",
            name="ck_consumer_inbox_kafka_offset_nonneg",
        ),
        sa.PrimaryKeyConstraint("consumer_id", "event_id", name="pk_consumer_inbox"),
    )

    op.create_table(
        "research_job_event_projection",
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("last_event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_event_type", sa.Text(), nullable=False),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            f"last_event_type IN ({_SUPPORTED_EVENT_TYPES_SQL})",
            name="ck_research_job_event_projection_event_type",
        ),
        sa.PrimaryKeyConstraint(
            "research_job_id", name="pk_research_job_event_projection"
        ),
    )


def downgrade() -> None:
    op.drop_table("research_job_event_projection")
    op.drop_table("consumer_inbox")
