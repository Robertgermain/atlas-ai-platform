"""Create research_jobs table.

Revision ID: 20260808_0001
Revises:
Create Date: 2026-08-08 21:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260808_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "research_jobs",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
            name="ck_research_jobs_status",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_research_jobs_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(question)) > 0",
            name="ck_research_jobs_question_nonempty",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_research_jobs_updated_after_created",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'PENDING'
              AND started_at IS NULL
              AND finished_at IS NULL
              AND result IS NULL
              AND failure_reason IS NULL
            )
            OR
            (
              status = 'RUNNING'
              AND started_at IS NOT NULL
              AND finished_at IS NULL
              AND result IS NULL
              AND failure_reason IS NULL
              AND started_at >= created_at
              AND updated_at >= started_at
            )
            OR
            (
              status = 'COMPLETED'
              AND started_at IS NOT NULL
              AND finished_at IS NOT NULL
              AND result IS NOT NULL
              AND failure_reason IS NULL
              AND started_at >= created_at
              AND finished_at >= started_at
              AND updated_at >= finished_at
            )
            OR
            (
              status = 'FAILED'
              AND started_at IS NOT NULL
              AND finished_at IS NOT NULL
              AND failure_reason IS NOT NULL
              AND result IS NULL
              AND started_at >= created_at
              AND finished_at >= started_at
              AND updated_at >= finished_at
            )
            """,
            name="ck_research_jobs_status_fields",
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("research_jobs")
