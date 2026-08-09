"""Add workflow execution and node attempt history tables.

Revision ID: 20260809_0004
Revises: 20260809_0003
Create Date: 2026-08-09 12:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0004"
down_revision: str | None = "20260809_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_executions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("thread_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_workflow_executions_research_job_id",
        ),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'COMPLETED', 'FAILED', 'ABANDONED')",
            name="ck_workflow_executions_status",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_workflow_executions_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(research_job_id)) > 0",
            name="ck_workflow_executions_job_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(thread_id)) > 0",
            name="ck_workflow_executions_thread_id_nonempty",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'RUNNING'
              AND finished_at IS NULL
            )
            OR
            (
              status IN ('COMPLETED', 'FAILED', 'ABANDONED')
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
            )
            """,
            name="ck_workflow_executions_status_fields",
        ),
    )
    op.create_index(
        "ix_workflow_executions_research_job_id",
        "workflow_executions",
        ["research_job_id"],
    )
    op.create_index(
        "ix_workflow_executions_thread_id",
        "workflow_executions",
        ["thread_id"],
    )

    op.create_table(
        "workflow_node_executions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=False),
        sa.Column("node_name", sa.String(length=64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_workflow_node_executions_workflow_execution_id",
        ),
        sa.UniqueConstraint(
            "workflow_execution_id",
            "node_name",
            "attempt",
            name="uq_workflow_node_executions_attempt",
        ),
        sa.CheckConstraint(
            "status IN ('STARTED', 'COMPLETED', 'FAILED')",
            name="ck_workflow_node_executions_status",
        ),
        sa.CheckConstraint(
            "attempt >= 1",
            name="ck_workflow_node_executions_attempt_positive",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_workflow_node_executions_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(node_name)) > 0",
            name="ck_workflow_node_executions_node_name_nonempty",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'STARTED'
              AND finished_at IS NULL
              AND error IS NULL
            )
            OR
            (
              status = 'COMPLETED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
              AND error IS NULL
            )
            OR
            (
              status = 'FAILED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
              AND error IS NOT NULL
            )
            """,
            name="ck_workflow_node_executions_status_fields",
        ),
    )
    op.create_index(
        "ix_workflow_node_executions_workflow_execution_id",
        "workflow_node_executions",
        ["workflow_execution_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_workflow_node_executions_workflow_execution_id",
        table_name="workflow_node_executions",
    )
    op.drop_table("workflow_node_executions")
    op.drop_index(
        "ix_workflow_executions_thread_id",
        table_name="workflow_executions",
    )
    op.drop_index(
        "ix_workflow_executions_research_job_id",
        table_name="workflow_executions",
    )
    op.drop_table("workflow_executions")
