"""Add model invocation ledger tables.

Revision ID: 20260809_0005
Revises: 20260809_0004
Create Date: 2026-08-09 14:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0005"
down_revision: str | None = "20260809_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "model_invocations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("invocation_key", sa.String(length=64), nullable=False),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=True),
        sa.Column("node_name", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("prompt_version", sa.String(length=64), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "output_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("provider_request_id", sa.String(length=128), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("estimated_cost_usd", sa.Float(), nullable=True),
        sa.Column("pricing_version", sa.String(length=64), nullable=True),
        sa.Column("finish_outcome", sa.String(length=32), nullable=True),
        sa.Column("error_class", sa.String(length=128), nullable=True),
        sa.Column("retry_class", sa.String(length=64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_model_invocations_research_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_model_invocations_workflow_execution_id",
        ),
        sa.UniqueConstraint("invocation_key", name="uq_model_invocations_key"),
        sa.CheckConstraint(
            "status IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_model_invocations_status",
        ),
        sa.CheckConstraint(
            "node_name IN ('plan', 'draft')",
            name="ck_model_invocations_node_name",
        ),
        sa.CheckConstraint(
            "length(trim(invocation_key)) = 64",
            name="ck_model_invocations_key_len",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_model_invocations_id_nonempty",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'IN_PROGRESS'
              AND finished_at IS NULL
              AND output_json IS NULL
            )
            OR
            (
              status = 'SUCCEEDED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
              AND output_json IS NOT NULL
            )
            OR
            (
              status = 'FAILED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
              AND output_json IS NULL
            )
            """,
            name="ck_model_invocations_status_fields",
        ),
    )
    op.create_index(
        "ix_model_invocations_research_job_id",
        "model_invocations",
        ["research_job_id"],
    )
    op.create_index(
        "ix_model_invocations_workflow_execution_id",
        "model_invocations",
        ["workflow_execution_id"],
    )

    op.create_table(
        "model_invocation_attempts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("invocation_id", sa.String(length=36), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("provider_request_id", sa.String(length=128), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("estimated_cost_usd", sa.Float(), nullable=True),
        sa.Column("pricing_version", sa.String(length=64), nullable=True),
        sa.Column("finish_outcome", sa.String(length=32), nullable=True),
        sa.Column("error_class", sa.String(length=128), nullable=True),
        sa.Column("retry_class", sa.String(length=64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["invocation_id"],
            ["model_invocations.id"],
            name="fk_model_invocation_attempts_invocation_id",
        ),
        sa.UniqueConstraint(
            "invocation_id",
            "attempt",
            name="uq_model_invocation_attempts_number",
        ),
        sa.CheckConstraint(
            "status IN ('STARTED', 'SUCCEEDED', 'FAILED')",
            name="ck_model_invocation_attempts_status",
        ),
        sa.CheckConstraint(
            "attempt >= 1",
            name="ck_model_invocation_attempts_attempt_positive",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_model_invocation_attempts_id_nonempty",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'STARTED'
              AND finished_at IS NULL
            )
            OR
            (
              status IN ('SUCCEEDED', 'FAILED')
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
            )
            """,
            name="ck_model_invocation_attempts_status_fields",
        ),
    )
    op.create_index(
        "ix_model_invocation_attempts_invocation_id",
        "model_invocation_attempts",
        ["invocation_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_model_invocation_attempts_invocation_id",
        table_name="model_invocation_attempts",
    )
    op.drop_table("model_invocation_attempts")
    op.drop_index(
        "ix_model_invocations_workflow_execution_id",
        table_name="model_invocations",
    )
    op.drop_index(
        "ix_model_invocations_research_job_id",
        table_name="model_invocations",
    )
    op.drop_table("model_invocations")
