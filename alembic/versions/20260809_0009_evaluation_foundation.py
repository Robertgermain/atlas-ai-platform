"""Add candidate evaluation tables and evaluate model-ledger node.

Revision ID: 20260809_0009
Revises: 20260809_0008
Create Date: 2026-08-09 21:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0009"
down_revision: str | None = "20260809_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_workflow_executions_id_research_job_id",
        "workflow_executions",
        ["id", "research_job_id"],
    )

    op.create_table(
        "evaluation_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=False),
        sa.Column("evaluation_profile", sa.String(length=64), nullable=False),
        sa.Column("evaluation_attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("ownership_token", sa.String(length=64), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("job_claim_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=True),
        sa.Column("aggregate_score", sa.Float(), nullable=True),
        sa.Column("disposition_hint", sa.String(length=64), nullable=True),
        sa.Column(
            "grader_versions_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_evaluation_runs_research_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_evaluation_runs_workflow_execution_id",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id", "research_job_id"],
            ["workflow_executions.id", "workflow_executions.research_job_id"],
            name="fk_evaluation_runs_execution_job_pair",
        ),
        sa.UniqueConstraint(
            "workflow_execution_id",
            "evaluation_profile",
            "evaluation_attempt",
            name="uq_evaluation_runs_execution_profile_attempt",
        ),
        sa.CheckConstraint(
            "status IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_evaluation_runs_status",
        ),
        sa.CheckConstraint(
            "evaluation_profile = 'evaluation.candidate.v1'",
            name="ck_evaluation_runs_profile",
        ),
        sa.CheckConstraint(
            "evaluation_attempt >= 1",
            name="ck_evaluation_runs_attempt_positive",
        ),
        sa.CheckConstraint(
            "length(ownership_token) = 64",
            name="ck_evaluation_runs_ownership_token_len",
        ),
        sa.CheckConstraint(
            "length(input_fingerprint) = 64",
            name="ck_evaluation_runs_fingerprint_len",
        ),
        sa.CheckConstraint(
            "length(job_claim_fingerprint) = 64",
            name="ck_evaluation_runs_job_claim_fingerprint_len",
        ),
        sa.CheckConstraint(
            "aggregate_score IS NULL OR "
            "(aggregate_score >= 0 AND aggregate_score <= 1)",
            name="ck_evaluation_runs_aggregate_score_range",
        ),
        sa.CheckConstraint(
            """
            (
              status = 'IN_PROGRESS'
              AND finished_at IS NULL
              AND passed IS NULL
              AND aggregate_score IS NULL
            )
            OR
            (
              status = 'SUCCEEDED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
              AND passed IS NOT NULL
              AND aggregate_score IS NOT NULL
            )
            OR
            (
              status = 'FAILED'
              AND finished_at IS NOT NULL
              AND finished_at >= started_at
            )
            """,
            name="ck_evaluation_runs_status_fields",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_evaluation_runs_id_nonempty",
        ),
    )
    op.create_index(
        "ix_evaluation_runs_research_job_id",
        "evaluation_runs",
        ["research_job_id"],
    )
    op.create_index(
        "ix_evaluation_runs_workflow_execution_id",
        "evaluation_runs",
        ["workflow_execution_id"],
    )
    op.create_index(
        "ix_evaluation_runs_job_started",
        "evaluation_runs",
        ["research_job_id", "started_at"],
    )

    op.create_table(
        "evaluation_dimension_results",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("evaluation_run_id", sa.String(length=36), nullable=False),
        sa.Column("dimension_name", sa.String(length=64), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("method", sa.String(length=32), nullable=False),
        sa.Column("is_hard", sa.Boolean(), nullable=False),
        sa.Column("is_provisional", sa.Boolean(), nullable=False),
        sa.Column(
            "failure_codes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["evaluation_run_id"],
            ["evaluation_runs.id"],
            name="fk_evaluation_dimension_results_run_id",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "evaluation_run_id",
            "dimension_name",
            name="uq_evaluation_dimension_results_run_name",
        ),
        sa.CheckConstraint(
            "score >= 0 AND score <= 1",
            name="ck_evaluation_dimension_results_score_range",
        ),
        sa.CheckConstraint(
            "weight >= 0 AND weight <= 1",
            name="ck_evaluation_dimension_results_weight_range",
        ),
        sa.CheckConstraint(
            "method IN ('deterministic', 'llm', 'skipped')",
            name="ck_evaluation_dimension_results_method",
        ),
        sa.CheckConstraint(
            """
            dimension_name IN (
              'citation_integrity',
              'tool_use',
              'report_structure',
              'coverage',
              'completeness',
              'lexical_id_groundedness',
              'semantic_groundedness'
            )
            """,
            name="ck_evaluation_dimension_results_name",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_evaluation_dimension_results_id_nonempty",
        ),
    )
    op.create_index(
        "ix_evaluation_dimension_results_evaluation_run_id",
        "evaluation_dimension_results",
        ["evaluation_run_id"],
    )

    op.drop_constraint(
        "ck_model_invocations_node_name",
        "model_invocations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_model_invocations_node_name",
        "model_invocations",
        "node_name IN ('plan', 'draft', 'evaluate')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_model_invocations_node_name",
        "model_invocations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_model_invocations_node_name",
        "model_invocations",
        "node_name IN ('plan', 'draft')",
    )

    op.drop_index(
        "ix_evaluation_dimension_results_evaluation_run_id",
        table_name="evaluation_dimension_results",
    )
    op.drop_table("evaluation_dimension_results")
    op.drop_index("ix_evaluation_runs_job_started", table_name="evaluation_runs")
    op.drop_index(
        "ix_evaluation_runs_workflow_execution_id",
        table_name="evaluation_runs",
    )
    op.drop_index("ix_evaluation_runs_research_job_id", table_name="evaluation_runs")
    op.drop_table("evaluation_runs")
    op.drop_constraint(
        "uq_workflow_executions_id_research_job_id",
        "workflow_executions",
        type_="unique",
    )
