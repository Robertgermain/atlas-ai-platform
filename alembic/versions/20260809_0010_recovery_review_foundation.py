"""Add recovery, review, and job continuation fields for Slice 12B.

Revision ID: 20260809_0010
Revises: 20260809_0009
Create Date: 2026-08-09 22:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0010"
down_revision: str | None = "20260809_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "research_jobs",
        sa.Column(
            "repair_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "job_retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "evaluation_attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "research_jobs",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "continuation_mode",
            sa.String(length=32),
            nullable=False,
            server_default="NONE",
        ),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "claimed_continuation_mode",
            sa.String(length=32),
            nullable=False,
            server_default="NONE",
        ),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "active_workflow_execution_id",
            sa.String(length=36),
            nullable=True,
        ),
    )

    op.create_foreign_key(
        "fk_research_jobs_active_execution_job_pair",
        "research_jobs",
        "workflow_executions",
        ["active_workflow_execution_id", "id"],
        ["id", "research_job_id"],
        ondelete="NO ACTION",
    )
    op.create_index(
        "ix_research_jobs_active_workflow_execution_id",
        "research_jobs",
        ["active_workflow_execution_id"],
    )
    op.create_index(
        "ix_research_jobs_next_attempt_at",
        "research_jobs",
        ["next_attempt_at"],
    )

    op.drop_constraint("ck_research_jobs_status", "research_jobs", type_="check")
    op.create_check_constraint(
        "ck_research_jobs_status",
        "research_jobs",
        "status IN ('PENDING', 'RUNNING', 'AWAITING_REVIEW', 'COMPLETED', 'FAILED')",
    )

    op.drop_constraint(
        "ck_research_jobs_status_fields",
        "research_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_research_jobs_status_fields",
        "research_jobs",
        """
        (
          status = 'PENDING'
          AND finished_at IS NULL
          AND result IS NULL
          AND failure_reason IS NULL
          AND claim_token IS NULL
          AND lease_expires_at IS NULL
          AND claimed_continuation_mode = 'NONE'
          AND (
            (
              started_at IS NULL
              AND next_attempt_at IS NULL
              AND continuation_mode = 'NONE'
              AND active_workflow_execution_id IS NULL
            )
            OR
            (
              started_at IS NOT NULL
              AND next_attempt_at IS NOT NULL
              AND continuation_mode = 'JOB_RETRY'
              AND active_workflow_execution_id IS NULL
              AND started_at >= created_at
              AND updated_at >= started_at
            )
            OR
            (
              started_at IS NOT NULL
              AND next_attempt_at IS NOT NULL
              AND continuation_mode = 'REVIEW_COMPLETE'
              AND active_workflow_execution_id IS NOT NULL
              AND started_at >= created_at
              AND updated_at >= started_at
            )
          )
        )
        OR
        (
          status = 'RUNNING'
          AND started_at IS NOT NULL
          AND finished_at IS NULL
          AND result IS NULL
          AND failure_reason IS NULL
          AND next_attempt_at IS NULL
          AND continuation_mode = 'NONE'
          AND claimed_continuation_mode IN ('NONE', 'JOB_RETRY', 'REVIEW_COMPLETE')
          AND (
            (claimed_continuation_mode = 'REVIEW_COMPLETE'
             AND active_workflow_execution_id IS NOT NULL)
            OR
            (claimed_continuation_mode IN ('NONE', 'JOB_RETRY'))
          )
          AND started_at >= created_at
          AND updated_at >= started_at
        )
        OR
        (
          status = 'AWAITING_REVIEW'
          AND started_at IS NOT NULL
          AND finished_at IS NULL
          AND result IS NULL
          AND failure_reason IS NULL
          AND claim_token IS NULL
          AND lease_expires_at IS NULL
          AND next_attempt_at IS NULL
          AND continuation_mode = 'NONE'
          AND claimed_continuation_mode = 'NONE'
          AND active_workflow_execution_id IS NOT NULL
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
          AND claim_token IS NULL
          AND lease_expires_at IS NULL
          AND next_attempt_at IS NULL
          AND continuation_mode = 'NONE'
          AND claimed_continuation_mode = 'NONE'
          AND active_workflow_execution_id IS NULL
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
          AND claim_token IS NULL
          AND lease_expires_at IS NULL
          AND next_attempt_at IS NULL
          AND continuation_mode = 'NONE'
          AND claimed_continuation_mode = 'NONE'
          AND active_workflow_execution_id IS NULL
          AND started_at >= created_at
          AND finished_at >= started_at
          AND updated_at >= finished_at
        )
        """,
    )

    op.create_check_constraint(
        "ck_research_jobs_repair_count",
        "research_jobs",
        "repair_count >= 0 AND repair_count <= 1",
    )
    op.create_check_constraint(
        "ck_research_jobs_job_retry_count",
        "research_jobs",
        "job_retry_count >= 0 AND job_retry_count <= 2",
    )
    op.create_check_constraint(
        "ck_research_jobs_evaluation_attempt_count",
        "research_jobs",
        "evaluation_attempt_count >= 0 AND evaluation_attempt_count <= 4",
    )
    op.create_check_constraint(
        "ck_research_jobs_continuation_mode",
        "research_jobs",
        "continuation_mode IN ('NONE', 'JOB_RETRY', 'REVIEW_COMPLETE')",
    )
    op.create_check_constraint(
        "ck_research_jobs_claimed_continuation_mode",
        "research_jobs",
        "claimed_continuation_mode IN ('NONE', 'JOB_RETRY', 'REVIEW_COMPLETE')",
    )

    op.create_table(
        "policy_decisions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=True),
        sa.Column("evaluation_run_id", sa.String(length=36), nullable=True),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("failure_category", sa.String(length=64), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("decision_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_policy_decisions_research_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_policy_decisions_workflow_execution_id",
        ),
        sa.ForeignKeyConstraint(
            ["evaluation_run_id"],
            ["evaluation_runs.id"],
            name="fk_policy_decisions_evaluation_run_id",
        ),
        sa.CheckConstraint(
            "decision IN ("
            "'complete', 'repair', 'await_review', 'retry', 'terminal', 'reject')",
            name="ck_policy_decisions_decision",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_policy_decisions_id_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(failure_category)) > 0",
            name="ck_policy_decisions_category_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(reason_code)) > 0",
            name="ck_policy_decisions_reason_nonempty",
        ),
        sa.CheckConstraint(
            "length(decision_fingerprint) = 64",
            name="ck_policy_decisions_fingerprint_len",
        ),
        sa.UniqueConstraint(
            "research_job_id",
            "decision_fingerprint",
            name="uq_policy_decisions_job_fingerprint",
        ),
    )
    op.create_index(
        "ix_policy_decisions_research_job_id",
        "policy_decisions",
        ["research_job_id"],
    )

    op.create_table(
        "job_recovery_attempts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("policy_decision_id", sa.String(length=36), nullable=False),
        sa.Column(
            "abandoned_workflow_execution_id", sa.String(length=36), nullable=False
        ),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_job_recovery_attempts_research_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["policy_decision_id"],
            ["policy_decisions.id"],
            name="fk_job_recovery_attempts_policy_decision_id",
        ),
        sa.ForeignKeyConstraint(
            ["abandoned_workflow_execution_id"],
            ["workflow_executions.id"],
            name="fk_job_recovery_attempts_abandoned_execution_id",
        ),
        sa.UniqueConstraint(
            "research_job_id",
            "attempt_number",
            name="uq_job_recovery_attempts_job_attempt",
        ),
        sa.CheckConstraint(
            "attempt_number >= 1 AND attempt_number <= 2",
            name="ck_job_recovery_attempts_attempt_number",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_job_recovery_attempts_id_nonempty",
        ),
    )

    op.create_table(
        "human_review_decisions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_job_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=36), nullable=False),
        sa.Column("evaluation_run_id", sa.String(length=36), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("candidate_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["research_job_id"],
            ["research_jobs.id"],
            name="fk_human_review_decisions_research_job_id",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id", "research_job_id"],
            ["workflow_executions.id", "workflow_executions.research_job_id"],
            name="fk_human_review_decisions_execution_job_pair",
        ),
        sa.ForeignKeyConstraint(
            ["evaluation_run_id"],
            ["evaluation_runs.id"],
            name="fk_human_review_decisions_evaluation_run_id",
        ),
        sa.UniqueConstraint(
            "research_job_id",
            "idempotency_key",
            name="uq_human_review_decisions_job_idempotency",
        ),
        sa.CheckConstraint(
            "decision IN ('approve', 'reject')",
            name="ck_human_review_decisions_decision",
        ),
        sa.CheckConstraint(
            "length(candidate_fingerprint) = 64",
            name="ck_human_review_decisions_fingerprint_len",
        ),
        sa.CheckConstraint(
            "length(request_fingerprint) = 64",
            name="ck_human_review_decisions_request_fingerprint_len",
        ),
        sa.CheckConstraint(
            "length(trim(actor_id)) > 0",
            name="ck_human_review_decisions_actor_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(idempotency_key)) > 0",
            name="ck_human_review_decisions_idempotency_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_human_review_decisions_id_nonempty",
        ),
    )
    op.create_index(
        "ix_human_review_decisions_research_job_id",
        "human_review_decisions",
        ["research_job_id"],
    )


def downgrade() -> None:
    op.drop_table("human_review_decisions")
    op.drop_table("job_recovery_attempts")
    op.drop_table("policy_decisions")

    op.drop_constraint(
        "ck_research_jobs_claimed_continuation_mode",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_continuation_mode",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_evaluation_attempt_count",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_job_retry_count",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_repair_count",
        "research_jobs",
        type_="check",
    )

    op.drop_constraint(
        "ck_research_jobs_status_fields",
        "research_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_research_jobs_status_fields",
        "research_jobs",
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
    )

    op.drop_constraint("ck_research_jobs_status", "research_jobs", type_="check")
    op.create_check_constraint(
        "ck_research_jobs_status",
        "research_jobs",
        "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
    )

    op.drop_index("ix_research_jobs_next_attempt_at", table_name="research_jobs")
    op.drop_index(
        "ix_research_jobs_active_workflow_execution_id",
        table_name="research_jobs",
    )
    op.drop_constraint(
        "fk_research_jobs_active_execution_job_pair",
        "research_jobs",
        type_="foreignkey",
    )
    op.drop_column("research_jobs", "active_workflow_execution_id")
    op.drop_column("research_jobs", "claimed_continuation_mode")
    op.drop_column("research_jobs", "continuation_mode")
    op.drop_column("research_jobs", "next_attempt_at")
    op.drop_column("research_jobs", "evaluation_attempt_count")
    op.drop_column("research_jobs", "job_retry_count")
    op.drop_column("research_jobs", "repair_count")
