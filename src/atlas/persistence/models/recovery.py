"""ORM mappings for recovery, review, and policy-decision audit tables."""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class PolicyDecisionModel(Base):
    """Persisted deterministic policy decision for one evaluation or exception."""

    __tablename__ = "policy_decisions"
    __table_args__ = (
        CheckConstraint(
            "decision IN ("
            "'complete', 'repair', 'await_review', 'retry', 'terminal', 'reject')",
            name="ck_policy_decisions_decision",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_policy_decisions_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(failure_category)) > 0",
            name="ck_policy_decisions_category_nonempty",
        ),
        CheckConstraint(
            "length(trim(reason_code)) > 0",
            name="ck_policy_decisions_reason_nonempty",
        ),
        CheckConstraint(
            "length(decision_fingerprint) = 64",
            name="ck_policy_decisions_fingerprint_len",
        ),
        UniqueConstraint(
            "research_job_id",
            "decision_fingerprint",
            name="uq_policy_decisions_job_fingerprint",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("research_jobs.id", name="fk_policy_decisions_research_job_id"),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_policy_decisions_workflow_execution_id",
        ),
        nullable=True,
    )
    evaluation_run_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "evaluation_runs.id",
            name="fk_policy_decisions_evaluation_run_id",
        ),
        nullable=True,
    )
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    failure_category: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    decision_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class JobRecoveryAttemptModel(Base):
    """One row per job-level retry attempt."""

    __tablename__ = "job_recovery_attempts"
    __table_args__ = (
        UniqueConstraint(
            "research_job_id",
            "attempt_number",
            name="uq_job_recovery_attempts_job_attempt",
        ),
        CheckConstraint(
            "attempt_number >= 1 AND attempt_number <= 2",
            name="ck_job_recovery_attempts_attempt_number",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_job_recovery_attempts_id_nonempty",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey(
            "research_jobs.id",
            name="fk_job_recovery_attempts_research_job_id",
        ),
        nullable=False,
    )
    policy_decision_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "policy_decisions.id",
            name="fk_job_recovery_attempts_policy_decision_id",
        ),
        nullable=False,
    )
    abandoned_workflow_execution_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_job_recovery_attempts_abandoned_execution_id",
        ),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class HumanReviewDecisionModel(Base):
    """Operator approve/reject for jobs awaiting human review."""

    __tablename__ = "human_review_decisions"
    __table_args__ = (
        UniqueConstraint(
            "research_job_id",
            "idempotency_key",
            name="uq_human_review_decisions_job_idempotency",
        ),
        ForeignKeyConstraint(
            ["workflow_execution_id", "research_job_id"],
            ["workflow_executions.id", "workflow_executions.research_job_id"],
            name="fk_human_review_decisions_execution_job_pair",
        ),
        CheckConstraint(
            "decision IN ('approve', 'reject')",
            name="ck_human_review_decisions_decision",
        ),
        CheckConstraint(
            "length(candidate_fingerprint) = 64",
            name="ck_human_review_decisions_fingerprint_len",
        ),
        CheckConstraint(
            "length(request_fingerprint) = 64",
            name="ck_human_review_decisions_request_fingerprint_len",
        ),
        CheckConstraint(
            "length(trim(actor_id)) > 0",
            name="ck_human_review_decisions_actor_nonempty",
        ),
        CheckConstraint(
            "length(trim(idempotency_key)) > 0",
            name="ck_human_review_decisions_idempotency_nonempty",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_human_review_decisions_id_nonempty",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey(
            "research_jobs.id",
            name="fk_human_review_decisions_research_job_id",
        ),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
    )
    evaluation_run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "evaluation_runs.id",
            name="fk_human_review_decisions_evaluation_run_id",
        ),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    candidate_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
