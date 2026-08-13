"""ORM mapping for research jobs."""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class ResearchJobModel(Base):
    """Durable representation of a research job."""

    __tablename__ = "research_jobs"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key",
            name="uq_research_jobs_idempotency_key",
        ),
        CheckConstraint(
            "status IN ("
            "'PENDING', 'RUNNING', 'AWAITING_REVIEW', 'COMPLETED', 'FAILED'"
            ")",
            name="ck_research_jobs_status",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_research_jobs_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(question)) > 0",
            name="ck_research_jobs_question_nonempty",
        ),
        CheckConstraint(
            "updated_at >= created_at",
            name="ck_research_jobs_updated_after_created",
        ),
        CheckConstraint(
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
            name="ck_research_jobs_status_fields",
        ),
        CheckConstraint(
            """
            (
              idempotency_key IS NULL
              AND request_fingerprint IS NULL
            )
            OR
            (
              idempotency_key IS NOT NULL
              AND request_fingerprint IS NOT NULL
              AND length(trim(idempotency_key)) > 0
              AND length(request_fingerprint) = 64
            )
            """,
            name="ck_research_jobs_idempotency_pair",
        ),
        CheckConstraint(
            """
            (
              lease_expires_at IS NULL
              AND claim_token IS NULL
            )
            OR
            (
              lease_expires_at IS NOT NULL
              AND claim_token IS NOT NULL
              AND length(trim(claim_token)) > 0
              AND length(claim_token) = 64
            )
            """,
            name="ck_research_jobs_claim_lease_pair",
        ),
        CheckConstraint(
            "repair_count >= 0 AND repair_count <= 1",
            name="ck_research_jobs_repair_count",
        ),
        CheckConstraint(
            "job_retry_count >= 0 AND job_retry_count <= 2",
            name="ck_research_jobs_job_retry_count",
        ),
        CheckConstraint(
            "evaluation_attempt_count >= 0 AND evaluation_attempt_count <= 4",
            name="ck_research_jobs_evaluation_attempt_count",
        ),
        CheckConstraint(
            "continuation_mode IN ('NONE', 'JOB_RETRY', 'REVIEW_COMPLETE')",
            name="ck_research_jobs_continuation_mode",
        ),
        CheckConstraint(
            "claimed_continuation_mode IN ('NONE', 'JOB_RETRY', 'REVIEW_COMPLETE')",
            name="ck_research_jobs_claimed_continuation_mode",
        ),
        ForeignKeyConstraint(
            ["active_workflow_execution_id", "id"],
            ["workflow_executions.id", "workflow_executions.research_job_id"],
            name="fk_research_jobs_active_execution_job_pair",
            ondelete="NO ACTION",
        ),
        CheckConstraint(
            "traceparent IS NULL OR ("
            "traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' AND "
            "traceparent !~ '^00-0{32}-[0-9a-f]{16}-[0-9a-f]{2}$' AND "
            "traceparent !~ '^00-[0-9a-f]{32}-0{16}-[0-9a-f]{2}$'"
            ")",
            name="ck_research_jobs_traceparent_format",
        ),
        CheckConstraint(
            "initial_traceparent_consumed_at IS NULL OR traceparent IS NOT NULL",
            name="ck_research_jobs_initial_traceparent_consumed_pair",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    claim_token: Mapped[str | None] = mapped_column(String(64), nullable=True)

    repair_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    job_retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    evaluation_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    continuation_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="NONE"
    )
    claimed_continuation_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="NONE"
    )
    active_workflow_execution_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    #: The W3C ``traceparent`` active when the API created this row (Slice
    #: 15A3). Persistence-only: never exposed through any public API or
    #: domain model. Written once at insert and never overwritten afterward.
    traceparent: Mapped[str | None] = mapped_column(String(55), nullable=True)
    #: Set atomically, exactly once, in the same transaction as the first
    #: successful claim of this row that both has a non-null ``traceparent``
    #: and finds this column still ``NULL``. See
    #: ``atlas.persistence.repositories.research_job.claim_next`` and the
    #: Slice 15A3 migration docstring for the full contract this enforces.
    initial_traceparent_consumed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
