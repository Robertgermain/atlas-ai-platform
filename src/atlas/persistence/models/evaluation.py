"""ORM mappings for candidate evaluation runs and dimension results."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class EvaluationRunModel(Base):
    """One fenced evaluation attempt for a workflow execution."""

    __tablename__ = "evaluation_runs"
    __table_args__ = (
        UniqueConstraint(
            "workflow_execution_id",
            "evaluation_profile",
            "evaluation_attempt",
            name="uq_evaluation_runs_execution_profile_attempt",
        ),
        CheckConstraint(
            "status IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_evaluation_runs_status",
        ),
        CheckConstraint(
            "evaluation_profile IN ("
            "'evaluation.candidate.v1', "
            "'evaluation.candidate.fake.v1', "
            "'evaluation.v1'"
            ")",
            name="ck_evaluation_runs_profile",
        ),
        CheckConstraint(
            "evaluation_attempt >= 1",
            name="ck_evaluation_runs_attempt_positive",
        ),
        CheckConstraint(
            "length(ownership_token) = 64",
            name="ck_evaluation_runs_ownership_token_len",
        ),
        CheckConstraint(
            "length(input_fingerprint) = 64",
            name="ck_evaluation_runs_fingerprint_len",
        ),
        CheckConstraint(
            "length(job_claim_fingerprint) = 64",
            name="ck_evaluation_runs_job_claim_fingerprint_len",
        ),
        CheckConstraint(
            "aggregate_score IS NULL OR "
            "(aggregate_score >= 0 AND aggregate_score <= 1)",
            name="ck_evaluation_runs_aggregate_score_range",
        ),
        CheckConstraint(
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
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey(
            "research_jobs.id",
            name="fk_evaluation_runs_research_job_id",
        ),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_evaluation_runs_workflow_execution_id",
        ),
        nullable=False,
        index=True,
    )
    evaluation_profile: Mapped[str] = mapped_column(String(64), nullable=False)
    evaluation_attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    ownership_token: Mapped[str] = mapped_column(String(64), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    job_claim_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    aggregate_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    disposition_hint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    grader_versions_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class EvaluationDimensionResultModel(Base):
    """Normalized per-dimension score for an evaluation run."""

    __tablename__ = "evaluation_dimension_results"
    __table_args__ = (
        UniqueConstraint(
            "evaluation_run_id",
            "dimension_name",
            name="uq_evaluation_dimension_results_run_name",
        ),
        CheckConstraint(
            "score >= 0 AND score <= 1",
            name="ck_evaluation_dimension_results_score_range",
        ),
        CheckConstraint(
            "weight >= 0 AND weight <= 1",
            name="ck_evaluation_dimension_results_weight_range",
        ),
        CheckConstraint(
            "method IN ('deterministic', 'llm', 'skipped')",
            name="ck_evaluation_dimension_results_method",
        ),
        CheckConstraint(
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
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    evaluation_run_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "evaluation_runs.id",
            name="fk_evaluation_dimension_results_run_id",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )
    dimension_name: Mapped[str] = mapped_column(String(64), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    is_hard: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_provisional: Mapped[bool] = mapped_column(Boolean, nullable=False)
    failure_codes: Mapped[list[Any]] = mapped_column(
        JSONB(none_as_null=True),
        nullable=False,
    )
    weight: Mapped[float] = mapped_column(Float, nullable=False)
