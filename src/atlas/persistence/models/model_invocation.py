"""ORM mappings for model invocation ledger tables."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
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


class ModelInvocationModel(Base):
    """Logical idempotent model invocation with cached validated output."""

    __tablename__ = "model_invocations"
    __table_args__ = (
        UniqueConstraint("invocation_key", name="uq_model_invocations_key"),
        CheckConstraint(
            "status IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_model_invocations_status",
        ),
        CheckConstraint(
            "node_name IN ('plan', 'draft', 'evaluate')",
            name="ck_model_invocations_node_name",
        ),
        CheckConstraint(
            "length(trim(invocation_key)) = 64",
            name="ck_model_invocations_key_len",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("research_jobs.id", name="fk_model_invocations_research_job_id"),
        nullable=False,
        index=True,
    )
    workflow_execution_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_model_invocations_workflow_execution_id",
        ),
        nullable=True,
        index=True,
    )
    node_name: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    output_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    provider_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    pricing_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    finish_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(128), nullable=True)
    retry_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class ModelInvocationAttemptModel(Base):
    """Physical provider-call attempt for a logical model invocation."""

    __tablename__ = "model_invocation_attempts"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            "attempt",
            name="uq_model_invocation_attempts_number",
        ),
        CheckConstraint(
            "status IN ('STARTED', 'SUCCEEDED', 'FAILED')",
            name="ck_model_invocation_attempts_status",
        ),
        CheckConstraint(
            "attempt >= 1",
            name="ck_model_invocation_attempts_attempt_positive",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "model_invocations.id",
            name="fk_model_invocation_attempts_invocation_id",
        ),
        nullable=False,
        index=True,
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    provider_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    pricing_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    finish_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(128), nullable=True)
    retry_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
