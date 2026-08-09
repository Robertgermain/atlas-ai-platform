"""ORM mappings for tool invocation ledger tables."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class ToolInvocationModel(Base):
    """Logical idempotent tool invocation with cached validated summary."""

    __tablename__ = "tool_invocations"
    __table_args__ = (
        UniqueConstraint("invocation_key", name="uq_tool_invocations_key"),
        CheckConstraint(
            "status IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')",
            name="ck_tool_invocations_status",
        ),
        CheckConstraint(
            "origin IN ('WORKFLOW', 'MCP')",
            name="ck_tool_invocations_origin",
        ),
        CheckConstraint(
            "length(trim(invocation_key)) = 64",
            name="ck_tool_invocations_key_len",
        ),
        CheckConstraint(
            """
            (
              origin = 'WORKFLOW'
              AND research_job_id IS NOT NULL
              AND workflow_execution_id IS NOT NULL
              AND node_name IS NOT NULL
              AND actor_id IS NULL
            )
            OR
            (
              origin = 'MCP'
              AND research_job_id IS NULL
              AND workflow_execution_id IS NULL
              AND node_name IS NULL
              AND workflow_node_attempt IS NULL
              AND actor_id IS NOT NULL
            )
            """,
            name="ck_tool_invocations_origin_fields",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    origin: Mapped[str] = mapped_column(String(16), nullable=False)
    research_job_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("research_jobs.id", name="fk_tool_invocations_research_job_id"),
        nullable=True,
        index=True,
    )
    workflow_execution_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_tool_invocations_workflow_execution_id",
        ),
        nullable=True,
        index=True,
    )
    node_name: Mapped[str | None] = mapped_column(String(32), nullable=True)
    workflow_node_attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    actor_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tool_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tool_version: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    tool_policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    output_summary_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True),
        nullable=True,
    )
    content_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    byte_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
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


class ToolInvocationAttemptModel(Base):
    """Physical tool-call attempt for a logical tool invocation."""

    __tablename__ = "tool_invocation_attempts"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            "attempt",
            name="uq_tool_invocation_attempts_number",
        ),
        CheckConstraint(
            "status IN ('STARTED', 'SUCCEEDED', 'FAILED')",
            name="ck_tool_invocation_attempts_status",
        ),
        CheckConstraint(
            "attempt >= 1",
            name="ck_tool_invocation_attempts_attempt_positive",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "tool_invocations.id",
            name="fk_tool_invocation_attempts_invocation_id",
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
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
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
