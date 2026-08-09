"""ORM mappings for workflow execution history (Atlas-owned audit tables)."""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class WorkflowExecutionModel(Base):
    """One row per worker processing attempt for a research job."""

    __tablename__ = "workflow_executions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('RUNNING', 'COMPLETED', 'FAILED', 'ABANDONED')",
            name="ck_workflow_executions_status",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_workflow_executions_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(research_job_id)) > 0",
            name="ck_workflow_executions_job_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(thread_id)) > 0",
            name="ck_workflow_executions_thread_id_nonempty",
        ),
        CheckConstraint(
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

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_job_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("research_jobs.id", name="fk_workflow_executions_research_job_id"),
        nullable=False,
        index=True,
    )
    thread_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class WorkflowNodeExecutionModel(Base):
    """One row per node execution attempt within a workflow execution."""

    __tablename__ = "workflow_node_executions"
    __table_args__ = (
        UniqueConstraint(
            "workflow_execution_id",
            "node_name",
            "attempt",
            name="uq_workflow_node_executions_attempt",
        ),
        CheckConstraint(
            "status IN ('STARTED', 'COMPLETED', 'FAILED')",
            name="ck_workflow_node_executions_status",
        ),
        CheckConstraint(
            "attempt >= 1",
            name="ck_workflow_node_executions_attempt_positive",
        ),
        CheckConstraint(
            "length(trim(id)) > 0",
            name="ck_workflow_node_executions_id_nonempty",
        ),
        CheckConstraint(
            "length(trim(node_name)) > 0",
            name="ck_workflow_node_executions_node_name_nonempty",
        ),
        CheckConstraint(
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

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_execution_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "workflow_executions.id",
            name="fk_workflow_node_executions_workflow_execution_id",
        ),
        nullable=False,
        index=True,
    )
    node_name: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
