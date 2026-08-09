"""ORM mapping for research jobs."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from atlas.persistence.models.base import Base


class ResearchJobModel(Base):
    """Durable representation of a research job."""

    __tablename__ = "research_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')",
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
            name="ck_research_jobs_status_fields",
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
