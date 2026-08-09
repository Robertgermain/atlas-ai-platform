"""Add nullable idempotency metadata to research_jobs.

Revision ID: 20260808_0002
Revises: 20260808_0001
Create Date: 2026-08-08 23:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260808_0002"
down_revision: str | None = "20260808_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "research_jobs",
        sa.Column("idempotency_key", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "research_jobs",
        sa.Column("request_fingerprint", sa.String(length=64), nullable=True),
    )
    op.create_check_constraint(
        "ck_research_jobs_idempotency_pair",
        "research_jobs",
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
    )
    op.create_unique_constraint(
        "uq_research_jobs_idempotency_key",
        "research_jobs",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_research_jobs_idempotency_key",
        "research_jobs",
        type_="unique",
    )
    op.drop_constraint(
        "ck_research_jobs_idempotency_pair",
        "research_jobs",
        type_="check",
    )
    op.drop_column("research_jobs", "request_fingerprint")
    op.drop_column("research_jobs", "idempotency_key")
