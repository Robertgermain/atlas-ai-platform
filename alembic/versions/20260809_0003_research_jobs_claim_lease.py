"""Add claim lease and claim token columns to research_jobs.

Revision ID: 20260809_0003
Revises: 20260808_0002
Create Date: 2026-08-09 00:50:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0003"
down_revision: str | None = "20260808_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "research_jobs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "research_jobs",
        sa.Column("claim_token", sa.String(length=64), nullable=True),
    )
    op.create_check_constraint(
        "ck_research_jobs_claim_lease_pair",
        "research_jobs",
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
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_research_jobs_claim_lease_pair",
        "research_jobs",
        type_="check",
    )
    op.drop_column("research_jobs", "claim_token")
    op.drop_column("research_jobs", "lease_expires_at")
