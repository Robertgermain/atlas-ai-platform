"""Add durable job-level evaluation-profile binding (Slice 15C1 freeze).

Revision ID: 20260813_0015
Revises: 20260812_0014
Create Date: 2026-08-13 23:00:00.000000

``research_jobs.evaluation_profile`` is bound atomically on the first
successful worker claim. Never-started PENDING jobs may remain NULL.
Started and terminal jobs must have one of:

- ``evaluation.candidate.v1`` (skipped semantic)
- ``evaluation.candidate.fake.v1`` (fake semantic)
- ``evaluation.v1`` (live semantic)

Existing jobs are backfilled to ``evaluation.candidate.v1``. Historical
``evaluation_runs`` rows are not rewritten. The evaluation-run profile
CHECK is widened to the three approved identities.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260813_0015"
down_revision: str | None = "20260812_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ALLOWED_PROFILES_SQL = (
    "'evaluation.candidate.v1', 'evaluation.candidate.fake.v1', 'evaluation.v1'"
)


def upgrade() -> None:
    op.add_column(
        "research_jobs",
        sa.Column("evaluation_profile", sa.String(length=64), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE research_jobs "
            "SET evaluation_profile = 'evaluation.candidate.v1' "
            "WHERE evaluation_profile IS NULL"
        )
    )
    op.create_check_constraint(
        "ck_research_jobs_evaluation_profile_allowed",
        "research_jobs",
        (
            "evaluation_profile IS NULL OR "
            f"evaluation_profile IN ({_ALLOWED_PROFILES_SQL})"
        ),
    )
    # PostgreSQL CHECK accepts UNKNOWN, so `evaluation_profile IN (...)` cannot
    # reject NULL. Started/terminal rows must use IS NOT NULL.
    op.create_check_constraint(
        "ck_research_jobs_started_has_evaluation_profile",
        "research_jobs",
        "(status = 'PENDING' AND started_at IS NULL) OR evaluation_profile IS NOT NULL",
    )

    op.drop_constraint("ck_evaluation_runs_profile", "evaluation_runs", type_="check")
    op.create_check_constraint(
        "ck_evaluation_runs_profile",
        "evaluation_runs",
        f"evaluation_profile IN ({_ALLOWED_PROFILES_SQL})",
    )


def downgrade() -> None:
    op.drop_constraint("ck_evaluation_runs_profile", "evaluation_runs", type_="check")
    op.create_check_constraint(
        "ck_evaluation_runs_profile",
        "evaluation_runs",
        "evaluation_profile = 'evaluation.candidate.v1'",
    )

    op.drop_constraint(
        "ck_research_jobs_started_has_evaluation_profile",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_evaluation_profile_allowed",
        "research_jobs",
        type_="check",
    )
    op.drop_column("research_jobs", "evaluation_profile")
