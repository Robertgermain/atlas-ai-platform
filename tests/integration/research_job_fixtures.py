"""Test-only helpers for simulating an already-claimed research job.

Production workers bind ``evaluation_profile`` in ``claim_next``. Many
legacy integration tests start a job through the domain mapper instead.
Those fixtures must bind a profile *before* the PENDING → started
transition so ``ck_research_jobs_started_has_evaluation_profile`` stays
strict. This module must never be imported by production code.
"""

from __future__ import annotations

from datetime import datetime

from atlas.evaluation.contracts import EVALUATION_PROFILE_CANDIDATE
from atlas.persistence.mappers.research_job import apply_domain_to_orm, to_domain
from atlas.persistence.models import ResearchJobModel


def bind_profile_and_start_claimed_job(
    model: ResearchJobModel,
    *,
    at: datetime,
    claim_token: str,
    lease_expires_at: datetime,
    evaluation_profile: str = EVALUATION_PROFILE_CANDIDATE,
) -> None:
    """Bind a profile, then start and attach claim metadata on the ORM row.

    An existing ORM profile is preserved. A missing profile is set to
    ``evaluation_profile`` before ``job.start()``.
    """
    if model.evaluation_profile is None:
        model.evaluation_profile = evaluation_profile
    if model.status == "PENDING":
        job = to_domain(model)
        job.start(at=at)
        apply_domain_to_orm(job, model)
    model.claim_token = claim_token
    model.lease_expires_at = lease_expires_at
