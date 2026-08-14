"""Map between ResearchJob domain entities and ORM models."""

from __future__ import annotations

from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.persistence.models import ResearchJobModel


def to_orm(job: ResearchJob) -> ResearchJobModel:
    """Convert a domain research job into an ORM model instance."""
    return ResearchJobModel(
        id=job.id,
        question=job.question,
        status=job.status.value,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        result=job.result,
        failure_reason=job.failure_reason,
    )


def apply_domain_to_orm(job: ResearchJob, model: ResearchJobModel) -> None:
    """Copy domain fields onto an existing ORM row.

    Evaluation-profile identity is persistence-only and is not a domain
    field. This mapper never invents a profile; an existing ORM value is
    preserved. Production binding happens in ``claim_next``.
    """
    model.question = job.question
    model.status = job.status.value
    model.created_at = job.created_at
    model.updated_at = job.updated_at
    model.started_at = job.started_at
    model.finished_at = job.finished_at
    model.result = job.result
    model.failure_reason = job.failure_reason


def to_domain(model: ResearchJobModel) -> ResearchJob:
    """Convert an ORM row into a reconstituted domain research job."""
    return ResearchJob.reconstitute(
        id=model.id,
        question=model.question,
        status=ResearchJobStatus(model.status),
        created_at=model.created_at,
        updated_at=model.updated_at,
        started_at=model.started_at,
        finished_at=model.finished_at,
        result=model.result,
        failure_reason=model.failure_reason,
    )
