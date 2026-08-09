"""Application ports for research-job persistence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.orm import Session

from atlas.domain import ResearchJob


@dataclass(frozen=True, slots=True)
class ResearchJobIdempotencyRecord:
    """Application-facing job plus stored request fingerprint for idempotent replay."""

    job: ResearchJob
    request_fingerprint: str


class ResearchJobRepository(Protocol):
    """Persistence operations required by the research-job application service."""

    def add(
        self,
        session: Session,
        job: ResearchJob,
        *,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> None:
        """Insert a new research job with required idempotency metadata."""

    def get(self, session: Session, job_id: str) -> ResearchJob | None:
        """Load a research job by id, or None if missing."""

    def get_by_idempotency_key(
        self,
        session: Session,
        idempotency_key: str,
    ) -> ResearchJobIdempotencyRecord | None:
        """Load a job and its stored fingerprint by idempotency key."""

    def save(self, session: Session, job: ResearchJob) -> None:
        """Update an existing research job."""
