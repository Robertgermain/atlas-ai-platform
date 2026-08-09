"""Application ports for research-job persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.orm import Session

from atlas.domain import ResearchJob


@dataclass(frozen=True, slots=True)
class ResearchJobIdempotencyRecord:
    """Application-facing job plus stored request fingerprint for idempotent replay."""

    job: ResearchJob
    request_fingerprint: str


@dataclass(frozen=True, slots=True)
class ClaimedResearchJob:
    """Domain job plus opaque claim token for fenced worker finalization."""

    job: ResearchJob
    claim_token: str


class ResearchJobRepository(Protocol):
    """Persistence operations required by research-job application services."""

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

    def claim_next(
        self,
        session: Session,
        *,
        now: datetime,
        lease_expires_at: datetime,
        claim_token: str,
    ) -> ClaimedResearchJob | None:
        """Atomically claim the next eligible job and attach claim metadata."""

    def finalize_completion(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        result: str,
        at: datetime,
    ) -> bool:
        """Complete a RUNNING job when the claim token still owns it."""

    def finalize_failure(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        reason: str,
        at: datetime,
    ) -> bool:
        """Fail a RUNNING job when the claim token still owns it."""
