"""Application ports for research-job persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.orm import Session

from atlas.application.job_processing import ContinuationMode
from atlas.domain import ResearchJob


@dataclass(frozen=True, slots=True)
class ResearchJobIdempotencyRecord:
    """Application-facing job plus stored request fingerprint for idempotent replay."""

    job: ResearchJob
    request_fingerprint: str


@dataclass(frozen=True, slots=True)
class ClaimedResearchJob:
    """Domain job plus opaque claim token and continuation context for fenced worker.

    ``traceparent``/``use_traceparent_as_parent`` (Slice 15A3) are the
    persistence-layer's resolved tracing decision for this specific claim --
    the worker must not attempt to reconstruct eligibility itself from
    ``continuation_mode``/``active_workflow_execution_id``. See
    ``atlas.persistence.repositories.research_job.claim_next`` for exactly
    how/when ``use_traceparent_as_parent`` becomes ``True`` (at most once per
    row, ever) and the Slice 15A3 migration docstring for the full contract.
    """

    job: ResearchJob
    claim_token: str
    continuation_mode: ContinuationMode
    active_workflow_execution_id: str | None
    traceparent: str | None = None
    use_traceparent_as_parent: bool = False
    evaluation_profile: str | None = None


class ResearchJobRepository(Protocol):
    """Persistence operations required by research-job application services."""

    def add(
        self,
        session: Session,
        job: ResearchJob,
        *,
        idempotency_key: str,
        request_fingerprint: str,
        traceparent: str | None = None,
    ) -> None:
        """Insert a new research job with required idempotency metadata.

        ``traceparent`` (Slice 15A3) is the W3C trace context active at
        submission time, stored verbatim for later worker-side continuation
        decisions. Persistence-only: never re-exposed through any public API
        or domain model.
        """

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
        evaluation_profile: str | None = None,
    ) -> ClaimedResearchJob | None:
        """Atomically claim the next eligible job and attach claim metadata.

        ``evaluation_profile`` is the worker's composed profile. Unbound jobs
        are bound to it in this transaction. Jobs bound to a different profile
        are not claimed.
        """

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

    def set_active_workflow_execution(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        execution_id: str,
        at: datetime,
    ) -> bool:
        """Bind a workflow execution to a claimed RUNNING job."""

    def schedule_retry(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        next_attempt_at: datetime,
        at: datetime,
    ) -> bool:
        """Transition a claimed RUNNING job to delayed PENDING with JOB_RETRY mode."""

    def transition_awaiting_review(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Transition a claimed RUNNING job to AWAITING_REVIEW."""

    def approve_review_to_pending(
        self,
        session: Session,
        *,
        job_id: str,
        execution_id: str,
        next_attempt_at: datetime,
        at: datetime,
    ) -> bool:
        """Transition AWAITING_REVIEW → delayed PENDING with REVIEW_COMPLETE mode."""

    def fail_from_review(
        self,
        session: Session,
        *,
        job_id: str,
        reason: str,
        at: datetime,
    ) -> bool:
        """Transition AWAITING_REVIEW → FAILED (operator reject)."""
