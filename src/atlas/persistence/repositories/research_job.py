"""SQLAlchemy repository for research jobs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas.application.ports import ClaimedResearchJob, ResearchJobIdempotencyRecord
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.persistence.exceptions import (
    IdempotencyKeyConflictError,
    ResearchJobAlreadyExistsError,
    ResearchJobNotFoundError,
)
from atlas.persistence.mappers.research_job import (
    apply_domain_to_orm,
    to_domain,
    to_orm,
)
from atlas.persistence.models import ResearchJobModel

_UNIQUE_VIOLATION_SQLSTATE = "23505"
_PRIMARY_KEY_CONSTRAINT = "research_jobs_pkey"
_IDEMPOTENCY_KEY_CONSTRAINT = "uq_research_jobs_idempotency_key"


def _is_unique_violation(error: IntegrityError) -> bool:
    """Return True when the integrity failure is a PostgreSQL unique/PK violation."""
    orig = error.orig
    if orig is None:
        return False
    sqlstate = getattr(orig, "sqlstate", None)
    if sqlstate == _UNIQUE_VIOLATION_SQLSTATE:
        return True
    return getattr(orig, "pgcode", None) == _UNIQUE_VIOLATION_SQLSTATE


def _constraint_name(error: IntegrityError) -> str | None:
    orig = error.orig
    if orig is None:
        return None
    diag = getattr(orig, "diag", None)
    if diag is not None:
        name = getattr(diag, "constraint_name", None)
        if isinstance(name, str) and name:
            return name
    return None


class SqlAlchemyResearchJobRepository:
    """Persist and load ResearchJob aggregates with an explicit session."""

    def add(
        self,
        session: Session,
        job: ResearchJob,
        *,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> None:
        """Insert a new research job with required idempotency metadata."""
        model = to_orm(job)
        model.idempotency_key = idempotency_key
        model.request_fingerprint = request_fingerprint
        session.add(model)
        try:
            session.flush()
        except IntegrityError as err:
            if _is_unique_violation(err):
                constraint = _constraint_name(err)
                if constraint == _PRIMARY_KEY_CONSTRAINT:
                    raise ResearchJobAlreadyExistsError(job.id) from err
                if constraint == _IDEMPOTENCY_KEY_CONSTRAINT:
                    raise IdempotencyKeyConflictError() from err
            raise

    def get(self, session: Session, job_id: str) -> ResearchJob | None:
        """Load a research job by id, or None if missing."""
        model = session.get(ResearchJobModel, job_id)
        if model is None:
            return None
        return to_domain(model)

    def get_by_idempotency_key(
        self,
        session: Session,
        idempotency_key: str,
    ) -> ResearchJobIdempotencyRecord | None:
        """Load a job and its stored fingerprint by idempotency key."""
        statement = select(ResearchJobModel).where(
            ResearchJobModel.idempotency_key == idempotency_key
        )
        model = session.execute(statement).scalar_one_or_none()
        if model is None:
            return None
        fingerprint = model.request_fingerprint
        if fingerprint is None:
            return None
        return ResearchJobIdempotencyRecord(
            job=to_domain(model),
            request_fingerprint=fingerprint,
        )

    def save(self, session: Session, job: ResearchJob) -> None:
        """Update an existing research job."""
        model = session.get(ResearchJobModel, job.id)
        if model is None:
            raise ResearchJobNotFoundError(job.id)
        apply_domain_to_orm(job, model)
        session.flush()

    def claim_next(
        self,
        session: Session,
        *,
        now: datetime,
        lease_expires_at: datetime,
        claim_token: str,
    ) -> ClaimedResearchJob | None:
        """Atomically claim the next eligible job and attach claim metadata."""
        statement = (
            select(ResearchJobModel)
            .where(
                or_(
                    ResearchJobModel.status == ResearchJobStatus.PENDING.value,
                    and_(
                        ResearchJobModel.status == ResearchJobStatus.RUNNING.value,
                        ResearchJobModel.lease_expires_at.is_not(None),
                        ResearchJobModel.lease_expires_at < now,
                    ),
                )
            )
            .order_by(ResearchJobModel.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        model = session.execute(statement).scalar_one_or_none()
        if model is None:
            return None

        job = to_domain(model)
        if job.status is ResearchJobStatus.PENDING:
            job.start(at=now)
        elif job.status is not ResearchJobStatus.RUNNING:
            return None

        apply_domain_to_orm(job, model)
        model.claim_token = claim_token
        model.lease_expires_at = lease_expires_at
        session.flush()
        return ClaimedResearchJob(job=to_domain(model), claim_token=claim_token)

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
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token):
            return False
        assert model is not None

        job = to_domain(model)
        job.complete(result, at=at)
        apply_domain_to_orm(job, model)
        model.claim_token = None
        model.lease_expires_at = None
        session.flush()
        return True

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
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token):
            return False
        assert model is not None

        job = to_domain(model)
        job.fail(reason, at=at)
        apply_domain_to_orm(job, model)
        model.claim_token = None
        model.lease_expires_at = None
        session.flush()
        return True

    @staticmethod
    def _owns_running_claim(
        model: ResearchJobModel | None,
        *,
        claim_token: str,
    ) -> bool:
        if model is None:
            return False
        if model.status != ResearchJobStatus.RUNNING.value:
            return False
        if model.claim_token != claim_token:
            return False
        return True
