"""SQLAlchemy repository for research jobs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from atlas.application.job_processing import ContinuationMode
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
        traceparent: str | None = None,
    ) -> None:
        """Insert a new research job with required idempotency metadata."""
        model = to_orm(job)
        model.idempotency_key = idempotency_key
        model.request_fingerprint = request_fingerprint
        model.traceparent = traceparent
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
                    and_(
                        ResearchJobModel.status == ResearchJobStatus.PENDING.value,
                        or_(
                            ResearchJobModel.next_attempt_at.is_(None),
                            ResearchJobModel.next_attempt_at <= now,
                        ),
                    ),
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

        if model.status == ResearchJobStatus.PENDING.value:
            try:
                effective_mode = ContinuationMode(model.continuation_mode)
            except ValueError as exc:
                raise ValueError(
                    f"Unknown continuation_mode: {model.continuation_mode!r}"
                ) from exc
            model.claimed_continuation_mode = effective_mode.value
            model.continuation_mode = ContinuationMode.NONE.value

            job = to_domain(model)
            if job.started_at is None:
                job.start(at=now)
            else:
                job.resume_from_pending(at=now)

            apply_domain_to_orm(job, model)
            model.claim_token = claim_token
            model.lease_expires_at = lease_expires_at
            model.next_attempt_at = None
            use_traceparent_as_parent = self._maybe_consume_initial_traceparent(
                model, now=now
            )
            session.flush()
            return ClaimedResearchJob(
                job=to_domain(model),
                claim_token=claim_token,
                continuation_mode=effective_mode,
                active_workflow_execution_id=model.active_workflow_execution_id,
                traceparent=model.traceparent,
                use_traceparent_as_parent=use_traceparent_as_parent,
            )

        if model.status == ResearchJobStatus.RUNNING.value:
            try:
                claimed_mode = ContinuationMode(model.claimed_continuation_mode)
            except ValueError as exc:
                raise ValueError(
                    "Unknown claimed_continuation_mode: "
                    f"{model.claimed_continuation_mode!r}"
                ) from exc
            model.claim_token = claim_token
            model.lease_expires_at = lease_expires_at
            job = to_domain(model)
            apply_domain_to_orm(job, model)
            # A RUNNING-status claim is always a crash/lease reclaim of a row
            # some earlier claim already started processing (this branch is
            # never reached for a row's very first claim -- that is always
            # PENDING, above). By the time any transaction reaches this
            # branch, the row's first-ever PENDING claim already either
            # consumed `initial_traceparent_consumed_at` in its own committed
            # transaction (if it had a stored `traceparent`) or found none to
            # consume -- either way this call is always a no-op here and
            # always returns False. It is still routed through the same
            # helper (rather than hardcoding `False`) so the one durable rule
            # -- "consume at most once, ever" -- has exactly one
            # implementation to audit instead of two that must agree.
            use_traceparent_as_parent = self._maybe_consume_initial_traceparent(
                model, now=now
            )
            session.flush()
            return ClaimedResearchJob(
                job=to_domain(model),
                claim_token=claim_token,
                continuation_mode=claimed_mode,
                active_workflow_execution_id=model.active_workflow_execution_id,
                traceparent=model.traceparent,
                use_traceparent_as_parent=use_traceparent_as_parent,
            )

        return None

    @staticmethod
    def _maybe_consume_initial_traceparent(
        model: ResearchJobModel, *, now: datetime
    ) -> bool:
        """Atomically grant first-parent eligibility to at most one claim, ever.

        Safe without a separate ``UPDATE ... WHERE`` guard because the
        caller (``claim_next``) already holds this row's exclusive lock
        (``with_for_update(skip_locked=True)``) for the remainder of this
        transaction -- no concurrent transaction can observe or mutate
        ``initial_traceparent_consumed_at`` on this row until this one
        commits or rolls back. Returns ``True`` only the one time this call
        itself transitions the column from ``NULL`` to non-``NULL``; every
        later call for the same row (regardless of ``continuation_mode``)
        always returns ``False``. Returns ``False`` without any mutation when
        the row has no stored ``traceparent`` to consume.
        """
        if model.traceparent is None:
            return False
        if model.initial_traceparent_consumed_at is not None:
            return False
        model.initial_traceparent_consumed_at = now
        return True

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
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None

        job = to_domain(model)
        job.complete(result, at=at)
        apply_domain_to_orm(job, model)
        self._clear_claim_and_continuation(model)
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
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None

        job = to_domain(model)
        job.fail(reason, at=at)
        apply_domain_to_orm(job, model)
        self._clear_claim_and_continuation(model)
        session.flush()
        return True

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
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None
        model.active_workflow_execution_id = execution_id
        session.flush()
        return True

    def schedule_retry(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        next_attempt_at: datetime,
        at: datetime,
        abandon_execution_id: str | None = None,
    ) -> bool:
        """Transition a claimed RUNNING job to delayed PENDING with JOB_RETRY mode.

        Verifies the claim before any mutation. When ``abandon_execution_id`` is
        set, abandons that RUNNING execution for this job in the same transaction
        before the job transition; both succeed or neither does.
        """
        from atlas.persistence.models.workflow import WorkflowExecutionModel

        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None

        if abandon_execution_id is not None:
            if model.active_workflow_execution_id != abandon_execution_id:
                return False
            result = session.execute(
                update(WorkflowExecutionModel)
                .where(
                    WorkflowExecutionModel.id == abandon_execution_id,
                    WorkflowExecutionModel.research_job_id == job_id,
                    WorkflowExecutionModel.status == "RUNNING",
                )
                .values(status="ABANDONED", finished_at=at)
            )
            if int(getattr(result, "rowcount", 0) or 0) == 0:
                return False

        model.job_retry_count = model.job_retry_count + 1
        job = to_domain(model)
        job.return_to_pending(at=at)
        apply_domain_to_orm(job, model)
        model.claim_token = None
        model.lease_expires_at = None
        model.continuation_mode = ContinuationMode.JOB_RETRY
        model.claimed_continuation_mode = ContinuationMode.NONE
        model.next_attempt_at = next_attempt_at
        model.active_workflow_execution_id = None
        session.flush()
        return True

    def transition_awaiting_review(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Transition a claimed RUNNING job to AWAITING_REVIEW."""
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None

        job = to_domain(model)
        job.await_review(at=at)
        apply_domain_to_orm(job, model)
        model.claim_token = None
        model.lease_expires_at = None
        model.claimed_continuation_mode = ContinuationMode.NONE
        session.flush()
        return True

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
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if model is None:
            return False
        if model.status != ResearchJobStatus.AWAITING_REVIEW.value:
            return False

        job = to_domain(model)
        job.approve_to_pending(at=at)
        apply_domain_to_orm(job, model)
        model.continuation_mode = ContinuationMode.REVIEW_COMPLETE
        model.next_attempt_at = next_attempt_at
        model.active_workflow_execution_id = execution_id
        session.flush()
        return True

    def fail_from_review(
        self,
        session: Session,
        *,
        job_id: str,
        reason: str,
        at: datetime,
    ) -> bool:
        """Transition AWAITING_REVIEW → FAILED (operator reject)."""
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if model is None:
            return False
        if model.status != ResearchJobStatus.AWAITING_REVIEW.value:
            return False

        job = to_domain(model)
        job.fail_from_review(reason, at=at)
        apply_domain_to_orm(job, model)
        self._clear_claim_and_continuation(model)
        session.flush()
        return True

    def increment_repair_count(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Increment repair_count by 1 under claim fence (max 1)."""
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None
        if model.repair_count >= 1:
            return False
        model.repair_count = model.repair_count + 1
        session.flush()
        return True

    def increment_evaluation_attempt_count(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Increment evaluation_attempt_count by 1 under claim fence (max 4)."""
        model = session.get(ResearchJobModel, job_id, with_for_update=True)
        if not self._owns_running_claim(model, claim_token=claim_token, at=at):
            return False
        assert model is not None
        if model.evaluation_attempt_count >= 4:
            return False
        model.evaluation_attempt_count = model.evaluation_attempt_count + 1
        session.flush()
        return True

    def get_attempt_counts(
        self,
        session: Session,
        *,
        job_id: str,
    ) -> tuple[int, int, int]:
        """Return (repair_count, job_retry_count, evaluation_attempt_count)."""
        model = session.get(ResearchJobModel, job_id)
        if model is None:
            return (0, 0, 0)
        return (
            model.repair_count,
            model.job_retry_count,
            model.evaluation_attempt_count,
        )

    @staticmethod
    def _owns_running_claim(
        model: ResearchJobModel | None,
        *,
        claim_token: str,
        at: datetime,
    ) -> bool:
        if model is None:
            return False
        if model.status != ResearchJobStatus.RUNNING.value:
            return False
        if model.claim_token != claim_token:
            return False
        if model.lease_expires_at is None:
            return False
        if model.lease_expires_at <= at:
            return False
        return True

    @staticmethod
    def _clear_claim_and_continuation(model: ResearchJobModel) -> None:
        """Reset all claim, lease, continuation, and active execution fields."""
        model.claim_token = None
        model.lease_expires_at = None
        model.next_attempt_at = None
        model.continuation_mode = ContinuationMode.NONE
        model.claimed_continuation_mode = ContinuationMode.NONE
        model.active_workflow_execution_id = None
