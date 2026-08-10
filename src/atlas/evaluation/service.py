"""Fenced evaluation service with ownership-token finalization."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from atlas.evaluation.claim_fingerprint import fingerprint_job_claim_token
from atlas.evaluation.contracts import (
    DimensionResult,
    DispositionHint,
    EvaluationProfile,
    EvaluationRunResult,
)
from atlas.evaluation.errors import (
    EvaluationAttemptCapError,
    EvaluationConflictError,
    EvaluationInProgressError,
    EvaluationNotFoundError,
    EvaluationOwnershipLostError,
    EvaluationValidationError,
)
from atlas.evaluation.repository import SqlAlchemyEvaluationRepository
from atlas.persistence.db import session_scope
from atlas.persistence.models.research_job import ResearchJobModel
from atlas.persistence.models.workflow import WorkflowExecutionModel
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.recovery.policy import MAX_EVALUATION_ATTEMPTS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker


class EvaluationService:
    """Begin/resume evaluation runs and finalize under ownership fencing."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: SqlAlchemyEvaluationRepository | None = None,
        job_repository: SqlAlchemyResearchJobRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or SqlAlchemyEvaluationRepository()
        self._job_repository = job_repository or SqlAlchemyResearchJobRepository()

    @staticmethod
    def _new_ownership_token() -> str:
        return secrets.token_hex(32)

    @staticmethod
    def _assert_execution_belongs_to_job(
        session: Session,
        *,
        workflow_execution_id: str,
        research_job_id: str,
    ) -> None:
        execution = session.get(WorkflowExecutionModel, workflow_execution_id)
        if execution is None or execution.research_job_id != research_job_id:
            raise EvaluationValidationError(
                "Workflow execution does not belong to the research job."
            )

    @staticmethod
    def _assert_holds_current_job_claim(
        session: Session,
        *,
        research_job_id: str,
        job_claim_token: str,
        now: datetime,
    ) -> str:
        """Prove possession of the job's current valid claim; return fingerprint.

        Compares the presented token to the durable job claim in-memory only.
        Never persists or logs the raw token.
        """
        presented_fingerprint = fingerprint_job_claim_token(job_claim_token)
        row = session.get(ResearchJobModel, research_job_id)
        if row is None:
            raise EvaluationValidationError(
                "Research job is missing for evaluation claim proof."
            )
        if not (
            row.status == "RUNNING"
            and row.claim_token is not None
            and row.lease_expires_at is not None
            and row.lease_expires_at > now
            and row.claim_token == job_claim_token
        ):
            raise EvaluationValidationError(
                "Current valid job claim is required to create or reclaim "
                "an evaluation."
            )
        return presented_fingerprint

    def begin_or_resume(
        self,
        *,
        execution_id: str,
        profile: EvaluationProfile,
        attempt: int,
        fingerprint: str,
        job_id: str,
        deadline: datetime,
        job_claim_token: str,
    ) -> tuple[str, str, EvaluationRunResult | None]:
        """Lock or create the evaluation attempt row.

        Returns ``(run_id, ownership_token, replay_result)``. When a matching
        succeeded fingerprint exists, ``replay_result`` is set and the returned
        ownership token is empty (no further finalize needed).

        Create/reclaim requires proof of the current valid research-job claim.
        Succeeded same-fingerprint replay is a durable read and does not require
        a live claim. Raw claim tokens are never persisted.
        """
        now = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            self._assert_execution_belongs_to_job(
                session,
                workflow_execution_id=execution_id,
                research_job_id=job_id,
            )
            existing = self._repository.get_by_execution_key(
                session,
                workflow_execution_id=execution_id,
                evaluation_profile=profile,
                evaluation_attempt=attempt,
                for_update=True,
            )
            if existing is not None and existing.status == "SUCCEEDED":
                if existing.input_fingerprint != fingerprint:
                    raise EvaluationConflictError()
                replay = self._repository.to_result(session, existing)
                return existing.id, "", replay

            claim_fingerprint = self._assert_holds_current_job_claim(
                session,
                research_job_id=job_id,
                job_claim_token=job_claim_token,
                now=now,
            )

            if existing is not None and existing.status == "IN_PROGRESS":
                if now <= existing.deadline_at:
                    # Unexpired: in progress for same or competing callers.
                    raise EvaluationInProgressError()
                # Expired evaluation + caller proved current valid job claim.
                # - Same originating claim still valid: competing owners cannot
                #   pass claim proof; owning claim may refresh eval ownership.
                # - Different current claim (new processing owner): reclaim OK.
                # - No valid claim: anonymous reclaim impossible (proof above).
                if existing.input_fingerprint != fingerprint:
                    raise EvaluationConflictError()
                ownership_token = self._new_ownership_token()
                self._repository.reclaim_run(
                    session,
                    run_id=existing.id,
                    ownership_token=ownership_token,
                    input_fingerprint=fingerprint,
                    job_claim_fingerprint=claim_fingerprint,
                    deadline_at=deadline,
                    at=now,
                )
                return existing.id, ownership_token, None

            if existing is not None and existing.status == "FAILED":
                if existing.input_fingerprint != fingerprint:
                    raise EvaluationConflictError()
                ownership_token = self._new_ownership_token()
                self._repository.reclaim_run(
                    session,
                    run_id=existing.id,
                    ownership_token=ownership_token,
                    input_fingerprint=fingerprint,
                    job_claim_fingerprint=claim_fingerprint,
                    deadline_at=deadline,
                    at=now,
                )
                return existing.id, ownership_token, None

            run_id = str(uuid4())
            ownership_token = self._new_ownership_token()
            # Reserve job-global attempt slot in the same TX as create only.
            # Reclaim/replay paths above never reach here, so they cannot
            # double-increment. Crash before commit rolls back both.
            reserved = self._job_repository.increment_evaluation_attempt_count(
                session,
                job_id=job_id,
                claim_token=job_claim_token,
                at=now,
            )
            if not reserved:
                _, _, eac = self._job_repository.get_attempt_counts(
                    session, job_id=job_id
                )
                if eac >= MAX_EVALUATION_ATTEMPTS:
                    raise EvaluationAttemptCapError()
                from atlas.application.exceptions import ClaimOwnershipError

                raise ClaimOwnershipError("increment_evaluation_attempt_count")
            try:
                self._repository.create_run(
                    session,
                    run_id=run_id,
                    research_job_id=job_id,
                    workflow_execution_id=execution_id,
                    evaluation_profile=profile,
                    evaluation_attempt=attempt,
                    ownership_token=ownership_token,
                    input_fingerprint=fingerprint,
                    job_claim_fingerprint=claim_fingerprint,
                    deadline_at=deadline,
                    at=now,
                )
            except IntegrityError as exc:
                raise EvaluationInProgressError() from exc
            return run_id, ownership_token, None

    def finalize_success(
        self,
        *,
        run_id: str,
        ownership_token: str,
        aggregate: float,
        passed: bool,
        dimensions: list[DimensionResult],
        disposition_hint: DispositionHint | str,
        grader_versions: dict[str, str],
    ) -> EvaluationRunResult:
        """Replace dimensions and fence SUCCEEDED under ownership check."""
        now = datetime.now(UTC)
        _KNOWN_HINTS: set[str] = {
            "complete",
            "terminal",
            "repair",
            "await_review",
            "retry",
        }
        hint: DispositionHint = (
            disposition_hint if disposition_hint in _KNOWN_HINTS else "terminal"  # type: ignore[assignment]
        )
        with session_scope(self._session_factory) as session:
            record = self._repository.get_by_id(
                session,
                run_id,
                for_update=True,
            )
            if (
                record is None
                or record.status != "IN_PROGRESS"
                or record.ownership_token != ownership_token
            ):
                raise EvaluationOwnershipLostError()
            self._repository.replace_dimensions(
                session,
                evaluation_run_id=run_id,
                dimensions=dimensions,
            )
            ok = self._repository.finalize_success(
                session,
                run_id=run_id,
                ownership_token=ownership_token,
                aggregate_score=aggregate,
                passed=passed,
                disposition_hint=hint,
                grader_versions=grader_versions,
                at=now,
            )
            if not ok:
                raise EvaluationOwnershipLostError()
            refreshed = self._repository.get_by_id(session, run_id)
            if refreshed is None:
                raise EvaluationNotFoundError()
            return self._repository.to_result(session, refreshed)

    def finalize_failure(
        self,
        *,
        run_id: str,
        ownership_token: str,
        error_class: str,
    ) -> None:
        """Fence FAILED for a process-level evaluation failure.

        Persists only a sanitized error class name — never raw messages.
        """
        sanitized = error_class.strip() or "EvaluationUnexpectedError"
        if (
            any(ch.isspace() for ch in sanitized)
            or not sanitized.replace("_", "").isalnum()
        ):
            sanitized = "EvaluationUnexpectedError"
        now = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            record = self._repository.get_by_id(
                session,
                run_id,
                for_update=True,
            )
            if (
                record is None
                or record.status != "IN_PROGRESS"
                or record.ownership_token != ownership_token
            ):
                raise EvaluationOwnershipLostError()
            self._repository.clear_dimensions(session, evaluation_run_id=run_id)
            ok = self._repository.finalize_failure(
                session,
                run_id=run_id,
                ownership_token=ownership_token,
                at=now,
                grader_versions={"error_class": sanitized},
            )
            if not ok:
                raise EvaluationOwnershipLostError()

    def get_latest_for_job(self, job_id: str) -> EvaluationRunResult | None:
        with session_scope(self._session_factory) as session:
            record = self._repository.get_latest_for_job(
                session,
                research_job_id=job_id,
            )
            if record is None:
                return None
            return self._repository.to_result(session, record)

    def get_by_job(self, job_id: str) -> list[EvaluationRunResult]:
        with session_scope(self._session_factory) as session:
            records = self._repository.list_for_job(
                session,
                research_job_id=job_id,
            )
            return [self._repository.to_result(session, record) for record in records]
