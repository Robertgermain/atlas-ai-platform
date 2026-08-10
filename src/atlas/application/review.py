"""Operator review service for human approve/reject decisions."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from langchain_core.runnables import RunnableConfig

from atlas.evaluation.service import EvaluationService
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.recovery import SqlAlchemyRecoveryRepository
from atlas.persistence.repositories.research_job import (
    SqlAlchemyResearchJobRepository,
)
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.workflow.graph import build_research_graph
from atlas.workflow.processor import create_checkpoint_runtime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker


class ReviewConflictError(Exception):
    """Raised when a review key replay conflicts."""


class ReviewReadinessError(Exception):
    """Raised when the job is not ready for review."""


class ReviewNotFoundError(Exception):
    """Raised when the research job does not exist."""


class ReviewService:
    """Coordinate operator approve/reject decisions for AWAITING_REVIEW jobs."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        database_url: str,
        job_repo: SqlAlchemyResearchJobRepository | None = None,
        recovery_repo: SqlAlchemyRecoveryRepository | None = None,
        workflow_repo: SqlAlchemyWorkflowRepository | None = None,
        evaluation_service: EvaluationService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._database_url = database_url
        self._job_repo = job_repo or SqlAlchemyResearchJobRepository()
        self._recovery_repo = recovery_repo or SqlAlchemyRecoveryRepository()
        self._workflow_repo = workflow_repo or SqlAlchemyWorkflowRepository()
        self._evaluation_service = evaluation_service or EvaluationService(
            session_factory=session_factory,
        )

    def submit_decision(
        self,
        *,
        job_id: str,
        decision: str,
        actor_id: str,
        idempotency_key: str,
        evaluation_run_id: str | None = None,
    ) -> tuple[str, str]:
        """Process a review decision. Returns (decision_id, status).

        status is 'created' for new or 'replayed' for idempotent replay.
        Raises ReviewConflictError on key reuse with different payload.
        Raises ReviewReadinessError when readiness checks fail.
        Raises ReviewNotFoundError when the job is missing.
        """
        cleaned_actor = actor_id.strip()
        if not cleaned_actor:
            raise ValueError("actor_id must be non-empty")
        if decision not in {"approve", "reject"}:
            raise ValueError("decision must be approve or reject")

        request_fingerprint = self._fingerprint_request(
            decision=decision,
            actor_id=cleaned_actor,
            evaluation_run_id=evaluation_run_id,
        )

        with session_scope(self._session_factory) as session:
            from atlas.persistence.models import ResearchJobModel

            job_model = session.get(ResearchJobModel, job_id, with_for_update=True)
            if job_model is None:
                raise ReviewNotFoundError("Job not found")

            existing = self._recovery_repo.get_review_decision_by_idempotency(
                session,
                research_job_id=job_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                if existing.request_fingerprint == request_fingerprint:
                    return (existing.id, "replayed")
                raise ReviewConflictError(
                    "Idempotency key reused with different payload"
                )

            if job_model.status != "AWAITING_REVIEW":
                raise ReviewReadinessError(
                    f"Job status is {job_model.status}, not AWAITING_REVIEW"
                )

            exec_id = job_model.active_workflow_execution_id
            if not exec_id:
                raise ReviewReadinessError("No active workflow execution for review")

            exec_model = self._workflow_repo.get_execution(
                session, execution_id=exec_id
            )
            if (
                exec_model is None
                or exec_model.research_job_id != job_id
                or exec_model.status != "RUNNING"
            ):
                raise ReviewReadinessError(
                    "Active workflow execution is not resumable for this job"
                )

            runs = self._evaluation_service.get_by_job(job_id)
            latest_failed = None
            for run in reversed(runs):
                if (
                    run.status == "SUCCEEDED"
                    and not run.passed
                    and run.workflow_execution_id == exec_id
                ):
                    latest_failed = run
                    break

            if latest_failed is None:
                raise ReviewReadinessError(
                    "No failed evaluation run found for review target"
                )

            target_eval_id = evaluation_run_id or latest_failed.run_id
            if target_eval_id != latest_failed.run_id:
                matching = next(
                    (run for run in runs if run.run_id == target_eval_id),
                    None,
                )
                if (
                    matching is None
                    or matching.workflow_execution_id != exec_id
                    or matching.passed
                    or matching.status != "SUCCEEDED"
                ):
                    raise ReviewReadinessError(
                        "evaluation_run_id does not match review target"
                    )
                latest_failed = matching

            candidate_fingerprint = latest_failed.input_fingerprint

        # Checkpoint readiness outside the job row lock but before durable write.
        if not self._checkpoint_ready_for_complete(exec_id):
            raise ReviewReadinessError(
                "Checkpoint is not resumable with next==('complete',)"
            )

        with session_scope(self._session_factory) as session:
            from atlas.persistence.models import ResearchJobModel

            job_model = session.get(ResearchJobModel, job_id, with_for_update=True)
            if job_model is None:
                raise ReviewNotFoundError("Job not found")

            existing = self._recovery_repo.get_review_decision_by_idempotency(
                session,
                research_job_id=job_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                if existing.request_fingerprint == request_fingerprint:
                    return (existing.id, "replayed")
                raise ReviewConflictError(
                    "Idempotency key reused with different payload"
                )

            if job_model.status != "AWAITING_REVIEW":
                raise ReviewReadinessError(
                    f"Job status is {job_model.status}, not AWAITING_REVIEW"
                )
            if job_model.active_workflow_execution_id != exec_id:
                raise ReviewReadinessError("Active execution changed during review")

            at = datetime.now(UTC)
            decision_id = str(uuid4())

            self._recovery_repo.insert_review_decision(
                session,
                id=decision_id,
                research_job_id=job_id,
                workflow_execution_id=exec_id,
                evaluation_run_id=target_eval_id,
                decision=decision,
                candidate_fingerprint=candidate_fingerprint,
                actor_id=cleaned_actor,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                created_at=at,
            )

            if decision == "approve":
                ok = self._job_repo.approve_review_to_pending(
                    session,
                    job_id=job_id,
                    execution_id=exec_id,
                    next_attempt_at=at,
                    at=at,
                )
                if not ok:
                    raise ReviewConflictError(
                        "approve_review_to_pending failed (state conflict)"
                    )
            elif decision == "reject":
                fail_ok = self._workflow_repo.fail_execution(
                    session, execution_id=exec_id, at=at
                )
                if not fail_ok:
                    raise ReviewConflictError(
                        "fail_execution failed (execution not RUNNING)"
                    )
                ok = self._job_repo.fail_from_review(
                    session,
                    job_id=job_id,
                    reason="Rejected by operator review",
                    at=at,
                )
                if not ok:
                    raise ReviewConflictError(
                        "fail_from_review failed (state conflict)"
                    )

            return (decision_id, "created")

    def _checkpoint_ready_for_complete(self, execution_id: str) -> bool:
        """Return True when durable checkpoint next is exactly ('complete',)."""
        runtime = create_checkpoint_runtime(self._database_url)
        try:
            graph = build_research_graph(checkpointer=runtime.checkpointer)
            config: RunnableConfig = {
                "configurable": {"thread_id": execution_id},
            }
            snapshot = graph.get_state(config)
            return snapshot.next == ("complete",)
        finally:
            runtime.close()

    @staticmethod
    def _fingerprint_request(
        *,
        decision: str,
        actor_id: str,
        evaluation_run_id: str | None,
    ) -> str:
        payload = json.dumps(
            {
                "actor_id": actor_id,
                "decision": decision,
                "evaluation_run_id": evaluation_run_id or "",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
