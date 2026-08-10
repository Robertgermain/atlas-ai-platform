"""SQLAlchemy repository for recovery, review, and policy-decision records."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from atlas.persistence.models.recovery import (
    HumanReviewDecisionModel,
    JobRecoveryAttemptModel,
    PolicyDecisionModel,
)
from atlas.recovery.errors import PolicyDecisionConflictError


class SqlAlchemyRecoveryRepository:
    """CRUD helpers for recovery audit tables."""

    def insert_policy_decision(
        self,
        session: Session,
        *,
        id: str,
        research_job_id: str,
        workflow_execution_id: str | None,
        evaluation_run_id: str | None,
        decision: str,
        failure_category: str,
        reason_code: str,
        decision_fingerprint: str,
        created_at: datetime,
    ) -> str:
        """Insert a policy decision with transaction-safe fingerprint idempotency.

        Uses ``INSERT ... ON CONFLICT DO NOTHING RETURNING id`` so replay never
        calls ``session.rollback()`` and never discards other mutations in the
        caller's transaction.

        Returns the authoritative decision id (new on create, existing on
        identical replay). Raises ``PolicyDecisionConflictError`` when the same
        durable identity exists with inconsistent stored fields.
        """
        statement = (
            insert(PolicyDecisionModel)
            .values(
                id=id,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                evaluation_run_id=evaluation_run_id,
                decision=decision,
                failure_category=failure_category,
                reason_code=reason_code,
                decision_fingerprint=decision_fingerprint,
                created_at=created_at,
            )
            .on_conflict_do_nothing(constraint="uq_policy_decisions_job_fingerprint")
            .returning(PolicyDecisionModel.id)
        )
        inserted_id = session.execute(statement).scalar_one_or_none()
        if inserted_id is not None:
            return str(inserted_id)

        existing = session.execute(
            select(PolicyDecisionModel).where(
                and_(
                    PolicyDecisionModel.research_job_id == research_job_id,
                    PolicyDecisionModel.decision_fingerprint == decision_fingerprint,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            raise PolicyDecisionConflictError(
                "Policy decision conflict without an existing row."
            )
        if (
            existing.decision != decision
            or existing.failure_category != failure_category
            or existing.reason_code != reason_code
            or existing.workflow_execution_id != workflow_execution_id
            or existing.evaluation_run_id != evaluation_run_id
        ):
            raise PolicyDecisionConflictError(
                "Policy decision fingerprint reused with inconsistent fields."
            )
        return existing.id

    def insert_recovery_attempt(
        self,
        session: Session,
        *,
        id: str,
        research_job_id: str,
        policy_decision_id: str,
        abandoned_workflow_execution_id: str,
        attempt_number: int,
        next_attempt_at: datetime,
        created_at: datetime,
    ) -> None:
        session.add(
            JobRecoveryAttemptModel(
                id=id,
                research_job_id=research_job_id,
                policy_decision_id=policy_decision_id,
                abandoned_workflow_execution_id=abandoned_workflow_execution_id,
                attempt_number=attempt_number,
                next_attempt_at=next_attempt_at,
                created_at=created_at,
            )
        )
        session.flush()

    def get_recovery_attempt_by_policy(
        self,
        session: Session,
        *,
        policy_decision_id: str,
    ) -> JobRecoveryAttemptModel | None:
        statement = select(JobRecoveryAttemptModel).where(
            JobRecoveryAttemptModel.policy_decision_id == policy_decision_id
        )
        return session.execute(statement).scalar_one_or_none()

    def get_review_decision_by_idempotency(
        self,
        session: Session,
        *,
        research_job_id: str,
        idempotency_key: str,
    ) -> HumanReviewDecisionModel | None:
        statement = select(HumanReviewDecisionModel).where(
            and_(
                HumanReviewDecisionModel.research_job_id == research_job_id,
                HumanReviewDecisionModel.idempotency_key == idempotency_key,
            )
        )
        return session.execute(statement).scalar_one_or_none()

    def insert_review_decision(
        self,
        session: Session,
        *,
        id: str,
        research_job_id: str,
        workflow_execution_id: str,
        evaluation_run_id: str,
        decision: str,
        candidate_fingerprint: str,
        actor_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        created_at: datetime,
    ) -> None:
        session.add(
            HumanReviewDecisionModel(
                id=id,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                evaluation_run_id=evaluation_run_id,
                decision=decision,
                candidate_fingerprint=candidate_fingerprint,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                created_at=created_at,
            )
        )
        session.flush()

    def get_latest_approve_for_execution(
        self,
        session: Session,
        *,
        research_job_id: str,
        workflow_execution_id: str,
    ) -> HumanReviewDecisionModel | None:
        """Return the most recent approve decision for a specific execution."""
        statement = (
            select(HumanReviewDecisionModel)
            .where(
                and_(
                    HumanReviewDecisionModel.research_job_id == research_job_id,
                    HumanReviewDecisionModel.workflow_execution_id
                    == workflow_execution_id,
                    HumanReviewDecisionModel.decision == "approve",
                )
            )
            .order_by(HumanReviewDecisionModel.created_at.desc())
            .limit(1)
        )
        return session.execute(statement).scalar_one_or_none()
