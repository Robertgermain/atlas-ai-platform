"""SQLAlchemy repository for model invocation ledger tables."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from atlas.persistence.models.model_invocation import (
    ModelInvocationAttemptModel,
    ModelInvocationModel,
)
from atlas.persistence.models.research_job import ResearchJobModel


class ModelInvocationRecord:
    """Read model for a logical invocation row."""

    __slots__ = (
        "id",
        "invocation_key",
        "research_job_id",
        "workflow_execution_id",
        "node_name",
        "provider",
        "model",
        "prompt_version",
        "input_fingerprint",
        "status",
        "output_json",
        "provider_request_id",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "latency_ms",
        "estimated_cost_usd",
        "pricing_version",
        "finish_outcome",
        "error_class",
        "retry_class",
        "started_at",
        "finished_at",
    )

    def __init__(self, row: ModelInvocationModel) -> None:
        self.id = row.id
        self.invocation_key = row.invocation_key
        self.research_job_id = row.research_job_id
        self.workflow_execution_id = row.workflow_execution_id
        self.node_name = row.node_name
        self.provider = row.provider
        self.model = row.model
        self.prompt_version = row.prompt_version
        self.input_fingerprint = row.input_fingerprint
        self.status = row.status
        self.output_json = row.output_json
        self.provider_request_id = row.provider_request_id
        self.input_tokens = row.input_tokens
        self.output_tokens = row.output_tokens
        self.total_tokens = row.total_tokens
        self.latency_ms = row.latency_ms
        self.estimated_cost_usd = row.estimated_cost_usd
        self.pricing_version = row.pricing_version
        self.finish_outcome = row.finish_outcome
        self.error_class = row.error_class
        self.retry_class = row.retry_class
        self.started_at = row.started_at
        self.finished_at = row.finished_at


class ModelInvocationAttemptRecord:
    """Read model for a physical attempt row."""

    __slots__ = (
        "id",
        "invocation_id",
        "attempt",
        "status",
        "deadline_at",
        "started_at",
        "finished_at",
        "provider_request_id",
        "error_class",
    )

    def __init__(self, row: ModelInvocationAttemptModel) -> None:
        self.id = row.id
        self.invocation_id = row.invocation_id
        self.attempt = row.attempt
        self.status = row.status
        self.deadline_at = row.deadline_at
        self.started_at = row.started_at
        self.finished_at = row.finished_at
        self.provider_request_id = row.provider_request_id
        self.error_class = row.error_class


class SqlAlchemyModelInvocationRepository:
    """Persist logical invocations and physical provider attempts."""

    def get_by_key(
        self,
        session: Session,
        invocation_key: str,
        *,
        for_update: bool = False,
    ) -> ModelInvocationRecord | None:
        stmt = select(ModelInvocationModel).where(
            ModelInvocationModel.invocation_key == invocation_key
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = session.execute(stmt).scalar_one_or_none()
        return ModelInvocationRecord(row) if row is not None else None

    def get_attempt(
        self,
        session: Session,
        *,
        attempt_id: str,
    ) -> ModelInvocationAttemptRecord | None:
        row = session.get(ModelInvocationAttemptModel, attempt_id)
        return ModelInvocationAttemptRecord(row) if row is not None else None

    def create_invocation(
        self,
        session: Session,
        *,
        invocation_id: str,
        invocation_key: str,
        research_job_id: str,
        workflow_execution_id: str,
        node_name: str,
        provider: str,
        model: str,
        prompt_version: str,
        input_fingerprint: str,
        at: datetime,
    ) -> None:
        session.add(
            ModelInvocationModel(
                id=invocation_id,
                invocation_key=invocation_key,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                node_name=node_name,
                provider=provider,
                model=model,
                prompt_version=prompt_version,
                input_fingerprint=input_fingerprint,
                status="IN_PROGRESS",
                started_at=at,
                finished_at=None,
            )
        )
        session.flush()

    def reopen_invocation(
        self,
        session: Session,
        *,
        invocation_id: str,
        workflow_execution_id: str,
        at: datetime,
    ) -> None:
        row = session.get(ModelInvocationModel, invocation_id)
        if row is None:
            raise LookupError("model invocation not found")
        row.status = "IN_PROGRESS"
        row.workflow_execution_id = workflow_execution_id
        row.output_json = None
        row.provider_request_id = None
        row.input_tokens = None
        row.output_tokens = None
        row.total_tokens = None
        row.latency_ms = None
        row.estimated_cost_usd = None
        row.pricing_version = None
        row.finish_outcome = None
        row.error_class = None
        row.retry_class = None
        row.started_at = at
        row.finished_at = None
        session.flush()

    def begin_attempt(
        self,
        session: Session,
        *,
        attempt_id: str,
        invocation_id: str,
        deadline_at: datetime,
        at: datetime,
    ) -> int:
        next_attempt = session.execute(
            select(
                func.coalesce(func.max(ModelInvocationAttemptModel.attempt), 0)
            ).where(ModelInvocationAttemptModel.invocation_id == invocation_id)
        ).scalar_one()
        attempt_number = int(next_attempt) + 1
        session.add(
            ModelInvocationAttemptModel(
                id=attempt_id,
                invocation_id=invocation_id,
                attempt=attempt_number,
                status="STARTED",
                deadline_at=deadline_at,
                started_at=at,
                finished_at=None,
            )
        )
        session.flush()
        return attempt_number

    def latest_attempt(
        self,
        session: Session,
        *,
        invocation_id: str,
    ) -> ModelInvocationAttemptRecord | None:
        row = session.execute(
            select(ModelInvocationAttemptModel)
            .where(ModelInvocationAttemptModel.invocation_id == invocation_id)
            .order_by(ModelInvocationAttemptModel.attempt.desc())
            .limit(1)
        ).scalar_one_or_none()
        return ModelInvocationAttemptRecord(row) if row is not None else None

    def complete_attempt(
        self,
        session: Session,
        *,
        attempt_id: str,
        provider_request_id: str | None,
        input_tokens: int | None,
        output_tokens: int | None,
        total_tokens: int | None,
        latency_ms: int,
        estimated_cost_usd: float | None,
        pricing_version: str | None,
        finish_outcome: str,
        at: datetime,
    ) -> bool:
        """Transition STARTED→SUCCEEDED. Returns False if ownership was lost."""
        result = session.execute(
            update(ModelInvocationAttemptModel)
            .where(
                ModelInvocationAttemptModel.id == attempt_id,
                ModelInvocationAttemptModel.status == "STARTED",
            )
            .values(
                status="SUCCEEDED",
                provider_request_id=provider_request_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
                estimated_cost_usd=estimated_cost_usd,
                pricing_version=pricing_version,
                finish_outcome=finish_outcome,
                error_class=None,
                retry_class=None,
                finished_at=at,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def fail_attempt(
        self,
        session: Session,
        *,
        attempt_id: str,
        error_class: str,
        retry_class: str,
        at: datetime,
    ) -> bool:
        """Transition STARTED→FAILED. Returns False if ownership was lost."""
        result = session.execute(
            update(ModelInvocationAttemptModel)
            .where(
                ModelInvocationAttemptModel.id == attempt_id,
                ModelInvocationAttemptModel.status == "STARTED",
            )
            .values(
                status="FAILED",
                error_class=error_class,
                retry_class=retry_class,
                finished_at=at,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def complete_invocation_for_attempt(
        self,
        session: Session,
        *,
        invocation_id: str,
        attempt_id: str,
        output_json: dict[str, Any],
        provider_request_id: str | None,
        input_tokens: int | None,
        output_tokens: int | None,
        total_tokens: int | None,
        latency_ms: int,
        estimated_cost_usd: float | None,
        pricing_version: str | None,
        finish_outcome: str,
        at: datetime,
    ) -> bool:
        """Complete the logical invocation only if ``attempt_id`` is still active.

        Active means: the attempt belongs to this invocation, is SUCCEEDED, and
        has the maximum attempt number for the invocation. Returns False when a
        newer attempt has superseded ownership.
        """
        max_attempt = (
            select(func.max(ModelInvocationAttemptModel.attempt))
            .where(ModelInvocationAttemptModel.invocation_id == invocation_id)
            .scalar_subquery()
        )
        ownership = (
            select(ModelInvocationAttemptModel.id)
            .where(
                ModelInvocationAttemptModel.id == attempt_id,
                ModelInvocationAttemptModel.invocation_id == invocation_id,
                ModelInvocationAttemptModel.status == "SUCCEEDED",
                ModelInvocationAttemptModel.attempt == max_attempt,
            )
            .exists()
        )
        result = session.execute(
            update(ModelInvocationModel)
            .where(
                ModelInvocationModel.id == invocation_id,
                ownership,
            )
            .values(
                status="SUCCEEDED",
                output_json=output_json,
                provider_request_id=provider_request_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
                estimated_cost_usd=estimated_cost_usd,
                pricing_version=pricing_version,
                finish_outcome=finish_outcome,
                error_class=None,
                retry_class=None,
                finished_at=at,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def mark_invocation_failed_for_attempt(
        self,
        session: Session,
        *,
        invocation_id: str,
        attempt_id: str,
        error_class: str,
        retry_class: str,
        at: datetime,
    ) -> bool:
        """Fail the logical invocation only if ``attempt_id`` is still active.

        Active means: the attempt belongs to this invocation, is FAILED, and
        has the maximum attempt number. Used both for reclaim and provider
        failure finalization.
        """
        max_attempt = (
            select(func.max(ModelInvocationAttemptModel.attempt))
            .where(ModelInvocationAttemptModel.invocation_id == invocation_id)
            .scalar_subquery()
        )
        ownership = (
            select(ModelInvocationAttemptModel.id)
            .where(
                ModelInvocationAttemptModel.id == attempt_id,
                ModelInvocationAttemptModel.invocation_id == invocation_id,
                ModelInvocationAttemptModel.status == "FAILED",
                ModelInvocationAttemptModel.attempt == max_attempt,
            )
            .exists()
        )
        result = session.execute(
            update(ModelInvocationModel)
            .where(
                and_(
                    ModelInvocationModel.id == invocation_id,
                    ownership,
                    ModelInvocationModel.status.in_(("IN_PROGRESS", "FAILED")),
                )
            )
            .values(
                status="FAILED",
                output_json=None,
                error_class=error_class,
                retry_class=retry_class,
                finished_at=at,
            )
        )
        return int(getattr(result, "rowcount", 0) or 0) == 1

    def job_has_valid_claim(
        self,
        session: Session,
        *,
        research_job_id: str,
        now: datetime,
    ) -> bool:
        row = session.get(ResearchJobModel, research_job_id)
        if row is None:
            return False
        return (
            row.status == "RUNNING"
            and row.claim_token is not None
            and row.lease_expires_at is not None
            and row.lease_expires_at > now
        )
