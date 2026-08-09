"""SQLAlchemy repository for tool invocation ledger tables."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from atlas.persistence.models.research_job import ResearchJobModel
from atlas.persistence.models.tool_invocation import (
    ToolInvocationAttemptModel,
    ToolInvocationModel,
)


class ToolInvocationRecord:
    """Read model for a logical tool invocation row."""

    __slots__ = (
        "id",
        "invocation_key",
        "origin",
        "research_job_id",
        "workflow_execution_id",
        "node_name",
        "workflow_node_attempt",
        "actor_id",
        "tool_id",
        "tool_version",
        "provider",
        "tool_policy_version",
        "input_fingerprint",
        "status",
        "output_summary_json",
        "content_digest",
        "byte_length",
        "latency_ms",
        "error_class",
        "retry_class",
        "started_at",
        "finished_at",
    )

    def __init__(self, row: ToolInvocationModel) -> None:
        self.id = row.id
        self.invocation_key = row.invocation_key
        self.origin = row.origin
        self.research_job_id = row.research_job_id
        self.workflow_execution_id = row.workflow_execution_id
        self.node_name = row.node_name
        self.workflow_node_attempt = row.workflow_node_attempt
        self.actor_id = row.actor_id
        self.tool_id = row.tool_id
        self.tool_version = row.tool_version
        self.provider = row.provider
        self.tool_policy_version = row.tool_policy_version
        self.input_fingerprint = row.input_fingerprint
        self.status = row.status
        self.output_summary_json = row.output_summary_json
        self.content_digest = row.content_digest
        self.byte_length = row.byte_length
        self.latency_ms = row.latency_ms
        self.error_class = row.error_class
        self.retry_class = row.retry_class
        self.started_at = row.started_at
        self.finished_at = row.finished_at


class ToolInvocationAttemptRecord:
    """Read model for a physical tool attempt row."""

    __slots__ = (
        "id",
        "invocation_id",
        "attempt",
        "status",
        "deadline_at",
        "started_at",
        "finished_at",
        "error_class",
    )

    def __init__(self, row: ToolInvocationAttemptModel) -> None:
        self.id = row.id
        self.invocation_id = row.invocation_id
        self.attempt = row.attempt
        self.status = row.status
        self.deadline_at = row.deadline_at
        self.started_at = row.started_at
        self.finished_at = row.finished_at
        self.error_class = row.error_class


class SqlAlchemyToolInvocationRepository:
    """Persist logical tool invocations and physical attempts."""

    def get_by_key(
        self,
        session: Session,
        invocation_key: str,
        *,
        for_update: bool = False,
    ) -> ToolInvocationRecord | None:
        stmt = select(ToolInvocationModel).where(
            ToolInvocationModel.invocation_key == invocation_key
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = session.execute(stmt).scalar_one_or_none()
        return ToolInvocationRecord(row) if row is not None else None

    def create_invocation(
        self,
        session: Session,
        *,
        invocation_id: str,
        invocation_key: str,
        origin: str,
        research_job_id: str | None,
        workflow_execution_id: str | None,
        node_name: str | None,
        workflow_node_attempt: int | None,
        actor_id: str | None,
        tool_id: str,
        tool_version: str,
        provider: str,
        tool_policy_version: str,
        input_fingerprint: str,
        at: datetime,
    ) -> None:
        session.add(
            ToolInvocationModel(
                id=invocation_id,
                invocation_key=invocation_key,
                origin=origin,
                research_job_id=research_job_id,
                workflow_execution_id=workflow_execution_id,
                node_name=node_name,
                workflow_node_attempt=workflow_node_attempt,
                actor_id=actor_id,
                tool_id=tool_id,
                tool_version=tool_version,
                provider=provider,
                tool_policy_version=tool_policy_version,
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
        workflow_execution_id: str | None,
        workflow_node_attempt: int | None,
        at: datetime,
    ) -> None:
        row = session.get(ToolInvocationModel, invocation_id)
        if row is None:
            raise LookupError("tool invocation not found")
        row.status = "IN_PROGRESS"
        if row.origin == "WORKFLOW":
            row.workflow_execution_id = workflow_execution_id
            row.workflow_node_attempt = workflow_node_attempt
        row.output_summary_json = None
        row.content_digest = None
        row.byte_length = None
        row.latency_ms = None
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
                func.coalesce(func.max(ToolInvocationAttemptModel.attempt), 0)
            ).where(ToolInvocationAttemptModel.invocation_id == invocation_id)
        ).scalar_one()
        attempt_number = int(next_attempt) + 1
        session.add(
            ToolInvocationAttemptModel(
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
    ) -> ToolInvocationAttemptRecord | None:
        row = session.execute(
            select(ToolInvocationAttemptModel)
            .where(ToolInvocationAttemptModel.invocation_id == invocation_id)
            .order_by(ToolInvocationAttemptModel.attempt.desc())
            .limit(1)
        ).scalar_one_or_none()
        return ToolInvocationAttemptRecord(row) if row is not None else None

    def complete_attempt(
        self,
        session: Session,
        *,
        attempt_id: str,
        latency_ms: int,
        at: datetime,
    ) -> bool:
        result = session.execute(
            update(ToolInvocationAttemptModel)
            .where(
                ToolInvocationAttemptModel.id == attempt_id,
                ToolInvocationAttemptModel.status == "STARTED",
            )
            .values(
                status="SUCCEEDED",
                latency_ms=latency_ms,
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
        result = session.execute(
            update(ToolInvocationAttemptModel)
            .where(
                ToolInvocationAttemptModel.id == attempt_id,
                ToolInvocationAttemptModel.status == "STARTED",
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
        output_summary_json: dict[str, Any],
        content_digest: str | None,
        byte_length: int | None,
        latency_ms: int,
        at: datetime,
    ) -> bool:
        max_attempt = (
            select(func.max(ToolInvocationAttemptModel.attempt))
            .where(ToolInvocationAttemptModel.invocation_id == invocation_id)
            .scalar_subquery()
        )
        ownership = (
            select(ToolInvocationAttemptModel.id)
            .where(
                ToolInvocationAttemptModel.id == attempt_id,
                ToolInvocationAttemptModel.invocation_id == invocation_id,
                ToolInvocationAttemptModel.status == "SUCCEEDED",
                ToolInvocationAttemptModel.attempt == max_attempt,
            )
            .exists()
        )
        result = session.execute(
            update(ToolInvocationModel)
            .where(
                ToolInvocationModel.id == invocation_id,
                ownership,
            )
            .values(
                status="SUCCEEDED",
                output_summary_json=output_summary_json,
                content_digest=content_digest,
                byte_length=byte_length,
                latency_ms=latency_ms,
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
        max_attempt = (
            select(func.max(ToolInvocationAttemptModel.attempt))
            .where(ToolInvocationAttemptModel.invocation_id == invocation_id)
            .scalar_subquery()
        )
        ownership = (
            select(ToolInvocationAttemptModel.id)
            .where(
                ToolInvocationAttemptModel.id == attempt_id,
                ToolInvocationAttemptModel.invocation_id == invocation_id,
                ToolInvocationAttemptModel.status == "FAILED",
                ToolInvocationAttemptModel.attempt == max_attempt,
            )
            .exists()
        )
        result = session.execute(
            update(ToolInvocationModel)
            .where(
                and_(
                    ToolInvocationModel.id == invocation_id,
                    ownership,
                    ToolInvocationModel.status.in_(("IN_PROGRESS", "FAILED")),
                )
            )
            .values(
                status="FAILED",
                output_summary_json=None,
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
