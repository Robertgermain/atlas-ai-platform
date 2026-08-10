"""SQLAlchemy repository for workflow execution audit history."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from atlas.persistence.models.research_job import ResearchJobModel
from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)

__all__ = ["SqlAlchemyWorkflowRepository"]


class SqlAlchemyWorkflowRepository:
    """Persist workflow and per-attempt node execution records."""

    def abandon_unfinished_for_job(
        self,
        session: Session,
        *,
        research_job_id: str,
        at: datetime,
    ) -> int:
        """Mark unfinished executions for a job as ABANDONED. Returns row count."""
        result = session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.research_job_id == research_job_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="ABANDONED", finished_at=at)
        )
        return int(getattr(result, "rowcount", 0) or 0)

    def abandon_execution(
        self,
        session: Session,
        *,
        execution_id: str,
        at: datetime,
    ) -> bool:
        """Mark a single RUNNING execution as ABANDONED. Returns True when updated.

        Prefer ``abandon_execution_for_claim`` for processor-owned transitions.
        """
        result = session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="ABANDONED", finished_at=at)
        )
        return int(getattr(result, "rowcount", 0) or 0) > 0

    def get_execution(
        self,
        session: Session,
        *,
        execution_id: str,
    ) -> WorkflowExecutionModel | None:
        """Load a workflow execution by id, or None if missing."""
        return session.get(WorkflowExecutionModel, execution_id)

    def create_execution(
        self,
        session: Session,
        *,
        research_job_id: str,
        at: datetime,
        thread_id: str | None = None,
    ) -> str:
        """Insert a RUNNING workflow execution for one worker processing attempt.

        Checkpoint identity is ``thread_id = workflow_execution_id`` (1:1).
        An explicit ``thread_id`` is accepted only for legacy test helpers; when
        omitted, the new execution id is used.
        """
        execution_id = str(uuid4())
        session.add(
            WorkflowExecutionModel(
                id=execution_id,
                research_job_id=research_job_id,
                thread_id=thread_id or execution_id,
                status="RUNNING",
                started_at=at,
                finished_at=None,
            )
        )
        session.flush()
        return execution_id

    def complete_execution(
        self,
        session: Session,
        *,
        execution_id: str,
        at: datetime,
    ) -> bool:
        """Mark RUNNING execution COMPLETED. Returns True when a row was updated.

        Prefer ``complete_execution_for_claim`` for processor-owned transitions.
        """
        result = session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="COMPLETED", finished_at=at)
        )
        return int(getattr(result, "rowcount", 0) or 0) > 0

    def fail_execution(
        self,
        session: Session,
        *,
        execution_id: str,
        at: datetime,
    ) -> bool:
        """Mark RUNNING execution FAILED. Returns True when a row was updated.

        Prefer ``fail_execution_for_claim`` for processor-owned transitions.
        Operator rejection may use this after an AWAITING_REVIEW job lock.
        """
        result = session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="FAILED", finished_at=at)
        )
        return int(getattr(result, "rowcount", 0) or 0) > 0

    def complete_execution_for_claim(
        self,
        session: Session,
        *,
        execution_id: str,
        research_job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Complete RUNNING execution only under a valid job claim fence."""
        return self._terminal_execution_for_claim(
            session,
            execution_id=execution_id,
            research_job_id=research_job_id,
            claim_token=claim_token,
            at=at,
            target_status="COMPLETED",
        )

    def fail_execution_for_claim(
        self,
        session: Session,
        *,
        execution_id: str,
        research_job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Fail RUNNING execution only under a valid job claim fence."""
        return self._terminal_execution_for_claim(
            session,
            execution_id=execution_id,
            research_job_id=research_job_id,
            claim_token=claim_token,
            at=at,
            target_status="FAILED",
        )

    def abandon_execution_for_claim(
        self,
        session: Session,
        *,
        execution_id: str,
        research_job_id: str,
        claim_token: str,
        at: datetime,
    ) -> bool:
        """Abandon RUNNING execution only under a valid job claim fence."""
        return self._terminal_execution_for_claim(
            session,
            execution_id=execution_id,
            research_job_id=research_job_id,
            claim_token=claim_token,
            at=at,
            target_status="ABANDONED",
        )

    def _terminal_execution_for_claim(
        self,
        session: Session,
        *,
        execution_id: str,
        research_job_id: str,
        claim_token: str,
        at: datetime,
        target_status: str,
    ) -> bool:
        """Atomically terminalize an execution when the job claim still owns it.

        Requires in one statement: job RUNNING, matching claim token, unexpired
        lease, active execution binding, execution belonging to the job, and
        execution still RUNNING.
        """
        result = session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.research_job_id == research_job_id,
                WorkflowExecutionModel.status == "RUNNING",
                WorkflowExecutionModel.id.in_(
                    select(ResearchJobModel.active_workflow_execution_id).where(
                        and_(
                            ResearchJobModel.id == research_job_id,
                            ResearchJobModel.status == "RUNNING",
                            ResearchJobModel.claim_token == claim_token,
                            ResearchJobModel.lease_expires_at.is_not(None),
                            ResearchJobModel.lease_expires_at > at,
                            ResearchJobModel.active_workflow_execution_id
                            == execution_id,
                        )
                    )
                ),
            )
            .values(status=target_status, finished_at=at)
        )
        return int(getattr(result, "rowcount", 0) or 0) > 0

    def begin_node_attempt(
        self,
        session: Session,
        *,
        workflow_execution_id: str,
        node_name: str,
        at: datetime,
    ) -> int:
        """Insert a STARTED node attempt; return the attempt number (>= 1)."""
        current_max = session.scalar(
            select(func.max(WorkflowNodeExecutionModel.attempt)).where(
                WorkflowNodeExecutionModel.workflow_execution_id
                == workflow_execution_id,
                WorkflowNodeExecutionModel.node_name == node_name,
            )
        )
        attempt = 1 if current_max is None else int(current_max) + 1
        session.add(
            WorkflowNodeExecutionModel(
                id=str(uuid4()),
                workflow_execution_id=workflow_execution_id,
                node_name=node_name,
                attempt=attempt,
                status="STARTED",
                started_at=at,
                finished_at=None,
                error=None,
            )
        )
        session.flush()
        return attempt

    def complete_node_attempt(
        self,
        session: Session,
        *,
        workflow_execution_id: str,
        node_name: str,
        attempt: int,
        at: datetime,
    ) -> None:
        session.execute(
            update(WorkflowNodeExecutionModel)
            .where(
                WorkflowNodeExecutionModel.workflow_execution_id
                == workflow_execution_id,
                WorkflowNodeExecutionModel.node_name == node_name,
                WorkflowNodeExecutionModel.attempt == attempt,
                WorkflowNodeExecutionModel.status == "STARTED",
            )
            .values(status="COMPLETED", finished_at=at, error=None)
        )

    def fail_node_attempt(
        self,
        session: Session,
        *,
        workflow_execution_id: str,
        node_name: str,
        attempt: int,
        error: str,
        at: datetime,
    ) -> None:
        session.execute(
            update(WorkflowNodeExecutionModel)
            .where(
                WorkflowNodeExecutionModel.workflow_execution_id
                == workflow_execution_id,
                WorkflowNodeExecutionModel.node_name == node_name,
                WorkflowNodeExecutionModel.attempt == attempt,
                WorkflowNodeExecutionModel.status == "STARTED",
            )
            .values(status="FAILED", finished_at=at, error=error)
        )
