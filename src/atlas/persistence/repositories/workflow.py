"""SQLAlchemy repository for workflow execution audit history."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)


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

    def create_execution(
        self,
        session: Session,
        *,
        research_job_id: str,
        thread_id: str,
        at: datetime,
    ) -> str:
        """Insert a RUNNING workflow execution for one worker processing attempt."""
        execution_id = str(uuid4())
        session.add(
            WorkflowExecutionModel(
                id=execution_id,
                research_job_id=research_job_id,
                thread_id=thread_id,
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
    ) -> None:
        session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="COMPLETED", finished_at=at)
        )

    def fail_execution(
        self,
        session: Session,
        *,
        execution_id: str,
        at: datetime,
    ) -> None:
        session.execute(
            update(WorkflowExecutionModel)
            .where(
                WorkflowExecutionModel.id == execution_id,
                WorkflowExecutionModel.status == "RUNNING",
            )
            .values(status="FAILED", finished_at=at)
        )

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
