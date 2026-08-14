"""SELECT-only advisory snapshot reader.

Never inserts, updates, or deletes. Callers must use
``advisory_read_only_scope``.

Latest-row selection is deterministic: workflow executions and evaluation
runs order by ``started_at DESC, id DESC``; limited recovery decisions order
by ``created_at DESC, id DESC``. The latest evaluation run is selected once
and its dimensions are loaded by that run id.
"""

from __future__ import annotations

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from atlas.advisor.errors import AdvisoryJobNotFoundError
from atlas.advisor.snapshot import (
    CountGroup,
    DeadLetterRow,
    DimensionRow,
    EvaluationRunRow,
    JobRow,
    NodeRow,
    OutboxRow,
    RecoveryRow,
    SnapshotLoad,
)
from atlas.persistence.models.consumer import (
    ConsumerDeadLetterModel,
    ResearchJobEventProjectionModel,
)
from atlas.persistence.models.evaluation import (
    EvaluationDimensionResultModel,
    EvaluationRunModel,
)
from atlas.persistence.models.model_invocation import ModelInvocationModel
from atlas.persistence.models.outbox import OutboxEventModel
from atlas.persistence.models.recovery import (
    HumanReviewDecisionModel,
    JobRecoveryAttemptModel,
    PolicyDecisionModel,
)
from atlas.persistence.models.research_job import ResearchJobModel
from atlas.persistence.models.tool_invocation import ToolInvocationModel
from atlas.persistence.models.workflow import (
    WorkflowExecutionModel,
    WorkflowNodeExecutionModel,
)


class SqlAlchemyAdvisorySnapshotReader:
    """Load approved columns for one research job."""

    def load(self, session: Session, research_job_id: str) -> SnapshotLoad:
        job_model = session.get(ResearchJobModel, research_job_id)
        if job_model is None:
            raise AdvisoryJobNotFoundError()
        job = JobRow(
            research_job_id=job_model.id,
            status=job_model.status,
            evaluation_profile=job_model.evaluation_profile,
            continuation_mode=job_model.continuation_mode,
            repair_count=job_model.repair_count,
            job_retry_count=job_model.job_retry_count,
            evaluation_attempt_count=job_model.evaluation_attempt_count,
        )
        evaluation_run_model = _latest_evaluation_run(session, research_job_id)
        evaluation_run = (
            None
            if evaluation_run_model is None
            else EvaluationRunRow(
                status=evaluation_run_model.status,
                profile=evaluation_run_model.evaluation_profile,
                passed=evaluation_run_model.passed,
                aggregate_score=evaluation_run_model.aggregate_score,
                disposition_hint=evaluation_run_model.disposition_hint,
            )
        )
        evaluation_run_id = (
            None if evaluation_run_model is None else evaluation_run_model.id
        )
        return SnapshotLoad(
            job=job,
            nodes=_load_nodes(session, research_job_id),
            model_groups=_load_model_groups(session, research_job_id),
            tool_groups=_load_tool_groups(session, research_job_id),
            evaluation_run=evaluation_run,
            dimensions=_load_dimensions(session, evaluation_run_id),
            recovery=_load_recovery(session, research_job_id),
            review_counts=_load_review_counts(session, research_job_id),
            outbox=_load_outbox(session, research_job_id),
            projection_event_type=_load_projection(session, research_job_id),
            dead_letters=_load_dead_letters(session, research_job_id),
        )


def _load_nodes(session: Session, research_job_id: str) -> tuple[NodeRow, ...]:
    execution_id = session.scalar(
        select(WorkflowExecutionModel.id)
        .where(WorkflowExecutionModel.research_job_id == research_job_id)
        .order_by(
            WorkflowExecutionModel.started_at.desc(),
            WorkflowExecutionModel.id.desc(),
        )
        .limit(1)
    )
    if execution_id is None:
        return ()
    rows = session.execute(
        select(
            WorkflowNodeExecutionModel.node_name,
            WorkflowNodeExecutionModel.attempt,
            WorkflowNodeExecutionModel.status,
            WorkflowNodeExecutionModel.error,
        )
        .where(WorkflowNodeExecutionModel.workflow_execution_id == execution_id)
        .order_by(
            WorkflowNodeExecutionModel.node_name.asc(),
            WorkflowNodeExecutionModel.attempt.asc(),
        )
    ).all()
    return tuple(
        NodeRow(
            node_name=str(row.node_name),
            attempt=int(row.attempt),
            status=str(row.status),
            error=row.error,
        )
        for row in rows
    )


def _load_model_groups(
    session: Session, research_job_id: str
) -> tuple[CountGroup, ...]:
    rows = session.execute(
        select(
            ModelInvocationModel.node_name,
            ModelInvocationModel.provider,
            ModelInvocationModel.status,
            ModelInvocationModel.retry_class,
            ModelInvocationModel.error_class,
            func.count().label("n"),
        )
        .where(ModelInvocationModel.research_job_id == research_job_id)
        .group_by(
            ModelInvocationModel.node_name,
            ModelInvocationModel.provider,
            ModelInvocationModel.status,
            ModelInvocationModel.retry_class,
            ModelInvocationModel.error_class,
        )
        .order_by(
            ModelInvocationModel.node_name.asc(),
            ModelInvocationModel.provider.asc(),
            ModelInvocationModel.status.asc(),
        )
    ).all()
    return tuple(
        CountGroup(
            keys=(
                str(row.node_name),
                str(row.provider),
                str(row.status),
                str(row.retry_class or ""),
                str(row.error_class or ""),
            ),
            count=int(row.n),
        )
        for row in rows
    )


def _load_tool_groups(session: Session, research_job_id: str) -> tuple[CountGroup, ...]:
    rows = session.execute(
        select(
            ToolInvocationModel.tool_id,
            ToolInvocationModel.provider,
            ToolInvocationModel.status,
            ToolInvocationModel.retry_class,
            func.count().label("n"),
        )
        .where(ToolInvocationModel.research_job_id == research_job_id)
        .group_by(
            ToolInvocationModel.tool_id,
            ToolInvocationModel.provider,
            ToolInvocationModel.status,
            ToolInvocationModel.retry_class,
        )
        .order_by(
            ToolInvocationModel.tool_id.asc(),
            ToolInvocationModel.provider.asc(),
            ToolInvocationModel.status.asc(),
        )
    ).all()
    return tuple(
        CountGroup(
            keys=(
                str(row.tool_id),
                str(row.provider),
                str(row.status),
                str(row.retry_class or ""),
            ),
            count=int(row.n),
        )
        for row in rows
    )


def _latest_evaluation_run(
    session: Session, research_job_id: str
) -> EvaluationRunModel | None:
    return session.execute(
        select(EvaluationRunModel)
        .where(EvaluationRunModel.research_job_id == research_job_id)
        .order_by(
            EvaluationRunModel.started_at.desc(),
            EvaluationRunModel.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()


def _load_dimensions(
    session: Session, evaluation_run_id: str | None
) -> tuple[DimensionRow, ...]:
    if evaluation_run_id is None:
        return ()
    rows = session.execute(
        select(EvaluationDimensionResultModel)
        .where(EvaluationDimensionResultModel.evaluation_run_id == evaluation_run_id)
        .order_by(EvaluationDimensionResultModel.dimension_name.asc())
    ).scalars()
    result: list[DimensionRow] = []
    for item in rows:
        codes = item.failure_codes or []
        result.append(
            DimensionRow(
                name=item.dimension_name,
                score=float(item.score),
                passed=bool(item.passed),
                method=item.method,
                failure_codes=tuple(str(code) for code in codes),
            )
        )
    return tuple(result)


def _load_recovery(session: Session, research_job_id: str) -> tuple[RecoveryRow, ...]:
    rows = session.execute(
        select(PolicyDecisionModel, JobRecoveryAttemptModel.attempt_number)
        .outerjoin(
            JobRecoveryAttemptModel,
            JobRecoveryAttemptModel.policy_decision_id == PolicyDecisionModel.id,
        )
        .where(PolicyDecisionModel.research_job_id == research_job_id)
        .order_by(
            PolicyDecisionModel.created_at.desc(),
            PolicyDecisionModel.id.desc(),
        )
        .limit(8)
    ).all()
    return tuple(
        RecoveryRow(
            decision=item.decision,
            failure_category=item.failure_category,
            reason_code=item.reason_code,
            attempt_number=None if attempt_number is None else int(attempt_number),
        )
        for item, attempt_number in rows
    )


def _load_review_counts(
    session: Session, research_job_id: str
) -> tuple[tuple[str, int], ...]:
    rows = session.execute(
        select(
            HumanReviewDecisionModel.decision,
            func.count().label("n"),
        )
        .where(HumanReviewDecisionModel.research_job_id == research_job_id)
        .group_by(HumanReviewDecisionModel.decision)
        .order_by(HumanReviewDecisionModel.decision.asc())
    ).all()
    return tuple((str(row.decision), int(row.n)) for row in rows)


def _load_outbox(session: Session, research_job_id: str) -> tuple[OutboxRow, ...]:
    unpublished = case(
        (OutboxEventModel.published_at.is_(None), 1),
        else_=0,
    )
    published = case(
        (OutboxEventModel.published_at.is_not(None), 1),
        else_=0,
    )
    rows = session.execute(
        select(
            OutboxEventModel.event_type,
            func.sum(unpublished).label("unpublished_count"),
            func.sum(published).label("published_count"),
            func.max(OutboxEventModel.publish_attempts).label("max_attempts"),
            func.max(OutboxEventModel.last_publish_error_class).label("error_class"),
        )
        .where(OutboxEventModel.aggregate_id == research_job_id)
        .group_by(OutboxEventModel.event_type)
        .order_by(OutboxEventModel.event_type.asc())
    ).all()
    return tuple(
        OutboxRow(
            event_type=str(row.event_type),
            unpublished_count=int(row.unpublished_count or 0),
            published_count=int(row.published_count or 0),
            max_publish_attempts=int(row.max_attempts or 0),
            last_publish_error_class=row.error_class,
        )
        for row in rows
    )


def _load_projection(session: Session, research_job_id: str) -> str | None:
    row = session.get(ResearchJobEventProjectionModel, research_job_id)
    if row is None:
        return None
    return str(row.last_event_type)


def _load_dead_letters(
    session: Session, research_job_id: str
) -> tuple[DeadLetterRow, ...]:
    rows = session.execute(
        select(
            ConsumerDeadLetterModel.failure_code,
            ConsumerDeadLetterModel.replay_state,
            ConsumerDeadLetterModel.replay_eligible,
            func.count().label("n"),
        )
        .where(ConsumerDeadLetterModel.aggregate_id == research_job_id)
        .group_by(
            ConsumerDeadLetterModel.failure_code,
            ConsumerDeadLetterModel.replay_state,
            ConsumerDeadLetterModel.replay_eligible,
        )
        .order_by(
            ConsumerDeadLetterModel.failure_code.asc(),
            ConsumerDeadLetterModel.replay_state.asc(),
        )
    ).all()
    return tuple(
        DeadLetterRow(
            failure_code=str(row.failure_code),
            replay_state=str(row.replay_state),
            replay_eligible=bool(row.replay_eligible),
            count=int(row.n),
        )
        for row in rows
    )
