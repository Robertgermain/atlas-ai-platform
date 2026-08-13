"""LangGraph research processor and Postgres checkpointer lifecycle."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sqlalchemy import select

from atlas.application.exceptions import ClaimOwnershipError
from atlas.application.job_processing import (
    CompletedProcessing,
    ContinuationMode,
    PausedForReview,
    ProcessingOutcome,
    RetryScheduled,
    TerminalFailed,
)
from atlas.config.settings import Settings, get_settings
from atlas.evaluation.contracts import EvaluationCandidateInput, ToolSummaryRow
from atlas.evaluation.errors import EvaluationError, EvaluationTerminalError
from atlas.evaluation.runner import EvaluationRunner
from atlas.evaluation.service import EvaluationService
from atlas.eventing.builders import (
    build_research_job_awaiting_review,
    build_research_job_retry_scheduled,
)
from atlas.evidence.contracts import ClaimStructured
from atlas.evidence.service import EvidenceIngestService, ReportArtifactService
from atlas.models.composition import build_planner_and_drafter
from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.outbox.ports import OutboxEnqueuer
from atlas.persistence.db import session_scope
from atlas.persistence.models.evidence import EvidenceJobLinkModel
from atlas.persistence.models.tool_invocation import ToolInvocationModel
from atlas.persistence.models.workflow import WorkflowExecutionModel
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from atlas.persistence.repositories.recovery import SqlAlchemyRecoveryRepository
from atlas.persistence.repositories.research_job import (
    SqlAlchemyResearchJobRepository,
)
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
from atlas.recovery.fingerprint import fingerprint_policy_decision
from atlas.recovery.policy import (
    AttemptCounts,
    decide_for_evaluation,
    decide_for_exception,
    schedule_next_attempt_at,
)
from atlas.tools.composition import build_research_executor
from atlas.workflow.graph import (
    NodeAuditHooks,
    WorkflowRuntimeContext,
    build_research_graph,
    initial_graph_state,
)

if TYPE_CHECKING:
    from langgraph.graph.state import CompiledStateGraph
    from sqlalchemy.orm import Session, sessionmaker

    from atlas.evaluation.contracts import EvaluationRunResult
    from atlas.evidence.service import CitationValidator
    from atlas.models.ports import ResearchDrafter, ResearchPlanner
    from atlas.tools.runner import ResearchPlanExecutor
    from atlas.workflow.graph import ResearchGraphState

logger = logging.getLogger(__name__)


class _BoundEvaluationRunner:
    """Typed evaluation runner with fail-closed provenance for ``evaluate_node``."""

    def __init__(
        self,
        *,
        runner: EvaluationRunner,
        citation_validator: CitationValidator | None,
        evidence_ingest: EvidenceIngestService | None,
    ) -> None:
        self._runner = runner
        self._citation_validator = citation_validator
        self._evidence_ingest = evidence_ingest

    def run(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        job_claim_token: str,
        provenance_ok: bool = True,
    ) -> EvaluationRunResult:
        return self._runner.run(
            candidate=candidate,
            workflow_execution_id=workflow_execution_id,
            deadline=deadline,
            job_claim_token=job_claim_token,
            provenance_ok=provenance_ok,
        )

    def provenance_ok_for_claims(
        self,
        *,
        job_id: str,
        claims: list[ClaimStructured],
    ) -> bool:
        """Fail closed when claims exist but provenance deps are missing."""
        if not claims:
            return True
        if self._citation_validator is None:
            return False
        if self._evidence_ingest is None:
            return False
        try:
            self._citation_validator.validate(research_job_id=job_id, claims=claims)
            for claim in claims:
                for evidence_item_id in claim.evidence_item_ids:
                    item = self._evidence_ingest.get_item(evidence_item_id)
                    if not (
                        item.document_id
                        and item.source_id
                        and item.canonical_uri
                        and item.document_content_sha256
                    ):
                        return False
        except EvaluationError:
            raise
        except Exception as exc:
            from atlas.evaluation.errors import (
                EvaluationTerminalError,
                sanitize_evaluation_error,
            )

            raise EvaluationTerminalError(sanitize_evaluation_error(exc)) from None
        return True


def to_psycopg_conninfo(database_url: str) -> str:
    """Convert an Atlas SQLAlchemy URL to a psycopg connection string."""
    if database_url.startswith("postgresql+psycopg://"):
        return "postgresql://" + database_url.removeprefix("postgresql+psycopg://")
    if database_url.startswith("postgresql://"):
        return database_url
    raise ValueError(
        "Unsupported database URL scheme for LangGraph PostgresSaver; "
        "expected postgresql+psycopg:// or postgresql://"
    )


@dataclass
class CheckpointRuntime:
    """Owns the checkpointer connection pool for a worker process."""

    pool: ConnectionPool
    checkpointer: PostgresSaver

    def close(self) -> None:
        """Close the underlying connection pool."""
        self.pool.close()


def create_checkpoint_runtime(database_url: str) -> CheckpointRuntime:
    """Create a sync PostgresSaver backed by a psycopg connection pool."""
    conninfo = to_psycopg_conninfo(database_url)
    pool = ConnectionPool(
        conninfo=conninfo,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        },
        open=True,
    )
    checkpointer = PostgresSaver(cast(Any, pool))
    return CheckpointRuntime(pool=pool, checkpointer=checkpointer)


def initialize_checkpointer_schema(runtime: CheckpointRuntime) -> None:
    """Create LangGraph checkpoint tables once at worker/process startup."""
    runtime.checkpointer.setup()


def sanitize_node_error(exc: Exception) -> str:
    """Persist a bounded, class-only error string for node audit rows.

    Never includes the raw exception message, question text, tool inputs,
    credentials, URLs, provider responses, or other arbitrary exception text.
    """
    return f"{type(exc).__name__}: node execution failed"


class RepositoryNodeAuditHooks(NodeAuditHooks):
    """Write per-attempt node history through the workflow repository."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: SqlAlchemyWorkflowRepository,
        workflow_execution_id: str,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._workflow_execution_id = workflow_execution_id

    def begin(self, node_name: str) -> int:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            return self._repository.begin_node_attempt(
                session,
                workflow_execution_id=self._workflow_execution_id,
                node_name=node_name,
                at=at,
            )

    def complete(self, node_name: str, attempt: int) -> None:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            self._repository.complete_node_attempt(
                session,
                workflow_execution_id=self._workflow_execution_id,
                node_name=node_name,
                attempt=attempt,
                at=at,
            )

    def fail(self, node_name: str, attempt: int, error: Exception) -> None:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            self._repository.fail_node_attempt(
                session,
                workflow_execution_id=self._workflow_execution_id,
                node_name=node_name,
                attempt=attempt,
                error=sanitize_node_error(error),
                at=at,
            )


class LangGraphResearchProcessor:
    """ResearchJobProcessor backed by the LangGraph research workflow.

    Handles continuation modes (NONE, JOB_RETRY, REVIEW_COMPLETE), policy
    decisions (repair, await_review, retry, terminal, complete), and
    claim-fenced execution lifecycle.
    """

    def __init__(
        self,
        *,
        checkpointer: PostgresSaver,
        session_factory: sessionmaker[Session],
        settings: Settings | None = None,
        repository: SqlAlchemyWorkflowRepository | None = None,
        interrupt_after: Sequence[str] | None = ("await_review",),
        node_counters: dict[str, int] | None = None,
        planner: ResearchPlanner | None = None,
        drafter: ResearchDrafter | None = None,
        research_executor: ResearchPlanExecutor | None = None,
        outbox: OutboxEnqueuer | None = None,
        metrics: AtlasMetrics | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings or get_settings()
        self._repository = repository or SqlAlchemyWorkflowRepository()
        self._job_repo = SqlAlchemyResearchJobRepository()
        self._recovery_repo = SqlAlchemyRecoveryRepository()
        self._outbox = outbox or SqlAlchemyOutboxRepository()
        self._metrics = metrics or default_metrics()
        self._node_counters = node_counters
        self._planner_override = planner
        self._drafter_override = drafter
        self._research_executor_override = research_executor
        self._graph: CompiledStateGraph[
            ResearchGraphState,
            WorkflowRuntimeContext,
            ResearchGraphState,
            ResearchGraphState,
        ] = build_research_graph(
            checkpointer=checkpointer,
            interrupt_after=interrupt_after,
        )

    def __call__(
        self,
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: ContinuationMode | str = ContinuationMode.NONE,
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        from atlas.evaluation.claim_fingerprint import fingerprint_job_claim_token

        fingerprint_job_claim_token(claim_token)
        if not isinstance(continuation_mode, ContinuationMode):
            continuation_mode = ContinuationMode(continuation_mode)

        with session_scope(self._session_factory) as session:
            from atlas.persistence.models import ResearchJobModel

            job_model = session.get(ResearchJobModel, job_id)
            if job_model is not None:
                if active_workflow_execution_id is None:
                    active_workflow_execution_id = (
                        job_model.active_workflow_execution_id
                    )
                if (
                    continuation_mode == ContinuationMode.NONE
                    and job_model.status == "COMPLETED"
                    and isinstance(job_model.result, str)
                    and job_model.result.strip()
                ):
                    # Idempotent replay after job completion.
                    completed_id = job_model.active_workflow_execution_id
                    if completed_id is None:
                        completed = session.scalars(
                            select(WorkflowExecutionModel)
                            .where(
                                WorkflowExecutionModel.research_job_id == job_id,
                                WorkflowExecutionModel.status == "COMPLETED",
                            )
                            .order_by(WorkflowExecutionModel.finished_at.desc())
                            .limit(1)
                        ).first()
                        completed_id = completed.id if completed else job_id
                    return CompletedProcessing(
                        result=job_model.result,
                        workflow_execution_id=completed_id,
                    )

        # Idempotent short-circuit: already-completed active execution under this job.
        if continuation_mode == ContinuationMode.NONE and active_workflow_execution_id:
            short = self._completed_execution_result(
                execution_id=active_workflow_execution_id,
                job_id=job_id,
            )
            if short is not None:
                return short

        execution_id: str | None = None

        try:
            execution_id = self._bind(
                job_id=job_id,
                claim_token=claim_token,
                continuation_mode=continuation_mode,
                active_workflow_execution_id=active_workflow_execution_id,
            )

            config: RunnableConfig = {"configurable": {"thread_id": execution_id}}

            hooks = RepositoryNodeAuditHooks(
                session_factory=self._session_factory,
                repository=self._repository,
                workflow_execution_id=execution_id,
            )
            context = self._build_context(
                workflow_execution_id=execution_id,
                hooks=hooks,
                job_claim_token=claim_token,
                job_id=job_id,
            )

            if continuation_mode == ContinuationMode.REVIEW_COMPLETE:
                return self._invoke_review_complete(
                    job_id=job_id,
                    claim_token=claim_token,
                    execution_id=execution_id,
                    config=config,
                    context=context,
                )

            snapshot = self._graph.get_state(config)
            if snapshot.values and snapshot.next:
                final_state = self._graph.invoke(None, config, context=context)
            elif snapshot.values and not snapshot.next:
                existing = snapshot.values.get("result")
                if isinstance(existing, str) and existing.strip():
                    with session_scope(self._session_factory) as session:
                        ok = self._repository.complete_execution_for_claim(
                            session,
                            execution_id=execution_id,
                            research_job_id=job_id,
                            claim_token=claim_token,
                            at=datetime.now(UTC),
                        )
                        if not ok:
                            raise ClaimOwnershipError("complete_execution")
                    return CompletedProcessing(
                        result=existing,
                        workflow_execution_id=execution_id,
                    )
                final_state = self._graph.invoke(
                    initial_graph_state(job_id=job_id, question=question),
                    config,
                    context=context,
                )
            else:
                final_state = self._graph.invoke(
                    initial_graph_state(job_id=job_id, question=question),
                    config,
                    context=context,
                )

            return self._handle_post_invoke(
                final_state=final_state,
                config=config,
                job_id=job_id,
                claim_token=claim_token,
                execution_id=execution_id,
            )

        except RuntimeError:
            # Preserve intentional interrupt / fail-closed runtime errors for callers.
            raise
        except Exception as exc:
            return self._handle_exception(
                exc=exc,
                job_id=job_id,
                claim_token=claim_token,
                execution_id=execution_id,
            )

    def _bind(
        self,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: ContinuationMode | str,
        active_workflow_execution_id: str | None,
    ) -> str:
        """Bind to an execution: resume existing or create fresh."""
        if not isinstance(continuation_mode, ContinuationMode):
            continuation_mode = ContinuationMode(continuation_mode)
        at = datetime.now(UTC)

        if continuation_mode == ContinuationMode.REVIEW_COMPLETE:
            if not active_workflow_execution_id:
                raise ValueError(
                    "REVIEW_COMPLETE requires active_workflow_execution_id"
                )
            with session_scope(self._session_factory) as session:
                exec_model = self._repository.get_execution(
                    session, execution_id=active_workflow_execution_id
                )
                if exec_model is None or exec_model.status != "RUNNING":
                    raise RuntimeError(
                        "REVIEW_COMPLETE execution not found or not RUNNING"
                    )
            return active_workflow_execution_id

        if continuation_mode == ContinuationMode.NONE and active_workflow_execution_id:
            with session_scope(self._session_factory) as session:
                exec_model = self._repository.get_execution(
                    session, execution_id=active_workflow_execution_id
                )
                if exec_model is not None and exec_model.status == "RUNNING":
                    return active_workflow_execution_id
                if exec_model is not None and exec_model.status == "COMPLETED":
                    return active_workflow_execution_id

        # JOB_RETRY and fresh NONE without a resumable active execution: create
        # a new execution + bind in one transaction. Bind failure rolls back create.
        with session_scope(self._session_factory) as session:
            self._repository.abandon_unfinished_for_job(
                session, research_job_id=job_id, at=at
            )
            execution_id = self._repository.create_execution(
                session,
                research_job_id=job_id,
                at=at,
            )
            ok = self._job_repo.set_active_workflow_execution(
                session,
                job_id=job_id,
                claim_token=claim_token,
                execution_id=execution_id,
                at=at,
            )
            if not ok:
                raise ClaimOwnershipError("set_active_workflow_execution")
        return execution_id

    def _completed_execution_result(
        self,
        *,
        execution_id: str,
        job_id: str,
    ) -> CompletedProcessing | None:
        """Return durable result when the active execution already completed."""
        with session_scope(self._session_factory) as session:
            exec_model = self._repository.get_execution(
                session, execution_id=execution_id
            )
            if (
                exec_model is None
                or exec_model.research_job_id != job_id
                or exec_model.status != "COMPLETED"
            ):
                return None
        config: RunnableConfig = {"configurable": {"thread_id": execution_id}}
        snapshot = self._graph.get_state(config)
        if snapshot.next:
            return None
        existing = snapshot.values.get("result") if snapshot.values else None
        if isinstance(existing, str) and existing.strip():
            return CompletedProcessing(
                result=existing,
                workflow_execution_id=execution_id,
            )
        return None

    def _invoke_review_complete(
        self,
        *,
        job_id: str,
        claim_token: str,
        execution_id: str,
        config: RunnableConfig,
        context: WorkflowRuntimeContext,
    ) -> ProcessingOutcome:
        """Handle REVIEW_COMPLETE: verify checkpoint, invoke complete only."""
        snapshot = self._graph.get_state(config)
        if not snapshot.next or snapshot.next != ("complete",):
            with session_scope(self._session_factory) as session:
                ok = self._repository.fail_execution_for_claim(
                    session,
                    execution_id=execution_id,
                    research_job_id=job_id,
                    claim_token=claim_token,
                    at=datetime.now(UTC),
                )
                if not ok:
                    raise ClaimOwnershipError("fail_execution")
            return TerminalFailed(
                reason_code="REVIEW_COMPLETE_BAD_CHECKPOINT",
                workflow_execution_id=execution_id,
            )

        final_state = self._graph.invoke(None, config, context=context)
        if not isinstance(final_state, dict):
            raise RuntimeError("Workflow returned a non-mapping state")

        snapshot_after = self._graph.get_state(config)
        if snapshot_after.next:
            raise RuntimeError("Workflow interrupted after review-complete invoke")

        result = final_state.get("result")
        if not isinstance(result, str) or not result.strip():
            raise RuntimeError("Workflow finished without a result")

        with session_scope(self._session_factory) as session:
            ok = self._repository.complete_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=claim_token,
                at=datetime.now(UTC),
            )
            if not ok:
                raise ClaimOwnershipError("complete_execution")
        return CompletedProcessing(
            result=result,
            workflow_execution_id=execution_id,
        )

    def _handle_post_invoke(
        self,
        *,
        final_state: Any,
        config: RunnableConfig,
        job_id: str,
        claim_token: str,
        execution_id: str,
    ) -> ProcessingOutcome:
        """Check for interrupts after graph invoke and route accordingly."""
        if not isinstance(final_state, dict):
            raise RuntimeError("Workflow returned a non-mapping state")

        snapshot_after = self._graph.get_state(config)

        if snapshot_after.next:
            if snapshot_after.next == ("complete",):
                disposition = final_state.get("disposition", "")
                if disposition == "await_review":
                    at = datetime.now(UTC)
                    decision_id = str(uuid4())
                    with session_scope(self._session_factory) as session:
                        rc, jrc, eac = self._job_repo.get_attempt_counts(
                            session, job_id=job_id
                        )
                        fp = fingerprint_policy_decision(
                            research_job_id=job_id,
                            workflow_execution_id=execution_id,
                            evaluation_run_id=final_state.get("evaluation_run_id"),
                            decision="await_review",
                            failure_category="NEEDS_HUMAN_REVIEW",
                            reason_code="AWAIT_REVIEW_POLICY",
                            repair_count=rc,
                            job_retry_count=jrc,
                            evaluation_attempt_count=eac,
                        )
                        authoritative_id = self._recovery_repo.insert_policy_decision(
                            session,
                            id=decision_id,
                            research_job_id=job_id,
                            workflow_execution_id=execution_id,
                            evaluation_run_id=final_state.get("evaluation_run_id"),
                            decision="await_review",
                            failure_category="NEEDS_HUMAN_REVIEW",
                            reason_code="AWAIT_REVIEW_POLICY",
                            decision_fingerprint=fp,
                            created_at=at,
                        )
                        created = authoritative_id == decision_id
                        if created:
                            ok = self._job_repo.transition_awaiting_review(
                                session,
                                job_id=job_id,
                                claim_token=claim_token,
                                at=at,
                            )
                            if not ok:
                                raise ClaimOwnershipError("transition_awaiting_review")
                            self._outbox.enqueue(
                                session,
                                build_research_job_awaiting_review(
                                    research_job_id=job_id,
                                    workflow_execution_id=execution_id,
                                    entered_review_at=at,
                                ),
                            )
                    return PausedForReview(
                        workflow_execution_id=execution_id,
                    )

            with session_scope(self._session_factory) as session:
                ok = self._repository.fail_execution_for_claim(
                    session,
                    execution_id=execution_id,
                    research_job_id=job_id,
                    claim_token=claim_token,
                    at=datetime.now(UTC),
                )
                if not ok:
                    raise ClaimOwnershipError("fail_execution")
            raise RuntimeError("Workflow interrupted before completion")

        result = final_state.get("result")
        if not isinstance(result, str) or not result.strip():
            raise RuntimeError("Workflow finished without a result")

        with session_scope(self._session_factory) as session:
            ok = self._repository.complete_execution_for_claim(
                session,
                execution_id=execution_id,
                research_job_id=job_id,
                claim_token=claim_token,
                at=datetime.now(UTC),
            )
            if not ok:
                raise ClaimOwnershipError("complete_execution")
        return CompletedProcessing(
            result=result,
            workflow_execution_id=execution_id,
        )

    def _handle_exception(
        self,
        *,
        exc: Exception,
        job_id: str,
        claim_token: str,
        execution_id: str | None,
    ) -> ProcessingOutcome:
        """Categorize exception and decide retry vs terminal."""
        if isinstance(exc, EvaluationTerminalError):
            if execution_id is not None:
                with session_scope(self._session_factory) as session:
                    ok = self._repository.fail_execution_for_claim(
                        session,
                        execution_id=execution_id,
                        research_job_id=job_id,
                        claim_token=claim_token,
                        at=datetime.now(UTC),
                    )
                    if not ok:
                        raise ClaimOwnershipError("fail_execution")
            return TerminalFailed(
                reason_code="EvaluationTerminalError",
                workflow_execution_id=execution_id,
            )

        with session_scope(self._session_factory) as session:
            repair_count, retry_count, eval_count = self._job_repo.get_attempt_counts(
                session, job_id=job_id
            )

        counts = AttemptCounts(
            repair_count=repair_count,
            job_retry_count=retry_count,
            evaluation_attempt_count=eval_count,
        )
        decision = decide_for_exception(exc=exc, counts=counts)

        if decision.action == "retry":
            at = datetime.now(UTC)
            attempt_number = retry_count + 1
            next_at = schedule_next_attempt_at(
                now=at,
                attempt_number=attempt_number,
                base_seconds=self._settings.retry_base_seconds,
                max_backoff_seconds=self._settings.retry_max_backoff_seconds,
                jitter_max_seconds=self._settings.retry_jitter_max_seconds,
            )
            decision_id = str(uuid4())

            with session_scope(self._session_factory) as session:
                fp = fingerprint_policy_decision(
                    research_job_id=job_id,
                    workflow_execution_id=execution_id,
                    evaluation_run_id=None,
                    decision=decision.action,
                    failure_category=decision.failure_category.value,
                    reason_code=decision.reason_code,
                    repair_count=repair_count,
                    job_retry_count=retry_count,
                    evaluation_attempt_count=eval_count,
                )
                authoritative_id = self._recovery_repo.insert_policy_decision(
                    session,
                    id=decision_id,
                    research_job_id=job_id,
                    workflow_execution_id=execution_id,
                    evaluation_run_id=None,
                    decision=decision.action,
                    failure_category=decision.failure_category.value,
                    reason_code=decision.reason_code,
                    decision_fingerprint=fp,
                    created_at=at,
                )
                created = authoritative_id == decision_id
                if created:
                    if execution_id:
                        self._recovery_repo.insert_recovery_attempt(
                            session,
                            id=str(uuid4()),
                            research_job_id=job_id,
                            policy_decision_id=authoritative_id,
                            abandoned_workflow_execution_id=execution_id,
                            attempt_number=attempt_number,
                            next_attempt_at=next_at,
                            created_at=at,
                        )
                    ok = self._job_repo.schedule_retry(
                        session,
                        job_id=job_id,
                        claim_token=claim_token,
                        next_attempt_at=next_at,
                        at=at,
                        abandon_execution_id=execution_id,
                    )
                    if not ok:
                        raise ClaimOwnershipError("schedule_retry")
                    self._outbox.enqueue(
                        session,
                        build_research_job_retry_scheduled(
                            research_job_id=job_id,
                            abandoned_workflow_execution_id=execution_id,
                            job_retry_count=attempt_number,
                            next_attempt_at=next_at,
                            occurred_at=at,
                        ),
                    )
                else:
                    existing_attempt = (
                        self._recovery_repo.get_recovery_attempt_by_policy(
                            session,
                            policy_decision_id=authoritative_id,
                        )
                    )
                    if existing_attempt is not None:
                        next_at = existing_attempt.next_attempt_at
                        attempt_number = existing_attempt.attempt_number

            # Emitted only after the ``with`` block above has committed, and
            # only for a freshly-persisted decision -- ``created`` is False
            # on an idempotent replay of the same decision fingerprint, which
            # must never double-count (Slice 15A2 correction).
            if created:
                self._metrics.observe_recovery_decision(
                    action=decision.action,
                    failure_category=decision.failure_category.value,
                )

            return RetryScheduled(
                workflow_execution_id=execution_id or "",
                next_attempt_at=next_at,
                attempt_number=attempt_number,
            )

        if execution_id:
            with session_scope(self._session_factory) as session:
                ok = self._repository.fail_execution_for_claim(
                    session,
                    execution_id=execution_id,
                    research_job_id=job_id,
                    claim_token=claim_token,
                    at=datetime.now(UTC),
                )
                if not ok:
                    raise ClaimOwnershipError("fail_execution")
        return TerminalFailed(
            reason_code=decision.reason_code,
            workflow_execution_id=execution_id,
        )

    def _build_context(
        self,
        *,
        workflow_execution_id: str,
        hooks: NodeAuditHooks | None,
        job_claim_token: str,
        job_id: str = "",
    ) -> WorkflowRuntimeContext:
        from atlas.embeddings.composition import build_text_embedder
        from atlas.evidence.retrieve import EvidenceEmbeddingService, EvidenceRetriever
        from atlas.evidence.service import CitationValidator
        from atlas.specialists.citation_verifier import DurableCitationVerifier
        from atlas.specialists.planner import BoundedPlannerSpecialist
        from atlas.specialists.research import GovernedResearchRetrievalSpecialist
        from atlas.specialists.synthesizer import BoundedReportSynthesizer

        embedder = build_text_embedder(self._settings)
        embedding_service = EvidenceEmbeddingService(
            session_factory=self._session_factory,
            embedder=embedder,
            embedding_profile=self._settings.embedding_profile,
        )
        evidence_ingest = EvidenceIngestService(
            session_factory=self._session_factory,
            embedding_service=embedding_service,
        )
        report_service = ReportArtifactService(session_factory=self._session_factory)
        evidence_retriever = EvidenceRetriever(
            session_factory=self._session_factory,
            embedder=embedder,
            embedding_profile=self._settings.embedding_profile,
            use_hnsw=self._settings.retrieval_use_hnsw,
        )
        research_executor = self._research_executor_override or build_research_executor(
            self._settings,
            session_factory=self._session_factory,
            use_ledger=True,
            evidence_ingest=evidence_ingest,
        )
        citation_verifier = DurableCitationVerifier(
            citation_validator=CitationValidator(session_factory=self._session_factory),
            evidence_ingest=evidence_ingest,
        )
        if self._planner_override is not None and self._drafter_override is not None:
            planner = self._planner_override
            drafter = self._drafter_override
        elif self._settings.model_provider == "fake":
            from atlas.models.fakes import (
                DeterministicResearchDrafter,
                DeterministicResearchPlanner,
            )

            planner = DeterministicResearchPlanner()
            drafter = DeterministicResearchDrafter()
        else:
            planner, drafter = build_planner_and_drafter(
                self._settings,
                session_factory=self._session_factory,
                workflow_execution_id=workflow_execution_id,
            )

        evaluation_service = EvaluationService(session_factory=self._session_factory)
        citation_validator = CitationValidator(session_factory=self._session_factory)
        session_factory = self._session_factory

        def load_linked_ids(
            candidate: EvaluationCandidateInput,
            _workflow_execution_id: str,
        ) -> set[str]:
            with session_scope(session_factory) as session:
                rows = session.execute(
                    select(EvidenceJobLinkModel.evidence_item_id).where(
                        EvidenceJobLinkModel.research_job_id == candidate.job_id
                    )
                ).scalars()
                return {str(item_id) for item_id in rows}

        def load_tool_rows(
            candidate: EvaluationCandidateInput,
            workflow_execution_id: str,
        ) -> list[ToolSummaryRow]:
            with session_scope(session_factory) as session:
                rows = session.execute(
                    select(
                        ToolInvocationModel.node_name,
                        ToolInvocationModel.origin,
                        ToolInvocationModel.tool_id,
                        ToolInvocationModel.status,
                    ).where(
                        ToolInvocationModel.research_job_id == candidate.job_id,
                        ToolInvocationModel.workflow_execution_id
                        == workflow_execution_id,
                        ToolInvocationModel.node_name.is_not(None),
                    )
                ).all()
                return [
                    ToolSummaryRow(
                        node_name=str(node_name),
                        origin=str(origin),
                        tool_id=str(tool_id),
                        status=str(status),
                    )
                    for node_name, origin, tool_id, status in rows
                    if node_name
                ]

        evaluation_runner = _BoundEvaluationRunner(
            runner=EvaluationRunner(
                evaluation_service=evaluation_service,
                load_linked_ids=load_linked_ids,
                load_tool_rows=load_tool_rows,
                semantic_grader=None,
                max_logical_calls=self._settings.tool_max_logical_calls_per_research_node,
            ),
            citation_validator=citation_validator,
            evidence_ingest=evidence_ingest,
        )

        def _policy_callback(
            passed: bool,
            eval_run_id: str,
            repair_count: int,
            evaluation_attempt: int,
        ) -> str:
            if passed:
                return "complete"
            if not eval_run_id:
                return "terminal"
            eval_svc = evaluation_service
            runs = eval_svc.get_by_job(job_id)
            eval_result = None
            for run in runs:
                if run.run_id == eval_run_id:
                    eval_result = run
                    break
            if eval_result is None:
                return "terminal"
            with session_scope(session_factory) as sess:
                rc, jrc, eac = self._job_repo.get_attempt_counts(sess, job_id=job_id)
            counts = AttemptCounts(
                repair_count=max(rc, repair_count),
                job_retry_count=jrc,
                evaluation_attempt_count=max(eac, evaluation_attempt),
            )
            policy_decision = decide_for_evaluation(
                result=eval_result,
                dimensions=list(eval_result.dimensions),
                counts=counts,
            )
            at = datetime.now(UTC)
            with session_scope(session_factory) as sess:
                fp = fingerprint_policy_decision(
                    research_job_id=job_id,
                    workflow_execution_id=workflow_execution_id,
                    evaluation_run_id=eval_run_id,
                    decision=policy_decision.action,
                    failure_category=policy_decision.failure_category.value,
                    reason_code=policy_decision.reason_code,
                    repair_count=counts.repair_count,
                    job_retry_count=counts.job_retry_count,
                    evaluation_attempt_count=counts.evaluation_attempt_count,
                )
                proposed_id = str(uuid4())
                authoritative_id = self._recovery_repo.insert_policy_decision(
                    sess,
                    id=proposed_id,
                    research_job_id=job_id,
                    workflow_execution_id=workflow_execution_id,
                    evaluation_run_id=eval_run_id,
                    decision=policy_decision.action,
                    failure_category=policy_decision.failure_category.value,
                    reason_code=policy_decision.reason_code,
                    decision_fingerprint=fp,
                    created_at=at,
                )
                created = authoritative_id == proposed_id
                if created and policy_decision.action == "repair":
                    ok = self._job_repo.increment_repair_count(
                        sess,
                        job_id=job_id,
                        claim_token=job_claim_token,
                        at=at,
                    )
                    if not ok:
                        raise ClaimOwnershipError("increment_repair_count")
            # Emitted only after the ``with`` block above has committed, and
            # only for a freshly-persisted decision (Slice 15A2 correction) --
            # see ``_handle_exception``'s equivalent comment.
            if created:
                self._metrics.observe_recovery_decision(
                    action=policy_decision.action,
                    failure_category=policy_decision.failure_category.value,
                )
            return policy_decision.action

        recovery_repo = self._recovery_repo

        def _completion_auth_checker(
            *,
            job_id: str,
            workflow_execution_id: str,
            candidate_fingerprint: str,
        ) -> bool:
            if not candidate_fingerprint or len(candidate_fingerprint) != 64:
                return False
            with session_scope(session_factory) as session:
                decision = recovery_repo.get_latest_approve_for_execution(
                    session,
                    research_job_id=job_id,
                    workflow_execution_id=workflow_execution_id,
                )
                if decision is None:
                    return False
                return decision.candidate_fingerprint == candidate_fingerprint

        return WorkflowRuntimeContext(
            planner_specialist=BoundedPlannerSpecialist(planner),
            research_specialist=GovernedResearchRetrievalSpecialist(
                research_executor=research_executor,
                evidence_ingest=evidence_ingest,
                evidence_retriever=evidence_retriever,
            ),
            synthesizer=BoundedReportSynthesizer(
                drafter=drafter,
                evidence_ingest=evidence_ingest,
            ),
            citation_verifier=citation_verifier,
            plan_prompt_version=self._settings.plan_prompt_version,
            draft_prompt_version=self._settings.draft_prompt_version,
            workflow_execution_id=workflow_execution_id,
            hooks=hooks,
            node_counters=self._node_counters,
            report_service=report_service,
            retrieval_k=self._settings.retrieval_default_k,
            evaluation_runner=evaluation_runner,
            job_claim_token=job_claim_token,
            policy_callback=_policy_callback,
            completion_auth_checker=_completion_auth_checker,
            metrics=self._metrics,
        )
