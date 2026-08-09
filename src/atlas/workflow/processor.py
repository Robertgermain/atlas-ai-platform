"""LangGraph research processor and Postgres checkpointer lifecycle."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from atlas.config.settings import Settings, get_settings
from atlas.evidence.service import EvidenceIngestService, ReportArtifactService
from atlas.models.composition import build_planner_and_drafter
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.workflow import SqlAlchemyWorkflowRepository
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

    from atlas.models.ports import ResearchDrafter, ResearchPlanner
    from atlas.tools.runner import ResearchPlanExecutor
    from atlas.workflow.graph import ResearchGraphState


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

    Does not finalize ResearchJob rows; the worker retains job lifecycle ownership.
    Plan and draft use ResearchPlanner/ResearchDrafter ports from runtime context.
    Research uses governed ResearchPlanExecutor tools from runtime context.
    """

    def __init__(
        self,
        *,
        checkpointer: PostgresSaver,
        session_factory: sessionmaker[Session],
        settings: Settings | None = None,
        repository: SqlAlchemyWorkflowRepository | None = None,
        interrupt_after: Sequence[str] | None = None,
        node_counters: dict[str, int] | None = None,
        planner: ResearchPlanner | None = None,
        drafter: ResearchDrafter | None = None,
        research_executor: ResearchPlanExecutor | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings or get_settings()
        self._repository = repository or SqlAlchemyWorkflowRepository()
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

    def __call__(self, question: str, *, job_id: str) -> str:
        thread_id = job_id
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        at = datetime.now(UTC)

        with session_scope(self._session_factory) as session:
            self._repository.abandon_unfinished_for_job(
                session,
                research_job_id=job_id,
                at=at,
            )
            execution_id = self._repository.create_execution(
                session,
                research_job_id=job_id,
                thread_id=thread_id,
                at=at,
            )

        hooks = RepositoryNodeAuditHooks(
            session_factory=self._session_factory,
            repository=self._repository,
            workflow_execution_id=execution_id,
        )
        context = self._build_context(
            workflow_execution_id=execution_id,
            hooks=hooks,
        )
        try:
            result = self._invoke(
                question=question,
                job_id=job_id,
                config=config,
                context=context,
            )
        except Exception:
            with session_scope(self._session_factory) as session:
                self._repository.fail_execution(
                    session,
                    execution_id=execution_id,
                    at=datetime.now(UTC),
                )
            raise
        else:
            with session_scope(self._session_factory) as session:
                self._repository.complete_execution(
                    session,
                    execution_id=execution_id,
                    at=datetime.now(UTC),
                )
            return result

    def _build_context(
        self,
        *,
        workflow_execution_id: str,
        hooks: NodeAuditHooks | None,
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
        )

    def _invoke(
        self,
        *,
        question: str,
        job_id: str,
        config: RunnableConfig,
        context: WorkflowRuntimeContext,
    ) -> str:
        snapshot = self._graph.get_state(config)
        if snapshot.values and snapshot.next:
            final_state = self._graph.invoke(None, config, context=context)
        elif snapshot.values and not snapshot.next:
            existing = snapshot.values.get("result")
            if isinstance(existing, str) and existing.strip():
                return existing
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

        if not isinstance(final_state, dict):
            raise RuntimeError("Workflow returned a non-mapping state")
        # After interrupt_after, invoke returns values while work remains pending.
        snapshot_after = self._graph.get_state(config)
        if snapshot_after.next:
            raise RuntimeError("Workflow interrupted before completion")
        result = final_state.get("result")
        if not isinstance(result, str) or not result.strip():
            raise RuntimeError("Workflow finished without a result")
        return result
