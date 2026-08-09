"""Typed LangGraph research workflow with workflow runtime context."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypedDict, cast

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import Runtime

from atlas.evidence.contracts import ClaimStructured, SourceKind
from atlas.evidence.retrieve import EvidenceRetriever
from atlas.evidence.service import EvidenceIngestService, ReportArtifactService
from atlas.models.contracts import DraftRequest, PlanRequest
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.models.ports import ResearchDrafter, ResearchPlanner
from atlas.tools.composition import build_fake_registry, build_tool_budgets
from atlas.tools.contracts import ToolId
from atlas.tools.registry import default_permission_policy
from atlas.tools.runner import (
    ResearchPlanExecutor,
    SimpleResearchExecutor,
    default_tool_call_context,
)
from atlas.workflow.fakes import format_research_report

NODE_NAMES: tuple[str, ...] = (
    "validate",
    "plan",
    "research",
    "draft",
    "complete",
)


class ResearchGraphState(TypedDict):
    """Typed channels for the research graph."""

    job_id: str
    question: str
    plan: list[str]
    findings: list[str]
    evidence_item_ids: list[str]
    draft: str
    claims: list[dict[str, Any]]
    result: str


class NodeAuditHooks:
    """Optional hooks invoked around each graph node (audit / test spies)."""

    def begin(self, node_name: str) -> int:
        """Record node start; return attempt number."""
        raise NotImplementedError

    def complete(self, node_name: str, attempt: int) -> None:
        """Record successful node completion."""
        raise NotImplementedError

    def fail(self, node_name: str, attempt: int, error: Exception) -> None:
        """Record node failure with a sanitized error."""
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class WorkflowRuntimeContext:
    """LangGraph runtime context for model- and tool-backed nodes.

    Passed via ``graph.invoke(..., context=...)`` and read through
    ``Runtime[WorkflowRuntimeContext]``. Not stored in checkpoints.
    """

    planner: ResearchPlanner
    drafter: ResearchDrafter
    research_executor: ResearchPlanExecutor
    plan_prompt_version: str
    draft_prompt_version: str
    workflow_execution_id: str | None = None
    hooks: NodeAuditHooks | None = None
    node_counters: dict[str, int] | None = None
    evidence_ingest: EvidenceIngestService | None = None
    report_service: ReportArtifactService | None = None
    evidence_retriever: EvidenceRetriever | None = None
    retrieval_k: int = 5


# Backward-compatible alias during Milestone 9 rename.
ModelRuntimeContext = WorkflowRuntimeContext


def default_fake_runtime_context(
    *,
    hooks: NodeAuditHooks | None = None,
    node_counters: dict[str, int] | None = None,
    plan_prompt_version: str = "plan.v1",
    draft_prompt_version: str = "draft.v2",
    fetch_enabled: bool = False,
    workflow_execution_id: str | None = None,
    evidence_ingest: EvidenceIngestService | None = None,
    report_service: ReportArtifactService | None = None,
    evidence_retriever: EvidenceRetriever | None = None,
    retrieval_k: int = 5,
) -> WorkflowRuntimeContext:
    """Build a deterministic fake planner/drafter/tools runtime context."""
    from atlas.config.settings import Settings

    settings = Settings(
        tool_provider="fake",
        tool_fetch_enabled=fetch_enabled,
    )
    registry = build_fake_registry()
    budgets = build_tool_budgets(settings)
    policy = default_permission_policy()
    executor = SimpleResearchExecutor(
        search_tool=registry.get(ToolId.WEB_SEARCH),
        fetch_tool=registry.get(ToolId.FETCH_URL),
        fetch_enabled=fetch_enabled,
        budgets=budgets,
        policy_assert=policy.assert_allowed,
        evidence_ingest=evidence_ingest,
    )
    return WorkflowRuntimeContext(
        planner=DeterministicResearchPlanner(),
        drafter=DeterministicResearchDrafter(),
        research_executor=executor,
        plan_prompt_version=plan_prompt_version,
        draft_prompt_version=draft_prompt_version,
        workflow_execution_id=workflow_execution_id,
        hooks=hooks,
        node_counters=node_counters,
        evidence_ingest=evidence_ingest,
        report_service=report_service,
        evidence_retriever=evidence_retriever,
        retrieval_k=retrieval_k,
    )


def initial_graph_state(*, job_id: str, question: str) -> ResearchGraphState:
    """Build the initial state for a new workflow thread."""
    return {
        "job_id": job_id,
        "question": question,
        "plan": [],
        "findings": [],
        "evidence_item_ids": [],
        "draft": "",
        "claims": [],
        "result": "",
    }


def _state_evidence_ids(state: ResearchGraphState) -> list[str]:
    raw = state.get("evidence_item_ids")
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


def _state_claims(state: ResearchGraphState) -> list[ClaimStructured]:
    raw = state.get("claims")
    if not isinstance(raw, list) or not raw:
        return []
    claims: list[ClaimStructured] = []
    for item in raw:
        if isinstance(item, ClaimStructured):
            claims.append(item)
        elif isinstance(item, dict):
            claims.append(ClaimStructured.model_validate(item))
    return claims


def validate_node(state: ResearchGraphState) -> dict[str, Any]:
    """Reject empty job_id or question."""
    job_id = state["job_id"].strip()
    question = state["question"].strip()
    if not job_id:
        raise ValueError("job_id must be non-empty")
    if not question:
        raise ValueError("question must be non-empty")
    return {"job_id": job_id, "question": question}


def plan_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Produce a bounded research plan through the planner port."""
    result = runtime.context.planner.plan(
        PlanRequest(
            job_id=state["job_id"],
            question=state["question"],
            prompt_version=runtime.context.plan_prompt_version,
        )
    )
    return {"plan": list(result.tasks)}


def research_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
    *,
    workflow_node_attempt: int | None = None,
) -> dict[str, Any]:
    """Run governed research tools, retrieve corpus evidence, and merge IDs."""
    plan = state["plan"]
    if len(plan) != 3:
        raise ValueError("plan must contain exactly three tasks")
    context = default_tool_call_context(
        research_job_id=state["job_id"],
        workflow_execution_id=runtime.context.workflow_execution_id,
        workflow_node_attempt=workflow_node_attempt,
    )
    outcome = runtime.context.research_executor.research(
        plan=plan,
        context=context,
    )
    if len(outcome.findings) != 3:
        raise ValueError("research must produce exactly three findings")

    evidence_ids = list(outcome.evidence_item_ids)
    retriever = runtime.context.evidence_retriever
    ingest = runtime.context.evidence_ingest
    if retriever is not None:
        query = " ".join([state["question"], *plan])
        # Default path follows settings.retrieval_use_hnsw (HNSW locally).
        # Offline CI eval forces mode="exact" so approximate index behavior
        # cannot gate Recall@K / MRR thresholds.
        retrieved = retriever.retrieve(
            query=query,
            k=runtime.context.retrieval_k,
            source_kinds=[SourceKind.CORPUS_TEXT],
            research_job_id=None,
            include_operator_corpus=True,
        )
        retrieved_ids = [item.evidence.id for item in retrieved]
        if retrieved_ids and ingest is not None:
            ingest.link_evidence_to_job(
                research_job_id=state["job_id"],
                evidence_item_ids=retrieved_ids,
                workflow_execution_id=runtime.context.workflow_execution_id,
            )
        # Preserve ranking: search evidence first, then retrieved; dedupe.
        seen = set(evidence_ids)
        for item_id in retrieved_ids:
            if item_id not in seen:
                evidence_ids.append(item_id)
                seen.add(item_id)

    return {
        "findings": list(outcome.findings),
        "evidence_item_ids": evidence_ids,
    }


def draft_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Draft an intermediate report through the drafter port."""
    plan = state["plan"]
    findings = state["findings"]
    if len(findings) != len(plan):
        raise ValueError("findings must align with plan tasks")
    evidence_ids = _state_evidence_ids(state)
    evidence_pack = []
    if runtime.context.evidence_ingest is not None and evidence_ids:
        evidence_pack = runtime.context.evidence_ingest.build_drafter_evidence_pack(
            evidence_ids
        )
    result = runtime.context.drafter.draft(
        DraftRequest(
            job_id=state["job_id"],
            question=state["question"],
            plan=plan,
            findings=findings,
            prompt_version=runtime.context.draft_prompt_version,
            evidence=evidence_pack,
        )
    )
    claims = list(result.claims)
    if not evidence_pack:
        claims = []
    return {
        "draft": result.draft,
        "claims": [claim.model_dump() for claim in claims],
    }


def complete_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Format the final stable research report and persist the artifact."""
    draft = state["draft"]
    if not draft.strip():
        raise ValueError("draft must be non-empty before complete")
    claims = _state_claims(state)
    if not _state_evidence_ids(state):
        claims = []
    report = format_research_report(
        question=state["question"],
        plan=state["plan"],
        findings=state["findings"],
        draft=draft,
        claims=claims,
    )
    if (
        runtime.context.report_service is not None
        and runtime.context.workflow_execution_id is not None
    ):
        runtime.context.report_service.persist_final(
            research_job_id=state["job_id"],
            workflow_execution_id=runtime.context.workflow_execution_id,
            body_text=report,
            claims=claims,
        )
    return {"result": report}


NodeFn = Callable[..., Mapping[str, Any]]


def _wrap_node(
    node_name: str,
    node_fn: NodeFn,
) -> Callable[[ResearchGraphState, Runtime[WorkflowRuntimeContext]], dict[str, Any]]:
    def wrapped(
        state: ResearchGraphState,
        runtime: Runtime[WorkflowRuntimeContext],
    ) -> dict[str, Any]:
        counters = runtime.context.node_counters
        if counters is not None:
            counters[node_name] = counters.get(node_name, 0) + 1

        hooks = runtime.context.hooks
        attempt: int | None = None
        if hooks is not None:
            attempt = hooks.begin(node_name)
        try:
            if node_name == "research":
                result = dict(node_fn(state, runtime, workflow_node_attempt=attempt))
            else:
                result = dict(node_fn(state, runtime))
        except Exception as exc:
            if hooks is not None and attempt is not None:
                hooks.fail(node_name, attempt, exc)
            raise
        if hooks is not None and attempt is not None:
            hooks.complete(node_name, attempt)
        return result

    return wrapped


def build_research_graph(
    *,
    checkpointer: object,
    interrupt_after: Sequence[str] | None = None,
) -> CompiledStateGraph[
    ResearchGraphState,
    WorkflowRuntimeContext,
    ResearchGraphState,
    ResearchGraphState,
]:
    """Compile the five-node research graph with the given checkpointer."""
    graph: StateGraph[ResearchGraphState, WorkflowRuntimeContext] = StateGraph(
        ResearchGraphState,
        context_schema=WorkflowRuntimeContext,
    )
    # LangGraph NodeCallable typing rejects our wrapped callables without cast.
    graph.add_node(
        "validate",
        cast(Any, _wrap_node("validate", _as_runtime_node(validate_node))),
    )
    graph.add_node("plan", cast(Any, _wrap_node("plan", plan_node)))
    graph.add_node("research", cast(Any, _wrap_node("research", research_node)))
    graph.add_node("draft", cast(Any, _wrap_node("draft", draft_node)))
    graph.add_node("complete", cast(Any, _wrap_node("complete", complete_node)))
    graph.add_edge(START, "validate")
    graph.add_edge("validate", "plan")
    graph.add_edge("plan", "research")
    graph.add_edge("research", "draft")
    graph.add_edge("draft", "complete")
    graph.add_edge("complete", END)
    return graph.compile(
        checkpointer=cast(Any, checkpointer),
        interrupt_after=list(interrupt_after) if interrupt_after else None,
    )


def _as_runtime_node(
    node_fn: Callable[[ResearchGraphState], Mapping[str, Any]],
) -> Callable[[ResearchGraphState, Runtime[WorkflowRuntimeContext]], Mapping[str, Any]]:
    def adapted(
        state: ResearchGraphState,
        runtime: Runtime[WorkflowRuntimeContext],
    ) -> Mapping[str, Any]:
        del runtime
        return node_fn(state)

    return adapted
