"""Typed LangGraph research workflow with specialist runtime context."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict, cast
from uuid import uuid4

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import Runtime

from atlas.evaluation.contracts import (
    EVALUATION_PROFILE,
    EvaluationCandidateInput,
    EvaluationRunResult,
    ToolSummaryRow,
)
from atlas.evaluation.errors import (
    EvaluationError,
    EvaluationTerminalError,
    EvaluationValidationError,
    sanitize_evaluation_error,
)
from atlas.evaluation.ports import EvaluationNodeRunner
from atlas.evidence.contracts import ClaimStructured
from atlas.evidence.retrieve import EvidenceRetriever
from atlas.evidence.service import (
    EvidenceIngestService,
    ReportArtifactService,
)
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.specialists.contracts import (
    CitationVerifierInput,
    PlannerInput,
    ResearchSpecialistInput,
    SynthesizerInput,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.ports import (
    CitationVerifier,
    PlannerSpecialist,
    ReportSynthesizer,
    ResearchRetrievalSpecialist,
)
from atlas.specialists.research import GovernedResearchRetrievalSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.tools.composition import build_fake_registry, build_tool_budgets
from atlas.tools.contracts import ToolId
from atlas.tools.registry import default_permission_policy
from atlas.tools.runner import SimpleResearchExecutor
from atlas.workflow.fakes import format_research_report

NODE_NAMES: tuple[str, ...] = (
    "validate",
    "plan",
    "research",
    "draft",
    "verify_citations",
    "evaluate",
    "policy",
    "repair",
    "await_review",
    "complete",
    "terminal",
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
    evaluation_passed: bool
    evaluation_run_id: str
    disposition: str
    repair_count: int
    evaluation_attempt: int
    evaluation_fingerprint: str


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


PolicyCallback = Callable[
    [bool, str, int, int],
    str,
]

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkflowRuntimeContext:
    """LangGraph runtime context for specialist-backed nodes.

    Passed via ``graph.invoke(..., context=...)`` and read through
    ``Runtime[WorkflowRuntimeContext]``. Not stored in checkpoints.
    """

    planner_specialist: PlannerSpecialist
    research_specialist: ResearchRetrievalSpecialist
    synthesizer: ReportSynthesizer
    citation_verifier: CitationVerifier
    plan_prompt_version: str
    draft_prompt_version: str
    workflow_execution_id: str | None = None
    hooks: NodeAuditHooks | None = None
    node_counters: dict[str, int] | None = None
    report_service: ReportArtifactService | None = None
    retrieval_k: int = 5
    evaluation_runner: EvaluationNodeRunner | None = None
    job_claim_token: str | None = None
    policy_callback: PolicyCallback | None = None
    # Never checkpointed. Validates human-review override for complete.
    completion_auth_checker: Callable[..., bool] | None = None
    # ``None`` (not a mutable default) resolves to the process-wide
    # ``default_metrics()`` singleton in ``_wrap_node`` -- tests inject an
    # isolated ``AtlasMetrics`` instance here instead.
    metrics: AtlasMetrics | None = None


# Isolated fake claim for pure unit graph tests (not a production secret).
UNIT_TEST_JOB_CLAIM_TOKEN = "0" * 64


# Backward-compatible alias during Milestone 9 rename.
ModelRuntimeContext = WorkflowRuntimeContext


class _AlwaysPassEvaluationRunner:
    """In-memory evaluation stub for pure unit graph tests (no Postgres)."""

    def run(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        job_claim_token: str,
        provenance_ok: bool = True,
    ) -> EvaluationRunResult:
        del deadline, provenance_ok, job_claim_token
        return EvaluationRunResult(
            run_id=f"fake-eval-{uuid4()}",
            research_job_id=candidate.job_id,
            workflow_execution_id=workflow_execution_id or "fake-execution",
            evaluation_profile=candidate.evaluation_profile,
            evaluation_attempt=candidate.evaluation_attempt,
            status="SUCCEEDED",
            input_fingerprint="0" * 64,
            passed=True,
            aggregate_score=1.0,
            disposition_hint="complete",
            dimensions=[],
            grader_versions={},
        )

    def provenance_ok_for_claims(
        self,
        *,
        job_id: str,
        claims: list[ClaimStructured],
    ) -> bool:
        del job_id
        # Unit graph stub: claims without a durable validator fail closed.
        return not claims


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
    citation_verifier: CitationVerifier | None = None,
    evaluation_runner: EvaluationNodeRunner | None = None,
    job_claim_token: str = UNIT_TEST_JOB_CLAIM_TOKEN,
    metrics: AtlasMetrics | None = None,
) -> WorkflowRuntimeContext:
    """Build a deterministic fake specialist runtime context."""
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
    planner = BoundedPlannerSpecialist(DeterministicResearchPlanner())
    research = GovernedResearchRetrievalSpecialist(
        research_executor=executor,
        evidence_ingest=evidence_ingest,
        evidence_retriever=evidence_retriever,
    )
    synthesizer = BoundedReportSynthesizer(
        drafter=DeterministicResearchDrafter(),
        evidence_ingest=evidence_ingest,
    )
    verifier = citation_verifier
    if verifier is None:
        verifier = _NoOpCitationVerifier()
    return WorkflowRuntimeContext(
        planner_specialist=planner,
        research_specialist=research,
        synthesizer=synthesizer,
        citation_verifier=verifier,
        plan_prompt_version=plan_prompt_version,
        draft_prompt_version=draft_prompt_version,
        workflow_execution_id=workflow_execution_id,
        hooks=hooks,
        node_counters=node_counters,
        report_service=report_service,
        retrieval_k=retrieval_k,
        evaluation_runner=(
            evaluation_runner
            if evaluation_runner is not None
            else _AlwaysPassEvaluationRunner()
        ),
        job_claim_token=job_claim_token,
        metrics=metrics,
    )


class _NoOpCitationVerifier:
    """Unit-test verifier when no durable evidence store is wired."""

    def run(self, request: CitationVerifierInput) -> Any:
        from atlas.specialists.contracts import CitationVerifierOutput

        if request.claims:
            raise RuntimeError(
                "citation verifier requires durable evidence wiring for claims"
            )
        return CitationVerifierOutput(claims=[])


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
        "evaluation_passed": False,
        "evaluation_run_id": "",
        "disposition": "",
        "repair_count": 0,
        "evaluation_attempt": 0,
        "evaluation_fingerprint": "",
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


def _state_findings(state: ResearchGraphState) -> list[str]:
    raw = state.get("findings")
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


def _state_plan(state: ResearchGraphState) -> list[str]:
    raw = state.get("plan")
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


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
    """Produce a bounded research plan through the planner specialist."""
    result = runtime.context.planner_specialist.run(
        PlannerInput(
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
    """Run the research/retrieval specialist for tools, retrieval, and linking."""
    plan = _state_plan(state)
    result = runtime.context.research_specialist.run(
        ResearchSpecialistInput(
            job_id=state["job_id"],
            question=state["question"],
            plan=plan,
            workflow_execution_id=runtime.context.workflow_execution_id,
            workflow_node_attempt=workflow_node_attempt,
            retrieval_k=runtime.context.retrieval_k,
        )
    )
    return {
        "findings": list(result.findings),
        "evidence_item_ids": list(result.evidence_item_ids),
    }


def draft_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Synthesize the draft through the report synthesizer (node name stays draft)."""
    repair_count = cast(Mapping[str, Any], state).get("repair_count", 0)
    if not isinstance(repair_count, int):
        repair_count = 0
    prompt_version = runtime.context.draft_prompt_version
    if repair_count > 0:
        prompt_version = "draft.repair.v1"
    result = runtime.context.synthesizer.run(
        SynthesizerInput(
            job_id=state["job_id"],
            question=state["question"],
            plan=_state_plan(state),
            findings=_state_findings(state),
            evidence_item_ids=_state_evidence_ids(state),
            prompt_version=prompt_version,
        )
    )
    return {
        "draft": result.draft,
        "claims": [claim.model_dump() for claim in result.claims],
    }


def verify_citations_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Deterministically verify claim citations against durable job evidence."""
    claims = _state_claims(state)
    verified = runtime.context.citation_verifier.run(
        CitationVerifierInput(
            research_job_id=state["job_id"],
            claims=claims,
        )
    )
    return {"claims": [claim.model_dump() for claim in verified.claims]}


def _candidate_tool_summary(state: ResearchGraphState) -> list[ToolSummaryRow]:
    """Prefer tool summary stamped on state by research when present."""
    raw = cast(Mapping[str, Any], state).get("tool_summary")
    if not isinstance(raw, list) or not raw:
        return []
    rows: list[ToolSummaryRow] = []
    for item in raw:
        if isinstance(item, ToolSummaryRow):
            rows.append(item)
        elif isinstance(item, dict):
            rows.append(ToolSummaryRow.model_validate(item))
    return rows


def evaluate_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Grade the candidate report before accepted persistence."""
    runner = runtime.context.evaluation_runner
    if runner is None:
        raise EvaluationError("evaluation_runner is required")

    repair_count = cast(Mapping[str, Any], state).get("repair_count", 0)
    if not isinstance(repair_count, int):
        repair_count = 0
    evaluation_attempt_raw = cast(Mapping[str, Any], state).get("evaluation_attempt", 0)
    if not isinstance(evaluation_attempt_raw, int):
        evaluation_attempt_raw = 0
    evaluation_attempt = evaluation_attempt_raw + 1

    claims = _state_claims(state)
    candidate = EvaluationCandidateInput(
        job_id=state["job_id"],
        question=state["question"],
        plan=_state_plan(state),
        findings=_state_findings(state),
        draft=state["draft"],
        claims=claims,
        evidence_item_ids=_state_evidence_ids(state),
        tool_summary=_candidate_tool_summary(state),
        repair_count=repair_count,
        evaluation_attempt=evaluation_attempt,
        evaluation_profile=EVALUATION_PROFILE,
    )
    try:
        provenance_ok = runner.provenance_ok_for_claims(
            job_id=state["job_id"],
            claims=claims,
        )
    except EvaluationTerminalError:
        raise
    except EvaluationError:
        raise
    except Exception as exc:
        raise EvaluationTerminalError(sanitize_evaluation_error(exc)) from None

    deadline = datetime.now(UTC) + timedelta(seconds=25)
    execution_id = runtime.context.workflow_execution_id or ""
    job_claim_token = runtime.context.job_claim_token
    if not job_claim_token:
        raise EvaluationValidationError(
            "Job claim token is required for production evaluation."
        )
    try:
        result = runner.run(
            candidate=candidate,
            workflow_execution_id=execution_id,
            deadline=deadline,
            job_claim_token=job_claim_token,
            provenance_ok=provenance_ok,
        )
    except EvaluationTerminalError:
        raise
    except EvaluationError:
        raise
    except Exception as exc:
        raise EvaluationError(sanitize_evaluation_error(exc)) from None

    return {
        "evaluation_passed": bool(result.passed),
        "evaluation_run_id": result.run_id,
        "evaluation_attempt": evaluation_attempt,
        "evaluation_fingerprint": result.input_fingerprint,
    }


def policy_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Determine recovery disposition via the policy engine callback."""
    if state.get("evaluation_passed"):
        return {"disposition": "complete"}

    callback = runtime.context.policy_callback
    if callback is None:
        return {"disposition": "terminal"}

    repair_count = cast(Mapping[str, Any], state).get("repair_count", 0)
    if not isinstance(repair_count, int):
        repair_count = 0
    evaluation_attempt = cast(Mapping[str, Any], state).get("evaluation_attempt", 0)
    if not isinstance(evaluation_attempt, int):
        evaluation_attempt = 0
    run_id = state.get("evaluation_run_id", "")

    disposition = callback(False, run_id, repair_count, evaluation_attempt)
    return {"disposition": disposition}


def route_after_policy(
    state: ResearchGraphState,
) -> Literal["complete", "repair", "await_review", "terminal"]:
    """Route based on disposition from policy node."""
    disposition = cast(Mapping[str, Any], state).get("disposition", "")
    if disposition == "complete":
        return "complete"
    if disposition == "repair":
        return "repair"
    if disposition == "await_review":
        return "await_review"
    return "terminal"


def repair_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Increment repair_count; routing re-enters draft."""
    del runtime
    repair_count = cast(Mapping[str, Any], state).get("repair_count", 0)
    if not isinstance(repair_count, int):
        repair_count = 0
    return {"repair_count": repair_count + 1}


def await_review_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """No-op marker; interrupt_after triggers pause here."""
    del state, runtime
    return {}


def terminal_node(state: ResearchGraphState) -> dict[str, Any]:
    """Terminal failure path for failed candidate evaluation."""
    del state
    raise EvaluationTerminalError("evaluation failed")


def complete_node(
    state: ResearchGraphState,
    runtime: Runtime[WorkflowRuntimeContext],
) -> dict[str, Any]:
    """Format the final report and persist with defense-in-depth citation checks.

    Authorization requires either a passing evaluation or a durable human-review
    override for this exact execution and candidate fingerprint. Disposition
    ``await_review`` alone never authorizes persistence.
    """
    eval_passed = bool(state.get("evaluation_passed", False))
    fingerprint = cast(Mapping[str, Any], state).get("evaluation_fingerprint", "")
    if not isinstance(fingerprint, str):
        fingerprint = ""
    execution_id = runtime.context.workflow_execution_id
    if not eval_passed:
        checker = runtime.context.completion_auth_checker
        if checker is None or execution_id is None:
            raise ValueError(
                "complete_node requires evaluation_passed or override authorization"
            )
        if not checker(
            job_id=state["job_id"],
            workflow_execution_id=execution_id,
            candidate_fingerprint=fingerprint,
        ):
            raise ValueError("complete_node human-review override authorization failed")

    draft = state["draft"]
    if not draft.strip():
        raise ValueError("draft must be non-empty before complete")
    claims = _state_claims(state)
    report = format_research_report(
        question=state["question"],
        plan=_state_plan(state),
        findings=_state_findings(state),
        draft=draft,
        claims=claims,
    )
    if (
        runtime.context.report_service is not None
        and runtime.context.workflow_execution_id is not None
    ):
        claim_token = runtime.context.job_claim_token
        if not claim_token:
            raise ValueError("Job claim token is required for report persistence.")
        runtime.context.report_service.persist_final(
            research_job_id=state["job_id"],
            workflow_execution_id=runtime.context.workflow_execution_id,
            body_text=report,
            claims=claims,
            claim_token=claim_token,
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
        metrics = runtime.context.metrics or default_metrics()
        attempt: int | None = None
        if hooks is not None:
            attempt = hooks.begin(node_name)
        started_at = time.perf_counter()
        try:
            if node_name == "research":
                result = dict(node_fn(state, runtime, workflow_node_attempt=attempt))
            else:
                result = dict(node_fn(state, runtime))
        except Exception as exc:
            if hooks is not None and attempt is not None:
                hooks.fail(node_name, attempt, exc)
            metrics.observe_workflow_node(
                node_name=node_name,
                outcome="failed",
                duration_seconds=time.perf_counter() - started_at,
            )
            raise
        if hooks is not None and attempt is not None:
            hooks.complete(node_name, attempt)
        metrics.observe_workflow_node(
            node_name=node_name,
            outcome="completed",
            duration_seconds=time.perf_counter() - started_at,
        )
        return result

    return wrapped


DEFAULT_INTERRUPT_AFTER: tuple[str, ...] = ("await_review",)


def build_research_graph(
    *,
    checkpointer: object,
    interrupt_after: Sequence[str] | None = DEFAULT_INTERRUPT_AFTER,
) -> CompiledStateGraph[
    ResearchGraphState,
    WorkflowRuntimeContext,
    ResearchGraphState,
    ResearchGraphState,
]:
    """Compile the research graph with the given checkpointer."""
    graph: StateGraph[ResearchGraphState, WorkflowRuntimeContext] = StateGraph(
        ResearchGraphState,
        context_schema=WorkflowRuntimeContext,
    )
    graph.add_node(
        "validate",
        cast(Any, _wrap_node("validate", _as_runtime_node(validate_node))),
    )
    graph.add_node("plan", cast(Any, _wrap_node("plan", plan_node)))
    graph.add_node("research", cast(Any, _wrap_node("research", research_node)))
    graph.add_node("draft", cast(Any, _wrap_node("draft", draft_node)))
    graph.add_node(
        "verify_citations",
        cast(Any, _wrap_node("verify_citations", verify_citations_node)),
    )
    graph.add_node("evaluate", cast(Any, _wrap_node("evaluate", evaluate_node)))
    graph.add_node("policy", cast(Any, _wrap_node("policy", policy_node)))
    graph.add_node("repair", cast(Any, _wrap_node("repair", repair_node)))
    graph.add_node(
        "await_review", cast(Any, _wrap_node("await_review", await_review_node))
    )
    graph.add_node("complete", cast(Any, _wrap_node("complete", complete_node)))
    graph.add_node(
        "terminal",
        cast(Any, _wrap_node("terminal", _as_runtime_node(terminal_node))),
    )
    graph.add_edge(START, "validate")
    graph.add_edge("validate", "plan")
    graph.add_edge("plan", "research")
    graph.add_edge("research", "draft")
    graph.add_edge("draft", "verify_citations")
    graph.add_edge("verify_citations", "evaluate")
    graph.add_edge("evaluate", "policy")
    graph.add_conditional_edges(
        "policy",
        route_after_policy,
        {
            "complete": "complete",
            "repair": "repair",
            "await_review": "await_review",
            "terminal": "terminal",
        },
    )
    graph.add_edge("repair", "draft")
    graph.add_edge("await_review", "complete")
    graph.add_edge("complete", END)
    graph.add_edge("terminal", END)
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
