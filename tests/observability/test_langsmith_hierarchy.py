"""Native LangGraph runs plus explicit Atlas traces for spike-proven gaps."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langsmith import get_current_run_tree

from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.evaluation.contracts import EvaluationCandidateInput
from atlas.evaluation.runner import EvaluationRunner
from atlas.evidence.retrieve import EvidenceRetriever
from atlas.models.contracts import DraftRequest, PlanRequest, PlanResult
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.observability.langsmith import (
    TracedResearchDrafter,
    TracedResearchPlanner,
    reset_langsmith_for_tests,
    trace_research_job,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.workflow.graph import (
    build_research_graph,
    default_fake_runtime_context,
    initial_graph_state,
)
from tests.observability.langsmith_fakes import arm_dummy_langsmith

NATIVE_GRAPH_NAMES = frozenset(
    {
        "atlas.research_job",
        "atlas.research_graph",
        "validate",
        "plan",
        "research",
        "draft",
        "verify_citations",
        "evaluate",
        "policy",
        "complete",
    }
)
EXPLICIT_MODEL_NAMES = frozenset({"model.plan", "model.draft"})
SPECIALIST_RUN_NAMES = frozenset(
    {
        "BoundedPlannerSpecialist",
        "GovernedResearchRetrievalSpecialist",
        "BoundedReportSynthesizer",
        "DurableCitationVerifier",
    }
)


@pytest.fixture(autouse=True)
def _reset_langsmith_handle() -> Iterator[None]:
    reset_langsmith_for_tests()
    yield
    reset_langsmith_for_tests()


def _flatten_names(tree: Any | None) -> set[str]:
    names: set[str] = set()
    if tree is None:
        return names
    name = getattr(tree, "name", None)
    if isinstance(name, str):
        names.add(name)
    for child in getattr(tree, "child_runs", None) or []:
        names.update(_flatten_names(child))
    return names


def _find_named(tree: Any | None, name: str) -> Any | None:
    if tree is None:
        return None
    if getattr(tree, "name", None) == name:
        return tree
    for child in getattr(tree, "child_runs", None) or []:
        found = _find_named(child, name)
        if found is not None:
            return found
    return None


def _assert_no_direct_llm_in_llm(tree: Any | None) -> None:
    if tree is None:
        return
    children = getattr(tree, "child_runs", None) or []
    if getattr(tree, "run_type", None) == "llm":
        for child in children:
            assert getattr(child, "run_type", None) != "llm"
    for child in children:
        _assert_no_direct_llm_in_llm(child)


def _invoke_under_job(
    *,
    job_id: str,
    execution_id: str,
    fn: Any,
) -> tuple[Any, set[str], Any]:
    captured: dict[str, Any] = {}

    def _run() -> Any:
        result = fn()
        captured["tree"] = get_current_run_tree()
        captured["names"] = _flatten_names(captured["tree"])
        return result

    result = trace_research_job(
        job_id=job_id, workflow_execution_id=execution_id, fn=_run
    )
    return result, captured["names"], captured["tree"]


def test_native_langgraph_hierarchy_without_explicit_model_wraps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)
    assert handle.enabled is True
    question = "Explain reliability"
    context = default_fake_runtime_context()
    graph = build_research_graph(checkpointer=InMemorySaver())
    config: RunnableConfig = {
        "configurable": {"thread_id": "ls-native-1"},
        "run_name": "atlas.research_graph",
        "tags": ["atlas", "research-job"],
    }

    def _invoke() -> object:
        return graph.invoke(
            initial_graph_state(job_id="ls-native-1", question=question),
            config,
            context=context,
        )

    _result, names, tree = _invoke_under_job(
        job_id="ls-native-1", execution_id="ls-native-1", fn=_invoke
    )
    handle.close()
    assert NATIVE_GRAPH_NAMES <= names
    assert names.isdisjoint(EXPLICIT_MODEL_NAMES)
    assert names.isdisjoint(SPECIALIST_RUN_NAMES)
    _assert_no_direct_llm_in_llm(tree)


def test_traced_adapters_add_model_plan_and_draft_under_native_nodes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)
    context = default_fake_runtime_context()
    context = replace(
        context,
        planner_specialist=BoundedPlannerSpecialist(
            TracedResearchPlanner(DeterministicResearchPlanner())
        ),
        synthesizer=BoundedReportSynthesizer(
            drafter=TracedResearchDrafter(DeterministicResearchDrafter()),
        ),
    )
    graph = build_research_graph(checkpointer=InMemorySaver())
    config: RunnableConfig = {
        "configurable": {"thread_id": "ls-wrapped-1"},
        "run_name": "atlas.research_graph",
        "tags": ["atlas", "research-job"],
    }

    def _invoke() -> object:
        return graph.invoke(
            initial_graph_state(job_id="ls-wrapped-1", question="Explain reliability"),
            config,
            context=context,
        )

    _result, names, tree = _invoke_under_job(
        job_id="ls-wrapped-1", execution_id="ls-wrapped-1", fn=_invoke
    )
    handle.close()
    assert NATIVE_GRAPH_NAMES <= names
    assert EXPLICIT_MODEL_NAMES <= names
    assert names.isdisjoint(SPECIALIST_RUN_NAMES)
    plan_run = _find_named(tree, "model.plan")
    draft_run = _find_named(tree, "model.draft")
    assert plan_run is not None
    assert draft_run is not None
    assert plan_run.run_type == "llm"
    assert draft_run.run_type == "llm"
    _assert_no_direct_llm_in_llm(tree)


def test_traced_planner_and_drafter_emit_explicit_runs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)
    planner = TracedResearchPlanner(DeterministicResearchPlanner())
    drafter = TracedResearchDrafter(DeterministicResearchDrafter())

    def _run() -> None:
        plan = planner.plan(
            PlanRequest(
                job_id="ls-adapt-1",
                question="Explain reliability",
                prompt_version="plan.v1",
            )
        )
        drafter.draft(
            DraftRequest(
                job_id="ls-adapt-1",
                question="Explain reliability",
                plan=list(plan.tasks),
                findings=["a finding about reliability"],
                prompt_version="draft.v2",
            )
        )

    _result, names, tree = _invoke_under_job(
        job_id="ls-adapt-1", execution_id="ls-adapt-1", fn=_run
    )
    handle.close()
    assert "atlas.research_job" in names
    assert EXPLICIT_MODEL_NAMES <= names
    plan_run = _find_named(tree, "model.plan")
    draft_run = _find_named(tree, "model.draft")
    assert plan_run is not None
    assert draft_run is not None
    assert plan_run.run_type == "llm"
    assert draft_run.run_type == "llm"
    _assert_no_direct_llm_in_llm(tree)


def test_langchain_backed_planner_uses_chain_parent_not_nested_llm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)

    class _LangChainBackedPlanner:
        def __init__(self) -> None:
            self._model = FakeListChatModel(responses=["ok"])

        def plan(self, request: PlanRequest) -> PlanResult:
            self._model.invoke(request.question)
            return DeterministicResearchPlanner().plan(request)

    planner = TracedResearchPlanner(_LangChainBackedPlanner(), native_llm=True)

    def _run() -> None:
        planner.plan(
            PlanRequest(
                job_id="ls-lc-1",
                question="Explain reliability",
                prompt_version="plan.v1",
            )
        )

    _result, names, tree = _invoke_under_job(
        job_id="ls-lc-1", execution_id="ls-lc-1", fn=_run
    )
    handle.close()
    assert "model.plan" in names
    plan_run = _find_named(tree, "model.plan")
    assert plan_run is not None
    assert plan_run.run_type == "chain"
    llm_children = [
        child
        for child in (getattr(plan_run, "child_runs", None) or [])
        if getattr(child, "run_type", None) == "llm"
    ]
    assert llm_children
    _assert_no_direct_llm_in_llm(tree)


def test_evaluation_runner_emits_run_and_dimension_traces(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)
    runner = EvaluationRunner(evaluation_service=object())  # type: ignore[arg-type]
    candidate = EvaluationCandidateInput(
        job_id="ls-eval-1",
        question="Explain reliability",
        plan=[
            "Clarify reliability scope",
            "Gather citationevidence sources",
            "Identify residualrisks issues",
        ],
        findings=[
            "Clarify reliability scope is required",
            "Gather citationevidence sources from the pack",
            "Identify residualrisks issues for operators",
        ],
        draft="The report will clarify reliability scope.",
        claims=[],
        evidence_item_ids=[],
    )

    def _run() -> None:
        runner._grade(
            candidate=candidate,
            linked_ids=set(),
            tool_rows=[],
            provenance_ok=True,
        )

    _result, names, _tree = _invoke_under_job(
        job_id="ls-eval-1", execution_id="ls-eval-1", fn=_run
    )
    handle.close()
    assert "dimension.citation_integrity" in names
    assert "dimension.tool_use" in names
    assert "dimension.report_structure" in names
    assert "dimension.coverage" in names
    assert "dimension.completeness" in names
    assert "dimension.lexical_id_groundedness" in names
    assert "dimension.semantic_groundedness" in names


def test_retrieval_emits_explicit_retriever_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    handle, _session = arm_dummy_langsmith(monkeypatch, tmp_path)

    class _FakeRepo:
        def retrieve_exact(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

        def retrieve_hnsw(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

    class _FakeSession:
        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    retriever = EvidenceRetriever(
        session_factory=lambda: _FakeSession(),  # type: ignore[arg-type]
        embedder=DeterministicFakeEmbedder(),
        repository=_FakeRepo(),  # type: ignore[arg-type]
        use_hnsw=False,
    )

    def _run() -> None:
        retriever.retrieve(query="reliability", k=3, research_job_id="ls-ret-1")

    _result, names, _tree = _invoke_under_job(
        job_id="ls-ret-1", execution_id="ls-ret-1", fn=_run
    )
    handle.close()
    assert "retrieval" in names
