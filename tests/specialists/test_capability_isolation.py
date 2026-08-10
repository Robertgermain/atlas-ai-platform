"""Capability isolation for Milestone 11 specialists (composition/spy evidence).

These tests prove each specialist receives only the ports it is allowed to use.
They do not introduce a permission framework.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver

from atlas.evidence.service import CitationValidator, EvidenceIngestService
from atlas.models.contracts import (
    DraftRequest,
    DraftResult,
    PlanRequest,
    PlanResult,
)
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.specialists.citation_verifier import DurableCitationVerifier
from atlas.specialists.contracts import (
    CitationVerifierInput,
    PlannerInput,
    ResearchSpecialistInput,
    SynthesizerInput,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.research import GovernedResearchRetrievalSpecialist
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.tools.runner import ResearchNodeOutcome
from atlas.workflow.graph import (
    NODE_NAMES,
    UNIT_TEST_JOB_CLAIM_TOKEN,
    WorkflowRuntimeContext,
    build_research_graph,
    complete_node,
)


class _ForbiddenPort:
    """Raises if any specialist incorrectly reaches a disallowed capability."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"forbidden capability accessed: {name}")

    def __call__(self, *args: object, **kwargs: object) -> Any:
        del args, kwargs
        raise AssertionError("forbidden capability invoked")


class _RecordingPlanner:
    def __init__(self) -> None:
        self.calls = 0

    def plan(self, request: PlanRequest) -> PlanResult:
        self.calls += 1
        return DeterministicResearchPlanner().plan(request)


class _RecordingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def research(self, *, plan: list[str], context: object) -> ResearchNodeOutcome:
        del plan, context
        self.calls += 1
        return ResearchNodeOutcome(findings=["finding"], evidence_item_ids=[])


class _RecordingDrafter:
    def __init__(self) -> None:
        self.calls = 0

    def draft(self, request: DraftRequest) -> DraftResult:
        self.calls += 1
        return DeterministicResearchDrafter().draft(request)


class _RecordingRetriever:
    def __init__(self) -> None:
        self.calls = 0

    def retrieve(self, **kwargs: object) -> list[object]:
        del kwargs
        self.calls += 1
        return []


class _RecordingIngest:
    def __init__(self) -> None:
        self.pack_calls = 0
        self.link_calls = 0

    def build_drafter_evidence_pack(self, evidence_item_ids: list[str]) -> list[object]:
        del evidence_item_ids
        self.pack_calls += 1
        return []

    def link_evidence_to_job(self, **kwargs: object) -> None:
        del kwargs
        self.link_calls += 1


class _RecordingReportService:
    def __init__(self) -> None:
        self.persist_calls = 0

    def persist_final(self, **kwargs: object) -> object:
        del kwargs
        self.persist_calls += 1
        return object()


def test_planner_receives_only_model_planner_port() -> None:
    planner = _RecordingPlanner()
    specialist = BoundedPlannerSpecialist(planner)
    assert specialist._planner is planner
    assert not hasattr(specialist, "_research_executor")
    assert not hasattr(specialist, "_evidence_retriever")
    assert not hasattr(specialist, "_drafter")
    assert not hasattr(specialist, "_citation_validator")
    assert not hasattr(specialist, "_report_service")

    output = specialist.run(
        PlannerInput(job_id="job-1", question="Isolation?", prompt_version="plan.v1")
    )
    assert len(output.tasks) == 3
    assert planner.calls == 1


def test_research_specialist_is_sole_holder_of_executor_and_retriever() -> None:
    executor = _RecordingExecutor()
    retriever = _RecordingRetriever()
    ingest = _RecordingIngest()
    research = GovernedResearchRetrievalSpecialist(
        research_executor=executor,
        evidence_ingest=ingest,  # type: ignore[arg-type]
        evidence_retriever=retriever,  # type: ignore[arg-type]
    )
    planner = BoundedPlannerSpecialist(_RecordingPlanner())
    synthesizer = BoundedReportSynthesizer(
        drafter=_RecordingDrafter(),
        evidence_ingest=ingest,  # type: ignore[arg-type]
    )
    verifier = DurableCitationVerifier(
        citation_validator=_ForbiddenPort(),  # type: ignore[arg-type]
        evidence_ingest=_ForbiddenPort(),  # type: ignore[arg-type]
    )

    assert research._research_executor is executor
    assert research._evidence_retriever is not None
    assert not hasattr(planner, "_research_executor")
    assert not hasattr(planner, "_evidence_retriever")
    assert not hasattr(synthesizer, "_research_executor")
    assert not hasattr(synthesizer, "_evidence_retriever")
    assert not hasattr(verifier, "_research_executor")
    assert not hasattr(verifier, "_evidence_retriever")

    research.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert executor.calls == 1
    assert retriever.calls == 1


def test_synthesizer_receives_drafter_and_pack_but_no_tool_executor() -> None:
    drafter = _RecordingDrafter()
    ingest = _RecordingIngest()
    synthesizer = BoundedReportSynthesizer(
        drafter=drafter,
        evidence_ingest=ingest,  # type: ignore[arg-type]
    )
    assert synthesizer._drafter is drafter
    assert synthesizer._evidence_ingest is not None
    assert not hasattr(synthesizer, "_research_executor")
    assert not hasattr(synthesizer, "_evidence_retriever")
    assert not hasattr(synthesizer, "_tool_registry")

    synthesizer.run(
        SynthesizerInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
            findings=["f1"],
            evidence_item_ids=["ev-1"],
            prompt_version="draft.v2",
        )
    )
    assert drafter.calls == 1
    assert ingest.pack_calls == 1
    assert ingest.link_calls == 0


def test_citation_verifier_uses_only_deterministic_evidence_services() -> None:
    validator = type(
        "V",
        (),
        {
            "validate": staticmethod(
                lambda **kwargs: (_ for _ in ()).throw(AssertionError())
            )
        },
    )()
    ingest = type(
        "I",
        (),
        {
            "get_item": staticmethod(
                lambda *_a, **_k: (_ for _ in ()).throw(AssertionError())
            )
        },
    )()
    # Empty claims short-circuit before durable services are touched.
    verifier = DurableCitationVerifier(
        citation_validator=validator,
        evidence_ingest=ingest,
    )
    assert verifier._citation_validator is validator
    assert verifier._evidence_ingest is ingest
    assert not hasattr(verifier, "_planner")
    assert not hasattr(verifier, "_drafter")
    assert not hasattr(verifier, "_research_executor")
    assert not hasattr(verifier, "_tool_registry")
    assert isinstance(CitationValidator, type)
    assert isinstance(EvidenceIngestService, type)

    output = verifier.run(CitationVerifierInput(research_job_id="job-1", claims=[]))
    assert output.claims == []


def test_complete_node_formats_and_persists_without_models_or_tools() -> None:
    report_service = _RecordingReportService()
    forbidden = _ForbiddenPort()
    context = WorkflowRuntimeContext(
        planner_specialist=forbidden,
        research_specialist=forbidden,
        synthesizer=forbidden,
        citation_verifier=forbidden,
        plan_prompt_version="plan.v1",
        draft_prompt_version="draft.v2",
        workflow_execution_id="exec-1",
        report_service=report_service,  # type: ignore[arg-type]
        job_claim_token=UNIT_TEST_JOB_CLAIM_TOKEN,
    )
    runtime = SimpleNamespace(context=context)
    state = {
        "job_id": "job-1",
        "question": "Complete isolation",
        "plan": ["a", "b", "c"],
        "findings": ["f1"],
        "evidence_item_ids": [],
        "draft": "Draft body",
        "claims": [],
        "result": "",
        "evaluation_passed": True,
    }
    result = complete_node(state, runtime)  # type: ignore[arg-type]
    assert "Question:" in result["result"]
    assert "Draft:" in result["result"]
    assert report_service.persist_calls == 1


def test_linear_graph_has_no_autonomous_specialist_loop() -> None:
    assert NODE_NAMES == (
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
    graph = build_research_graph(checkpointer=InMemorySaver())
    spec = graph.get_graph()
    edge_pairs = {(edge.source, edge.target) for edge in spec.edges}
    assert ("validate", "plan") in edge_pairs
    assert ("plan", "research") in edge_pairs
    assert ("research", "draft") in edge_pairs
    assert ("draft", "verify_citations") in edge_pairs
    assert ("verify_citations", "evaluate") in edge_pairs
    assert ("evaluate", "policy") in edge_pairs
    assert ("policy", "complete") in edge_pairs
    assert ("policy", "terminal") in edge_pairs
    assert ("policy", "repair") in edge_pairs
    assert ("policy", "await_review") in edge_pairs
    assert ("repair", "draft") in edge_pairs
    assert ("await_review", "complete") in edge_pairs
    assert ("verify_citations", "complete") not in edge_pairs
    assert any(src == "complete" and "end" in tgt.lower() for src, tgt in edge_pairs)
    assert any(src == "terminal" and "end" in tgt.lower() for src, tgt in edge_pairs)
    assert ("research", "plan") not in edge_pairs
    assert ("complete", "validate") not in edge_pairs
