"""Unit tests for Milestone 11 specialist boundaries."""

from __future__ import annotations

import pytest

from atlas.evidence.contracts import (
    ClaimStructured,
    EvidenceContextItem,
    EvidenceStrength,
)
from atlas.models.contracts import (
    DraftRequest,
    DraftResult,
    FinishOutcome,
    ModelCallMeta,
    ProviderId,
    RetryClass,
)
from atlas.models.fakes import (
    DeterministicResearchDrafter,
    DeterministicResearchPlanner,
)
from atlas.specialists.contracts import (
    PlannerInput,
    ResearchSpecialistInput,
    SynthesizerInput,
)
from atlas.specialists.errors import (
    SpecialistConfigurationError,
    SpecialistValidationError,
)
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.research import (
    GovernedResearchRetrievalSpecialist,
    merge_evidence_ids_preserving_order,
)
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.tools.runner import ResearchNodeOutcome


class _FixedExecutor:
    def __init__(
        self,
        findings: list[str],
        evidence_item_ids: list[str] | None = None,
    ) -> None:
        self._findings = findings
        self._evidence_item_ids = list(evidence_item_ids or [])

    def research(
        self,
        *,
        plan: list[str],
        context: object,
    ) -> ResearchNodeOutcome:
        del plan, context
        return ResearchNodeOutcome(
            findings=list(self._findings),
            evidence_item_ids=list(self._evidence_item_ids),
        )


class _FakeRetriever:
    def __init__(self, evidence_ids: list[str]) -> None:
        self._evidence_ids = evidence_ids

    def retrieve(self, **kwargs: object) -> list[object]:
        del kwargs

        class _Hit:
            def __init__(self, evidence_id: str) -> None:
                self.evidence = type("E", (), {"id": evidence_id})()

        return [_Hit(item_id) for item_id in self._evidence_ids]


class _FakeIngest:
    def link_evidence_to_job(self, **kwargs: object) -> None:
        del kwargs


class _IllegalClaimDrafter:
    def draft(self, request: DraftRequest) -> DraftResult:
        return DraftResult(
            draft="Illegal citation draft",
            claims=[
                ClaimStructured(
                    text="Unsupported claim",
                    evidence_item_ids=["not-in-pack"],
                )
            ],
            meta=ModelCallMeta(
                provider=ProviderId.FAKE,
                model="test",
                prompt_version=request.prompt_version,
                latency_ms=0,
                finish_outcome=FinishOutcome.COMPLETED,
                retry_class=RetryClass.NONE,
                status="succeeded",
            ),
        )


def test_planner_specialist_enforces_three_tasks() -> None:
    specialist = BoundedPlannerSpecialist(DeterministicResearchPlanner())
    output = specialist.run(
        PlannerInput(
            job_id="job-1",
            question="What is Atlas?",
            prompt_version="plan.v1",
        )
    )
    assert len(output.tasks) == 3
    assert output.specialist_id == "planner"


def test_planner_specialist_rejects_empty_question() -> None:
    specialist = BoundedPlannerSpecialist(DeterministicResearchPlanner())
    with pytest.raises(SpecialistValidationError):
        specialist.run(
            PlannerInput(job_id="job-1", question="   ", prompt_version="plan.v1")
        )


def test_research_specialist_allows_fewer_than_three_findings() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(["only one finding"]),
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.findings == ["only one finding"]
    assert output.evidence_item_ids == []


def test_research_specialist_does_not_pad_findings() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor([]),
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.findings == []


def test_merge_evidence_ids_dedupes_tool_and_retrieval_preserving_order() -> None:
    merged = merge_evidence_ids_preserving_order(
        ["tool-a", "tool-b", "tool-a", "tool-c"],
        ["tool-b", "retr-x", "tool-c", "retr-y", "retr-x"],
    )
    assert merged == ["tool-a", "tool-b", "tool-c", "retr-x", "retr-y"]


def test_research_specialist_dedupes_tool_and_retrieval_ids_in_order() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(
            ["finding"],
            evidence_item_ids=["tool-a", "tool-b", "tool-a", "tool-c"],
        ),
        evidence_ingest=_FakeIngest(),  # type: ignore[arg-type]
        evidence_retriever=_FakeRetriever(
            ["tool-b", "retr-x", "tool-c", "retr-y", "retr-x"]
        ),  # type: ignore[arg-type]
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.evidence_item_ids == [
        "tool-a",
        "tool-b",
        "tool-c",
        "retr-x",
        "retr-y",
    ]


def test_research_specialist_allows_neither_retriever_nor_ingest() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(["finding"]),
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.findings == ["finding"]


def test_research_specialist_allows_ingest_without_retriever() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(["finding"], evidence_item_ids=["tool-a"]),
        evidence_ingest=_FakeIngest(),  # type: ignore[arg-type]
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.evidence_item_ids == ["tool-a"]


def test_research_specialist_allows_retriever_with_ingest() -> None:
    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(["finding"], evidence_item_ids=["tool-a"]),
        evidence_ingest=_FakeIngest(),  # type: ignore[arg-type]
        evidence_retriever=_FakeRetriever(["retr-x"]),  # type: ignore[arg-type]
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.evidence_item_ids == ["tool-a", "retr-x"]


def test_research_specialist_rejects_retriever_without_ingest() -> None:
    with pytest.raises(
        SpecialistConfigurationError,
        match="retriever requires evidence ingest",
    ):
        GovernedResearchRetrievalSpecialist(
            research_executor=_FixedExecutor(["finding"]),
            evidence_retriever=_FakeRetriever(["retr-x"]),  # type: ignore[arg-type]
        )


def test_synthesizer_rejects_claims_outside_pack() -> None:
    class _PackIngest:
        def build_drafter_evidence_pack(
            self, evidence_item_ids: list[str]
        ) -> list[EvidenceContextItem]:
            del evidence_item_ids
            return [
                EvidenceContextItem(
                    evidence_item_id="pack-1",
                    text="pack text",
                    source_display_uri="corpus://pack",
                    strength=EvidenceStrength.DOCUMENT_CHUNK,
                    trust_label="[operator_corpus]",
                )
            ]

    specialist = BoundedReportSynthesizer(
        drafter=_IllegalClaimDrafter(),
        evidence_ingest=_PackIngest(),  # type: ignore[arg-type]
    )
    with pytest.raises(SpecialistValidationError, match="outside the pack"):
        specialist.run(
            SynthesizerInput(
                job_id="job-1",
                question="Q",
                plan=["a", "b", "c"],
                findings=["f1"],
                evidence_item_ids=["pack-1"],
                prompt_version="draft.v2",
            )
        )


def test_synthesizer_rejects_claims_when_pack_empty() -> None:
    specialist = BoundedReportSynthesizer(
        drafter=_IllegalClaimDrafter(),
        evidence_ingest=None,
    )
    with pytest.raises(SpecialistValidationError, match="evidence pack"):
        specialist.run(
            SynthesizerInput(
                job_id="job-1",
                question="Q",
                plan=["a", "b", "c"],
                findings=["f1"],
                evidence_item_ids=[],
                prompt_version="draft.v2",
            )
        )


def test_synthesizer_accepts_empty_claims_without_pack() -> None:
    specialist = BoundedReportSynthesizer(
        drafter=DeterministicResearchDrafter(),
        evidence_ingest=None,
    )
    output = specialist.run(
        SynthesizerInput(
            job_id="job-1",
            question="Q",
            plan=["a", "b", "c"],
            findings=["f1"],
            evidence_item_ids=[],
            prompt_version="draft.v2",
        )
    )
    assert output.draft
    assert output.claims == []
