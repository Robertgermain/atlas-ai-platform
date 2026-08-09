"""Deterministic boundary/ablation evidence (unit, no database).

Labeled architecture evidence only — not semantic-quality grading (Milestone 12).
Database-backed ablation cases live in
``tests/integration/test_specialists_boundary_ablation.py``.
"""

from __future__ import annotations

from types import SimpleNamespace

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
    PlanRequest,
    ProviderId,
    RetryClass,
)
from atlas.specialists.contracts import (
    PlannerInput,
    ResearchSpecialistInput,
    SynthesizerInput,
)
from atlas.specialists.errors import SpecialistValidationError
from atlas.specialists.planner import BoundedPlannerSpecialist
from atlas.specialists.research import (
    GovernedResearchRetrievalSpecialist,
    merge_evidence_ids_preserving_order,
)
from atlas.specialists.synthesizer import BoundedReportSynthesizer
from atlas.tools.runner import ResearchNodeOutcome


def _meta(prompt_version: str) -> ModelCallMeta:
    return ModelCallMeta(
        provider=ProviderId.FAKE,
        model="ablation",
        prompt_version=prompt_version,
        latency_ms=0,
        finish_outcome=FinishOutcome.COMPLETED,
        retry_class=RetryClass.NONE,
        status="succeeded",
    )


class _FixedTaskPlanner:
    """Returns invalid task lists without constructing PlanResult."""

    def __init__(self, tasks: list[str]) -> None:
        self._tasks = tasks

    def plan(self, request: PlanRequest) -> SimpleNamespace:
        del request
        return SimpleNamespace(tasks=list(self._tasks))


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


class _OutsidePackDrafter:
    def draft(self, request: DraftRequest) -> DraftResult:
        return DraftResult(
            draft="Cites outside pack",
            claims=[
                ClaimStructured(
                    text="Bad claim",
                    evidence_item_ids=["not-in-pack"],
                )
            ],
            meta=_meta(request.prompt_version),
        )


@pytest.mark.parametrize(
    "tasks",
    [
        ["only-one"],
        ["a", "b"],
        ["a", "b", "c", "d"],
        ["a", " ", "c"],
    ],
)
def test_ablation_planner_rejects_non_three_nonempty_tasks(tasks: list[str]) -> None:
    """Planner boundary: only exactly three non-empty tasks are accepted."""
    specialist = BoundedPlannerSpecialist(_FixedTaskPlanner(tasks))  # type: ignore[arg-type]
    with pytest.raises(SpecialistValidationError):
        specialist.run(
            PlannerInput(
                job_id="job-ablation-plan",
                question="Need a valid plan",
                prompt_version="plan.v1",
            )
        )


def test_ablation_research_order_dedupe_and_no_fabricated_findings() -> None:
    """Research boundary: tool-first merge/dedupe; findings never padded."""
    assert merge_evidence_ids_preserving_order(
        ["tool-a", "tool-b", "tool-a", "tool-c"],
        ["tool-b", "retr-x", "tool-c", "retr-y"],
    ) == ["tool-a", "tool-b", "tool-c", "retr-x", "retr-y"]

    specialist = GovernedResearchRetrievalSpecialist(
        research_executor=_FixedExecutor(
            [],
            evidence_item_ids=["tool-a", "tool-b", "tool-a"],
        ),
        evidence_ingest=_FakeIngest(),  # type: ignore[arg-type]
        evidence_retriever=_FakeRetriever(["tool-b", "retr-x"]),  # type: ignore[arg-type]
    )
    output = specialist.run(
        ResearchSpecialistInput(
            job_id="job-ablation-research",
            question="Q",
            plan=["a", "b", "c"],
        )
    )
    assert output.findings == []
    assert output.evidence_item_ids == ["tool-a", "tool-b", "retr-x"]


def test_ablation_synthesizer_rejects_claim_outside_pack() -> None:
    """Synthesizer boundary: pack-scope claims only (no silent stripping)."""

    class _PackIngest:
        def build_drafter_evidence_pack(
            self, evidence_item_ids: list[str]
        ) -> list[EvidenceContextItem]:
            del evidence_item_ids
            return [
                EvidenceContextItem(
                    evidence_item_id="pack-1",
                    text="in pack",
                    source_display_uri="corpus://pack",
                    strength=EvidenceStrength.DOCUMENT_CHUNK,
                    trust_label="[operator_corpus]",
                )
            ]

    specialist = BoundedReportSynthesizer(
        drafter=_OutsidePackDrafter(),
        evidence_ingest=_PackIngest(),  # type: ignore[arg-type]
    )
    with pytest.raises(SpecialistValidationError, match="outside the pack"):
        specialist.run(
            SynthesizerInput(
                job_id="job-ablation-synth",
                question="Q",
                plan=["a", "b", "c"],
                findings=["f"],
                evidence_item_ids=["pack-1"],
                prompt_version="draft.v2",
            )
        )
