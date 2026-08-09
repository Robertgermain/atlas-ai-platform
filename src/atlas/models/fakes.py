"""Deterministic planner/drafter implementations (no LangChain required)."""

from __future__ import annotations

from atlas.evidence.contracts import ClaimStructured
from atlas.models.contracts import (
    DraftRequest,
    DraftResult,
    FinishOutcome,
    ModelCallMeta,
    PlanRequest,
    PlanResult,
    ProviderId,
    RetryClass,
)


def _build_plan(question: str) -> list[str]:
    return [
        f"Clarify scope: {question}",
        f"Gather background: {question}",
        f"Identify risks and open questions: {question}",
    ]


def _build_draft(*, question: str, plan: list[str], findings: list[str]) -> str:
    plan_lines = "\n".join(
        f"{index}. {task}" for index, task in enumerate(plan, start=1)
    )
    finding_lines = "\n".join(f"- {finding}" for finding in findings)
    return (
        f"Draft synthesis for: {question}\n"
        f"Covered plan items:\n{plan_lines}\n"
        f"Evidence:\n{finding_lines}"
    )


def _build_claims(request: DraftRequest) -> list[ClaimStructured]:
    if not request.evidence:
        return []
    claims: list[ClaimStructured] = []
    for item in request.evidence:
        claims.append(
            ClaimStructured(
                text=f"Supported by evidence from {item.source_display_uri}",
                evidence_item_ids=[item.evidence_item_id],
            )
        )
    return claims


class DeterministicResearchPlanner:
    """Atlas fake planner implementing ResearchPlanner without LangChain."""

    def plan(self, request: PlanRequest) -> PlanResult:
        return PlanResult(
            tasks=_build_plan(request.question),
            meta=ModelCallMeta(
                provider=ProviderId.FAKE,
                model="deterministic-fake",
                prompt_version=request.prompt_version,
                latency_ms=0,
                finish_outcome=FinishOutcome.COMPLETED,
                retry_class=RetryClass.NONE,
                status="succeeded",
            ),
        )


class DeterministicResearchDrafter:
    """Atlas fake drafter implementing ResearchDrafter without LangChain."""

    def draft(self, request: DraftRequest) -> DraftResult:
        return DraftResult(
            draft=_build_draft(
                question=request.question,
                plan=list(request.plan),
                findings=list(request.findings),
            ),
            claims=_build_claims(request),
            meta=ModelCallMeta(
                provider=ProviderId.FAKE,
                model="deterministic-fake",
                prompt_version=request.prompt_version,
                latency_ms=0,
                finish_outcome=FinishOutcome.COMPLETED,
                retry_class=RetryClass.NONE,
                status="succeeded",
            ),
        )
