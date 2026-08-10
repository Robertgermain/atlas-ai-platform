"""Deterministic candidate graders and offline fake semantic grader."""

from __future__ import annotations

import re
from typing import Any

from atlas.evaluation.aggregation import (
    HARD_PASS_SCORE,
    PROVISIONAL_SOFT_PASS_THRESHOLD,
    weight_for,
)
from atlas.evaluation.contracts import (
    DimensionResult,
    EvaluationCandidateInput,
    ToolSummaryRow,
)

_SIGNIFICANT_TOKEN = re.compile(r"[a-z0-9]{4,}")

GRADER_VERSIONS: dict[str, str] = {
    "citation_integrity": "deterministic.v1",
    "tool_use": "deterministic.v1",
    "report_structure": "deterministic.v1",
    "coverage": "provisional.deterministic.v1",
    "completeness": "provisional.deterministic.v1",
    "lexical_id_groundedness": "provisional.deterministic.v1",
    "semantic_groundedness": "fake.llm.v1",
}


def _soft_passed(score: float) -> bool:
    return score >= PROVISIONAL_SOFT_PASS_THRESHOLD


def _hard_passed(score: float) -> bool:
    return score == HARD_PASS_SCORE


def grade_citation_integrity(
    candidate: EvaluationCandidateInput,
    *,
    linked_ids: set[str],
    provenance_ok: bool,
) -> DimensionResult:
    """Hard gate: every claim id must be linked and provenance must be intact."""
    codes: list[str] = []
    if not provenance_ok:
        codes.append("CITATION_PROVENANCE_INCOMPLETE")
    claims = candidate.claims
    if not claims:
        score = 1.0 if provenance_ok else 0.0
        return DimensionResult(
            name="citation_integrity",
            score=score,
            passed=_hard_passed(score),
            method="deterministic",
            is_hard=True,
            is_provisional=False,
            failure_codes=codes,
            weight=weight_for("citation_integrity"),
        )

    for claim in claims:
        ids = claim.evidence_item_ids
        if not ids:
            codes.append("CITATION_EMPTY_CLAIM")
            continue
        if any(item_id not in linked_ids for item_id in ids):
            codes.append("CITATION_UNLINKED")

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_codes = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    score = 1.0 if not unique_codes else 0.0
    return DimensionResult(
        name="citation_integrity",
        score=score,
        passed=_hard_passed(score),
        method="deterministic",
        is_hard=True,
        is_provisional=False,
        failure_codes=unique_codes,
        weight=weight_for("citation_integrity"),
    )


def grade_tool_use(
    tool_rows: list[ToolSummaryRow] | list[dict[str, Any]],
    *,
    max_logical_calls: int = 6,
    allow_zero_tools: bool = True,
) -> DimensionResult:
    """Hard gate for execution-scoped logical tool use.

    Rules:
    - Each row is one logical invocation (physical retries are not counted).
    - Only ``WORKFLOW`` origin is accepted for research-job evaluation.
    - Every workflow row must be attributed to the ``research`` node.
    - Unknown/unexpected origins fail closed.
    - Logical call count must be ``<= max_logical_calls``.
    - Zero tools pass when ``allow_zero_tools`` is true (legitimate no-tool path).
    """
    codes: list[str] = []
    normalized: list[ToolSummaryRow] = []
    for row in tool_rows:
        if isinstance(row, ToolSummaryRow):
            normalized.append(row)
        else:
            normalized.append(
                ToolSummaryRow(
                    node_name=str(row.get("node_name", "")),
                    origin=str(row.get("origin", "")),
                    tool_id=str(row.get("tool_id", "")),
                    status=str(row.get("status", "")),
                )
            )

    if not normalized:
        if allow_zero_tools:
            score = 1.0
        else:
            codes.append("TOOL_REQUIRED")
            score = 0.0
        return DimensionResult(
            name="tool_use",
            score=score,
            passed=_hard_passed(score),
            method="deterministic",
            is_hard=True,
            is_provisional=False,
            failure_codes=codes,
            weight=weight_for("tool_use"),
        )

    for row in normalized:
        origin = row.origin.strip().upper()
        if origin != "WORKFLOW":
            codes.append("TOOL_UNKNOWN_ORIGIN")
            break
        if row.node_name.strip() != "research":
            codes.append("TOOL_NODE_VIOLATION")
            break

    if not codes and len(normalized) > max_logical_calls:
        codes.append("TOOL_BUDGET_EXCEEDED")

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_codes: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    score = 0.0 if unique_codes else 1.0
    return DimensionResult(
        name="tool_use",
        score=score,
        passed=_hard_passed(score),
        method="deterministic",
        is_hard=True,
        is_provisional=False,
        failure_codes=unique_codes,
        weight=weight_for("tool_use"),
    )


def grade_report_structure(
    preview_report: str,
    *,
    draft: str,
    plan: list[str],
) -> DimensionResult:
    """Hard empty-draft gate; provisional section labels on the preview report."""
    codes: list[str] = []
    if not draft.strip():
        codes.append("STRUCTURE_EMPTY_DRAFT")
    required = ("Question:", "Plan:", "Findings:", "Draft:")
    missing = [label for label in required if label not in preview_report]
    if missing:
        codes.append("STRUCTURE_MISSING_SECTION")
    if not plan:
        codes.append("STRUCTURE_EMPTY_PLAN")

    score = 0.0 if codes else 1.0
    return DimensionResult(
        name="report_structure",
        score=score,
        passed=_hard_passed(score),
        method="deterministic",
        is_hard=True,
        is_provisional=True,
        failure_codes=codes,
        weight=weight_for("report_structure"),
    )


def grade_coverage(
    *,
    linked_count: int,
    has_claims: bool,
    min_linked: int = 1,
    golden_facets_hit: float | None = None,
) -> DimensionResult:
    """Provisional soft coverage heuristic or golden facet hit ratio."""
    codes: list[str] = []
    if golden_facets_hit is not None:
        score = float(golden_facets_hit)
        if score < PROVISIONAL_SOFT_PASS_THRESHOLD:
            codes.append("COVERAGE_FACET_MISSING")
    elif has_claims:
        score = 1.0 if linked_count >= min_linked else 0.0
        if score < 1.0:
            codes.append("COVERAGE_BELOW_MIN")
    else:
        score = 1.0

    return DimensionResult(
        name="coverage",
        score=score,
        passed=_soft_passed(score),
        method="deterministic",
        is_hard=False,
        is_provisional=True,
        failure_codes=codes,
        weight=weight_for("coverage"),
    )


def _significant_tokens(text: str) -> set[str]:
    return set(_SIGNIFICANT_TOKEN.findall(text.lower()))


def grade_completeness(
    *,
    plan: list[str],
    findings: list[str],
    draft: str,
    golden_ratio: float | None = None,
) -> DimensionResult:
    """Provisional soft completeness via plan-token presence or golden ratio."""
    codes: list[str] = []
    if golden_ratio is not None:
        score = float(golden_ratio)
        if score < PROVISIONAL_SOFT_PASS_THRESHOLD:
            codes.append("COMPLETENESS_FACET_MISSING")
        return DimensionResult(
            name="completeness",
            score=score,
            passed=_soft_passed(score),
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            failure_codes=codes,
            weight=weight_for("completeness"),
        )

    if not plan:
        return DimensionResult(
            name="completeness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            failure_codes=[],
            weight=weight_for("completeness"),
        )

    corpus = " ".join([*findings, draft])
    corpus_tokens = _significant_tokens(corpus)
    satisfied = 0
    for task in plan:
        tokens = _significant_tokens(task)
        # Tasks without significant tokens are vacuously satisfied.
        if not tokens or tokens.issubset(corpus_tokens):
            satisfied += 1
        else:
            codes.append("COMPLETENESS_FACET_MISSING")

    if codes:
        codes = ["COMPLETENESS_FACET_MISSING"]

    score = satisfied / len(plan)
    return DimensionResult(
        name="completeness",
        score=score,
        passed=_soft_passed(score),
        method="deterministic",
        is_hard=False,
        is_provisional=True,
        failure_codes=codes,
        weight=weight_for("completeness"),
    )


def grade_lexical_id_groundedness(
    claims: list[Any],
    linked_ids: set[str],
) -> DimensionResult:
    """Provisional soft lexical ID check (not semantic groundedness)."""
    if not claims:
        return DimensionResult(
            name="lexical_id_groundedness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
            failure_codes=[],
            weight=weight_for("lexical_id_groundedness"),
        )

    supported = 0
    codes: list[str] = []
    for claim in claims:
        ids = list(getattr(claim, "evidence_item_ids", []) or [])
        if ids and set(ids).issubset(linked_ids):
            supported += 1
        else:
            codes.append("GROUNDEDNESS_ID_OUTSIDE_LINKS")

    if codes:
        codes = ["GROUNDEDNESS_ID_OUTSIDE_LINKS"]
    score = supported / len(claims)
    return DimensionResult(
        name="lexical_id_groundedness",
        score=score,
        passed=_soft_passed(score),
        method="deterministic",
        is_hard=False,
        is_provisional=True,
        failure_codes=codes,
        weight=weight_for("lexical_id_groundedness"),
    )


class FakeSemanticGroundednessGrader:
    """Offline semantic grader; never calls a network provider."""

    version: str = GRADER_VERSIONS["semantic_groundedness"]

    def grade(
        self,
        candidate: EvaluationCandidateInput,
        *,
        linked_ids: set[str] | None = None,
    ) -> DimensionResult:
        ids = linked_ids if linked_ids is not None else set(candidate.evidence_item_ids)
        claims = candidate.claims
        if not claims:
            score = 1.0
            codes: list[str] = []
        elif all(set(claim.evidence_item_ids).issubset(ids) for claim in claims):
            score = 1.0
            codes = []
        else:
            score = 0.5
            codes = ["SEMANTIC_GROUNDEDNESS_WEAK"]

        return DimensionResult(
            name="semantic_groundedness",
            score=score,
            passed=_soft_passed(score),
            method="llm",
            is_hard=False,
            is_provisional=True,
            failure_codes=codes,
            weight=weight_for("semantic_groundedness", semantic_present=True),
        )


def skipped_semantic_dimension() -> DimensionResult:
    """Dimension placeholder when no semantic grader is configured."""
    return DimensionResult(
        name="semantic_groundedness",
        score=0.0,
        passed=True,
        method="skipped",
        is_hard=False,
        is_provisional=True,
        failure_codes=[],
        weight=0.0,
    )
