"""Unit tests for deterministic graders, aggregation, and fingerprints."""

from __future__ import annotations

from atlas.evaluation.aggregation import (
    HARD_PASS_SCORE,
    PROVISIONAL_SOFT_PASS_THRESHOLD,
    aggregate_dimensions,
)
from atlas.evaluation.contracts import (
    DimensionResult,
    EvaluationCandidateInput,
    ToolSummaryRow,
)
from atlas.evaluation.fingerprint import fingerprint_candidate
from atlas.evaluation.graders import (
    FakeSemanticGroundednessGrader,
    grade_citation_integrity,
    grade_completeness,
    grade_coverage,
    grade_lexical_id_groundedness,
    grade_report_structure,
    grade_tool_use,
)
from atlas.evidence.contracts import ClaimStructured
from atlas.workflow.fakes import format_research_report


def _candidate(**overrides: object) -> EvaluationCandidateInput:
    base: dict[str, object] = {
        "job_id": "job-grader",
        "question": "Grader unit question",
        "plan": ["Clarify evaluationgate scope"],
        "findings": ["Clarify evaluationgate scope observed"],
        "draft": "Clarify evaluationgate scope in the draft.",
        "claims": [],
        "evidence_item_ids": [],
        "tool_summary": [],
    }
    base.update(overrides)
    return EvaluationCandidateInput.model_validate(base)


def test_grade_citation_integrity_pass_and_unlinked() -> None:
    linked = {"ev-1"}
    ok = _candidate(
        claims=[
            ClaimStructured(text="Supported claim", evidence_item_ids=["ev-1"]),
        ],
        evidence_item_ids=["ev-1"],
    )
    passed = grade_citation_integrity(ok, linked_ids=linked, provenance_ok=True)
    assert passed.passed is True
    assert passed.score == HARD_PASS_SCORE
    assert passed.is_hard is True

    bad = _candidate(
        claims=[
            ClaimStructured(text="Unlinked claim", evidence_item_ids=["ev-missing"]),
        ],
        evidence_item_ids=["ev-1"],
    )
    failed = grade_citation_integrity(bad, linked_ids=linked, provenance_ok=True)
    assert failed.passed is False
    assert failed.score == 0.0
    assert "CITATION_UNLINKED" in failed.failure_codes


def test_grade_citation_integrity_provenance_gate() -> None:
    candidate = _candidate()
    failed = grade_citation_integrity(
        candidate,
        linked_ids=set(),
        provenance_ok=False,
    )
    assert failed.passed is False
    assert "CITATION_PROVENANCE_INCOMPLETE" in failed.failure_codes


def test_grade_tool_use_research_ok_draft_violation() -> None:
    ok = grade_tool_use([ToolSummaryRow(node_name="research", origin="WORKFLOW")])
    assert ok.passed is True
    assert ok.score == HARD_PASS_SCORE

    bad = grade_tool_use([ToolSummaryRow(node_name="draft", origin="WORKFLOW")])
    assert bad.passed is False
    assert "TOOL_NODE_VIOLATION" in bad.failure_codes


def test_grade_tool_use_unknown_origin_fails_closed() -> None:
    bad = grade_tool_use([ToolSummaryRow(node_name="research", origin="CUSTOM_ORIGIN")])
    assert bad.passed is False
    assert "TOOL_UNKNOWN_ORIGIN" in bad.failure_codes


def test_grade_tool_use_logical_budget_six_pass_seven_fail() -> None:
    six = [
        ToolSummaryRow(
            node_name="research",
            origin="WORKFLOW",
            tool_id=f"tool-{i}",
            status="SUCCEEDED",
        )
        for i in range(6)
    ]
    assert grade_tool_use(six, max_logical_calls=6).passed is True

    seven = six + [
        ToolSummaryRow(
            node_name="research",
            origin="WORKFLOW",
            tool_id="tool-6",
            status="SUCCEEDED",
        )
    ]
    failed = grade_tool_use(seven, max_logical_calls=6)
    assert failed.passed is False
    assert "TOOL_BUDGET_EXCEEDED" in failed.failure_codes


def test_grade_tool_use_zero_tools_allowed() -> None:
    assert grade_tool_use([], allow_zero_tools=True).passed is True


def test_grade_tool_use_counts_logical_rows_not_physical_attempts() -> None:
    """Physical retries are not represented as extra ToolSummaryRow entries."""
    logical = [
        ToolSummaryRow(
            node_name="research",
            origin="WORKFLOW",
            tool_id="search",
            status="SUCCEEDED",
        )
        for _ in range(6)
    ]
    # Six logical rows pass even if each had multiple physical attempts upstream.
    assert grade_tool_use(logical, max_logical_calls=6).passed is True


def test_grade_report_structure_empty_draft_hard_fail() -> None:
    candidate = _candidate(draft="")
    preview = format_research_report(
        question=candidate.question,
        plan=list(candidate.plan),
        findings=list(candidate.findings),
        draft=candidate.draft,
    )
    result = grade_report_structure(
        preview,
        draft=candidate.draft,
        plan=list(candidate.plan),
    )
    assert result.passed is False
    assert result.is_hard is True
    assert "STRUCTURE_EMPTY_DRAFT" in result.failure_codes


def test_grade_report_structure_empty_plan_hard_fail() -> None:
    candidate = _candidate(plan=[], draft="A well-formed draft without a plan.")
    preview = format_research_report(
        question=candidate.question,
        plan=list(candidate.plan),
        findings=list(candidate.findings),
        draft=candidate.draft,
    )
    result = grade_report_structure(
        preview,
        draft=candidate.draft,
        plan=list(candidate.plan),
    )
    assert result.passed is False
    assert result.is_hard is True
    assert "STRUCTURE_EMPTY_PLAN" in result.failure_codes
    assert "STRUCTURE_EMPTY_DRAFT" not in result.failure_codes


def test_grade_report_structure_missing_section_hard_fail() -> None:
    """A preview report missing a required label fails closed.

    Production formatting (``format_research_report``) always emits all four
    required labels; this test constructs a malformed preview directly to
    exercise the structure gate's defensive label-search branch.
    """
    candidate = _candidate()
    malformed_preview = (
        f"Question:\n{candidate.question}\n\n"
        f"Plan:\n1. {candidate.plan[0]}\n\n"
        f"Draft:\n{candidate.draft}"
    )
    result = grade_report_structure(
        malformed_preview,
        draft=candidate.draft,
        plan=list(candidate.plan),
    )
    assert result.passed is False
    assert result.is_hard is True
    assert "STRUCTURE_MISSING_SECTION" in result.failure_codes
    assert "STRUCTURE_EMPTY_DRAFT" not in result.failure_codes
    assert "STRUCTURE_EMPTY_PLAN" not in result.failure_codes


def test_grade_coverage_thin_linked_count() -> None:
    thin = grade_coverage(linked_count=0, has_claims=True)
    assert thin.passed is False
    assert thin.score == 0.0
    assert "COVERAGE_BELOW_MIN" in thin.failure_codes

    covered = grade_coverage(linked_count=1, has_claims=True)
    assert covered.passed is True


def test_grade_completeness_missing_plan_tokens() -> None:
    result = grade_completeness(
        plan=["Investigate xyzzywordalpha thoroughly"],
        findings=["unrelated note"],
        draft="still missing the unique token",
    )
    assert result.passed is False
    assert result.score < PROVISIONAL_SOFT_PASS_THRESHOLD
    assert "COMPLETENESS_FACET_MISSING" in result.failure_codes


def test_grade_completeness_golden_ratio_override_below_threshold() -> None:
    """golden_completeness_ratio overrides the lexical heuristic entirely.

    This override is fixture/test-only scaffolding: production workflow
    composition (``atlas.workflow.graph``) never sets this field, so this
    only proves the override branch itself behaves correctly, not that any
    real golden-facet dataset exists.
    """
    result = grade_completeness(
        plan=["Investigate xyzzywordalpha thoroughly"],
        findings=["unrelated note"],
        draft="still missing the unique token",
        golden_ratio=0.5,
    )
    assert result.score == 0.5
    assert result.passed is False
    assert "COMPLETENESS_FACET_MISSING" in result.failure_codes


def test_grade_completeness_golden_ratio_override_at_threshold_passes() -> None:
    result = grade_completeness(
        plan=["Investigate xyzzywordalpha thoroughly"],
        findings=["unrelated note"],
        draft="still missing the unique token",
        golden_ratio=PROVISIONAL_SOFT_PASS_THRESHOLD,
    )
    assert result.score == PROVISIONAL_SOFT_PASS_THRESHOLD
    assert result.passed is True
    assert result.failure_codes == []


def test_grade_lexical_id_groundedness() -> None:
    claims = [ClaimStructured(text="ok", evidence_item_ids=["ev-1"])]
    ok = grade_lexical_id_groundedness(claims, {"ev-1"})
    assert ok.passed is True
    bad = grade_lexical_id_groundedness(claims, set())
    assert bad.passed is False
    assert "GROUNDEDNESS_ID_OUTSIDE_LINKS" in bad.failure_codes


def test_aggregation_hard_gates_require_exact_one() -> None:
    dimensions = [
        DimensionResult(
            name="citation_integrity",
            score=0.99,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="tool_use",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="report_structure",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=True,
        ),
        DimensionResult(
            name="coverage",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        DimensionResult(
            name="completeness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        DimensionResult(
            name="lexical_id_groundedness",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=False,
            is_provisional=True,
        ),
        DimensionResult(
            name="semantic_groundedness",
            score=0.0,
            passed=True,
            method="skipped",
            is_hard=False,
            is_provisional=True,
            weight=0.0,
        ),
    ]
    _, passed, stamped = aggregate_dimensions(dimensions)
    assert passed is False
    citation = next(item for item in stamped if item.name == "citation_integrity")
    assert citation.passed is False


def test_aggregation_soft_threshold_boundary() -> None:
    soft_names = ("coverage", "completeness", "lexical_id_groundedness")
    base = [
        DimensionResult(
            name="citation_integrity",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="tool_use",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=False,
        ),
        DimensionResult(
            name="report_structure",
            score=1.0,
            passed=True,
            method="deterministic",
            is_hard=True,
            is_provisional=True,
        ),
        DimensionResult(
            name="semantic_groundedness",
            score=0.0,
            passed=True,
            method="skipped",
            is_hard=False,
            is_provisional=True,
            weight=0.0,
        ),
    ]

    for name in soft_names:
        below = [
            *base,
            *[
                DimensionResult(
                    name=soft,  # type: ignore[arg-type]
                    score=(
                        PROVISIONAL_SOFT_PASS_THRESHOLD - 0.01 if soft == name else 1.0
                    ),
                    passed=True,
                    method="deterministic",
                    is_hard=False,
                    is_provisional=True,
                )
                for soft in soft_names
            ],
        ]
        _, passed_below, stamped_below = aggregate_dimensions(below)
        assert passed_below is False
        assert next(item for item in stamped_below if item.name == name).passed is False

        at = [
            *base,
            *[
                DimensionResult(
                    name=soft,  # type: ignore[arg-type]
                    score=(PROVISIONAL_SOFT_PASS_THRESHOLD if soft == name else 1.0),
                    passed=True,
                    method="deterministic",
                    is_hard=False,
                    is_provisional=True,
                )
                for soft in soft_names
            ],
        ]
        _, passed_at, stamped_at = aggregate_dimensions(at)
        assert passed_at is True
        assert next(item for item in stamped_at if item.name == name).passed is True


def test_fingerprint_stability_and_claim_order() -> None:
    first = _candidate(
        claims=[
            ClaimStructured(text="Alpha claim", evidence_item_ids=["ev-2", "ev-1"]),
            ClaimStructured(text="Beta claim", evidence_item_ids=["ev-3"]),
        ],
        evidence_item_ids=["ev-3", "ev-1", "ev-2"],
        tool_summary=[
            ToolSummaryRow(node_name="research", origin="WORKFLOW", tool_id="a"),
            ToolSummaryRow(node_name="research", origin="WORKFLOW", tool_id="b"),
        ],
    )
    second = _candidate(
        claims=[
            ClaimStructured(text="Beta claim", evidence_item_ids=["ev-3"]),
            ClaimStructured(text="Alpha claim", evidence_item_ids=["ev-1", "ev-2"]),
        ],
        evidence_item_ids=["ev-1", "ev-2", "ev-3"],
        tool_summary=[
            ToolSummaryRow(node_name="research", origin="WORKFLOW", tool_id="b"),
            ToolSummaryRow(node_name="research", origin="WORKFLOW", tool_id="a"),
        ],
    )
    hash_a = fingerprint_candidate(first)
    hash_b = fingerprint_candidate(first)
    hash_c = fingerprint_candidate(second)
    assert hash_a == hash_b
    assert len(hash_a) == 64
    assert hash_a == hash_c


def test_fingerprint_changes_when_linked_evidence_or_tools_change() -> None:
    from atlas.evaluation.fingerprint import fingerprint_grading_snapshot

    candidate = _candidate(
        claims=[ClaimStructured(text="claim", evidence_item_ids=["ev-1"])],
        evidence_item_ids=["ev-1"],
        tool_summary=[
            ToolSummaryRow(
                node_name="research",
                origin="WORKFLOW",
                tool_id="search",
                status="SUCCEEDED",
            )
        ],
    )
    base = fingerprint_grading_snapshot(
        candidate,
        linked_evidence_ids={"ev-1"},
        tool_rows=list(candidate.tool_summary),
        provenance_ok=True,
        max_logical_calls=6,
    )
    mutated_evidence = fingerprint_grading_snapshot(
        candidate,
        linked_evidence_ids={"ev-1", "ev-2"},
        tool_rows=list(candidate.tool_summary),
        provenance_ok=True,
        max_logical_calls=6,
    )
    mutated_tools = fingerprint_grading_snapshot(
        candidate,
        linked_evidence_ids={"ev-1"},
        tool_rows=[
            *candidate.tool_summary,
            ToolSummaryRow(
                node_name="research",
                origin="WORKFLOW",
                tool_id="fetch",
                status="SUCCEEDED",
            ),
        ],
        provenance_ok=True,
        max_logical_calls=6,
    )
    assert base != mutated_evidence
    assert base != mutated_tools


def test_fake_semantic_grader_used_in_isolation() -> None:
    candidate = _candidate(
        claims=[ClaimStructured(text="ok", evidence_item_ids=["ev-1"])],
        evidence_item_ids=["ev-1"],
    )
    result = FakeSemanticGroundednessGrader().grade(
        candidate,
        linked_ids={"ev-1"},
    )
    assert result.name == "semantic_groundedness"
    assert result.method == "llm"
    assert result.passed is True
