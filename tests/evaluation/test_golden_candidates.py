"""Offline candidate golden regression + human calibration for Slice 12A graders.

These fixtures are ``candidate_goldens.v1`` human-calibrated development
examples. Two conceptually separate checks are performed against the same
fixtures:

1. Grader regression: does the deterministic grader implementation still
   produce the documented ``grader_expected`` behavior for each case?
2. Human calibration comparison: how does the grader's actual output agree
   with a separate ``human_expected`` quality judgment?

This is a small, hand-authored, non-random case set. It is not a held-out
validation set, not independent statistical validation, and not proof of
production semantic quality. See ``_meta`` in ``candidate_goldens.v1.json``
for the full scope statement.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas.evaluation.aggregation import aggregate_dimensions
from atlas.evaluation.contracts import (
    DimensionName,
    EvaluationCandidateInput,
    ToolSummaryRow,
)
from atlas.evaluation.fingerprint import fingerprint_grading_snapshot
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

GOLDENS_PATH = Path(__file__).with_name("candidate_goldens.v1.json")

# Approved outcome of the 2026-08-10 human calibration closeout. Derived from
# the fixture below, not asserted blindly: the paraphrase fixture is the sole
# approved grader/human disagreement (a known lexical-completeness false
# negative). If the fixture changes, these constants must be re-derived, not
# edited to force a match.
EXPECTED_GRADED_CASE_COUNT = 23
EXPECTED_FINGERPRINT_ONLY_CASE_COUNT = 2
APPROVED_KNOWN_FALSE_NEGATIVE_IDS = frozenset({"fail_paraphrased_plan_lexical_fn"})


def _load_goldens() -> dict[str, Any]:
    payload = json.loads(GOLDENS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("candidate goldens root must be an object")
    return payload


def _tools_from_raw(raw_tools: list[dict[str, Any]]) -> list[ToolSummaryRow]:
    return [
        ToolSummaryRow(
            node_name=row["node_name"],
            origin=row["origin"],
            tool_id=str(row.get("tool_id", "")),
            status=str(row.get("status", "")),
        )
        for row in raw_tools
    ]


def _candidate_from_raw(
    raw: dict[str, Any],
    *,
    job_id: str,
) -> EvaluationCandidateInput:
    claims = [
        ClaimStructured(
            text=item["text"],
            evidence_item_ids=list(item["evidence_item_ids"]),
        )
        for item in raw.get("claims", [])
    ]
    return EvaluationCandidateInput(
        job_id=job_id,
        question=raw["question"],
        plan=list(raw["plan"]),
        findings=list(raw["findings"]),
        draft=raw["draft"],
        claims=claims,
        evidence_item_ids=list(raw.get("evidence_item_ids", [])),
        tool_summary=_tools_from_raw(list(raw.get("tool_summary", []))),
        golden_facets_hit=raw.get("golden_facets_hit"),
        golden_completeness_ratio=raw.get("golden_completeness_ratio"),
    )


def _grade_candidate(
    candidate: EvaluationCandidateInput,
    *,
    provenance_ok: bool = True,
    preview_report_override: str | None = None,
) -> tuple[bool, dict[DimensionName, bool], str]:
    """Run the full deterministic grader stack, mirroring the runner's grading order.

    ``preview_report_override`` is a narrowly-scoped, test-only hook so a
    fixture can synthesize a malformed preview report (e.g. missing a
    required section label) without a real production code path that
    produces one. Production formatting always emits all required labels.
    """
    linked_ids = set(candidate.evidence_item_ids)
    if preview_report_override is not None:
        preview = preview_report_override
    else:
        preview = format_research_report(
            question=candidate.question,
            plan=list(candidate.plan),
            findings=list(candidate.findings),
            draft=candidate.draft,
            claims=list(candidate.claims) or None,
        )
    dimensions = [
        grade_citation_integrity(
            candidate,
            linked_ids=linked_ids,
            provenance_ok=provenance_ok,
        ),
        grade_tool_use(candidate.tool_summary, max_logical_calls=6),
        grade_report_structure(
            preview,
            draft=candidate.draft,
            plan=list(candidate.plan),
        ),
        grade_coverage(
            linked_count=len(linked_ids),
            has_claims=bool(candidate.claims),
            golden_facets_hit=candidate.golden_facets_hit,
        ),
        grade_completeness(
            plan=list(candidate.plan),
            findings=list(candidate.findings),
            draft=candidate.draft,
            golden_ratio=candidate.golden_completeness_ratio,
        ),
        grade_lexical_id_groundedness(candidate.claims, linked_ids),
        FakeSemanticGroundednessGrader().grade(candidate, linked_ids=linked_ids),
    ]
    _, overall_passed, stamped = aggregate_dimensions(dimensions)
    dimension_passed = {item.name: item.passed for item in stamped}
    rationale_bits = [
        f"{item.name}={'pass' if item.passed else 'fail'}"
        f" score={item.score:.2f}"
        + (f" codes={','.join(item.failure_codes)}" if item.failure_codes else "")
        for item in stamped
    ]
    return overall_passed, dimension_passed, "; ".join(rationale_bits)


def compute_provisional_confusion_metrics(
    *,
    predicted: list[bool],
    expected: list[bool],
) -> dict[str, float]:
    """Return provisional TP/FP/TN/FN plus precision/recall/F1 vs expected labels."""
    if len(predicted) != len(expected):
        raise ValueError("predicted and expected must have the same length")
    tp = fp = tn = fn = 0
    for pred, exp in zip(predicted, expected, strict=True):
        if pred and exp:
            tp += 1
        elif pred and not exp:
            fp += 1
        elif (not pred) and (not exp):
            tn += 1
        else:
            fn += 1
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (
        (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    )
    return {
        "tp": float(tp),
        "fp": float(fp),
        "tn": float(tn),
        "fn": float(fn),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "positive_class_count": float(sum(1 for item in expected if item)),
        "negative_class_count": float(sum(1 for item in expected if not item)),
        "graded_case_count": float(len(expected)),
    }


def _graded_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [case for case in cases if case.get("kind") != "fingerprint_delta"]


def _fingerprint_delta_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [case for case in cases if case.get("kind") == "fingerprint_delta"]


def test_candidate_goldens_meta_records_human_calibration_scope() -> None:
    payload = _load_goldens()
    meta = payload["_meta"]
    assert meta["label"] == "candidate_goldens.v1"
    assert meta["human_reviewed"] is True
    assert meta["human_reviewer"] == "project_owner"
    assert meta["reviewed_at"] == "2026-08-10"
    assert meta["frozen_profile"] is False
    for key in (
        "review_scope",
        "not_held_out_validation",
        "not_independent_statistical_validation",
        "not_production_semantic_quality_proof",
        "evaluation_profile_status",
    ):
        assert meta[key].strip(), f"missing honesty statement: {key}"


def test_candidate_goldens_grader_regression() -> None:
    """Grader regression: actual output must match documented grader_expected.

    This check is intentionally independent of human quality judgments — it
    only proves the deterministic grader implementation still behaves as
    documented for each fixture.
    """
    payload = _load_goldens()
    cases = payload["cases"]
    graded_cases = _graded_cases(cases)
    fingerprint_cases = _fingerprint_delta_cases(cases)

    assert len(graded_cases) == EXPECTED_GRADED_CASE_COUNT
    assert len(fingerprint_cases) == EXPECTED_FINGERPRINT_ONLY_CASE_COUNT

    for index, case in enumerate(fingerprint_cases):
        assert case.get("isolates"), f"missing isolates for {case.get('id')}"
        expected = case["expected"]
        assert expected["rationale"].strip()
        assert expected.get("known_heuristic_limitation", "").strip()

        base = _candidate_from_raw(
            case["base_candidate"],
            job_id=f"golden-{case['id']}-{index}",
        )
        linked = set(base.evidence_item_ids)
        tools = list(base.tool_summary)
        fp_a = fingerprint_grading_snapshot(
            base,
            linked_evidence_ids=linked,
            tool_rows=tools,
            provenance_ok=True,
            max_logical_calls=6,
        )
        if "mutated_linked_ids" in case:
            linked = set(case["mutated_linked_ids"])
        if "mutated_tool_summary" in case:
            tools = _tools_from_raw(list(case["mutated_tool_summary"]))
        fp_b = fingerprint_grading_snapshot(
            base,
            linked_evidence_ids=linked,
            tool_rows=tools,
            provenance_ok=True,
            max_logical_calls=6,
        )
        assert (fp_a != fp_b) is bool(expected["fingerprints_differ"]), case["id"]

    for index, case in enumerate(graded_cases):
        assert case.get("isolates"), f"missing isolates for {case.get('id')}"
        grader_expected = case["grader_expected"]
        assert grader_expected["rationale"].strip()
        assert grader_expected.get("known_heuristic_limitation", "").strip()
        assert case["human_expected"]["rationale"].strip()

        candidate = _candidate_from_raw(
            case["candidate"],
            job_id=f"golden-{case['id']}-{index}",
        )
        overall_passed, dimension_passed, rationale = _grade_candidate(
            candidate,
            provenance_ok=bool(case.get("provenance_ok", True)),
            preview_report_override=case.get("preview_report_override"),
        )
        assert rationale.strip(), f"missing rationale for {case['id']}"
        assert overall_passed is grader_expected["overall_passed"], case["id"]
        for name, expected_passed in grader_expected["dimension_passed"].items():
            assert dimension_passed[name] is expected_passed, (
                f"{case['id']}.{name}: got {dimension_passed[name]}"
            )


def test_candidate_goldens_human_calibration() -> None:
    """Human calibration: actual grader predictions vs. human_expected labels.

    Deliberately does not assert F1 == 1.0. The paraphrase fixture is an
    approved known disagreement (grader fails a semantically acceptable
    paraphrase on lexical grounds) and must appear as exactly one false
    negative. Any other disagreement is unexpected and must fail the test
    rather than being silently absorbed into the metrics.
    """
    payload = _load_goldens()
    cases = payload["cases"]
    graded_cases = _graded_cases(cases)
    assert len(graded_cases) == EXPECTED_GRADED_CASE_COUNT

    predicted_overall: list[bool] = []
    human_overall: list[bool] = []
    case_ids: list[str] = []

    for index, case in enumerate(graded_cases):
        candidate = _candidate_from_raw(
            case["candidate"],
            job_id=f"golden-human-{case['id']}-{index}",
        )
        overall_passed, _dimension_passed, _rationale = _grade_candidate(
            candidate,
            provenance_ok=bool(case.get("provenance_ok", True)),
            preview_report_override=case.get("preview_report_override"),
        )
        predicted_overall.append(overall_passed)
        human_overall.append(bool(case["human_expected"]["overall_passed"]))
        case_ids.append(case["id"])

    disagreements = [
        case_id
        for case_id, predicted, human in zip(
            case_ids, predicted_overall, human_overall, strict=True
        )
        if predicted is not human
    ]
    assert set(disagreements) == APPROVED_KNOWN_FALSE_NEGATIVE_IDS, (
        f"unexpected grader/human disagreement set: {disagreements}"
    )
    assert len(disagreements) == 1

    metrics = compute_provisional_confusion_metrics(
        predicted=predicted_overall,
        expected=human_overall,
    )
    assert set(metrics) >= {
        "tp",
        "fp",
        "tn",
        "fn",
        "precision",
        "recall",
        "f1",
        "positive_class_count",
        "negative_class_count",
        "graded_case_count",
    }

    # The approved paraphrase disagreement is a grader false negative: the
    # grader predicts fail (not-passed) while the human expects pass.
    for case_id, predicted, human in zip(
        case_ids, predicted_overall, human_overall, strict=True
    ):
        if case_id in APPROVED_KNOWN_FALSE_NEGATIVE_IDS:
            assert predicted is False
            assert human is True

    assert metrics["graded_case_count"] == float(EXPECTED_GRADED_CASE_COUNT)
    assert metrics["tp"] == 8.0
    assert metrics["fp"] == 0.0
    assert metrics["fn"] == 1.0
    assert metrics["tn"] == 14.0
    assert metrics["positive_class_count"] == 9.0
    assert metrics["negative_class_count"] == 14.0
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 8.0 / 9.0
    assert metrics["f1"] == 16.0 / 17.0
