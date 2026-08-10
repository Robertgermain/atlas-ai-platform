"""Offline candidate golden regression for Slice 12A graders.

These fixtures are ``candidate_goldens.v1`` calibration-development examples —
not a held-out validation set and not frozen ``evaluation.v1``.
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
) -> tuple[bool, dict[DimensionName, bool], str]:
    linked_ids = set(candidate.evidence_item_ids)
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


def test_candidate_goldens_match_graders_and_metrics() -> None:
    payload = _load_goldens()
    meta = payload["_meta"]
    assert meta["label"] == "candidate_goldens.v1"
    assert meta["human_reviewed"] is False
    assert meta.get("frozen_profile") is False
    cases = payload["cases"]
    assert len(cases) >= 18

    predicted_overall: list[bool] = []
    expected_overall: list[bool] = []
    graded = 0
    fingerprint_deltas = 0

    for index, case in enumerate(cases):
        assert case.get("isolates"), f"missing isolates for {case.get('id')}"
        expected = case["expected"]
        assert expected["rationale"].strip()
        assert expected.get("known_heuristic_limitation", "").strip()

        if case.get("kind") == "fingerprint_delta":
            fingerprint_deltas += 1
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
            continue

        candidate = _candidate_from_raw(
            case["candidate"],
            job_id=f"golden-{case['id']}-{index}",
        )
        overall_passed, dimension_passed, rationale = _grade_candidate(candidate)
        assert rationale.strip(), f"missing rationale for {case['id']}"
        assert overall_passed is expected["overall_passed"], case["id"]
        for name, expected_passed in expected["dimension_passed"].items():
            assert dimension_passed[name] is expected_passed, (
                f"{case['id']}.{name}: got {dimension_passed[name]}"
            )
        predicted_overall.append(overall_passed)
        expected_overall.append(bool(expected["overall_passed"]))
        graded += 1

    assert fingerprint_deltas >= 2
    assert graded >= 16

    metrics = compute_provisional_confusion_metrics(
        predicted=predicted_overall,
        expected=expected_overall,
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
    # Same fixtures are regression + calibration-development, so F1 is 1.0 here.
    # This is not independent validation evidence.
    assert metrics["f1"] == 1.0
    assert metrics["fp"] == 0.0
    assert metrics["fn"] == 0.0
    assert metrics["positive_class_count"] >= 3
    assert metrics["negative_class_count"] >= 3
