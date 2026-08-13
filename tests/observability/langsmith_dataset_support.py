"""Opt-in LangSmith dataset/experiment helpers (Slice 15B, tests only).

Production code must not import this module or know about
``tests/evaluation/candidate_goldens.v1.json``.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

from atlas.evaluation.aggregation import aggregate_dimensions
from atlas.evaluation.contracts import (
    DimensionName,
    EvaluationCandidateInput,
    ToolSummaryRow,
)
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

DATASET_NAME = "atlas.candidate_goldens.v1"
GOLDENS_PATH = (
    Path(__file__).resolve().parents[1] / "evaluation" / "candidate_goldens.v1.json"
)


def load_graded_golden_cases() -> list[dict[str, Any]]:
    payload = json.loads(GOLDENS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("candidate goldens root must be an object")
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise TypeError("candidate goldens cases must be a list")
    return [case for case in cases if case.get("kind") != "fingerprint_delta"]


def example_id_for(case_id: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{DATASET_NAME}:{case_id}")


def grader_expected_fingerprint(grader_expected: dict[str, Any]) -> str:
    payload = {
        "overall_passed": grader_expected["overall_passed"],
        "dimension_passed": grader_expected["dimension_passed"],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def example_inputs(case: dict[str, Any]) -> dict[str, str]:
    grader_expected = case["grader_expected"]
    return {
        "fixture_id": str(case["id"]),
        "label": str(case.get("isolates") or case["id"]),
        "fingerprint": grader_expected_fingerprint(grader_expected),
    }


def example_outputs(case: dict[str, Any]) -> dict[str, object]:
    grader_expected = case["grader_expected"]
    return {
        "overall_passed": bool(grader_expected["overall_passed"]),
        "dimension_passed": dict(grader_expected["dimension_passed"]),
    }


def _candidate_from_raw(
    raw: dict[str, Any], *, job_id: str
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
        tool_summary=[
            ToolSummaryRow(
                node_name=row["node_name"],
                origin=row["origin"],
                tool_id=str(row.get("tool_id", "")),
                status=str(row.get("status", "")),
            )
            for row in list(raw.get("tool_summary", []))
        ],
        golden_facets_hit=raw.get("golden_facets_hit"),
        golden_completeness_ratio=raw.get("golden_completeness_ratio"),
    )


def grade_case_booleans(case: dict[str, Any]) -> dict[str, object]:
    """Run deterministic graders locally; return booleans only (no bodies)."""
    candidate = _candidate_from_raw(
        case["candidate"], job_id=f"ls-dataset-{case['id']}"
    )
    linked_ids = set(candidate.evidence_item_ids)
    override = case.get("preview_report_override")
    if isinstance(override, str):
        preview = override
    else:
        preview = format_research_report(
            question=candidate.question,
            plan=list(candidate.plan),
            findings=list(candidate.findings),
            draft=candidate.draft,
            claims=list(candidate.claims) or None,
        )
    provenance_ok = bool(case.get("provenance_ok", True))
    dimensions = [
        grade_citation_integrity(
            candidate, linked_ids=linked_ids, provenance_ok=provenance_ok
        ),
        grade_tool_use(candidate.tool_summary, max_logical_calls=6),
        grade_report_structure(
            preview, draft=candidate.draft, plan=list(candidate.plan)
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
    dimension_passed: dict[DimensionName, bool] = {
        item.name: item.passed for item in stamped
    }
    return {
        "overall_passed": overall_passed,
        "dimension_passed": dimension_passed,
    }


def boolean_compare(run: object, example: object) -> dict[str, object]:
    """LangSmith evaluator: exact boolean match against grader_expected."""
    if isinstance(run, dict) and isinstance(example, dict):
        predicted = {
            "overall_passed": bool(run["overall_passed"]),
            "dimension_passed": dict(run["dimension_passed"]),
        }
        expected = {
            "overall_passed": bool(example["overall_passed"]),
            "dimension_passed": dict(example["dimension_passed"]),
        }
    else:
        run_outputs = dict(getattr(run, "outputs", None) or {})
        reference_outputs = dict(getattr(example, "outputs", None) or {})
        predicted = {
            "overall_passed": bool(run_outputs["overall_passed"]),
            "dimension_passed": dict(run_outputs["dimension_passed"]),
        }
        expected = {
            "overall_passed": bool(reference_outputs["overall_passed"]),
            "dimension_passed": dict(reference_outputs["dimension_passed"]),
        }
    passed = predicted == expected
    return {"key": "grader_expected_match", "score": 1.0 if passed else 0.0}


def dataset_examples() -> list[dict[str, object]]:
    examples: list[dict[str, object]] = []
    for case in load_graded_golden_cases():
        examples.append(
            {
                "example_id": example_id_for(str(case["id"])),
                "inputs": example_inputs(case),
                "outputs": example_outputs(case),
            }
        )
    return examples


def target_from_inputs(inputs: dict[str, Any]) -> dict[str, object]:
    fixture_id = str(inputs["fixture_id"])
    for case in load_graded_golden_cases():
        if str(case["id"]) == fixture_id:
            return grade_case_booleans(case)
    raise KeyError("unknown fixture_id")


def _example_uuid(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def ensure_golden_dataset(client: Any) -> object:
    """Read the golden dataset, creating it only on the SDK not-found error.

    Authentication, authorization, timeout, schema, and any other SDK
    failure propagate. Concurrent create races (conflict) re-read.
    """
    from langsmith.utils import LangSmithConflictError, LangSmithNotFoundError

    read_dataset = client.read_dataset
    try:
        return read_dataset(dataset_name=DATASET_NAME)
    except LangSmithNotFoundError:
        create_dataset = client.create_dataset
        try:
            return create_dataset(
                DATASET_NAME,
                description="Atlas candidate_goldens.v1 metadata-only regression",
            )
        except LangSmithConflictError:
            return read_dataset(dataset_name=DATASET_NAME)


def upsert_golden_examples(client: Any) -> list[uuid.UUID]:
    """Create or update every stable golden example id. Failures propagate."""
    expected = dataset_examples()
    list_examples = client.list_examples
    existing = {
        _example_uuid(row.id) for row in list_examples(dataset_name=DATASET_NAME)
    }
    to_create: list[dict[str, object]] = []
    to_update: list[dict[str, object]] = []
    for item in expected:
        example_id = _example_uuid(item["example_id"])
        payload: dict[str, object] = {
            "id": example_id,
            "inputs": item["inputs"],
            "outputs": item["outputs"],
        }
        if example_id in existing:
            to_update.append(payload)
        else:
            to_create.append(payload)
    if to_create:
        client.create_examples(dataset_name=DATASET_NAME, examples=to_create)
    if to_update:
        client.update_examples(dataset_name=DATASET_NAME, updates=to_update)
    return [_example_uuid(item["example_id"]) for item in expected]


def listed_example_ids(client: Any) -> set[uuid.UUID]:
    list_examples = client.list_examples
    return {_example_uuid(row.id) for row in list_examples(dataset_name=DATASET_NAME)}
