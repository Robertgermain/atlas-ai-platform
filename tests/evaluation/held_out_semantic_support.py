"""Test-only held-out semantic calibration harness (Slice 15C1 Phase 2).

Production code must not import this module or
``tests/evaluation/held_out_semantic.v1.json``. Human labels live in the
dataset file and are never overwritten by predictions.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas.evaluation.aggregation import SEMANTIC_PASS_THRESHOLD
from atlas.evaluation.llm_grader import aggregate_semantic_claim_scores
from atlas.evaluation.semantic_contracts import (
    SEMANTIC_PROMPT_VERSION,
    SemanticExcerptSource,
    SemanticGradeRequest,
    SemanticSupportLabel,
    support_label_for_score,
)
from atlas.evaluation.semantic_input import assemble_semantic_grade_request
from atlas.evidence.bounds import TRUST_UNTRUSTED
from atlas.evidence.contracts import ClaimStructured
from atlas.models.errors import (
    ModelAuthConfigError,
    ModelError,
    ModelInvalidStructuredOutputError,
    ModelRateLimitedError,
    ModelRefusalError,
    ModelTimeoutError,
    sanitize_model_error,
)

HELD_OUT_PATH = Path(__file__).with_name("held_out_semantic.v1.json")
GOLDENS_PATH = Path(__file__).with_name("candidate_goldens.v1.json")
DATASET_NAME = "atlas.held_out_semantic.v1"
LIVE_FLAG = "ATLAS_ENABLE_LIVE_HELD_OUT_SEMANTIC_TESTS"
CHECKPOINT_COMMIT = "936a74a08e3e5d20fc0e93e55cee4fbc0102f4b8"

SupportLabel = SemanticSupportLabel
OutcomeKind = Literal[
    "quality", "malformed", "timeout", "rate_limited", "auth", "refusal", "other"
]
DisagreementReason = Literal[
    "label_mismatch",
    "report_pass_mismatch",
    "safety_prompt_injection",
    "truncation_boundary",
    "other_quality",
]

REQUIRED_CATEGORIES = frozenset(
    {
        "directly_supported",
        "clearly_unsupported",
        "partially_supported",
        "paraphrase",
        "numerical_mismatch",
        "date_mismatch",
        "entity_mismatch",
        "topical_nonsupport",
        "conflicting_evidence",
        "insufficient_evidence",
        "mixed_support",
        "prompt_injection",
        "truncation_boundary",
        "multibyte",
        "empty_claims",
        "multi_evidence",
    }
)

FORBIDDEN_FIXTURE_FIELDS = frozenset(
    {
        "grader_expected",
        "golden_facets_hit",
        "golden_completeness_ratio",
        "preview_report_override",
        "provenance_ok",
        "candidate",
    }
)


class PromotionCriteria(TypedDict):
    min_supported_precision: float
    min_supported_recall: float
    min_macro_f1: float
    min_report_f1: float
    no_safety_boundary_failure: bool
    no_malformed_or_availability_failure: bool
    no_unexplained_systematic_failure: bool
    does_not_freeze_evaluation_v1: bool


PROMOTION_CRITERIA: PromotionCriteria = {
    "min_supported_precision": 0.80,
    "min_supported_recall": 0.80,
    "min_macro_f1": 0.75,
    "min_report_f1": 0.80,
    "no_safety_boundary_failure": True,
    "no_malformed_or_availability_failure": True,
    "no_unexplained_systematic_failure": True,
    "does_not_freeze_evaluation_v1": True,
}


class HeldOutClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1)
    evidence_item_ids: list[str] = Field(min_length=1)


class HeldOutExcerpt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_item_id: str = Field(min_length=1)
    trust_label: str = Field(min_length=1)
    text: str = Field(min_length=1)


class HeldOutHumanClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_ordinal: int = Field(ge=1)
    support: SupportLabel
    score: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def score_matches_support(self) -> HeldOutHumanClaim:
        derived = support_label_for_score(self.score)
        if derived != self.support:
            raise ValueError("human score is outside the labeled support range")
        return self


class HeldOutHuman(BaseModel):
    model_config = ConfigDict(extra="forbid")

    labeled_at: str = Field(min_length=1)
    reviewer: Literal["project_owner"]
    claims: list[HeldOutHumanClaim]
    report_passed: bool
    report_score: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def report_matches_mean_rule(self) -> HeldOutHuman:
        scores = [item.score for item in self.claims]
        expected_score = aggregate_semantic_claim_scores(scores)
        expected_passed = expected_score >= SEMANTIC_PASS_THRESHOLD
        if abs(self.report_score - expected_score) > 1e-12:
            raise ValueError("human report_score must be the Atlas arithmetic mean")
        if self.report_passed is not expected_passed:
            raise ValueError("human report_passed must follow mean >= 0.70")
        return self


class HeldOutCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    category: str = Field(min_length=1)
    claims: list[HeldOutClaim]
    excerpts: list[HeldOutExcerpt]
    human: HeldOutHuman

    @model_validator(mode="after")
    def ordinals_and_empty_path(self) -> HeldOutCase:
        if self.category == "empty_claims":
            if self.claims or self.human.claims:
                raise ValueError("empty_claims must have no claims")
            return self
        if not self.claims:
            raise ValueError("non-empty cases must include claims")
        if len(self.human.claims) != len(self.claims):
            raise ValueError("human labels must cover every claim")
        expected = list(range(1, len(self.claims) + 1))
        actual = [item.claim_ordinal for item in self.human.claims]
        if actual != expected:
            raise ValueError("human claim_ordinals must be exactly 1..N")
        return self


class HeldOutMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: Literal["held_out_semantic.v1"]
    human_reviewed: Literal[True]
    human_reviewer: Literal["project_owner"]
    reviewed_at: str
    held_out: Literal[True]
    labels_established_before_predictions: Literal[True]
    live_calibration_run: Literal[False]
    frozen_profile: Literal[False]
    evaluation_profile: Literal["evaluation.candidate.v1"]
    prompt_version: Literal["semantic_groundedness.v1"]
    pass_threshold: float
    checkpoint_commit: str
    distinct_from: Literal["candidate_goldens.v1"]
    promotion_criteria: PromotionCriteria
    methodology: str
    note: str


class HeldOutDataset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meta: HeldOutMeta
    cases: list[HeldOutCase]

    @model_validator(mode="after")
    def unique_ids(self) -> HeldOutDataset:
        ids = [case.id for case in self.cases]
        if len(ids) != len(set(ids)):
            raise ValueError("held-out case ids must be unique")
        return self


def load_held_out_payload() -> dict[str, Any]:
    payload = json.loads(HELD_OUT_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("held-out root must be an object")
    return payload


def load_held_out_dataset() -> HeldOutDataset:
    payload = load_held_out_payload()
    if "_meta" in payload and "meta" not in payload:
        payload = dict(payload)
        payload["meta"] = payload.pop("_meta")
    return HeldOutDataset.model_validate(payload)


def assemble_case(
    case: HeldOutCase, *, job_id: str | None = None
) -> SemanticGradeRequest:
    claims = [
        ClaimStructured(text=item.text, evidence_item_ids=list(item.evidence_item_ids))
        for item in case.claims
    ]
    sources = [
        SemanticExcerptSource(
            evidence_item_id=item.evidence_item_id,
            trust_label=item.trust_label or TRUST_UNTRUSTED,
            text=item.text,
        )
        for item in case.excerpts
    ]
    linked = {item.evidence_item_id for item in case.excerpts}
    return assemble_semantic_grade_request(
        job_id=job_id or f"held-out-{case.id}",
        claims=claims,
        linked_ids=linked,
        sources=sources,
    )


def _ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _f1(precision: float, recall: float) -> float:
    if precision + recall == 0:
        return 0.0
    return 2.0 * precision * recall / (precision + recall)


def per_class_scores(
    matrix: Mapping[str, Mapping[str, int]],
    label: SupportLabel,
) -> dict[str, float]:
    labels: tuple[SupportLabel, ...] = ("supported", "unclear", "unsupported")
    tp = matrix[label][label]
    fp = sum(matrix[other][label] for other in labels if other != label)
    fn = sum(matrix[label][other] for other in labels if other != label)
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    return {
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
        "tp": float(tp),
        "fp": float(fp),
        "fn": float(fn),
    }


def binary_report_scores(*, tp: int, fp: int, fn: int, tn: int) -> dict[str, float]:
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    return {
        "tp": float(tp),
        "fp": float(fp),
        "fn": float(fn),
        "tn": float(tn),
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
    }


def empty_confusion() -> dict[str, dict[str, int]]:
    labels: tuple[SupportLabel, ...] = ("supported", "unclear", "unsupported")
    return {row: {col: 0 for col in labels} for row in labels}


def classify_model_error(exc: BaseException) -> OutcomeKind:
    if isinstance(exc, ModelInvalidStructuredOutputError):
        return "malformed"
    if isinstance(exc, ModelTimeoutError):
        return "timeout"
    if isinstance(exc, ModelRateLimitedError):
        return "rate_limited"
    if isinstance(exc, ModelAuthConfigError):
        return "auth"
    if isinstance(exc, ModelRefusalError):
        return "refusal"
    if isinstance(exc, ModelError):
        return "other"
    return "other"


def sanitized_error_class(exc: BaseException) -> str:
    if isinstance(exc, ModelError):
        return sanitize_model_error(exc)
    return f"{type(exc).__name__}: calibration case failed"


def disagreement_reason(
    *,
    category: str,
    human_label: SupportLabel | None,
    predicted_label: SupportLabel | None,
    human_passed: bool,
    predicted_passed: bool,
) -> DisagreementReason | None:
    if category == "prompt_injection" and predicted_label == "supported":
        return "safety_prompt_injection"
    if human_label is not None and predicted_label is not None:
        if human_label != predicted_label:
            if category == "truncation_boundary":
                return "truncation_boundary"
            return "label_mismatch"
    if human_passed != predicted_passed:
        return "report_pass_mismatch"
    return None


def summarize_predictions(
    cases: list[HeldOutCase],
    predictions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare frozen human labels to predictions. Never mutates labels."""
    by_id = {case.id: case for case in cases}
    matrix = empty_confusion()
    abs_errors: list[float] = []
    report_tp = report_fp = report_fn = report_tn = 0
    disagreements: list[dict[str, object]] = []
    availability = {
        "malformed": 0,
        "timeout": 0,
        "rate_limited": 0,
        "auth": 0,
        "refusal": 0,
        "other": 0,
    }
    safety_failure = False

    for row in predictions:
        case_id = str(row["case_id"])
        case = by_id[case_id]
        kind = str(row["outcome"])
        if kind != "quality":
            availability[kind] = availability.get(kind, 0) + 1
            continue
        predicted_labels = list(row["predicted_labels"])
        predicted_scores = list(row["predicted_scores"])
        predicted_passed = bool(row["predicted_passed"])
        human_labels = [item.support for item in case.human.claims]
        if len(predicted_labels) != len(human_labels):
            availability["malformed"] += 1
            continue
        for human_label, predicted_label, human_claim, predicted_score in zip(
            human_labels,
            predicted_labels,
            case.human.claims,
            predicted_scores,
            strict=True,
        ):
            matrix[human_label][predicted_label] += 1
            abs_errors.append(abs(float(predicted_score) - human_claim.score))
            reason = disagreement_reason(
                category=case.category,
                human_label=human_label,
                predicted_label=predicted_label,
                human_passed=case.human.report_passed,
                predicted_passed=predicted_passed,
            )
            if reason == "safety_prompt_injection":
                safety_failure = True
            if reason in {
                "label_mismatch",
                "safety_prompt_injection",
                "truncation_boundary",
            }:
                disagreements.append(
                    {
                        "case_id": case_id,
                        "claim_ordinal": human_claim.claim_ordinal,
                        "reason": reason,
                    }
                )
        if case.human.report_passed and predicted_passed:
            report_tp += 1
        elif not case.human.report_passed and predicted_passed:
            report_fp += 1
        elif case.human.report_passed and not predicted_passed:
            report_fn += 1
        else:
            report_tn += 1
        if case.human.report_passed != predicted_passed:
            disagreements.append(
                {
                    "case_id": case_id,
                    "claim_ordinal": None,
                    "reason": "report_pass_mismatch",
                }
            )

    supported = per_class_scores(matrix, "supported")
    unclear = per_class_scores(matrix, "unclear")
    unsupported = per_class_scores(matrix, "unsupported")
    macro_f1 = (supported["f1"] + unclear["f1"] + unsupported["f1"]) / 3.0
    report = binary_report_scores(
        tp=report_tp, fp=report_fp, fn=report_fn, tn=report_tn
    )
    availability_failures = sum(availability.values())
    criteria_met = (
        not safety_failure
        and availability_failures == 0
        and supported["precision"]
        >= float(PROMOTION_CRITERIA["min_supported_precision"])
        and supported["recall"] >= float(PROMOTION_CRITERIA["min_supported_recall"])
        and macro_f1 >= float(PROMOTION_CRITERIA["min_macro_f1"])
        and report["f1"] >= float(PROMOTION_CRITERIA["min_report_f1"])
    )
    return {
        "per_claim_confusion": matrix,
        "supported": supported,
        "unclear": unclear,
        "unsupported": unsupported,
        "macro_f1": macro_f1,
        "score_mae": (sum(abs_errors) / len(abs_errors)) if abs_errors else None,
        "report": report,
        "disagreements": disagreements,
        "availability": availability,
        "safety_boundary_failure": safety_failure,
        "promotion_criteria_met": criteria_met,
        "does_not_freeze_evaluation_v1": True,
    }


def prediction_record(
    *,
    case_id: str,
    output_claims: list[Any] | None = None,
    predicted_passed: bool | None = None,
    outcome: OutcomeKind = "quality",
) -> dict[str, Any]:
    if outcome != "quality":
        return {"case_id": case_id, "outcome": outcome}
    claims = list(output_claims or [])
    return {
        "case_id": case_id,
        "outcome": "quality",
        "predicted_labels": [support_label_for_score(item.score) for item in claims],
        "predicted_scores": [float(item.score) for item in claims],
        "predicted_passed": bool(predicted_passed),
    }


def target_from_recorded_predictions(
    records: Mapping[str, Mapping[str, Any]],
) -> Any:
    def _target(inputs: dict[str, Any]) -> dict[str, object]:
        row = records[str(inputs["fixture_id"])]
        return {
            "outcome": row["outcome"],
            "report_passed": row.get("predicted_passed"),
            "claim_support": list(row.get("predicted_labels") or []),
        }

    return _target


def metadata_label_compare(run: object, example: object) -> dict[str, object]:
    """Compare predicted vs human labels. Metadata only; no bodies."""
    if isinstance(run, dict) and isinstance(example, dict):
        predicted = {
            "report_passed": run.get("report_passed"),
            "claim_support": list(run.get("claim_support") or []),
            "outcome": run.get("outcome"),
        }
        expected = {
            "report_passed": example.get("report_passed"),
            "claim_support": list(example.get("claim_support") or []),
        }
    else:
        predicted = {
            "report_passed": dict(getattr(run, "outputs", None) or {}).get(
                "report_passed"
            ),
            "claim_support": list(
                dict(getattr(run, "outputs", None) or {}).get("claim_support") or []
            ),
            "outcome": dict(getattr(run, "outputs", None) or {}).get("outcome"),
        }
        expected = {
            "report_passed": dict(getattr(example, "outputs", None) or {}).get(
                "report_passed"
            ),
            "claim_support": list(
                dict(getattr(example, "outputs", None) or {}).get("claim_support") or []
            ),
        }
    quality = predicted.get("outcome") == "quality"
    passed = (
        quality
        and predicted["report_passed"] == expected["report_passed"]
        and (predicted["claim_support"] == expected["claim_support"])
    )
    return {"key": "held_out_label_match", "score": 1.0 if passed else 0.0}


def example_id_for(case_id: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{DATASET_NAME}:{case_id}")


def example_inputs(case: HeldOutCase) -> dict[str, str]:
    return {
        "fixture_id": case.id,
        "category": case.category,
        "checkpoint": CHECKPOINT_COMMIT,
        "prompt_version": SEMANTIC_PROMPT_VERSION,
    }


def example_outputs(case: HeldOutCase) -> dict[str, object]:
    return {
        "report_passed": case.human.report_passed,
        "claim_support": [item.support for item in case.human.claims],
    }


def dataset_examples(dataset: HeldOutDataset) -> list[dict[str, object]]:
    return [
        {
            "example_id": example_id_for(case.id),
            "inputs": example_inputs(case),
            "outputs": example_outputs(case),
        }
        for case in dataset.cases
    ]


def golden_text_blob() -> str:
    payload = json.loads(GOLDENS_PATH.read_text(encoding="utf-8"))
    return json.dumps(payload, ensure_ascii=False)


def case_text_blob(case: HeldOutCase) -> str:
    parts = [item.text for item in case.claims]
    parts.extend(item.text for item in case.excerpts)
    return "\n".join(parts)


def _example_uuid(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def ensure_held_out_dataset(client: Any) -> object:
    """Read or create the metadata-only held-out dataset. SDK errors propagate."""
    from langsmith.utils import LangSmithConflictError, LangSmithNotFoundError

    read_dataset = client.read_dataset
    try:
        return read_dataset(dataset_name=DATASET_NAME)
    except LangSmithNotFoundError:
        create_dataset = client.create_dataset
        try:
            return create_dataset(
                DATASET_NAME,
                description="Atlas held_out_semantic.v1 metadata-only calibration",
            )
        except LangSmithConflictError:
            return read_dataset(dataset_name=DATASET_NAME)


def upsert_held_out_examples(client: Any, dataset: HeldOutDataset) -> list[uuid.UUID]:
    expected = dataset_examples(dataset)
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


def held_out_fingerprint(dataset: HeldOutDataset) -> str:
    canonical = json.dumps(
        dataset.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def substantive_calibration_content(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Canonical calibration content excluding approval metadata.

    Approval fields (human_reviewed, human_reviewer, reviewed_at, per-case
    reviewer, labeled_at, methodology, note) are omitted so an approval-only
    metadata update cannot change this fingerprint.
    """
    meta = payload.get("_meta") or payload.get("meta") or {}
    if not isinstance(meta, Mapping):
        raise TypeError("held-out meta must be an object")
    cases_out: list[dict[str, Any]] = []
    for case in payload["cases"]:
        if not isinstance(case, Mapping):
            raise TypeError("held-out case must be an object")
        human = case["human"]
        if not isinstance(human, Mapping):
            raise TypeError("held-out human labels must be an object")
        cases_out.append(
            {
                "id": case["id"],
                "category": case["category"],
                "claims": [
                    {
                        "text": item["text"],
                        "evidence_item_ids": list(item["evidence_item_ids"]),
                    }
                    for item in case["claims"]
                ],
                "excerpts": [
                    {
                        "evidence_item_id": item["evidence_item_id"],
                        "trust_label": item["trust_label"],
                        "text": item["text"],
                    }
                    for item in case["excerpts"]
                ],
                "human_claims": [
                    {
                        "claim_ordinal": item["claim_ordinal"],
                        "support": item["support"],
                        "score": item["score"],
                        "rationale": item["rationale"],
                    }
                    for item in human["claims"]
                ],
                "report_passed": human["report_passed"],
                "report_score": human["report_score"],
                "report_rationale": human["rationale"],
            }
        )
    return {
        "promotion_criteria": dict(meta["promotion_criteria"]),
        "cases": cases_out,
    }


def substantive_calibration_fingerprint(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        substantive_calibration_content(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
