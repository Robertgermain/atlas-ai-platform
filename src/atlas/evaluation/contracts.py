"""Typed contracts for candidate evaluation (Slice 12A).

Profile ``evaluation.candidate.v1`` is provisional and not a frozen
``evaluation.v1`` calibration. Soft dimensions are heuristics pending human
golden approval.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from atlas.evidence.contracts import ClaimStructured

EVALUATION_PROFILE: Literal["evaluation.candidate.v1"] = "evaluation.candidate.v1"

EvaluationProfile = Literal["evaluation.candidate.v1"]

DimensionName = Literal[
    "citation_integrity",
    "tool_use",
    "report_structure",
    "coverage",
    "completeness",
    "lexical_id_groundedness",
    "semantic_groundedness",
]

GraderMethod = Literal["deterministic", "llm", "skipped"]

EvaluationRunStatus = Literal["IN_PROGRESS", "SUCCEEDED", "FAILED"]

DispositionHint = Literal["complete", "terminal", "repair", "await_review", "retry"]


class DimensionResult(BaseModel):
    """One graded dimension with sanitized failure codes only."""

    name: DimensionName
    score: Annotated[float, Field(ge=0.0, le=1.0)]
    passed: bool
    method: GraderMethod
    is_hard: bool
    is_provisional: bool
    failure_codes: list[str] = Field(default_factory=list)
    weight: Annotated[float, Field(ge=0.0, le=1.0)] = 0.0

    @field_validator("failure_codes")
    @classmethod
    def validate_failure_codes(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for code in value:
            item = code.strip()
            if not item:
                raise ValueError("failure_codes must be non-empty strings")
            if any(ch.isspace() for ch in item):
                raise ValueError("failure_codes must not contain whitespace")
            if not item.replace("_", "").isalnum() or not item.isupper():
                raise ValueError("failure_codes must be UPPER_SNAKE sanitized codes")
            cleaned.append(item)
        return cleaned


class ToolSummaryRow(BaseModel):
    """Sanitized logical tool-ledger row for grading and fingerprints.

    Each row is one logical tool invocation (not a physical provider attempt).
    Bodies, args, and URLs are never included.
    """

    node_name: str
    origin: str
    tool_id: str = ""
    status: str = ""


class EvaluationCandidateInput(BaseModel):
    """Pre-persist candidate pack consumed by graders (no evidence bodies)."""

    job_id: str
    question: str
    plan: list[str]
    findings: list[str]
    draft: str
    claims: list[ClaimStructured] = Field(default_factory=list)
    evidence_item_ids: list[str] = Field(default_factory=list)
    tool_summary: list[ToolSummaryRow] = Field(default_factory=list)
    repair_count: Annotated[int, Field(ge=0)] = 0
    evaluation_attempt: Annotated[int, Field(ge=1)] = 1
    evaluation_profile: EvaluationProfile = EVALUATION_PROFILE
    golden_facets_hit: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    golden_completeness_ratio: Annotated[float, Field(ge=0.0, le=1.0)] | None = None

    @field_validator("job_id", "question")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("field must be non-empty")
        return cleaned

    @field_validator("draft")
    @classmethod
    def strip_draft(cls, value: str) -> str:
        # Empty draft is a graded hard failure, not a contract rejection.
        return value.strip()

    @field_validator("evidence_item_ids")
    @classmethod
    def validate_evidence_ids(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("evidence_item_ids must be non-empty strings")
        return cleaned


class EvaluationRunResult(BaseModel):
    """Durable evaluation outcome returned to callers and APIs."""

    run_id: str
    research_job_id: str
    workflow_execution_id: str
    evaluation_profile: EvaluationProfile
    evaluation_attempt: int
    status: EvaluationRunStatus
    input_fingerprint: str
    passed: bool | None
    aggregate_score: Annotated[float, Field(ge=0.0, le=1.0)] | None
    disposition_hint: DispositionHint | None
    dimensions: list[DimensionResult] = Field(default_factory=list)
    grader_versions: dict[str, str] = Field(default_factory=dict)
    ownership_token: str | None = None
