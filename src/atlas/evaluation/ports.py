"""Typed capability ports for evaluation services and graders."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from atlas.evaluation.contracts import (
    DimensionResult,
    EvaluationCandidateInput,
    EvaluationProfile,
    EvaluationRunResult,
)
from atlas.evaluation.semantic_contracts import SemanticGradeRequest
from atlas.evidence.contracts import ClaimStructured


class SemanticGroundednessGrader(Protocol):
    """Optional semantic groundedness grader (fake or live)."""

    version: str

    def grade(self, request: SemanticGradeRequest) -> DimensionResult: ...


class EvaluationServicePort(Protocol):
    """Durable evaluation begin/resume and fenced finalization."""

    def begin_or_resume(
        self,
        *,
        execution_id: str,
        profile: EvaluationProfile,
        attempt: int,
        fingerprint: str,
        job_id: str,
        deadline: datetime,
        job_claim_token: str,
    ) -> tuple[str, str, EvaluationRunResult | None]: ...

    def finalize_success(
        self,
        *,
        run_id: str,
        ownership_token: str,
        aggregate: float,
        passed: bool,
        dimensions: list[DimensionResult],
        disposition_hint: str,
        grader_versions: dict[str, str],
    ) -> EvaluationRunResult: ...

    def finalize_failure(
        self,
        *,
        run_id: str,
        ownership_token: str,
        error_class: str,
    ) -> None: ...

    def get_latest_for_job(self, job_id: str) -> EvaluationRunResult | None: ...

    def get_by_job(self, job_id: str) -> list[EvaluationRunResult]: ...


class EvaluationProvenancePort(Protocol):
    """Fail-closed citation and evidence provenance checks for evaluation."""

    def provenance_ok_for_claims(
        self,
        *,
        job_id: str,
        claims: list[ClaimStructured],
    ) -> bool: ...


class EvaluationNodeRunner(Protocol):
    """Typed runner consumed by ``evaluate_node`` (no getattr discovery)."""

    def run(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        job_claim_token: str,
        provenance_ok: bool = True,
    ) -> EvaluationRunResult: ...

    def provenance_ok_for_claims(
        self,
        *,
        job_id: str,
        claims: list[ClaimStructured],
    ) -> bool: ...
