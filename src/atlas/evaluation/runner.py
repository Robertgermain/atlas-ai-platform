"""Orchestrate candidate grading and durable evaluation persistence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

from atlas.evaluation.aggregation import aggregate_dimensions
from atlas.evaluation.contracts import (
    DimensionResult,
    EvaluationCandidateInput,
    EvaluationRunResult,
    ToolSummaryRow,
)
from atlas.evaluation.errors import (
    EvaluationError,
    EvaluationOwnershipLostError,
    EvaluationTerminalError,
    sanitize_evaluation_error,
)
from atlas.evaluation.fingerprint import fingerprint_grading_snapshot
from atlas.evaluation.graders import (
    GRADER_VERSIONS,
    FakeSemanticGroundednessGrader,
    grade_citation_integrity,
    grade_completeness,
    grade_coverage,
    grade_lexical_id_groundedness,
    grade_report_structure,
    grade_tool_use,
    skipped_semantic_dimension,
)
from atlas.evaluation.service import EvaluationService
from atlas.observability.langsmith import attach_run_metadata, trace_ai

if TYPE_CHECKING:
    from atlas.evaluation.ports import SemanticGroundednessGrader


LinkedIdsLoader = Callable[[EvaluationCandidateInput, str], set[str]]
ToolRowsLoader = Callable[[EvaluationCandidateInput, str], list[ToolSummaryRow]]

DEFAULT_MAX_LOGICAL_CALLS = 6


class EvaluationRunner:
    """Run hard/soft graders and persist a fenced evaluation attempt."""

    def __init__(
        self,
        *,
        evaluation_service: EvaluationService,
        load_linked_ids: LinkedIdsLoader | None = None,
        load_tool_rows: ToolRowsLoader | None = None,
        semantic_grader: SemanticGroundednessGrader | None = None,
        min_linked: int = 1,
        max_logical_calls: int = DEFAULT_MAX_LOGICAL_CALLS,
    ) -> None:
        self._service = evaluation_service
        self._load_linked_ids = load_linked_ids or self._default_linked_ids
        self._load_tool_rows = load_tool_rows or self._default_tool_rows
        self._semantic_grader = semantic_grader
        self._min_linked = min_linked
        self._max_logical_calls = max_logical_calls

    @staticmethod
    def _default_linked_ids(
        candidate: EvaluationCandidateInput,
        _workflow_execution_id: str,
    ) -> set[str]:
        return set(candidate.evidence_item_ids)

    @staticmethod
    def _default_tool_rows(
        candidate: EvaluationCandidateInput,
        _workflow_execution_id: str,
    ) -> list[ToolSummaryRow]:
        return list(candidate.tool_summary)

    def run(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        provenance_ok: bool = True,
        job_claim_token: str,
    ) -> EvaluationRunResult:
        def _execute() -> EvaluationRunResult:
            return self._run_owned(
                candidate=candidate,
                workflow_execution_id=workflow_execution_id,
                deadline=deadline,
                provenance_ok=provenance_ok,
                job_claim_token=job_claim_token,
            )

        return trace_ai(
            name="evaluation.run",
            run_type="chain",
            metadata={
                "atlas.research_job_id": candidate.job_id,
                "atlas.workflow_execution_id": workflow_execution_id,
                "atlas.evaluation_profile": candidate.evaluation_profile,
                "atlas.evaluation_attempt": candidate.evaluation_attempt,
                "atlas.node_name": "evaluate",
            },
            fn=_execute,
        )

    def _run_owned(
        self,
        *,
        candidate: EvaluationCandidateInput,
        workflow_execution_id: str,
        deadline: datetime,
        provenance_ok: bool,
        job_claim_token: str,
    ) -> EvaluationRunResult:
        linked_ids = self._load_linked_ids(candidate, workflow_execution_id)
        tool_rows = self._load_tool_rows(candidate, workflow_execution_id)
        fingerprint = fingerprint_grading_snapshot(
            candidate,
            linked_evidence_ids=linked_ids,
            tool_rows=tool_rows,
            provenance_ok=provenance_ok,
            max_logical_calls=self._max_logical_calls,
        )
        run_id, ownership_token, replay = self._service.begin_or_resume(
            execution_id=workflow_execution_id,
            profile=candidate.evaluation_profile,
            attempt=candidate.evaluation_attempt,
            fingerprint=fingerprint,
            job_id=candidate.job_id,
            deadline=deadline,
            job_claim_token=job_claim_token,
        )
        if replay is not None:
            attach_run_metadata(
                {
                    "atlas.evaluation_run_id": run_id,
                    "atlas.disposition_hint": "replay",
                }
            )
            return replay

        attach_run_metadata({"atlas.evaluation_run_id": run_id})

        try:
            dimensions, versions = self._grade(
                candidate=candidate,
                linked_ids=linked_ids,
                tool_rows=tool_rows,
                provenance_ok=provenance_ok,
            )
            aggregate, passed, stamped = aggregate_dimensions(dimensions)
            disposition = "complete" if passed else "terminal"
            attach_run_metadata(
                {
                    "atlas.evaluation_passed": passed,
                    "atlas.disposition_hint": disposition,
                }
            )
            return self._service.finalize_success(
                run_id=run_id,
                ownership_token=ownership_token,
                aggregate=aggregate,
                passed=passed,
                dimensions=stamped,
                disposition_hint=disposition,
                grader_versions=versions,
            )
        except EvaluationOwnershipLostError:
            # Never attempt failure finalization after ownership loss.
            raise
        except EvaluationError as exc:
            self._finalize_owned_failure(
                run_id=run_id,
                ownership_token=ownership_token,
                error_class=type(exc).__name__,
            )
            raise
        except Exception as exc:
            self._finalize_owned_failure(
                run_id=run_id,
                ownership_token=ownership_token,
                error_class="EvaluationUnexpectedError",
            )
            raise EvaluationTerminalError(sanitize_evaluation_error(exc)) from None

    def _finalize_owned_failure(
        self,
        *,
        run_id: str,
        ownership_token: str,
        error_class: str,
    ) -> None:
        try:
            self._service.finalize_failure(
                run_id=run_id,
                ownership_token=ownership_token,
                error_class=error_class,
            )
        except EvaluationOwnershipLostError:
            # Newer owner remains unchanged.
            raise

    def _grade(
        self,
        *,
        candidate: EvaluationCandidateInput,
        linked_ids: set[str],
        tool_rows: list[ToolSummaryRow],
        provenance_ok: bool,
    ) -> tuple[list[DimensionResult], dict[str, str]]:
        # Lazy import avoids evaluation↔workflow circular import at package load.
        from atlas.workflow.fakes import format_research_report

        preview = format_research_report(
            question=candidate.question,
            plan=list(candidate.plan),
            findings=list(candidate.findings),
            draft=candidate.draft,
            claims=list(candidate.claims) or None,
        )

        dimensions: list[DimensionResult] = [
            self._trace_dimension(
                "citation_integrity",
                GRADER_VERSIONS["citation_integrity"],
                lambda: grade_citation_integrity(
                    candidate,
                    linked_ids=linked_ids,
                    provenance_ok=provenance_ok,
                ),
            ),
            self._trace_dimension(
                "tool_use",
                GRADER_VERSIONS["tool_use"],
                lambda: grade_tool_use(
                    tool_rows,
                    max_logical_calls=self._max_logical_calls,
                ),
            ),
            self._trace_dimension(
                "report_structure",
                GRADER_VERSIONS["report_structure"],
                lambda: grade_report_structure(
                    preview,
                    draft=candidate.draft,
                    plan=list(candidate.plan),
                ),
            ),
            self._trace_dimension(
                "coverage",
                GRADER_VERSIONS["coverage"],
                lambda: grade_coverage(
                    linked_count=len(linked_ids),
                    has_claims=bool(candidate.claims),
                    min_linked=self._min_linked,
                    golden_facets_hit=candidate.golden_facets_hit,
                ),
            ),
            self._trace_dimension(
                "completeness",
                GRADER_VERSIONS["completeness"],
                lambda: grade_completeness(
                    plan=list(candidate.plan),
                    findings=list(candidate.findings),
                    draft=candidate.draft,
                    golden_ratio=candidate.golden_completeness_ratio,
                ),
            ),
            self._trace_dimension(
                "lexical_id_groundedness",
                GRADER_VERSIONS["lexical_id_groundedness"],
                lambda: grade_lexical_id_groundedness(candidate.claims, linked_ids),
            ),
        ]

        versions = {
            name: GRADER_VERSIONS[name]
            for name in (
                "citation_integrity",
                "tool_use",
                "report_structure",
                "coverage",
                "completeness",
                "lexical_id_groundedness",
            )
        }

        if self._semantic_grader is None:
            dimensions.append(
                self._trace_dimension(
                    "semantic_groundedness",
                    "skipped",
                    skipped_semantic_dimension,
                )
            )
            versions["semantic_groundedness"] = "skipped"
        else:
            semantic_version = self._semantic_grader_version()
            semantic = self._trace_dimension(
                "semantic_groundedness",
                semantic_version,
                lambda: self._grade_semantic(candidate, linked_ids),
            )
            dimensions.append(semantic)
            versions["semantic_groundedness"] = semantic_version

        return dimensions, versions

    def _semantic_grader_version(self) -> str:
        if isinstance(self._semantic_grader, FakeSemanticGroundednessGrader):
            return FakeSemanticGroundednessGrader.version
        version = getattr(self._semantic_grader, "version", None)
        if isinstance(version, str) and version:
            return version
        return GRADER_VERSIONS["semantic_groundedness"]

    def _grade_semantic(
        self,
        candidate: EvaluationCandidateInput,
        linked_ids: set[str],
    ) -> DimensionResult:
        assert self._semantic_grader is not None
        semantic = self._semantic_grader.grade(
            candidate,
            linked_ids=linked_ids,
        )
        if not isinstance(semantic, DimensionResult):
            raise TypeError("semantic grader must return DimensionResult")
        return semantic

    @staticmethod
    def _trace_dimension(
        name: str,
        grader_version: str,
        fn: Callable[[], DimensionResult],
    ) -> DimensionResult:
        def _run() -> DimensionResult:
            result = fn()
            attach_run_metadata(
                {
                    "atlas.evaluation_passed": result.passed,
                    "atlas.evaluation_score": result.score,
                    "atlas.grader_version": grader_version,
                }
            )
            return result

        return trace_ai(
            name=f"dimension.{name}",
            run_type="chain",
            metadata={
                "atlas.evaluation_dimension": name,
                "atlas.node_name": "evaluate",
            },
            fn=_run,
        )
