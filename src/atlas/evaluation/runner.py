"""Orchestrate candidate grading and durable evaluation persistence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

from atlas.evaluation.aggregation import aggregate_dimensions
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
    DimensionResult,
    EvaluationCandidateInput,
    EvaluationRunResult,
    ToolSummaryRow,
)
from atlas.evaluation.errors import (
    EvaluationError,
    EvaluationOwnershipLostError,
    EvaluationTerminalError,
    EvaluationValidationError,
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
from atlas.evaluation.semantic_contracts import (
    FAKE_SEMANTIC_GRADER_VERSION,
    LIVE_SEMANTIC_GRADER_VERSION,
    SEMANTIC_PROMPT_VERSION,
    SKIPPED_SEMANTIC_GRADER_VERSION,
    SemanticExcerptSource,
    SemanticGradeRequest,
    SemanticGraderVersion,
    SemanticPromptVersion,
)
from atlas.evaluation.semantic_input import assemble_semantic_grade_request
from atlas.evaluation.service import EvaluationService
from atlas.evidence.bounds import MAX_EVIDENCE_ITEMS_TO_DRAFTER
from atlas.models.errors import ModelError
from atlas.observability.langsmith import attach_run_metadata, trace_ai
from atlas.observability.metrics import AtlasMetrics, default_metrics

if TYPE_CHECKING:
    from atlas.evaluation.ports import SemanticGroundednessGrader


LinkedIdsLoader = Callable[[EvaluationCandidateInput, str], set[str]]
ToolRowsLoader = Callable[[EvaluationCandidateInput, str], list[ToolSummaryRow]]
ExcerptSourceLoader = Callable[[list[str]], list[SemanticExcerptSource]]

DEFAULT_MAX_LOGICAL_CALLS = 6


def _semantic_grader_outcome_for_error(exc: ModelError) -> str:
    """Map a typed model error onto the closed semantic-grader outcome set."""
    from atlas.models.errors import (
        ModelAttemptOwnershipLostError,
        ModelAuthConfigError,
        ModelInvalidRequestError,
        ModelInvalidStructuredOutputError,
        ModelInvocationInProgressError,
        ModelRateLimitedError,
        ModelRefusalError,
        ModelTemporaryError,
        ModelTimeoutError,
    )

    if isinstance(exc, ModelTimeoutError):
        return "timeout"
    if isinstance(exc, ModelRateLimitedError):
        return "rate_limited"
    if isinstance(exc, ModelAuthConfigError):
        return "auth_config"
    if isinstance(exc, ModelInvalidStructuredOutputError):
        return "malformed"
    if isinstance(exc, ModelRefusalError):
        return "refusal"
    if isinstance(exc, ModelAttemptOwnershipLostError):
        return "ownership_lost"
    if isinstance(exc, (ModelTemporaryError, ModelInvocationInProgressError)):
        return "unavailable"
    if isinstance(exc, ModelInvalidRequestError):
        return "config"
    return "other"


class EvaluationRunner:
    """Run hard/soft graders and persist a fenced evaluation attempt."""

    def __init__(
        self,
        *,
        evaluation_service: EvaluationService,
        load_linked_ids: LinkedIdsLoader | None = None,
        load_tool_rows: ToolRowsLoader | None = None,
        load_excerpt_sources: ExcerptSourceLoader | None = None,
        semantic_grader: SemanticGroundednessGrader | None = None,
        min_linked: int = 1,
        max_logical_calls: int = DEFAULT_MAX_LOGICAL_CALLS,
        metrics: AtlasMetrics | None = None,
        semantic_model_provider: str | None = None,
        semantic_model_name: str | None = None,
        semantic_temperature: float | None = None,
    ) -> None:
        self._service = evaluation_service
        self._load_linked_ids = load_linked_ids or self._default_linked_ids
        self._load_tool_rows = load_tool_rows or self._default_tool_rows
        self._load_excerpt_sources = load_excerpt_sources
        self._semantic_grader = semantic_grader
        self._min_linked = min_linked
        self._max_logical_calls = max_logical_calls
        self._metrics = metrics or default_metrics()
        self._semantic_model_provider = semantic_model_provider
        self._semantic_model_name = semantic_model_name
        self._semantic_temperature = semantic_temperature

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
        self._assert_profile_matches_grader(candidate.evaluation_profile)
        semantic_request, grader_version, prompt_version = (
            self._semantic_fingerprint_inputs(candidate, linked_ids)
        )
        live_provider, live_model, live_temperature = self._live_fingerprint_identity(
            grader_version
        )
        fingerprint = fingerprint_grading_snapshot(
            candidate,
            linked_evidence_ids=linked_ids,
            tool_rows=tool_rows,
            provenance_ok=provenance_ok,
            max_logical_calls=self._max_logical_calls,
            semantic_grader_version=grader_version,
            semantic_prompt_version=prompt_version,
            semantic_claims=(
                None if semantic_request is None else semantic_request.claims
            ),
            semantic_excerpts=(
                None if semantic_request is None else semantic_request.excerpts
            ),
            semantic_model_provider=live_provider,
            semantic_model_name=live_model,
            semantic_temperature=live_temperature,
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
                semantic_request=semantic_request,
            )
            aggregate, passed, stamped = aggregate_dimensions(dimensions)
            disposition = "complete" if passed else "terminal"
            attach_run_metadata(
                {
                    "atlas.evaluation_passed": passed,
                    "atlas.disposition_hint": disposition,
                }
            )
            result = self._service.finalize_success(
                run_id=run_id,
                ownership_token=ownership_token,
                aggregate=aggregate,
                passed=passed,
                dimensions=stamped,
                disposition_hint=disposition,
                grader_versions=versions,
            )
            self._observe_semantic_success(stamped)
            return result
        except EvaluationOwnershipLostError:
            # Never attempt failure finalization after ownership loss.
            raise
        except ModelError as exc:
            from atlas.models.errors import ModelAttemptOwnershipLostError

            outcome = _semantic_grader_outcome_for_error(exc)
            if isinstance(exc, ModelAttemptOwnershipLostError):
                self._metrics.observe_semantic_grader_outcome(outcome=outcome)
                raise
            self._finalize_owned_failure(
                run_id=run_id,
                ownership_token=ownership_token,
                error_class=type(exc).__name__,
            )
            self._metrics.observe_semantic_grader_outcome(outcome=outcome)
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
        semantic_request: SemanticGradeRequest | None,
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
                    SKIPPED_SEMANTIC_GRADER_VERSION,
                    skipped_semantic_dimension,
                    extra_metadata={"atlas.semantic_grader_outcome": "skipped"},
                )
            )
            versions["semantic_groundedness"] = SKIPPED_SEMANTIC_GRADER_VERSION
        else:
            semantic_version = self._semantic_grader_version()
            request = semantic_request or self._assemble_semantic_request(
                candidate, linked_ids
            )
            semantic = self._trace_dimension(
                "semantic_groundedness",
                semantic_version,
                lambda: self._grade_semantic(request),
            )
            dimensions.append(semantic)
            versions["semantic_groundedness"] = semantic_version

        return dimensions, versions

    def _semantic_fingerprint_inputs(
        self,
        candidate: EvaluationCandidateInput,
        linked_ids: set[str],
    ) -> tuple[
        SemanticGradeRequest | None,
        SemanticGraderVersion,
        SemanticPromptVersion,
    ]:
        if self._semantic_grader is None:
            return (
                None,
                SKIPPED_SEMANTIC_GRADER_VERSION,
                SKIPPED_SEMANTIC_GRADER_VERSION,
            )
        request = self._assemble_semantic_request(candidate, linked_ids)
        version = self._semantic_grader_version()
        grader_version: SemanticGraderVersion
        if version == FAKE_SEMANTIC_GRADER_VERSION:
            grader_version = FAKE_SEMANTIC_GRADER_VERSION
        else:
            grader_version = LIVE_SEMANTIC_GRADER_VERSION
        return request, grader_version, SEMANTIC_PROMPT_VERSION

    def _assert_profile_matches_grader(self, profile: str) -> None:
        grader = self._semantic_grader
        if profile == EVALUATION_PROFILE_CANDIDATE:
            if grader is not None:
                raise EvaluationValidationError(
                    "evaluation.candidate.v1 requires skipped semantic grading"
                )
            return
        if profile == EVALUATION_PROFILE_CANDIDATE_FAKE:
            if not isinstance(grader, FakeSemanticGroundednessGrader):
                raise EvaluationValidationError(
                    "evaluation.candidate.fake.v1 requires fake semantic grading"
                )
            return
        if profile == EVALUATION_PROFILE_V1:
            if grader is None or isinstance(grader, FakeSemanticGroundednessGrader):
                raise EvaluationValidationError(
                    "evaluation.v1 requires live semantic grading"
                )
            return
        raise EvaluationValidationError("Unsupported evaluation profile")

    def _live_fingerprint_identity(
        self, grader_version: SemanticGraderVersion
    ) -> tuple[str | None, str | None, float | None]:
        if grader_version != LIVE_SEMANTIC_GRADER_VERSION:
            return None, None, None
        provider = self._semantic_model_provider
        model_name = self._semantic_model_name
        temperature = self._semantic_temperature
        if provider is None or model_name is None or temperature is None:
            raise EvaluationValidationError(
                "live semantic fingerprints require provider/model/temperature"
            )
        return provider, model_name, temperature

    def _assemble_semantic_request(
        self,
        candidate: EvaluationCandidateInput,
        linked_ids: set[str],
    ) -> SemanticGradeRequest:
        cited_ids = {
            item_id for claim in candidate.claims for item_id in claim.evidence_item_ids
        }
        selected_ids = sorted(cited_ids & set(linked_ids))[
            :MAX_EVIDENCE_ITEMS_TO_DRAFTER
        ]
        sources: list[SemanticExcerptSource] = []
        if self._load_excerpt_sources is not None:
            sources = self._load_excerpt_sources(selected_ids)
        return assemble_semantic_grade_request(
            job_id=candidate.job_id,
            claims=list(candidate.claims),
            linked_ids=set(linked_ids),
            sources=sources,
        )

    def _semantic_grader_version(self) -> str:
        if isinstance(self._semantic_grader, FakeSemanticGroundednessGrader):
            return FakeSemanticGroundednessGrader.version
        version = getattr(self._semantic_grader, "version", None)
        if isinstance(version, str) and version:
            return version
        return LIVE_SEMANTIC_GRADER_VERSION

    def _grade_semantic(self, request: SemanticGradeRequest) -> DimensionResult:
        assert self._semantic_grader is not None
        try:
            semantic = self._semantic_grader.grade(request)
        except ModelError as exc:
            attach_run_metadata(
                {
                    "atlas.semantic_grader_outcome": _semantic_grader_outcome_for_error(
                        exc
                    )
                }
            )
            raise
        if not isinstance(semantic, DimensionResult):
            raise TypeError("semantic grader must return DimensionResult")
        attach_run_metadata(
            {
                "atlas.semantic_grader_outcome": (
                    "quality_pass" if semantic.passed else "quality_fail"
                )
            }
        )
        return semantic

    def _observe_semantic_success(self, dimensions: list[DimensionResult]) -> None:
        semantic = next(
            item for item in dimensions if item.name == "semantic_groundedness"
        )
        if semantic.method == "skipped":
            outcome = "skipped"
        elif semantic.passed:
            outcome = "quality_pass"
        else:
            outcome = "quality_fail"
        self._metrics.observe_semantic_grader_outcome(outcome=outcome)

    @staticmethod
    def _trace_dimension(
        name: str,
        grader_version: str,
        fn: Callable[[], DimensionResult],
        extra_metadata: dict[str, object] | None = None,
    ) -> DimensionResult:
        def _run() -> DimensionResult:
            result = fn()
            metadata: dict[str, object] = {
                "atlas.evaluation_passed": result.passed,
                "atlas.evaluation_score": result.score,
                "atlas.grader_version": grader_version,
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            attach_run_metadata(metadata)
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
