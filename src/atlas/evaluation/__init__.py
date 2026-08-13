"""Candidate evaluation package (Milestone 12 Slice 12A / Slice 15C1)."""

from atlas.evaluation.aggregation import (
    HARD_DIMENSIONS,
    PROVISIONAL_SOFT_DIMENSIONS,
    PROVISIONAL_SOFT_PASS_THRESHOLD,
    aggregate_dimensions,
)
from atlas.evaluation.claim_fingerprint import fingerprint_job_claim_token
from atlas.evaluation.composition import (
    build_semantic_grader,
    require_semantic_grader_mode,
)
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE,
    DimensionResult,
    EvaluationCandidateInput,
    EvaluationRunResult,
    ToolSummaryRow,
)
from atlas.evaluation.errors import (
    EvaluationConflictError,
    EvaluationError,
    EvaluationInProgressError,
    EvaluationNotFoundError,
    EvaluationOwnershipLostError,
    EvaluationStaleError,
    EvaluationTerminalError,
    EvaluationValidationError,
    SemanticGraderConfigurationError,
)
from atlas.evaluation.fingerprint import (
    fingerprint_candidate,
    fingerprint_grading_snapshot,
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
from atlas.evaluation.llm_grader import (
    LangChainSemanticGroundednessGrader,
    LiveSemanticGroundednessGrader,
    SemanticGroundednessOutput,
)
from atlas.evaluation.ports import (
    EvaluationNodeRunner,
    EvaluationServicePort,
    SemanticGroundednessGrader,
)
from atlas.evaluation.repository import SqlAlchemyEvaluationRepository
from atlas.evaluation.runner import EvaluationRunner
from atlas.evaluation.semantic_contracts import (
    SEMANTIC_PROMPT_VERSION,
    SemanticGradeRequest,
)
from atlas.evaluation.service import EvaluationService

__all__ = [
    "EVALUATION_PROFILE",
    "HARD_DIMENSIONS",
    "PROVISIONAL_SOFT_DIMENSIONS",
    "PROVISIONAL_SOFT_PASS_THRESHOLD",
    "SEMANTIC_PROMPT_VERSION",
    "DimensionResult",
    "EvaluationCandidateInput",
    "EvaluationConflictError",
    "EvaluationError",
    "EvaluationInProgressError",
    "EvaluationNodeRunner",
    "EvaluationNotFoundError",
    "EvaluationOwnershipLostError",
    "EvaluationRunner",
    "EvaluationRunResult",
    "EvaluationService",
    "EvaluationServicePort",
    "EvaluationStaleError",
    "EvaluationTerminalError",
    "EvaluationValidationError",
    "FakeSemanticGroundednessGrader",
    "LangChainSemanticGroundednessGrader",
    "LiveSemanticGroundednessGrader",
    "SemanticGradeRequest",
    "SemanticGraderConfigurationError",
    "SemanticGroundednessGrader",
    "SemanticGroundednessOutput",
    "SqlAlchemyEvaluationRepository",
    "ToolSummaryRow",
    "aggregate_dimensions",
    "build_semantic_grader",
    "fingerprint_candidate",
    "fingerprint_grading_snapshot",
    "fingerprint_job_claim_token",
    "grade_citation_integrity",
    "grade_completeness",
    "grade_coverage",
    "grade_lexical_id_groundedness",
    "grade_report_structure",
    "grade_tool_use",
    "require_semantic_grader_mode",
]
