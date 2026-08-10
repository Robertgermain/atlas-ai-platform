"""Recovery policy, categories, and backoff for Slice 12B."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal

from atlas.evaluation.aggregation import HARD_DIMENSIONS
from atlas.evaluation.contracts import DimensionResult, EvaluationRunResult

MAX_REPAIRS = 1
MAX_JOB_RETRIES = 2
MAX_EVALUATION_ATTEMPTS = 4
MAX_POLICY_WORKFLOW_EXECUTIONS = 3

DEFAULT_RETRY_BASE_SECONDS = 5.0
DEFAULT_RETRY_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_RETRY_JITTER_MAX_SECONDS = 1.0


class FailureCategory(StrEnum):
    """Sanitized Atlas-owned failure categories."""

    QUALITY_CITATION_INTEGRITY = "QUALITY_CITATION_INTEGRITY"
    QUALITY_STRUCTURE = "QUALITY_STRUCTURE"
    QUALITY_COVERAGE = "QUALITY_COVERAGE"
    QUALITY_GROUNDEDNESS = "QUALITY_GROUNDEDNESS"
    QUALITY_COMPLETENESS = "QUALITY_COMPLETENESS"
    QUALITY_TOOL_POLICY = "QUALITY_TOOL_POLICY"
    TRANSIENT_TIMEOUT = "TRANSIENT_TIMEOUT"
    TRANSIENT_RATE_LIMIT = "TRANSIENT_RATE_LIMIT"
    TRANSIENT_PROVIDER = "TRANSIENT_PROVIDER"
    PERMANENT_VALIDATION = "PERMANENT_VALIDATION"
    PERMANENT_AUTH_CONFIG = "PERMANENT_AUTH_CONFIG"
    PERMANENT_BUDGET_EXHAUSTED = "PERMANENT_BUDGET_EXHAUSTED"
    REPAIRABLE_DRAFT = "REPAIRABLE_DRAFT"
    NEEDS_HUMAN_REVIEW = "NEEDS_HUMAN_REVIEW"
    TERMINAL_UNKNOWN = "TERMINAL_UNKNOWN"


PolicyAction = Literal[
    "complete",
    "repair",
    "await_review",
    "retry",
    "terminal",
]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Deterministic recovery decision for one evaluation or exception."""

    action: PolicyAction
    failure_category: FailureCategory
    reason_code: str


@dataclass(frozen=True, slots=True)
class AttemptCounts:
    """Authoritative counters used by the policy engine."""

    repair_count: int
    job_retry_count: int
    evaluation_attempt_count: int


def compute_retry_delay_seconds(
    *,
    attempt_number: int,
    base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
    max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    jitter_max_seconds: float = DEFAULT_RETRY_JITTER_MAX_SECONDS,
    jitter_seconds: float | None = None,
) -> float:
    """Return delay for the Nth job-level retry (1-based).

    ``delay = min(max_backoff, base * 2^(attempt_number - 1)) + bounded_jitter``.
    Jitter is injectable for tests; when omitted, samples ``[0, jitter_max]``.
    """
    if attempt_number < 1:
        raise ValueError("attempt_number must be >= 1")
    exponential: float = min(
        max_backoff_seconds, base_seconds * (2 ** (attempt_number - 1))
    )
    if jitter_seconds is None:
        if jitter_max_seconds <= 0:
            jitter = 0.0
        else:
            jitter = secrets.randbelow(int(jitter_max_seconds * 1000) + 1) / 1000.0
    else:
        if jitter_seconds < 0 or jitter_seconds > jitter_max_seconds:
            raise ValueError("jitter_seconds must be within [0, jitter_max_seconds]")
        jitter = jitter_seconds
    return exponential + jitter


def schedule_next_attempt_at(
    *,
    now: datetime,
    attempt_number: int,
    base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
    max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    jitter_max_seconds: float = DEFAULT_RETRY_JITTER_MAX_SECONDS,
    jitter_seconds: float | None = None,
) -> datetime:
    """Compute UTC next_attempt_at for a job-level retry."""
    delay = compute_retry_delay_seconds(
        attempt_number=attempt_number,
        base_seconds=base_seconds,
        max_backoff_seconds=max_backoff_seconds,
        jitter_max_seconds=jitter_max_seconds,
        jitter_seconds=jitter_seconds,
    )
    return now.astimezone(UTC) + timedelta(seconds=delay)


_HARD_NAME_TO_CATEGORY: dict[str, FailureCategory] = {
    "citation_integrity": FailureCategory.QUALITY_CITATION_INTEGRITY,
    "tool_use": FailureCategory.QUALITY_TOOL_POLICY,
    "report_structure": FailureCategory.QUALITY_STRUCTURE,
}

_SOFT_NAME_TO_CATEGORY: dict[str, FailureCategory] = {
    "coverage": FailureCategory.QUALITY_COVERAGE,
    "completeness": FailureCategory.QUALITY_COMPLETENESS,
    "lexical_id_groundedness": FailureCategory.QUALITY_GROUNDEDNESS,
    "semantic_groundedness": FailureCategory.QUALITY_GROUNDEDNESS,
}

_TRANSIENT_EXCEPTION_NAMES = frozenset(
    {
        "ReadTimeout",
        "ConnectTimeout",
        "APITimeoutError",
        "RateLimitError",
        "ServiceUnavailableError",
        "InternalServerError",
        "APIConnectionError",
    }
)

_PERMANENT_EXCEPTION_NAMES = frozenset(
    {
        "ValueError",
        "ValidationError",
        "RequestValidationError",
        "EvaluationValidationError",
        "ToolBudgetExhaustedError",
        "AuthenticationError",
        "PermissionError",
    }
)


def categorize_exception(exc: BaseException) -> FailureCategory:
    """Map an exception to a sanitized failure category using isinstance checks.

    Falls back to class-name string matching only for third-party exceptions
    that Atlas cannot import (e.g. provider SDK errors caught as base Exception).
    Atlas-owned model/tool/embedding/evaluation exceptions are always classified
    via isinstance — never by accidental class-name matching.
    """
    from atlas.application.exceptions import ClaimOwnershipError
    from atlas.embeddings.errors import (
        EmbeddingAuthConfigError,
        EmbeddingConflictError,
        EmbeddingInvalidRequestError,
        EmbeddingProviderError,
        EmbeddingRateLimitedError,
        EmbeddingTimeoutError,
    )
    from atlas.evaluation.errors import (
        EvaluationAttemptCapError,
        EvaluationConflictError,
        EvaluationInProgressError,
        EvaluationNotFoundError,
        EvaluationOwnershipLostError,
        EvaluationStaleError,
        EvaluationTerminalError,
        EvaluationValidationError,
    )
    from atlas.evidence.errors import EvidenceOwnershipError
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
        ModelUnknownError,
    )
    from atlas.recovery.errors import PolicyDecisionConflictError
    from atlas.tools.errors import (
        ToolAttemptOwnershipLostError,
        ToolAuthConfigError,
        ToolBudgetExhaustedError,
        ToolContentRejectedError,
        ToolInvalidRequestError,
        ToolInvocationInProgressError,
        ToolPermissionDeniedError,
        ToolRateLimitedError,
        ToolSsrfBlockedError,
        ToolTemporaryError,
        ToolTimeoutError,
        ToolUnknownError,
    )

    # TERMINAL ownership/conflict — always non-retryable
    if isinstance(
        exc,
        (
            ClaimOwnershipError,
            PolicyDecisionConflictError,
            EvaluationOwnershipLostError,
            EvaluationConflictError,
            EvaluationInProgressError,
            EvaluationAttemptCapError,
            EvaluationTerminalError,
            EvaluationNotFoundError,
            EvaluationStaleError,
            EvidenceOwnershipError,
            ModelInvocationInProgressError,
            ModelAttemptOwnershipLostError,
            ToolInvocationInProgressError,
            ToolAttemptOwnershipLostError,
            EmbeddingConflictError,
        ),
    ):
        return FailureCategory.TERMINAL_UNKNOWN

    # Note: concurrent.futures.TimeoutError is an alias of builtin TimeoutError
    # on modern Python. Worker orchestration timeouts are handled terminal by the
    # worker itself (Future.result), not via this registry — do not special-case
    # TimeoutError here or model/tool string "Timeout" fallbacks become terminal.

    # TRANSIENT: timeouts
    if isinstance(exc, (ModelTimeoutError, ToolTimeoutError, EmbeddingTimeoutError)):
        return FailureCategory.TRANSIENT_TIMEOUT

    # TRANSIENT: rate limits
    if isinstance(
        exc,
        (ModelRateLimitedError, ToolRateLimitedError, EmbeddingRateLimitedError),
    ):
        return FailureCategory.TRANSIENT_RATE_LIMIT

    # TRANSIENT: temporary provider errors
    if isinstance(
        exc, (ModelTemporaryError, ToolTemporaryError, EmbeddingProviderError)
    ):
        return FailureCategory.TRANSIENT_PROVIDER

    # PERMANENT: auth/config / permission
    if isinstance(
        exc,
        (
            ModelAuthConfigError,
            ToolAuthConfigError,
            EmbeddingAuthConfigError,
            ToolPermissionDeniedError,
        ),
    ):
        return FailureCategory.PERMANENT_AUTH_CONFIG

    # PERMANENT: budget exhaustion
    if isinstance(exc, ToolBudgetExhaustedError):
        return FailureCategory.PERMANENT_BUDGET_EXHAUSTED

    # PERMANENT: validation / invalid request / structured output / content policy
    if isinstance(
        exc,
        (
            ModelInvalidRequestError,
            ModelInvalidStructuredOutputError,
            ModelRefusalError,
            EvaluationValidationError,
            ToolInvalidRequestError,
            ToolSsrfBlockedError,
            ToolContentRejectedError,
            EmbeddingInvalidRequestError,
        ),
    ):
        return FailureCategory.PERMANENT_VALIDATION

    # Atlas unknown provider outcomes — terminal, not name-fallback retryable
    if isinstance(exc, (ModelUnknownError, ToolUnknownError)):
        return FailureCategory.TERMINAL_UNKNOWN

    # Fallback: string-based classification for third-party exceptions only
    name = type(exc).__name__
    if name in _TRANSIENT_EXCEPTION_NAMES or "Timeout" in name or "RateLimit" in name:
        if "RateLimit" in name:
            return FailureCategory.TRANSIENT_RATE_LIMIT
        if "Timeout" in name:
            return FailureCategory.TRANSIENT_TIMEOUT
        return FailureCategory.TRANSIENT_PROVIDER
    if name in _PERMANENT_EXCEPTION_NAMES or "Budget" in name:
        if "Budget" in name:
            return FailureCategory.PERMANENT_BUDGET_EXHAUSTED
        if "Auth" in name or "Permission" in name:
            return FailureCategory.PERMANENT_AUTH_CONFIG
        return FailureCategory.PERMANENT_VALIDATION
    return FailureCategory.TERMINAL_UNKNOWN


def categorize_evaluation_failure(
    dimensions: list[DimensionResult],
) -> FailureCategory:
    """Derive a primary failure category from failed dimension results."""
    failed = [
        item for item in dimensions if not item.passed and item.method != "skipped"
    ]
    for item in failed:
        if item.name in HARD_DIMENSIONS:
            return _HARD_NAME_TO_CATEGORY[item.name]
    for item in failed:
        mapped = _SOFT_NAME_TO_CATEGORY.get(item.name)
        if mapped is not None:
            return FailureCategory.REPAIRABLE_DRAFT
    return FailureCategory.NEEDS_HUMAN_REVIEW


def decide_for_evaluation(
    *,
    result: EvaluationRunResult,
    dimensions: list[DimensionResult],
    counts: AttemptCounts,
) -> PolicyDecision:
    """Decide recovery action for a succeeded evaluation run."""
    if result.passed:
        return PolicyDecision(
            action="complete",
            failure_category=FailureCategory.NEEDS_HUMAN_REVIEW,
            reason_code="EVALUATION_PASSED",
        )

    if counts.evaluation_attempt_count >= MAX_EVALUATION_ATTEMPTS:
        return PolicyDecision(
            action="terminal",
            failure_category=FailureCategory.TERMINAL_UNKNOWN,
            reason_code="EVALUATION_ATTEMPT_CAP",
        )

    category = categorize_evaluation_failure(dimensions)

    # Citation integrity and tool policy are always immediately terminal
    if category in {
        FailureCategory.QUALITY_CITATION_INTEGRITY,
        FailureCategory.QUALITY_TOOL_POLICY,
    }:
        return PolicyDecision(
            action="terminal",
            failure_category=category,
            reason_code="HARD_QUALITY_FAIL",
        )

    # Structure failures: repair once, then terminal
    if category is FailureCategory.QUALITY_STRUCTURE:
        if counts.repair_count < MAX_REPAIRS:
            return PolicyDecision(
                action="repair",
                failure_category=category,
                reason_code="STRUCTURE_REPAIR",
            )
        return PolicyDecision(
            action="terminal",
            failure_category=category,
            reason_code="STRUCTURE_REPAIR_EXHAUSTED",
        )

    if category is FailureCategory.REPAIRABLE_DRAFT:
        if counts.repair_count < MAX_REPAIRS:
            return PolicyDecision(
                action="repair",
                failure_category=category,
                reason_code="SOFT_QUALITY_REPAIR",
            )
        return PolicyDecision(
            action="await_review",
            failure_category=FailureCategory.NEEDS_HUMAN_REVIEW,
            reason_code="REPAIR_EXHAUSTED",
        )

    return PolicyDecision(
        action="await_review",
        failure_category=FailureCategory.NEEDS_HUMAN_REVIEW,
        reason_code="AMBIGUOUS_QUALITY",
    )


def decide_for_exception(
    *,
    exc: BaseException,
    counts: AttemptCounts,
) -> PolicyDecision:
    """Decide recovery action for a pre-complete processing exception."""
    category = categorize_exception(exc)

    # Ownership/conflict is always terminal — never retry
    if category is FailureCategory.TERMINAL_UNKNOWN:
        from atlas.application.exceptions import ClaimOwnershipError
        from atlas.evaluation.errors import (
            EvaluationAttemptCapError,
            EvaluationConflictError,
            EvaluationInProgressError,
            EvaluationOwnershipLostError,
            EvaluationTerminalError,
        )
        from atlas.evidence.errors import EvidenceOwnershipError

        if isinstance(
            exc,
            (
                ClaimOwnershipError,
                EvaluationOwnershipLostError,
                EvaluationConflictError,
                EvaluationInProgressError,
                EvaluationAttemptCapError,
                EvaluationTerminalError,
                EvidenceOwnershipError,
            ),
        ):
            return PolicyDecision(
                action="terminal",
                failure_category=category,
                reason_code="OWNERSHIP_CONFLICT",
            )
        from atlas.embeddings.errors import EmbeddingConflictError
        from atlas.evaluation.errors import (
            EvaluationNotFoundError,
            EvaluationStaleError,
        )
        from atlas.models.errors import (
            ModelAttemptOwnershipLostError,
            ModelInvocationInProgressError,
            ModelUnknownError,
        )
        from atlas.recovery.errors import PolicyDecisionConflictError
        from atlas.tools.errors import (
            ToolAttemptOwnershipLostError,
            ToolInvocationInProgressError,
            ToolUnknownError,
        )

        if isinstance(
            exc,
            (
                PolicyDecisionConflictError,
                EvaluationNotFoundError,
                EvaluationStaleError,
                ModelInvocationInProgressError,
                ModelAttemptOwnershipLostError,
                ToolInvocationInProgressError,
                ToolAttemptOwnershipLostError,
                EmbeddingConflictError,
            ),
        ):
            return PolicyDecision(
                action="terminal",
                failure_category=category,
                reason_code="OWNERSHIP_CONFLICT",
            )
        if isinstance(exc, (ModelUnknownError, ToolUnknownError)):
            return PolicyDecision(
                action="terminal",
                failure_category=category,
                reason_code="UNCLASSIFIED_TERMINAL",
            )
        return PolicyDecision(
            action="terminal",
            failure_category=category,
            reason_code="UNCLASSIFIED_TERMINAL",
        )

    if (
        category.name.startswith("TRANSIENT_")
        and counts.job_retry_count < MAX_JOB_RETRIES
    ):
        return PolicyDecision(
            action="retry",
            failure_category=category,
            reason_code="TRANSIENT_RETRY",
        )
    if category.name.startswith("TRANSIENT_"):
        return PolicyDecision(
            action="terminal",
            failure_category=category,
            reason_code="RETRY_CAP",
        )
    if category.name.startswith("PERMANENT_"):
        return PolicyDecision(
            action="terminal",
            failure_category=category,
            reason_code="PERMANENT_FAIL",
        )
    return PolicyDecision(
        action="terminal",
        failure_category=category,
        reason_code="UNCLASSIFIED_TERMINAL",
    )
