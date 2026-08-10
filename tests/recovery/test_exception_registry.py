"""Tests for typed exception registry in atlas.recovery.policy."""

from __future__ import annotations

import pytest

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
from atlas.recovery.policy import (
    AttemptCounts,
    FailureCategory,
    categorize_exception,
    decide_for_exception,
)
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

ZERO_COUNTS = AttemptCounts(
    repair_count=0, job_retry_count=0, evaluation_attempt_count=0
)


class TestCategorizeExceptionIsinstance:
    """Verify isinstance classification for concrete Atlas exception classes."""

    @pytest.mark.parametrize(
        "exc,expected",
        [
            (ModelTimeoutError(), FailureCategory.TRANSIENT_TIMEOUT),
            (ToolTimeoutError(), FailureCategory.TRANSIENT_TIMEOUT),
            (EmbeddingTimeoutError(), FailureCategory.TRANSIENT_TIMEOUT),
            (ModelRateLimitedError(), FailureCategory.TRANSIENT_RATE_LIMIT),
            (ToolRateLimitedError(), FailureCategory.TRANSIENT_RATE_LIMIT),
            (EmbeddingRateLimitedError(), FailureCategory.TRANSIENT_RATE_LIMIT),
            (ModelTemporaryError(), FailureCategory.TRANSIENT_PROVIDER),
            (ToolTemporaryError(), FailureCategory.TRANSIENT_PROVIDER),
            (EmbeddingProviderError(), FailureCategory.TRANSIENT_PROVIDER),
            (ModelAuthConfigError(), FailureCategory.PERMANENT_AUTH_CONFIG),
            (ToolAuthConfigError(), FailureCategory.PERMANENT_AUTH_CONFIG),
            (EmbeddingAuthConfigError(), FailureCategory.PERMANENT_AUTH_CONFIG),
            (ToolPermissionDeniedError(), FailureCategory.PERMANENT_AUTH_CONFIG),
            (ToolBudgetExhaustedError(), FailureCategory.PERMANENT_BUDGET_EXHAUSTED),
            (ModelInvalidRequestError(), FailureCategory.PERMANENT_VALIDATION),
            (
                ModelInvalidStructuredOutputError(),
                FailureCategory.PERMANENT_VALIDATION,
            ),
            (ModelRefusalError(), FailureCategory.PERMANENT_VALIDATION),
            (EvaluationValidationError(), FailureCategory.PERMANENT_VALIDATION),
            (ToolInvalidRequestError(), FailureCategory.PERMANENT_VALIDATION),
            (ToolSsrfBlockedError(), FailureCategory.PERMANENT_VALIDATION),
            (ToolContentRejectedError(), FailureCategory.PERMANENT_VALIDATION),
            (EmbeddingInvalidRequestError(), FailureCategory.PERMANENT_VALIDATION),
            (ModelUnknownError(), FailureCategory.TERMINAL_UNKNOWN),
            (ToolUnknownError(), FailureCategory.TERMINAL_UNKNOWN),
        ],
    )
    def test_transient_permanent_classification(
        self,
        exc: BaseException,
        expected: FailureCategory,
    ) -> None:
        assert categorize_exception(exc) is expected

    @pytest.mark.parametrize(
        "exc",
        [
            ClaimOwnershipError(),
            PolicyDecisionConflictError(),
            EvaluationOwnershipLostError(),
            EvaluationConflictError(),
            EvaluationInProgressError(),
            EvaluationAttemptCapError(),
            EvaluationTerminalError(),
            EvaluationNotFoundError(),
            EvaluationStaleError(),
            EvidenceOwnershipError("test"),
            ModelInvocationInProgressError(),
            ModelAttemptOwnershipLostError(),
            ToolInvocationInProgressError(),
            ToolAttemptOwnershipLostError(),
            EmbeddingConflictError(),
        ],
    )
    def test_ownership_conflict_terminal(self, exc: BaseException) -> None:
        assert categorize_exception(exc) is FailureCategory.TERMINAL_UNKNOWN

    def test_unknown_exception_terminal(self) -> None:
        result = categorize_exception(RuntimeError("oops"))
        assert result is FailureCategory.TERMINAL_UNKNOWN


class TestDecideForExceptionOwnership:
    """Ownership/conflict exceptions always produce terminal decisions."""

    @pytest.mark.parametrize(
        "exc",
        [
            ClaimOwnershipError("set_active"),
            PolicyDecisionConflictError(),
            EvaluationOwnershipLostError(),
            EvaluationConflictError(),
            EvaluationAttemptCapError(),
            EvaluationNotFoundError(),
            EvaluationStaleError(),
            EvidenceOwnershipError("claim lost"),
            ModelInvocationInProgressError(),
            ModelAttemptOwnershipLostError(),
            ToolInvocationInProgressError(),
            ToolAttemptOwnershipLostError(),
            EmbeddingConflictError(),
        ],
    )
    def test_ownership_never_retried(self, exc: BaseException) -> None:
        decision = decide_for_exception(exc=exc, counts=ZERO_COUNTS)
        assert decision.action == "terminal"
        assert decision.reason_code == "OWNERSHIP_CONFLICT"

    @pytest.mark.parametrize(
        "exc",
        [
            ModelInvalidStructuredOutputError(),
            ModelRefusalError(),
            ToolInvalidRequestError(),
            ToolSsrfBlockedError(),
            ToolContentRejectedError(),
            EmbeddingInvalidRequestError(),
        ],
    )
    def test_permanent_validation_never_retried(self, exc: BaseException) -> None:
        decision = decide_for_exception(exc=exc, counts=ZERO_COUNTS)
        assert decision.action == "terminal"
        assert decision.reason_code == "PERMANENT_FAIL"

    @pytest.mark.parametrize(
        "exc",
        [ModelUnknownError(), ToolUnknownError()],
    )
    def test_unknown_atlas_errors_terminal(self, exc: BaseException) -> None:
        decision = decide_for_exception(exc=exc, counts=ZERO_COUNTS)
        assert decision.action == "terminal"
        assert decision.reason_code == "UNCLASSIFIED_TERMINAL"


class TestWorkerOrchestrationTimeout:
    """Worker Future.result timeout is terminal outside the policy registry.

    On modern Python, concurrent.futures.TimeoutError is an alias of builtin
    TimeoutError, so isinstance cannot distinguish orchestration timeouts from
    generic timeouts. The worker finalizes terminal directly.
    """

    def test_worker_handles_futures_timeout_terminal(self) -> None:
        from concurrent.futures import TimeoutError as FuturesTimeoutError

        from atlas.application.worker import PROCESSING_TIMEOUT_REASON

        assert FuturesTimeoutError is TimeoutError
        assert PROCESSING_TIMEOUT_REASON == "Processing timed out."
        # Builtin/generic TimeoutError remains transient via name fallback.
        assert categorize_exception(TimeoutError()) is FailureCategory.TRANSIENT_TIMEOUT


class TestStringFallbackClassification:
    """Verify fallback string matching for third-party exceptions only."""

    def test_timeout_in_name(self) -> None:
        class SomeCustomTimeout(Exception):
            pass

        assert (
            categorize_exception(SomeCustomTimeout())
            is FailureCategory.TRANSIENT_TIMEOUT
        )

    def test_ratelimit_in_name(self) -> None:
        class ProviderRateLimitHit(Exception):
            pass

        assert (
            categorize_exception(ProviderRateLimitHit())
            is FailureCategory.TRANSIENT_RATE_LIMIT
        )
