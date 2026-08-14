"""Sanitized evaluation-layer errors (no secrets, providers, or raw bodies)."""

from __future__ import annotations


class EvaluationError(Exception):
    """Base class for controlled evaluation failures."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.__class__.__name__)


class EvaluationValidationError(EvaluationError):
    """Raised when evaluation input fails validation."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation input is invalid.")


class EvaluationNotFoundError(EvaluationError):
    """Raised when an evaluation run cannot be found."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation run not found.")


class EvaluationConflictError(EvaluationError):
    """Raised when a succeeded run conflicts with a different fingerprint."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation fingerprint conflict.")


class EvaluationInProgressError(EvaluationError):
    """Raised when another non-stale evaluation owns the attempt."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation is already in progress.")


class EvaluationOwnershipLostError(EvaluationError):
    """Raised when conditional finalize loses ownership fencing."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation ownership was lost.")


class EvaluationTerminalError(EvaluationError):
    """Raised when evaluation cannot proceed due to a terminal condition."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation reached a terminal condition.")


class EvaluationAttemptCapError(EvaluationTerminalError):
    """Raised when the job-global evaluation attempt cap would be exceeded."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation attempt cap reached.")


class EvaluationStaleError(EvaluationError):
    """Raised when an evaluation attempt is stale and cannot be used."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Evaluation attempt is stale.")


class SemanticGraderConfigurationError(EvaluationError):
    """Worker refused evaluation composition due to invalid settings.

    Raised at worker startup, not from global Settings, so the API can
    construct Settings with ``semantic_grader_mode=live`` without failing.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Live semantic grader configuration is invalid.")


class EvaluationProfileMismatchError(EvaluationError):
    """Worker profile does not match the job's durable bound profile.

    Raised before evaluation or workflow mutation. The durable job profile
    is never overwritten.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(
            message or "Evaluation profile does not match the bound job profile."
        )


def sanitize_evaluation_error(exc: Exception) -> str:
    """Persist a class-only error string without raw exception text."""
    return f"{type(exc).__name__}: evaluation failed"
