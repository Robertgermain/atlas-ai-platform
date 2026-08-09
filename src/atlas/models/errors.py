"""Atlas-owned model/provider errors (no provider SDK types)."""

from __future__ import annotations

from atlas.models.contracts import RetryClass


class ModelError(Exception):
    """Base class for controlled model-integration failures."""

    retry_class: RetryClass = RetryClass.UNKNOWN

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.__class__.__name__)


class ModelTimeoutError(ModelError):
    retry_class = RetryClass.TIMEOUT


class ModelRateLimitedError(ModelError):
    retry_class = RetryClass.RATE_LIMITED


class ModelTemporaryError(ModelError):
    retry_class = RetryClass.TEMPORARY


class ModelAuthConfigError(ModelError):
    retry_class = RetryClass.AUTH_CONFIG


class ModelInvalidRequestError(ModelError):
    retry_class = RetryClass.INVALID_REQUEST


class ModelInvalidStructuredOutputError(ModelError):
    retry_class = RetryClass.INVALID_STRUCTURED_OUTPUT


class ModelRefusalError(ModelError):
    retry_class = RetryClass.REFUSAL


class ModelUnknownError(ModelError):
    retry_class = RetryClass.UNKNOWN


class ModelInvocationInProgressError(ModelError):
    """Another in-flight attempt owns this invocation key."""

    retry_class = RetryClass.TEMPORARY


class ModelAttemptOwnershipLostError(ModelError):
    """This physical attempt lost ledger ownership before finalization.

    Raised when a STARTED→terminal conditional update fails or the attempt is
    no longer the active attempt for the logical invocation (for example after
    a stale reclaim). A late provider result must not overwrite newer ledger
    state.
    """

    retry_class = RetryClass.TEMPORARY


def sanitize_model_error(exc: Exception) -> str:
    """Persist a class-only error string without raw provider messages."""
    return f"{type(exc).__name__}: model invocation failed"
