"""Atlas-owned tool errors (no provider SDK types)."""

from __future__ import annotations

from atlas.tools.contracts import ToolRetryClass


class ToolError(Exception):
    """Base class for controlled tool-integration failures."""

    retry_class: ToolRetryClass = ToolRetryClass.UNKNOWN

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.__class__.__name__)


class ToolTimeoutError(ToolError):
    retry_class = ToolRetryClass.TIMEOUT


class ToolRateLimitedError(ToolError):
    retry_class = ToolRetryClass.RATE_LIMITED


class ToolTemporaryError(ToolError):
    retry_class = ToolRetryClass.TEMPORARY


class ToolAuthConfigError(ToolError):
    retry_class = ToolRetryClass.AUTH_CONFIG


class ToolInvalidRequestError(ToolError):
    retry_class = ToolRetryClass.INVALID_REQUEST


class ToolPermissionDeniedError(ToolError):
    retry_class = ToolRetryClass.PERMISSION_DENIED


class ToolSsrfBlockedError(ToolError):
    retry_class = ToolRetryClass.SSRF_BLOCKED


class ToolContentRejectedError(ToolError):
    retry_class = ToolRetryClass.CONTENT_REJECTED


class ToolBudgetExhaustedError(ToolError):
    """Research-node tool budget exhausted; must not look like success."""

    retry_class = ToolRetryClass.BUDGET_EXHAUSTED


class ToolUnknownError(ToolError):
    retry_class = ToolRetryClass.UNKNOWN


class ToolInvocationInProgressError(ToolError):
    retry_class = ToolRetryClass.TEMPORARY


class ToolAttemptOwnershipLostError(ToolError):
    retry_class = ToolRetryClass.TEMPORARY


def sanitize_tool_error(exc: Exception) -> str:
    """Persist a class-only error string without raw provider or content text."""
    return f"{type(exc).__name__}: tool invocation failed"
