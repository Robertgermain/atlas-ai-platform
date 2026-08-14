"""Advisory-analyst errors (class-only; never include raw stored strings)."""

from __future__ import annotations


class AdvisoryError(Exception):
    """Base class for advisory CLI failures."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.__class__.__name__)


class AdvisoryConfigurationError(AdvisoryError):
    """Raised when live advisory composition pins disagree."""


class AdvisoryInputRejectedError(AdvisoryError):
    """Raised when CLI arguments fail closed."""


class AdvisoryJobNotFoundError(AdvisoryError):
    """Raised when the selected research job does not exist."""


class AdvisorySnapshotRejectedError(AdvisoryError):
    """Raised when assembled facts fail schema or byte bounds."""


class AdvisoryAnalysisTimeoutError(AdvisoryError):
    """Raised when the whole-analysis monotonic deadline is exhausted."""


class AdvisoryOutputRejectedError(AdvisoryError):
    """Raised when parsed model output fails post-parse safety validation."""
