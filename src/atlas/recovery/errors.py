"""Recovery-layer typed errors."""

from __future__ import annotations


class PolicyDecisionConflictError(Exception):
    """Raised when a policy fingerprint exists with inconsistent stored fields.

    Never includes raw claim tokens or secrets.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Policy decision fingerprint conflict.")
