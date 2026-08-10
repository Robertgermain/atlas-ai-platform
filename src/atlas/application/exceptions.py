"""Application-layer exceptions."""


class ApplicationError(Exception):
    """Base class for application use-case failures."""


class ResearchJobLookupError(ApplicationError):
    """Raised when a research job cannot be found by id."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__("Research job not found.")


class IdempotencyConflictError(ApplicationError):
    """Raised when an idempotency key is reused with a different request payload."""

    def __init__(self) -> None:
        super().__init__(
            "Idempotency key was already used with a different request payload."
        )


class ClaimOwnershipError(ApplicationError):
    """Raised when a claim-fenced mutation fails ownership verification.

    This indicates the worker's claim token no longer owns the job (another
    worker reclaimed or lease expired). The processor must not return success,
    pause, or retry outcomes after this error — the worker treats it as a
    safe no-op finalization.

    Never includes tokens, credentials, or sensitive claim metadata.
    """

    def __init__(self, operation: str = "") -> None:
        detail = f" during {operation}" if operation else ""
        super().__init__(f"Claim ownership lost{detail}.")
