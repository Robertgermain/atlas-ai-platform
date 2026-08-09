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
