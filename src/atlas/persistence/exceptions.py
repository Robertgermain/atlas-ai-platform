"""Persistence-layer errors."""


class PersistenceError(Exception):
    """Base class for persistence failures."""


class ResearchJobAlreadyExistsError(PersistenceError):
    """Raised when inserting a research job whose id already exists."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Research job already exists: {job_id}")


class IdempotencyKeyConflictError(PersistenceError):
    """Raised when inserting a research job whose idempotency key already exists."""

    def __init__(self) -> None:
        super().__init__("Idempotency key already exists.")


class ResearchJobNotFoundError(PersistenceError):
    """Raised when updating a research job that does not exist."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Research job not found: {job_id}")


class UnsafeTestDatabaseError(PersistenceError):
    """Raised when a destructive test operation targets a non-test database."""
