"""Domain-specific errors for Atlas."""


class DomainError(Exception):
    """Base class for domain rule violations."""


class InvalidResearchJobError(DomainError):
    """Raised when research-job field or timestamp invariants are violated."""


class InvalidTransitionError(DomainError):
    """Raised when a research-job lifecycle transition is not allowed."""

    def __init__(
        self,
        *,
        current: str,
        attempted: str,
    ) -> None:
        self.current = current
        self.attempted = attempted
        super().__init__(
            f"Cannot transition research job from {current} to {attempted}."
        )
