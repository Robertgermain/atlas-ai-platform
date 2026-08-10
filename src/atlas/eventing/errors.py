"""Errors for domain-event contract validation and serialization."""

from __future__ import annotations


class DomainEventError(ValueError):
    """Base class for typed domain-event contract failures."""


class DomainEventValidationError(DomainEventError):
    """Raised when an envelope or payload fails contract validation."""


class DomainEventSerializationError(DomainEventError):
    """Raised when canonical serialization cannot be produced safely."""
