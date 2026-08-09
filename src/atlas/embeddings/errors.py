"""Typed errors for embedding providers and persistence."""

from __future__ import annotations


class EmbeddingError(Exception):
    """Base class for embedding-layer failures."""


class EmbeddingAuthConfigError(EmbeddingError):
    """Raised when embedding provider configuration or credentials are invalid."""


class EmbeddingTimeoutError(EmbeddingError):
    """Raised when an embedding provider call times out."""


class EmbeddingRateLimitedError(EmbeddingError):
    """Raised when an embedding provider rate-limits the caller."""


class EmbeddingInvalidRequestError(EmbeddingError):
    """Raised when embedding input is invalid."""


class EmbeddingProviderError(EmbeddingError):
    """Raised for other controlled provider failures."""


class EmbeddingConflictError(EmbeddingError):
    """Raised when an existing embedding conflicts with a write attempt."""
