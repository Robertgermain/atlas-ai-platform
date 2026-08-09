"""Atlas embeddings package (Milestone 10B)."""

from atlas.embeddings.bounds import (
    EMBEDDING_DIMENSIONS_V1,
    EMBEDDING_PROFILE_V1,
)
from atlas.embeddings.composition import build_text_embedder
from atlas.embeddings.contracts import (
    EmbeddingProviderId,
    EmbedTextsRequest,
    EmbedTextsResult,
)
from atlas.embeddings.errors import (
    EmbeddingAuthConfigError,
    EmbeddingConflictError,
    EmbeddingError,
    EmbeddingInvalidRequestError,
    EmbeddingProviderError,
    EmbeddingRateLimitedError,
    EmbeddingTimeoutError,
)
from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.embeddings.ports import TextEmbedder

__all__ = [
    "EMBEDDING_DIMENSIONS_V1",
    "EMBEDDING_PROFILE_V1",
    "DeterministicFakeEmbedder",
    "EmbedTextsRequest",
    "EmbedTextsResult",
    "EmbeddingAuthConfigError",
    "EmbeddingConflictError",
    "EmbeddingError",
    "EmbeddingInvalidRequestError",
    "EmbeddingProviderError",
    "EmbeddingProviderId",
    "EmbeddingRateLimitedError",
    "EmbeddingTimeoutError",
    "TextEmbedder",
    "build_text_embedder",
]
