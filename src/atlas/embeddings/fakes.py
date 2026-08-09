"""Deterministic fake embeddings for pipeline tests (not semantic quality).

Fake embeddings validate storage, retrieval plumbing, filters, and fixture
geometry. They do **not** measure real-world semantic model quality.

Vectors are derived from SHA-256 token feature hashing into a fixed 1536-d
space and L2-normalized. Python's randomized ``hash()`` is never used.
"""

from __future__ import annotations

import hashlib
import math
import re

from atlas.embeddings.bounds import (
    EMBEDDING_DIMENSIONS_V1,
    EMBEDDING_PROFILE_V1,
)
from atlas.embeddings.contracts import (
    EmbeddingProviderId,
    EmbedTextsRequest,
    EmbedTextsResult,
)
from atlas.embeddings.errors import EmbeddingInvalidRequestError

_TOKEN_RE = re.compile(r"[a-z0-9_]+", re.IGNORECASE)


class DeterministicFakeEmbedder:
    """Stable lexical-geometry embedder for CI and offline evaluation."""

    def embed_texts(self, request: EmbedTextsRequest) -> EmbedTextsResult:
        if request.embedding_profile != EMBEDDING_PROFILE_V1:
            raise EmbeddingInvalidRequestError("unsupported embedding profile")
        vectors = [_embed_one(text) for text in request.texts]
        return EmbedTextsResult(
            embeddings=vectors,
            embedding_profile=EMBEDDING_PROFILE_V1,
            dimensions=EMBEDDING_DIMENSIONS_V1,
            model="deterministic-fake",
            provider=EmbeddingProviderId.FAKE,
        )


def _embed_one(text: str) -> list[float]:
    dims = EMBEDDING_DIMENSIONS_V1
    values = [0.0] * dims
    tokens = _TOKEN_RE.findall(text.lower())
    if not tokens:
        tokens = ["empty"]
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        # Spread each token across several dimensions using digest bytes.
        for offset in range(0, 32, 4):
            bucket = int.from_bytes(digest[offset : offset + 4], "big") % dims
            sign = 1.0 if digest[offset] % 2 == 0 else -1.0
            weight = 1.0 + (digest[offset + 1] / 255.0)
            values[bucket] += sign * weight
        # Secondary projection for bigram-ish stability.
        secondary = hashlib.sha256(digest).digest()
        bucket2 = int.from_bytes(secondary[:4], "big") % dims
        values[bucket2] += 0.35
    norm = math.sqrt(sum(v * v for v in values))
    if norm == 0.0:
        values[0] = 1.0
        return values
    return [v / norm for v in values]
