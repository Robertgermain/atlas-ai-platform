"""Embedding profile constants for Milestone 10B."""

from __future__ import annotations

EMBEDDING_PROFILE_V1 = "embeddings.v1"
EMBEDDING_DIMENSIONS_V1 = 1536
OPENAI_EMBEDDING_MODEL_V1 = "text-embedding-3-small"
MAX_EMBED_BATCH_SIZE = 32
MAX_EMBED_ITEMS_PER_CALL = 64

# HNSW over-fetch when metadata filters may discard ANN candidates.
# candidate_limit = min(max(k * multiplier, k), MAX_HNSW_CANDIDATES)
HNSW_CANDIDATE_MULTIPLIER = 8
MAX_HNSW_CANDIDATES = 64
