"""Unit tests for deterministic fake embeddings and metric helpers."""

from __future__ import annotations

from atlas.embeddings.bounds import EMBEDDING_DIMENSIONS_V1
from atlas.embeddings.contracts import EmbedTextsRequest
from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.evidence.metrics import mrr_at_k, recall_at_k


def test_fake_embedder_is_deterministic_and_1536d() -> None:
    embedder = DeterministicFakeEmbedder()
    request = EmbedTextsRequest(texts=["quantum cryptography protocols"])
    first = embedder.embed_texts(request)
    second = embedder.embed_texts(request)
    assert first.dimensions == EMBEDDING_DIMENSIONS_V1
    assert len(first.embeddings[0]) == EMBEDDING_DIMENSIONS_V1
    assert first.embeddings == second.embeddings
    assert all(x == x and abs(x) != float("inf") for x in first.embeddings[0])


def test_metric_helpers() -> None:
    relevant = ["a", "b"]
    ranked = ["x", "a", "b"]
    assert recall_at_k(relevant, ranked, k=3) == 1.0
    assert mrr_at_k(relevant, ranked, k=3) == 0.5
