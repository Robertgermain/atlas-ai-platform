"""Opt-in live OpenAI embedding verification (skipped by default)."""

from __future__ import annotations

import os

import pytest

from atlas.config.settings import Settings
from atlas.embeddings.bounds import EMBEDDING_DIMENSIONS_V1
from atlas.embeddings.composition import build_text_embedder
from atlas.embeddings.contracts import EmbedTextsRequest


@pytest.mark.skipif(
    os.environ.get("ATLAS_ENABLE_LIVE_EMBEDDING_TESTS") != "1",
    reason="Live embedding tests require ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1",
)
def test_live_openai_embedding_dimensions() -> None:
    settings = Settings(embedding_provider="openai")
    embedder = build_text_embedder(settings)
    result = embedder.embed_texts(
        EmbedTextsRequest(texts=["Atlas retrieval provenance check"])
    )
    assert result.dimensions == EMBEDDING_DIMENSIONS_V1
    assert len(result.embeddings[0]) == EMBEDDING_DIMENSIONS_V1
