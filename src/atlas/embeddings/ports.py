"""Embedding Protocol/port."""

from __future__ import annotations

from typing import Protocol

from atlas.embeddings.contracts import EmbedTextsRequest, EmbedTextsResult


class TextEmbedder(Protocol):
    """Produce versioned embeddings for one or more texts."""

    def embed_texts(self, request: EmbedTextsRequest) -> EmbedTextsResult: ...
