"""Compose embedding providers from settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.embeddings.errors import EmbeddingAuthConfigError
from atlas.embeddings.fakes import DeterministicFakeEmbedder
from atlas.embeddings.ports import TextEmbedder

if TYPE_CHECKING:
    from atlas.config.settings import Settings


def build_text_embedder(settings: Settings) -> TextEmbedder:
    """Return the configured embedder. Default is deterministic fake."""
    if settings.embedding_provider == "fake":
        return DeterministicFakeEmbedder()
    if settings.embedding_provider == "openai":
        key = settings.openai_api_key
        if key is None or not key.get_secret_value().strip():
            raise EmbeddingAuthConfigError("OpenAI API key is required for embeddings")
        from atlas.embeddings.langchain_openai import LangChainOpenAIEmbedder

        return LangChainOpenAIEmbedder(
            api_key=key.get_secret_value(),
            timeout_seconds=settings.embedding_call_timeout_seconds,
        )
    raise EmbeddingAuthConfigError("unsupported embedding provider")
