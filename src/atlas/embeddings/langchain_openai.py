"""LangChain-first OpenAI embedding adapter."""

from __future__ import annotations

import time

from langchain_openai import OpenAIEmbeddings
from pydantic import SecretStr

from atlas.embeddings.bounds import (
    EMBEDDING_DIMENSIONS_V1,
    EMBEDDING_PROFILE_V1,
    OPENAI_EMBEDDING_MODEL_V1,
)
from atlas.embeddings.contracts import (
    EmbeddingProviderId,
    EmbedTextsRequest,
    EmbedTextsResult,
)
from atlas.embeddings.errors import (
    EmbeddingAuthConfigError,
    EmbeddingError,
    EmbeddingInvalidRequestError,
    EmbeddingProviderError,
    EmbeddingRateLimitedError,
    EmbeddingTimeoutError,
)


class LangChainOpenAIEmbedder:
    """OpenAI embeddings via LangChain ``OpenAIEmbeddings`` only."""

    def __init__(
        self,
        *,
        api_key: str,
        timeout_seconds: float,
        model: str = OPENAI_EMBEDDING_MODEL_V1,
    ) -> None:
        if not api_key.strip():
            raise EmbeddingAuthConfigError("OpenAI API key is required")
        self._model = model
        self._embeddings = OpenAIEmbeddings(
            model=model,
            api_key=SecretStr(api_key),
            timeout=timeout_seconds,
            max_retries=0,
            dimensions=EMBEDDING_DIMENSIONS_V1,
        )

    def embed_texts(self, request: EmbedTextsRequest) -> EmbedTextsResult:
        if request.embedding_profile != EMBEDDING_PROFILE_V1:
            raise EmbeddingInvalidRequestError("unsupported embedding profile")
        started = time.perf_counter()
        try:
            vectors = self._embeddings.embed_documents(list(request.texts))
        except Exception as exc:
            raise _translate(exc) from None
        _ = int((time.perf_counter() - started) * 1000)
        return EmbedTextsResult(
            embeddings=[list(map(float, row)) for row in vectors],
            embedding_profile=EMBEDDING_PROFILE_V1,
            dimensions=EMBEDDING_DIMENSIONS_V1,
            model=self._model,
            provider=EmbeddingProviderId.OPENAI,
        )


def _translate(exc: BaseException) -> EmbeddingError:
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    if "timeout" in name or "timeout" in message:
        return EmbeddingTimeoutError("embedding provider timed out")
    if "ratelimit" in name or "rate_limit" in name or "rate limit" in message:
        return EmbeddingRateLimitedError("embedding provider rate limited")
    if "auth" in name or "authentication" in message or "api key" in message:
        return EmbeddingAuthConfigError("embedding provider authentication failed")
    if "badrequest" in name or "invalid" in name:
        return EmbeddingInvalidRequestError("embedding provider rejected request")
    return EmbeddingProviderError("embedding provider failed")
