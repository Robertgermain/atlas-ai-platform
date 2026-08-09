"""Typed contracts for embedding requests and results."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

from atlas.embeddings.bounds import EMBEDDING_DIMENSIONS_V1, EMBEDDING_PROFILE_V1


class EmbeddingProviderId(StrEnum):
    FAKE = "fake"
    OPENAI = "openai"


class EmbedTextsRequest(BaseModel):
    texts: Annotated[list[str], Field(min_length=1, max_length=64)]
    embedding_profile: str = EMBEDDING_PROFILE_V1

    @field_validator("texts")
    @classmethod
    def validate_texts(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("texts must be non-empty after strip")
        return cleaned


class EmbedTextsResult(BaseModel):
    embeddings: list[list[float]]
    embedding_profile: str
    dimensions: int
    model: str
    provider: EmbeddingProviderId

    @field_validator("embeddings")
    @classmethod
    def validate_embeddings(cls, value: list[list[float]]) -> list[list[float]]:
        if not value:
            raise ValueError("embeddings must be non-empty")
        width = len(value[0])
        if width != EMBEDDING_DIMENSIONS_V1:
            raise ValueError(
                f"embeddings must have {EMBEDDING_DIMENSIONS_V1} dimensions"
            )
        for row in value:
            if len(row) != width:
                raise ValueError("embedding rows must share dimensions")
            if any(not _finite(x) for x in row):
                raise ValueError("embedding values must be finite")
        return value


def _finite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))
