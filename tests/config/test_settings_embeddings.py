"""Configuration contract tests for embedding settings."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from atlas.config.settings import Settings


def test_default_embedding_profile_is_embeddings_v1() -> None:
    settings = Settings()
    assert settings.embedding_profile == "embeddings.v1"


def test_explicit_embeddings_v1_profile_accepted() -> None:
    settings = Settings(embedding_profile="embeddings.v1")
    assert settings.embedding_profile == "embeddings.v1"


def test_unsupported_embedding_profile_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"embedding_profile": "embeddings.v2"})
