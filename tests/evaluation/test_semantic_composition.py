"""Worker-only semantic grader composition (Slice 15C1)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.evaluation.composition import (
    build_semantic_grader,
    require_semantic_grader_mode,
)
from atlas.evaluation.errors import SemanticGraderConfigurationError
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.llm_grader import LangChainSemanticGroundednessGrader


def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ATLAS_SEMANTIC_GRADER_MODE", raising=False)
    monkeypatch.delenv("ATLAS_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)


def test_default_mode_is_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings()
    assert settings.semantic_grader_mode == "skipped"
    require_semantic_grader_mode(settings)
    assert (
        build_semantic_grader(
            settings,
            session_factory=object(),  # type: ignore[arg-type]
            workflow_execution_id="exec-1",
        )
        is None
    )


def test_fake_mode_is_offline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(semantic_grader_mode="fake")
    grader = build_semantic_grader(
        settings,
        session_factory=object(),  # type: ignore[arg-type]
        workflow_execution_id="exec-1",
    )
    assert isinstance(grader, FakeSemanticGroundednessGrader)


def test_live_with_fake_provider_fails_worker_composition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(semantic_grader_mode="live", model_provider="fake")
    with pytest.raises(SemanticGraderConfigurationError) as exc_info:
        require_semantic_grader_mode(settings)
    message = str(exc_info.value)
    assert "non-fake" in message
    assert "openai" not in message.lower()
    assert "sk-" not in message


def test_api_settings_can_construct_live_with_fake_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(
        semantic_grader_mode="live",
        model_provider="fake",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
    )
    assert settings.semantic_grader_mode == "live"
    assert settings.model_provider == "fake"


def test_live_mode_is_not_inferred_from_openai_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(
        model_provider="openai",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
        semantic_grader_mode="skipped",
    )
    assert (
        build_semantic_grader(
            settings,
            session_factory=object(),  # type: ignore[arg-type]
            workflow_execution_id="exec-1",
        )
        is None
    )


def test_live_mode_composes_langchain_grader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(
        semantic_grader_mode="live",
        model_provider="openai",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
        langsmith_api_key=SecretStr("lsv2_test_not_a_real_key"),
    )
    require_semantic_grader_mode(settings)

    class _FakeChat:
        pass

    monkeypatch.setattr(
        "atlas.evaluation.composition.build_chat_model",
        lambda _settings: _FakeChat(),
    )

    class _FakeService:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    monkeypatch.setattr(
        "atlas.evaluation.composition.ModelInvocationService",
        _FakeService,
    )
    grader = build_semantic_grader(
        settings,
        session_factory=object(),  # type: ignore[arg-type]
        workflow_execution_id="exec-live",
    )
    assert isinstance(grader, LangChainSemanticGroundednessGrader)
