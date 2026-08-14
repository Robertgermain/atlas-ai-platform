"""Worker-only evaluation-profile and semantic-grader composition."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from atlas.config.settings import Settings
from atlas.evaluation.composition import (
    build_semantic_grader,
    require_evaluation_composition,
    require_semantic_grader_mode,
    resolved_evaluation_profile,
)
from atlas.evaluation.contracts import (
    EVALUATION_PROFILE_CANDIDATE,
    EVALUATION_PROFILE_CANDIDATE_FAKE,
    EVALUATION_PROFILE_V1,
    PROFILE_FOR_SEMANTIC_MODE,
)
from atlas.evaluation.errors import SemanticGraderConfigurationError
from atlas.evaluation.graders import FakeSemanticGroundednessGrader
from atlas.evaluation.llm_grader import LangChainSemanticGroundednessGrader
from atlas.evaluation.semantic_contracts import (
    FROZEN_LIVE_SEMANTIC_MODEL,
    FROZEN_LIVE_SEMANTIC_PROVIDER,
    FROZEN_LIVE_SEMANTIC_TEMPERATURE,
)

_DERIVED: tuple[tuple[str, str], ...] = (
    ("skipped", EVALUATION_PROFILE_CANDIDATE),
    ("fake", EVALUATION_PROFILE_CANDIDATE_FAKE),
    ("live", EVALUATION_PROFILE_V1),
)


def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ATLAS_SEMANTIC_GRADER_MODE", raising=False)
    monkeypatch.delenv("ATLAS_EVALUATION_PROFILE", raising=False)
    monkeypatch.delenv("ATLAS_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("ATLAS_MODEL_NAME", raising=False)
    monkeypatch.delenv("ATLAS_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)


def _live_overrides() -> dict[str, object]:
    return {
        "model_provider": FROZEN_LIVE_SEMANTIC_PROVIDER,
        "model_name": FROZEN_LIVE_SEMANTIC_MODEL,
        "openai_api_key": SecretStr("sk-test-not-a-real-key"),
        "langsmith_api_key": SecretStr("lsv2_test_not_a_real_key"),
    }


def _live_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "evaluation_profile": EVALUATION_PROFILE_V1,
        "semantic_grader_mode": "live",
        **_live_overrides(),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_default_settings_resolve_to_candidate_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings()
    assert settings.semantic_grader_mode == "skipped"
    assert settings.evaluation_profile is None
    assert resolved_evaluation_profile(settings) == EVALUATION_PROFILE_CANDIDATE
    require_semantic_grader_mode(settings)
    assert (
        build_semantic_grader(
            settings,
            session_factory=object(),  # type: ignore[arg-type]
            workflow_execution_id="exec-1",
        )
        is None
    )


@pytest.mark.parametrize(("mode", "profile"), _DERIVED)
def test_unset_profile_derives_from_semantic_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: str,
    profile: str,
) -> None:
    _isolate(monkeypatch, tmp_path)
    kwargs: dict[str, object] = {"semantic_grader_mode": mode}
    if mode == "live":
        kwargs.update(_live_overrides())
    settings = Settings(**kwargs)  # type: ignore[arg-type]
    assert settings.evaluation_profile is None
    assert resolved_evaluation_profile(settings) == profile
    require_evaluation_composition(settings)


@pytest.mark.parametrize(("mode", "profile"), _DERIVED)
def test_explicit_matching_profile_and_mode_succeed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: str,
    profile: str,
) -> None:
    _isolate(monkeypatch, tmp_path)
    kwargs: dict[str, object] = {
        "semantic_grader_mode": mode,
        "evaluation_profile": profile,
    }
    if mode == "live":
        kwargs.update(_live_overrides())
    settings = Settings(**kwargs)  # type: ignore[arg-type]
    assert settings.evaluation_profile == profile
    assert resolved_evaluation_profile(settings) == profile
    require_evaluation_composition(settings)


@pytest.mark.parametrize(
    ("profile", "mode"),
    (
        (EVALUATION_PROFILE_CANDIDATE, "fake"),
        (EVALUATION_PROFILE_CANDIDATE, "live"),
        (EVALUATION_PROFILE_CANDIDATE_FAKE, "skipped"),
        (EVALUATION_PROFILE_V1, "skipped"),
        (EVALUATION_PROFILE_V1, "fake"),
    ),
)
def test_explicit_profile_mode_disagreement_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: str,
    mode: str,
) -> None:
    _isolate(monkeypatch, tmp_path)
    kwargs: dict[str, object] = {
        "evaluation_profile": profile,
        "semantic_grader_mode": mode,
    }
    if mode == "live":
        kwargs.update(_live_overrides())
    settings = Settings(**kwargs)  # type: ignore[arg-type]
    assert settings.evaluation_profile == profile
    assert resolved_evaluation_profile(settings) == profile
    with pytest.raises(SemanticGraderConfigurationError) as exc_info:
        require_evaluation_composition(settings)
    assert "does not match" in str(exc_info.value)


def test_derived_profile_matches_mode_table() -> None:
    assert PROFILE_FOR_SEMANTIC_MODE["skipped"] == EVALUATION_PROFILE_CANDIDATE
    assert PROFILE_FOR_SEMANTIC_MODE["fake"] == EVALUATION_PROFILE_CANDIDATE_FAKE
    assert PROFILE_FOR_SEMANTIC_MODE["live"] == EVALUATION_PROFILE_V1


def test_fake_mode_composes_fake_grader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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
    settings = _live_settings(model_provider="fake")
    with pytest.raises(SemanticGraderConfigurationError) as exc_info:
        require_semantic_grader_mode(settings)
    message = str(exc_info.value)
    assert "non-fake" in message
    assert "openai" not in message.lower()
    assert "sk-" not in message


def test_live_v1_requires_openai_gpt4o_mini_temperature_and_keys(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    require_evaluation_composition(_live_settings())

    with pytest.raises(SemanticGraderConfigurationError) as anthropic:
        require_evaluation_composition(_live_settings(model_provider="anthropic"))
    assert "openai" in str(anthropic.value)

    with pytest.raises(SemanticGraderConfigurationError) as model:
        require_evaluation_composition(_live_settings(model_name="gpt-4o"))
    assert FROZEN_LIVE_SEMANTIC_MODEL in str(model.value)

    with pytest.raises(SemanticGraderConfigurationError) as openai_key:
        require_evaluation_composition(_live_settings(openai_api_key=None))
    assert "OpenAI credential" in str(openai_key.value)

    with pytest.raises(SemanticGraderConfigurationError) as langsmith_key:
        require_evaluation_composition(_live_settings(langsmith_api_key=None))
    assert "LangSmith credential" in str(langsmith_key.value)

    with pytest.raises(SemanticGraderConfigurationError) as skipped:
        require_evaluation_composition(_live_settings(semantic_grader_mode="skipped"))
    assert "does not match" in str(skipped.value)


def test_api_settings_can_construct_live_without_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path)
    settings = Settings(
        semantic_grader_mode="live",
        model_provider="fake",
    )
    assert settings.semantic_grader_mode == "live"
    assert settings.model_provider == "fake"
    assert settings.evaluation_profile is None
    assert resolved_evaluation_profile(settings) == EVALUATION_PROFILE_V1


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
    settings = _live_settings()
    require_semantic_grader_mode(settings)
    assert FROZEN_LIVE_SEMANTIC_TEMPERATURE == 0.0

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
