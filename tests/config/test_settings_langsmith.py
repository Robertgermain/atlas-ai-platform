"""Settings validates LangSmith field types and URL syntax only (Slice 15B)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from atlas.config.settings import Settings
from atlas.observability.langsmith import (
    live_ai_provider_configured,
    require_langsmith_for_live_ai,
)
from atlas.observability.langsmith.errors import LangSmithConfigurationError

_DUMMY_LANGSMITH_KEY = "lsv2_test_not_a_real_key"


def _isolate_langsmith_settings_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Remove LangSmith process env and the repository ``.env`` before Settings().

    Offline tests must not inherit a developer's ignored ``.env`` or exported
    ``ATLAS_LANGSMITH_*`` variables. Call this before constructing
    ``Settings()``, then pass any dummy key the test itself needs.
    Does not touch ``ATLAS_ENABLE_LIVE_LANGSMITH_TESTS``.
    """
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_PROJECT", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_URL", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_TIMEOUT_MS", raising=False)
    monkeypatch.chdir(tmp_path)


def test_default_langsmith_fields_are_unset_and_hosted_url_is_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings()
    assert settings.langsmith_api_key is None
    assert settings.langsmith_api_url is None
    assert settings.langsmith_project == "atlas-local"
    assert settings.langsmith_timeout_ms == 5000


def test_https_langsmith_url_is_accepted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(langsmith_api_url="https://example.invalid/api")
    assert settings.langsmith_api_url == "https://example.invalid/api"


def test_loopback_http_langsmith_url_is_accepted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(langsmith_api_url="http://127.0.0.1:1984")
    assert settings.langsmith_api_url == "http://127.0.0.1:1984"


def test_empty_langsmith_url_becomes_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(langsmith_api_url="   ")
    assert settings.langsmith_api_url is None


def test_non_loopback_http_langsmith_url_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    with pytest.raises(ValidationError) as exc_info:
        Settings(langsmith_api_url="http://example.invalid/api")
    assert "HTTPS" in str(exc_info.value)


def test_langsmith_url_with_userinfo_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    with pytest.raises(ValidationError) as exc_info:
        Settings(langsmith_api_url="https://user:pass@example.invalid/api")
    assert "credentials" in str(exc_info.value)


def test_langsmith_project_rejects_unsupported_characters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    with pytest.raises(ValidationError):
        Settings(langsmith_project="atlas local")


def test_settings_allows_live_provider_without_langsmith_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The API constructs Settings with live-provider fields and no LangSmith key."""
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(
        model_provider="openai",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
        langsmith_api_key=None,
    )
    assert settings.model_provider == "openai"
    assert live_ai_provider_configured(settings) is True
    with pytest.raises(LangSmithConfigurationError) as exc_info:
        require_langsmith_for_live_ai(settings)
    message = str(exc_info.value)
    assert "LangSmith API key is required" in message
    assert "sk-test-not-a-real-key" not in message
    assert "http" not in message.lower()


def test_fake_providers_do_not_require_langsmith_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings()
    assert live_ai_provider_configured(settings) is False
    require_langsmith_for_live_ai(settings)


def test_live_embedding_provider_requires_langsmith_at_composition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(embedding_provider="openai")
    with pytest.raises(LangSmithConfigurationError):
        require_langsmith_for_live_ai(settings)


def test_live_tool_provider_requires_langsmith_at_composition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(tool_provider="tavily")
    with pytest.raises(LangSmithConfigurationError):
        require_langsmith_for_live_ai(settings)


def test_live_ai_with_langsmith_key_is_accepted_at_composition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_langsmith_settings_environment(monkeypatch, tmp_path)
    settings = Settings(
        model_provider="openai",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
        langsmith_api_key=SecretStr(_DUMMY_LANGSMITH_KEY),
    )
    require_langsmith_for_live_ai(settings)
