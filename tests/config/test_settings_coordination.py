"""Configuration contract tests for Slice 13A coordination settings."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from atlas.config.settings import Settings


def _settings_without_dotenv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Settings:
    """Build Settings ignoring a developer ``.env`` and CI provider overrides."""
    monkeypatch.delenv("ATLAS_COORDINATION_PROVIDER", raising=False)
    monkeypatch.delenv("ATLAS_RATE_LIMIT_MAX_REQUESTS", raising=False)
    monkeypatch.delenv("ATLAS_RATE_LIMIT_WINDOW_SECONDS", raising=False)
    monkeypatch.delenv("ATLAS_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("ATLAS_HEARTBEAT_TTL_SECONDS", raising=False)
    monkeypatch.delenv("ATLAS_REDIS_CONNECT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("ATLAS_REDIS_SOCKET_TIMEOUT_SECONDS", raising=False)
    monkeypatch.chdir(tmp_path)
    return Settings()


def test_default_coordination_provider_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # CI sets ATLAS_COORDINATION_PROVIDER=redis; assert the Field default when unset.
    settings = _settings_without_dotenv(monkeypatch, tmp_path)
    assert settings.coordination_provider == "noop"


def test_explicit_redis_provider_accepted() -> None:
    settings = Settings(coordination_provider="redis")
    assert settings.coordination_provider == "redis"


def test_unsupported_coordination_provider_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"coordination_provider": "celery"})


def test_default_rate_limit_settings_match_approved_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = _settings_without_dotenv(monkeypatch, tmp_path)
    assert settings.rate_limit_max_requests == 10
    assert settings.rate_limit_window_seconds == 60


def test_default_heartbeat_settings_match_approved_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = _settings_without_dotenv(monkeypatch, tmp_path)
    assert settings.heartbeat_interval_seconds == 5.0
    assert settings.heartbeat_ttl_seconds == 15


def test_heartbeat_ttl_exactly_twice_interval_accepted() -> None:
    settings = Settings(heartbeat_interval_seconds=5.0, heartbeat_ttl_seconds=10)
    assert settings.heartbeat_ttl_seconds == 10


def test_heartbeat_ttl_below_twice_interval_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        Settings(heartbeat_interval_seconds=5.0, heartbeat_ttl_seconds=9)
    messages = [str(err.get("msg", "")) for err in exc_info.value.errors()]
    assert any(
        "heartbeat_ttl_seconds must be at least twice heartbeat_interval_seconds" in msg
        for msg in messages
    )
    # Sanitized application message: no interpolated configured values.
    for msg in messages:
        if "heartbeat_ttl_seconds must be at least twice" in msg:
            assert "5.0" not in msg
            assert "9" not in msg


def test_heartbeat_timing_rejected_via_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ATLAS_HEARTBEAT_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("ATLAS_HEARTBEAT_TTL_SECONDS", "9")
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    # The application ValueError text is sanitized (no interpolated values).
    errors = exc_info.value.errors()
    assert any(
        "heartbeat_ttl_seconds must be at least twice heartbeat_interval_seconds"
        in str(err.get("msg", ""))
        for err in errors
    )


def test_default_redis_timeouts_are_bounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = _settings_without_dotenv(monkeypatch, tmp_path)
    assert settings.redis_connect_timeout_seconds == pytest.approx(0.2)
    assert settings.redis_socket_timeout_seconds == pytest.approx(0.2)


def test_rate_limit_max_requests_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"rate_limit_max_requests": 0})


def test_heartbeat_ttl_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"heartbeat_ttl_seconds": 0})
