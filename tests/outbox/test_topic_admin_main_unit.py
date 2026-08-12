"""Network-free unit tests for ``python -m atlas.outbox.topic_admin`` (Slice 14B).

``main()`` is a thin startup-boundary wrapper around the existing
``verify_broker_connectivity`` / ``ensure_topic_exists`` /
``verify_topic_partitioning`` functions -- these tests monkeypatch those
three collaborators directly rather than opening a real Kafka connection, so
this file never touches the network.
"""

from __future__ import annotations

import pytest

import atlas.outbox.topic_admin as topic_admin
from atlas.config.settings import Settings
from atlas.observability.testing import CapturedLogs, capture_logs
from atlas.outbox.errors import (
    KafkaProducerConfigurationError,
    KafkaTopicVerificationError,
)

# Fake sensitive content that must never reach a log line: a broker address
# embedded in an exception message. Used only to prove log sanitization; not
# a real secret.
_SENSITIVE_MESSAGE = "kafka bootstrap 203.0.113.9:9094 is unreachable"
_SENSITIVE_FRAGMENT = "203.0.113.9"


def _rendered(captured: CapturedLogs) -> str:
    return captured.text


@pytest.fixture(autouse=True)
def _settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings()
    monkeypatch.setattr(topic_admin, "get_settings", lambda: settings)
    return settings


def _assert_calls_in_order(calls: list[str]) -> None:
    assert calls == [
        "verify_broker_connectivity",
        "ensure_topic_exists",
        "verify_topic_partitioning",
    ]


def test_main_returns_zero_and_calls_functions_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        topic_admin,
        "verify_broker_connectivity",
        lambda **_kwargs: calls.append("verify_broker_connectivity"),
    )
    monkeypatch.setattr(
        topic_admin,
        "ensure_topic_exists",
        lambda **_kwargs: calls.append("ensure_topic_exists"),
    )
    monkeypatch.setattr(
        topic_admin,
        "verify_topic_partitioning",
        lambda **_kwargs: calls.append("verify_topic_partitioning"),
    )

    assert topic_admin.main() == 0
    _assert_calls_in_order(calls)


def test_main_returns_nonzero_when_settings_fail_to_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_settings() -> Settings:
        raise ValueError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "get_settings", _raise_settings)

    assert topic_admin.main() == 1


def test_main_logs_are_sanitized_when_settings_fail_to_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_settings() -> Settings:
        raise ValueError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "get_settings", _raise_settings)

    with capture_logs("atlas.outbox.topic_admin") as captured:
        topic_admin.main()

    rendered = _rendered(captured)
    assert _SENSITIVE_FRAGMENT not in rendered
    assert "ValueError" in rendered


def test_main_returns_nonzero_when_broker_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_broker(**_kwargs: object) -> None:
        raise KafkaTopicVerificationError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "verify_broker_connectivity", _raise_broker)
    ensure_called = False

    def _ensure(**_kwargs: object) -> None:
        nonlocal ensure_called
        ensure_called = True

    monkeypatch.setattr(topic_admin, "ensure_topic_exists", _ensure)

    assert topic_admin.main() == 1
    assert not ensure_called


def test_main_logs_are_sanitized_when_broker_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_broker(**_kwargs: object) -> None:
        raise KafkaTopicVerificationError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "verify_broker_connectivity", _raise_broker)

    with capture_logs("atlas.outbox.topic_admin") as captured:
        topic_admin.main()

    rendered = _rendered(captured)
    assert _SENSITIVE_FRAGMENT not in rendered
    assert "KafkaTopicVerificationError" in rendered


def test_main_returns_nonzero_when_topic_creation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        topic_admin, "verify_broker_connectivity", lambda **_kwargs: None
    )

    def _raise_ensure(**_kwargs: object) -> None:
        raise KafkaProducerConfigurationError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "ensure_topic_exists", _raise_ensure)
    verify_called = False

    def _verify(**_kwargs: object) -> None:
        nonlocal verify_called
        verify_called = True

    monkeypatch.setattr(topic_admin, "verify_topic_partitioning", _verify)

    assert topic_admin.main() == 1
    assert not verify_called


def test_main_returns_nonzero_when_partition_verification_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        topic_admin, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(topic_admin, "ensure_topic_exists", lambda **_kwargs: None)

    def _raise_verify(**_kwargs: object) -> None:
        raise KafkaTopicVerificationError("UnexpectedPartitionCount")

    monkeypatch.setattr(topic_admin, "verify_topic_partitioning", _raise_verify)

    assert topic_admin.main() == 1


def test_main_returns_nonzero_on_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_unexpected(**_kwargs: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "verify_broker_connectivity", _raise_unexpected)

    assert topic_admin.main() == 1


def test_main_logs_are_sanitized_on_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_unexpected(**_kwargs: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(topic_admin, "verify_broker_connectivity", _raise_unexpected)

    with capture_logs("atlas.outbox.topic_admin") as captured:
        topic_admin.main()

    rendered = _rendered(captured)
    assert _SENSITIVE_FRAGMENT not in rendered
    assert "RuntimeError" in rendered


def test_main_passes_settings_derived_arguments(
    monkeypatch: pytest.MonkeyPatch, _settings: Settings
) -> None:
    """Each delegated call must receive the settings-derived bootstrap servers
    and topic-verify timeout -- never a hardcoded or arbitrary value."""
    seen_kwargs: list[dict[str, object]] = []

    def _record(**kwargs: object) -> None:
        seen_kwargs.append(kwargs)

    monkeypatch.setattr(topic_admin, "verify_broker_connectivity", _record)
    monkeypatch.setattr(topic_admin, "ensure_topic_exists", _record)
    monkeypatch.setattr(topic_admin, "verify_topic_partitioning", _record)

    assert topic_admin.main() == 0
    assert len(seen_kwargs) == 3
    for kwargs in seen_kwargs:
        assert kwargs["bootstrap_servers"] == _settings.kafka_bootstrap_servers
        assert kwargs["timeout_seconds"] == _settings.kafka_topic_verify_timeout_seconds
