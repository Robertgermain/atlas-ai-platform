"""Configuration contract tests for Kafka relay settings (Slice 13C1)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from atlas.config.settings import Settings


def test_default_kafka_settings_satisfy_the_lease_margin() -> None:
    settings = Settings()
    assert settings.kafka_bootstrap_servers == "127.0.0.1:9094"
    assert settings.kafka_delivery_timeout_seconds == pytest.approx(10.0)
    assert settings.outbox_publish_lease_seconds == pytest.approx(30.0)
    assert settings.kafka_delivery_timeout_lease_margin_seconds == pytest.approx(5.0)


def test_delivery_timeout_exactly_at_margin_boundary_accepted() -> None:
    settings = Settings(
        kafka_delivery_timeout_seconds=25.0,
        kafka_delivery_timeout_lease_margin_seconds=5.0,
        outbox_publish_lease_seconds=30.0,
    )
    assert settings.kafka_delivery_timeout_seconds == pytest.approx(25.0)


def test_delivery_timeout_over_margin_boundary_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            kafka_delivery_timeout_seconds=26.0,
            kafka_delivery_timeout_lease_margin_seconds=5.0,
            outbox_publish_lease_seconds=30.0,
        )
    messages = [str(err.get("msg", "")) for err in exc_info.value.errors()]
    assert any(
        "kafka_delivery_timeout_seconds plus" in msg
        and "outbox_publish_lease_seconds" in msg
        for msg in messages
    )
    # Sanitized: no interpolated configured numeric values in the message.
    for msg in messages:
        if "kafka_delivery_timeout_seconds plus" in msg:
            assert "26.0" not in msg
            assert "30.0" not in msg


def test_delivery_timeout_lease_margin_rejected_via_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_KAFKA_DELIVERY_TIMEOUT_SECONDS", "29")
    monkeypatch.setenv("ATLAS_OUTBOX_PUBLISH_LEASE_SECONDS", "30")
    with pytest.raises(ValidationError):
        Settings()


def test_kafka_delivery_timeout_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"kafka_delivery_timeout_seconds": 0})


def test_kafka_socket_timeout_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"kafka_socket_timeout_seconds": 0})


def test_outbox_relay_poll_interval_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"outbox_relay_poll_interval_seconds": 0})
