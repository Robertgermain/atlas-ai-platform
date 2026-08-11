"""Configuration contract tests for consumer retry/DLQ/replay timing (Slice 13C2B)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from atlas.config.settings import Settings
from atlas.consumer.timing import (
    RetryTimingParameters,
    worst_case_total_processing_seconds,
)

_ENV_VARS = (
    "ATLAS_CONSUMER_MAX_POLL_INTERVAL_SECONDS",
    "ATLAS_CONSUMER_RETRY_MAX_ATTEMPTS",
    "ATLAS_CONSUMER_RETRY_BASE_SECONDS",
    "ATLAS_CONSUMER_RETRY_MAX_BACKOFF_SECONDS",
    "ATLAS_CONSUMER_RETRY_JITTER_MAX_SECONDS",
    "ATLAS_CONSUMER_RETRY_SAFETY_MARGIN_SECONDS",
    "ATLAS_CONSUMER_DB_CONNECT_TIMEOUT_SECONDS",
    "ATLAS_CONSUMER_DB_POOL_TIMEOUT_SECONDS",
    "ATLAS_CONSUMER_DB_STATEMENT_TIMEOUT_SECONDS",
    "ATLAS_CONSUMER_RETRY_PROCESSING_OVERHEAD_SECONDS",
    "ATLAS_CONSUMER_MAX_DB_ROUND_TRIPS_PER_ATTEMPT",
    "ATLAS_CONSUMER_REPLAY_LEASE_SECONDS",
)


def _isolate_consumer_retry_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def test_default_consumer_retry_settings_satisfy_the_timing_margin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_consumer_retry_environment(monkeypatch, tmp_path)
    settings = Settings()
    assert settings.consumer_retry_max_attempts == 3
    assert settings.consumer_retry_base_seconds == pytest.approx(1.0)
    assert settings.consumer_retry_max_backoff_seconds == pytest.approx(30.0)
    assert settings.consumer_retry_jitter_max_seconds == pytest.approx(0.0)
    assert settings.consumer_retry_safety_margin_seconds == pytest.approx(60.0)
    assert settings.consumer_db_connect_timeout_seconds == pytest.approx(5.0)
    assert settings.consumer_db_pool_timeout_seconds == pytest.approx(5.0)
    assert settings.consumer_db_statement_timeout_seconds == pytest.approx(5.0)
    assert settings.consumer_retry_processing_overhead_seconds == pytest.approx(2.0)
    assert settings.consumer_max_db_round_trips_per_attempt == 8
    assert settings.consumer_replay_lease_seconds == pytest.approx(90.0)
    assert settings.consumer_max_poll_interval_seconds == pytest.approx(300.0)


def test_default_worst_case_timing_matches_the_approved_calculation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """52 seconds per attempt, 219 seconds total, 219 < 300 (approved defaults)."""
    _isolate_consumer_retry_environment(monkeypatch, tmp_path)
    settings = Settings()
    params = RetryTimingParameters(
        max_attempts=settings.consumer_retry_max_attempts,
        base_seconds=settings.consumer_retry_base_seconds,
        max_backoff_seconds=settings.consumer_retry_max_backoff_seconds,
        jitter_max_seconds=settings.consumer_retry_jitter_max_seconds,
        safety_margin_seconds=settings.consumer_retry_safety_margin_seconds,
        db_connect_timeout_seconds=settings.consumer_db_connect_timeout_seconds,
        db_pool_timeout_seconds=settings.consumer_db_pool_timeout_seconds,
        db_statement_timeout_seconds=settings.consumer_db_statement_timeout_seconds,
        processing_overhead_seconds=settings.consumer_retry_processing_overhead_seconds,
        max_db_round_trips_per_attempt=settings.consumer_max_db_round_trips_per_attempt,
    )
    from atlas.consumer.timing import worst_case_attempt_seconds

    assert worst_case_attempt_seconds(params) == pytest.approx(52.0)
    total = worst_case_total_processing_seconds(params)
    assert total == pytest.approx(219.0)
    assert total < settings.consumer_max_poll_interval_seconds


def test_timing_margin_exactly_at_boundary_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The validator requires *strictly* less than, so equality is rejected."""
    _isolate_consumer_retry_environment(monkeypatch, tmp_path)
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            consumer_retry_max_attempts=1,
            consumer_retry_base_seconds=1.0,
            consumer_retry_max_backoff_seconds=1.0,
            consumer_retry_jitter_max_seconds=0.0,
            consumer_retry_safety_margin_seconds=0.0,
            consumer_db_connect_timeout_seconds=1.0,
            consumer_db_pool_timeout_seconds=1.0,
            consumer_db_statement_timeout_seconds=1.0,
            consumer_retry_processing_overhead_seconds=0.0,
            consumer_max_db_round_trips_per_attempt=1,
            consumer_max_poll_interval_seconds=3.0,
        )
    messages = [str(err.get("msg", "")) for err in exc_info.value.errors()]
    assert any("consumer_max_poll_interval_seconds" in msg for msg in messages)
    for msg in messages:
        # Sanitized: no interpolated configured numeric values in the message.
        assert "3.0" not in msg


def test_timing_margin_one_second_under_boundary_is_accepted() -> None:
    settings = Settings(
        consumer_retry_max_attempts=1,
        consumer_retry_base_seconds=1.0,
        consumer_retry_max_backoff_seconds=1.0,
        consumer_retry_jitter_max_seconds=0.0,
        consumer_retry_safety_margin_seconds=0.0,
        consumer_db_connect_timeout_seconds=1.0,
        consumer_db_pool_timeout_seconds=1.0,
        consumer_db_statement_timeout_seconds=1.0,
        consumer_retry_processing_overhead_seconds=0.0,
        consumer_max_db_round_trips_per_attempt=1,
        consumer_max_poll_interval_seconds=3.01,
    )
    assert settings.consumer_max_poll_interval_seconds == pytest.approx(3.01)


def test_timing_margin_rejected_via_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_consumer_retry_environment(monkeypatch, tmp_path)
    monkeypatch.setenv("ATLAS_CONSUMER_MAX_POLL_INTERVAL_SECONDS", "1")
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize(
    "field",
    [
        "consumer_retry_base_seconds",
        "consumer_retry_max_backoff_seconds",
        "consumer_db_connect_timeout_seconds",
        "consumer_db_pool_timeout_seconds",
        "consumer_db_statement_timeout_seconds",
    ],
)
def test_strictly_positive_timing_fields_reject_zero(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({field: 0})


@pytest.mark.parametrize(
    "field",
    ["consumer_retry_jitter_max_seconds", "consumer_retry_safety_margin_seconds"],
)
def test_nonnegative_timing_fields_accept_zero(field: str) -> None:
    settings = Settings.model_validate({field: 0})
    assert getattr(settings, field) == pytest.approx(0.0)


def test_retry_max_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"consumer_retry_max_attempts": 0})


def test_max_db_round_trips_must_be_at_least_one() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"consumer_max_db_round_trips_per_attempt": 0})


def test_replay_lease_seconds_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({"consumer_replay_lease_seconds": 0})


# --- Correction pass: the timing-margin validator must use the same
# --- ceiling-rounded effective connect/statement timeouts that
# --- ``atlas.consumer.db.build_consumer_engine`` applies at runtime, not
# --- the raw configured floats -- otherwise a fractional configured value
# --- could round up to a larger runtime timeout than a raw-float
# --- calculation assumed, silently invalidating the proven margin. ---------


def test_timing_margin_uses_the_ceiling_rounded_effective_timeouts_not_raw_floats() -> (
    None
):
    """A sub-second ``consumer_db_connect_timeout_seconds`` that ceiling-rounds
    up to a whole second must be reflected in the validator's own margin
    check.

    With ``connect_timeout_seconds=0.5`` and 10 round trips at
    ``statement_timeout_seconds=0.5`` (already whole milliseconds, so
    unaffected by ms rounding): a *raw*, unrounded calculation would total
    ``1.0 (pool) + 0.5 (connect) + 10*0.5 (statement) + 0 (overhead) == 6.5``
    seconds -- comfortably under a ``6.8``-second poll interval. The
    *effective* (ceiling-rounded) calculation this validator must actually
    use instead rounds ``connect_timeout_seconds`` up to ``1`` whole second,
    totaling ``7.0`` seconds -- which must be rejected as not strictly less
    than ``6.8``. If this test ever passes construction, the validator has
    regressed to using raw floats instead of effective runtime values.
    """
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            consumer_retry_max_attempts=1,
            consumer_retry_base_seconds=1.0,
            consumer_retry_max_backoff_seconds=1.0,
            consumer_retry_jitter_max_seconds=0.0,
            consumer_retry_safety_margin_seconds=0.0,
            consumer_db_connect_timeout_seconds=0.5,
            consumer_db_pool_timeout_seconds=1.0,
            consumer_db_statement_timeout_seconds=0.5,
            consumer_retry_processing_overhead_seconds=0.0,
            consumer_max_db_round_trips_per_attempt=10,
            consumer_max_poll_interval_seconds=6.8,
        )
    messages = [str(err.get("msg", "")) for err in exc_info.value.errors()]
    assert any("consumer_max_poll_interval_seconds" in msg for msg in messages)


def test_timing_margin_accepts_config_against_true_effective_bound() -> None:
    """The identical configuration above, but against a poll interval that
    correctly accounts for the effective (ceiling-rounded) 7.0-second
    worst-case attempt, must be accepted."""
    settings = Settings(
        consumer_retry_max_attempts=1,
        consumer_retry_base_seconds=1.0,
        consumer_retry_max_backoff_seconds=1.0,
        consumer_retry_jitter_max_seconds=0.0,
        consumer_retry_safety_margin_seconds=0.0,
        consumer_db_connect_timeout_seconds=0.5,
        consumer_db_pool_timeout_seconds=1.0,
        consumer_db_statement_timeout_seconds=0.5,
        consumer_retry_processing_overhead_seconds=0.0,
        consumer_max_db_round_trips_per_attempt=10,
        consumer_max_poll_interval_seconds=7.01,
    )
    assert settings.consumer_max_poll_interval_seconds == pytest.approx(7.01)
