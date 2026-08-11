"""Network-free unit tests for ``atlas.consumer.replay`` (the operator replay CLI)."""

from __future__ import annotations

import logging
from uuid import UUID, uuid4

import pytest

from atlas.consumer import replay as replay_module
from atlas.consumer.replay import compute_request_fingerprint, main
from atlas.consumer.replay_errors import (
    ReplayConflictError,
    ReplayNotFoundError,
    ReplayOwnershipLostError,
)
from atlas.persistence.repositories.consumer_dead_letter import (
    ReplayOutcome,
    ReplayResult,
)


class _StubReplayService:
    def __init__(
        self, *, result: ReplayResult | None = None, error: Exception | None = None
    ) -> None:
        self._result = result
        self._error = error
        self.calls: list[dict[str, object]] = []

    def replay(self, **kwargs: object) -> ReplayResult:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


@pytest.fixture(autouse=True)
def _patch_composition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never construct a real engine/session factory in these unit tests."""
    monkeypatch.setattr(
        replay_module,
        "build_consumer_engine",
        lambda url, **kwargs: object(),
    )
    monkeypatch.setattr(replay_module, "get_session_factory", lambda engine: object())


def _install_service(
    monkeypatch: pytest.MonkeyPatch, service: _StubReplayService
) -> None:
    monkeypatch.setattr(
        replay_module, "DeadLetterReplayService", lambda **kwargs: service
    )


# --- argument validation -----------------------------------------------------


def test_invalid_dead_letter_id_is_rejected_without_a_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    exit_code = main(
        [
            "--dead-letter-id",
            "not-a-uuid",
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1
    assert "Invalid --dead-letter-id" in caplog.text


def test_empty_actor_id_is_rejected() -> None:
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "   ",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1


def test_oversized_reason_is_rejected() -> None:
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "operator-1",
            "--reason",
            "x" * 513,
        ]
    )
    assert exit_code == 1


def test_oversized_idempotency_key_is_rejected() -> None:
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
            "--idempotency-key",
            "x" * 257,
        ]
    )
    assert exit_code == 1


def test_missing_required_arguments_exits_nonzero() -> None:
    with pytest.raises(SystemExit):
        main(["--actor-id", "operator-1"])


# --- successful replay outcomes ---------------------------------------------


def test_applied_outcome_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    dead_letter_id = uuid4()
    attempt_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id,
            attempt_id=attempt_id,
            outcome=ReplayOutcome.APPLIED,
        )
    )
    _install_service(monkeypatch, service)
    exit_code = main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 0
    assert len(service.calls) == 1


def test_duplicate_outcome_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    dead_letter_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id,
            attempt_id=uuid4(),
            outcome=ReplayOutcome.DUPLICATE,
        )
    )
    _install_service(monkeypatch, service)
    exit_code = main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 0


@pytest.mark.parametrize(
    "outcome",
    [ReplayOutcome.FAILED, ReplayOutcome.IN_PROGRESS, ReplayOutcome.LOST_OWNERSHIP],
)
def test_non_success_outcomes_exit_nonzero(
    monkeypatch: pytest.MonkeyPatch, outcome: ReplayOutcome
) -> None:
    dead_letter_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id, attempt_id=uuid4(), outcome=outcome
        )
    )
    _install_service(monkeypatch, service)
    exit_code = main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1


# --- rejected / errored replay outcomes -------------------------------------


@pytest.mark.parametrize(
    "error", [ReplayNotFoundError(), ReplayConflictError(), ReplayOwnershipLostError()]
)
def test_replay_errors_exit_nonzero_with_sanitized_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
) -> None:
    caplog.set_level(logging.ERROR)
    service = _StubReplayService(error=error)
    _install_service(monkeypatch, service)
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1
    assert "Replay rejected" in caplog.text
    assert error.__class__.__name__ in caplog.text


def test_unexpected_exception_is_sanitized_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    class _SensitiveError(RuntimeError):
        def __str__(self) -> str:
            return "database_url=postgresql://atlas:hunter2@10.0.0.5/atlas"

    service = _StubReplayService(error=_SensitiveError("boom"))
    _install_service(monkeypatch, service)
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1
    assert "hunter2" not in caplog.text
    assert "10.0.0.5" not in caplog.text
    assert "_SensitiveError" in caplog.text


def test_settings_or_engine_construction_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    def _raise_settings() -> None:
        raise RuntimeError("postgresql://atlas:hunter2@10.0.0.5/atlas")

    monkeypatch.setattr(replay_module, "get_settings", _raise_settings)
    exit_code = main(
        [
            "--dead-letter-id",
            str(uuid4()),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 1
    assert "hunter2" not in caplog.text
    assert "10.0.0.5" not in caplog.text


# --- idempotency key / fingerprint behavior ---------------------------------


def test_omitted_idempotency_key_is_generated_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dead_letter_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id,
            attempt_id=uuid4(),
            outcome=ReplayOutcome.APPLIED,
        )
    )
    _install_service(monkeypatch, service)
    main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    key = service.calls[0]["idempotency_key"]
    assert isinstance(key, str)
    assert 0 < len(key) <= 256


def test_request_fingerprint_is_deterministic_for_the_same_inputs() -> None:
    a = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-1", operator_reason="reason"
    )
    b = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-1", operator_reason="reason"
    )
    assert a == b
    assert len(a) == 64  # sha256 hex digest


def test_request_fingerprint_differs_when_actor_id_differs() -> None:
    a = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-1", operator_reason="reason"
    )
    b = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-2", operator_reason="reason"
    )
    assert a != b


def test_request_fingerprint_differs_when_reason_differs() -> None:
    a = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-1", operator_reason="reason-a"
    )
    b = compute_request_fingerprint(
        dead_letter_id="dl-1", actor_id="op-1", operator_reason="reason-b"
    )
    assert a != b


def test_replay_builds_the_consumer_engine_with_the_settings_derived_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blocker 1 drift guard: ``replay.py`` must pass the exact same settings
    fields into ``build_consumer_engine`` as ``python -m atlas.consumer`` does
    -- never a different or hardcoded value."""
    dead_letter_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id,
            attempt_id=uuid4(),
            outcome=ReplayOutcome.APPLIED,
        )
    )
    _install_service(monkeypatch, service)
    captured: dict[str, object] = {}

    def _capturing_build_consumer_engine(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(
        replay_module, "build_consumer_engine", _capturing_build_consumer_engine
    )
    from atlas.config import get_settings

    settings = get_settings()

    exit_code = main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert exit_code == 0
    assert captured["url"] == settings.database_url
    assert captured["connect_timeout_seconds"] == (
        settings.consumer_db_connect_timeout_seconds
    )
    assert captured["pool_timeout_seconds"] == (
        settings.consumer_db_pool_timeout_seconds
    )
    assert captured["statement_timeout_seconds"] == (
        settings.consumer_db_statement_timeout_seconds
    )


def test_replay_service_is_invoked_with_a_parsed_uuid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dead_letter_id = uuid4()
    service = _StubReplayService(
        result=ReplayResult(
            dead_letter_id=dead_letter_id,
            attempt_id=uuid4(),
            outcome=ReplayOutcome.APPLIED,
        )
    )
    _install_service(monkeypatch, service)
    main(
        [
            "--dead-letter-id",
            str(dead_letter_id),
            "--actor-id",
            "operator-1",
            "--reason",
            "testing",
        ]
    )
    assert service.calls[0]["dead_letter_id"] == dead_letter_id
    assert isinstance(service.calls[0]["dead_letter_id"], UUID)
