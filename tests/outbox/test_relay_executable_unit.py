"""Network-free unit tests for ``python -m atlas.outbox`` (Slice 13C1).

Covers the poll loop's termination rules directly (``_run_poll_loop``) and
``main()``'s startup composition/cleanup via monkeypatched collaborators.
No real PostgreSQL or Kafka connection is made in this file.
"""

from __future__ import annotations

import ast
import inspect
import logging
import signal
from collections.abc import Iterator

import pytest

import atlas.outbox.__main__ as outbox_main
from atlas.config.settings import Settings
from atlas.outbox.errors import (
    KafkaProducerConfigurationError,
    KafkaTopicVerificationError,
    OutboxError,
    RelayOwnershipError,
)
from atlas.outbox.relay import RelayBatchResult, RelayRunOutcome

# Fake sensitive content that must never reach a log line: a credential, a
# broker address, and a SQL fragment. Used only to prove log sanitization;
# none of it is a real secret.
_SENSITIVE_MESSAGE = (
    "postgresql://atlas:hunter2@10.0.0.5:5432/atlas_prod "
    "kafka bootstrap 203.0.113.9:9094 "
    "SELECT * FROM outbox_events WHERE claim_token='sekret-token'"
)
_SENSITIVE_FRAGMENTS = (
    "hunter2",
    "10.0.0.5",
    "203.0.113.9",
    "SELECT * FROM",
    "sekret-token",
)


def _assert_no_sensitive_fragments(text: str) -> None:
    for fragment in _SENSITIVE_FRAGMENTS:
        assert fragment not in text


def test_fake_event_producer_is_never_importable_from_the_executable() -> None:
    """Proves the executable never imports/constructs FakeEventProducer.

    Parses the module's actual import statements (ignoring prose in the
    module docstring, which mentions the fake only to document that it is
    test-only) so this fails if a future edit adds a real import.
    """
    tree = ast.parse(inspect.getsource(outbox_main))
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported_names.update(alias.asname or alias.name for alias in node.names)
            if node.module:
                imported_names.add(node.module)
        elif isinstance(node, ast.Import):
            imported_names.update(alias.asname or alias.name for alias in node.names)

    assert "FakeEventProducer" not in imported_names
    assert not any("fakes" in name for name in imported_names)
    assert "FakeEventProducer" not in outbox_main.__dict__


class _FakeRelay:
    def __init__(self, outcomes: list[RelayBatchResult], **_kwargs: object) -> None:
        self._outcomes = iter(outcomes)
        self.calls = 0

    def run_once(self) -> RelayBatchResult:
        self.calls += 1
        return next(self._outcomes)


class _RaisingRelay:
    def __init__(self, exc: Exception, **_kwargs: object) -> None:
        self._exc = exc
        self.calls = 0

    def run_once(self) -> RelayBatchResult:
        self.calls += 1
        raise self._exc


# --- _run_poll_loop -----------------------------------------------------


def test_poll_loop_stops_immediately_when_shutdown_already_requested() -> None:
    relay = _FakeRelay([])
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    exit_code = outbox_main._run_poll_loop(relay, settings, lambda: True)  # type: ignore[arg-type]
    assert exit_code == 0
    assert relay.calls == 0


def test_poll_loop_stops_after_current_record_no_further_claims() -> None:
    """Shutdown flips true after the first EMPTY result; no second run_once call."""
    relay = _FakeRelay(
        [RelayBatchResult(outcome=RelayRunOutcome.EMPTY, published_count=0)]
    )
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    calls_before_check = {"n": 0}

    def _shutdown() -> bool:
        # False on first check (loop enters), True from then on.
        should_stop = calls_before_check["n"] > 0
        calls_before_check["n"] += 1
        return should_stop

    exit_code = outbox_main._run_poll_loop(relay, settings, _shutdown)  # type: ignore[arg-type]
    assert exit_code == 0
    assert relay.calls == 1


def test_poll_loop_terminates_nonzero_on_fatal_failure() -> None:
    relay = _FakeRelay(
        [RelayBatchResult(outcome=RelayRunOutcome.FATAL_FAILURE, published_count=0)]
    )
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    assert relay.calls == 1


def test_poll_loop_terminates_nonzero_on_unexpected_failure_outcome() -> None:
    """An UNEXPECTED_FAILURE outcome must never be treated as backoff-and-retry."""
    relay = _FakeRelay(
        [
            RelayBatchResult(
                outcome=RelayRunOutcome.UNEXPECTED_FAILURE, published_count=0
            )
        ]
    )
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    assert relay.calls == 1


def test_poll_loop_backs_off_and_continues_on_recoverable_failure() -> None:
    relay = _FakeRelay(
        [
            RelayBatchResult(
                outcome=RelayRunOutcome.RECOVERABLE_FAILURE, published_count=0
            ),
            RelayBatchResult(outcome=RelayRunOutcome.PUBLISHED, published_count=1),
        ]
    )
    settings = Settings(outbox_relay_poll_interval_seconds=0.001)
    calls = {"n": 0}

    def _shutdown() -> bool:
        calls["n"] += 1
        return calls["n"] > 2

    exit_code = outbox_main._run_poll_loop(relay, settings, _shutdown)  # type: ignore[arg-type]
    assert exit_code == 0
    assert relay.calls == 2


def test_poll_loop_terminates_nonzero_on_unexpected_database_error() -> None:
    """A DB/session error not classified as an OutboxError still terminates."""
    relay = _RaisingRelay(RuntimeError("connection lost"))
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    assert relay.calls == 1


def test_poll_loop_terminates_nonzero_on_outbox_error() -> None:
    relay = _RaisingRelay(RelayOwnershipError("lock lost"))
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    assert relay.calls == 1


def test_poll_loop_logs_are_sanitized_on_outbox_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    relay = _RaisingRelay(OutboxError(_SENSITIVE_MESSAGE))
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    with caplog.at_level(logging.INFO):
        exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "OutboxError" in caplog.text


def test_poll_loop_logs_are_sanitized_on_unexpected_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    relay = _RaisingRelay(RuntimeError(_SENSITIVE_MESSAGE))
    settings = Settings(outbox_relay_poll_interval_seconds=0.01)
    with caplog.at_level(logging.INFO):
        exit_code = outbox_main._run_poll_loop(relay, settings, lambda: False)  # type: ignore[arg-type]
    assert exit_code == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text


# --- main() composition/cleanup -----------------------------------------


class _FakeProducer:
    instances: list[_FakeProducer] = []
    raise_on_close: Exception | None = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.closed = False
        self.close_timeout: float | None = None
        _FakeProducer.instances.append(self)

    def close(self, *, timeout_seconds: float) -> None:
        self.closed = True
        self.close_timeout = timeout_seconds
        if _FakeProducer.raise_on_close is not None:
            raise _FakeProducer.raise_on_close


class _FailingProducerCtor:
    def __call__(self, **kwargs: object) -> _FakeProducer:
        raise KafkaProducerConfigurationError("BadConfig")


class _FakeLock:
    instances: list[_FakeLock] = []
    raise_on_release: Exception | None = None

    def __init__(self, _engine: object) -> None:
        self.acquired = False
        self.released = False
        _FakeLock.instances.append(self)

    def acquire(self) -> None:
        self.acquired = True

    def release(self) -> None:
        self.released = True
        if _FakeLock.raise_on_release is not None:
            raise _FakeLock.raise_on_release


class _FailingAcquireLock(_FakeLock):
    def acquire(self) -> None:
        raise RelayOwnershipError("AlreadyHeld")


@pytest.fixture(autouse=True)
def _reset_fakes() -> Iterator[None]:
    _FakeProducer.instances.clear()
    _FakeProducer.raise_on_close = None
    _FakeLock.instances.clear()
    _FakeLock.raise_on_release = None
    yield
    _FakeProducer.instances.clear()
    _FakeProducer.raise_on_close = None
    _FakeLock.instances.clear()
    _FakeLock.raise_on_release = None


@pytest.fixture
def _patched_composition(monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings(
        kafka_bootstrap_servers="unit-test-broker:9092",
        outbox_relay_poll_interval_seconds=0.01,
    )
    monkeypatch.setattr(outbox_main, "get_settings", lambda: settings)
    monkeypatch.setattr(outbox_main, "get_engine", lambda _url: object())
    monkeypatch.setattr(outbox_main, "get_session_factory", lambda _engine: object())
    monkeypatch.setattr(outbox_main, "SqlAlchemyOutboxRepository", lambda: object())
    return settings


def test_main_exits_nonzero_when_producer_construction_fails(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FailingProducerCtor())
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    assert outbox_main.main() == 1
    assert _FakeLock.instances == []


class _SensitiveFailingProducerCtor:
    def __call__(self, **kwargs: object) -> _FakeProducer:
        raise KafkaProducerConfigurationError(_SENSITIVE_MESSAGE)


def test_main_logs_are_sanitized_when_producer_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        outbox_main, "KafkaEventProducer", _SensitiveFailingProducerCtor()
    )
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    with caplog.at_level(logging.INFO):
        assert outbox_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "KafkaProducerConfigurationError" in caplog.text


def test_main_exits_nonzero_on_advisory_lock_contention(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FailingAcquireLock)
    assert outbox_main.main() == 1
    assert _FakeProducer.instances[0].closed is True


class _SensitiveFailingAcquireLock(_FakeLock):
    def acquire(self) -> None:
        raise RelayOwnershipError(_SENSITIVE_MESSAGE)


def test_main_logs_are_sanitized_on_advisory_lock_contention(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(
        outbox_main, "PostgresOutboxRelayLock", _SensitiveFailingAcquireLock
    )
    with caplog.at_level(logging.INFO):
        assert outbox_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RelayOwnershipError" in caplog.text


def test_main_exits_nonzero_when_topic_verification_fails(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    monkeypatch.setattr(
        outbox_main,
        "verify_broker_connectivity",
        lambda **_kwargs: None,
    )

    def _fail_verify(**_kwargs: object) -> None:
        raise KafkaTopicVerificationError("TopicMissing")

    monkeypatch.setattr(outbox_main, "verify_topic_partitioning", _fail_verify)

    assert outbox_main.main() == 1
    assert _FakeProducer.instances[0].closed is True
    assert _FakeLock.instances[0].acquired is True
    assert _FakeLock.instances[0].released is True


def test_main_logs_are_sanitized_when_topic_verification_fails(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    monkeypatch.setattr(
        outbox_main, "verify_broker_connectivity", lambda **_kwargs: None
    )

    def _fail_verify(**_kwargs: object) -> None:
        raise KafkaTopicVerificationError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(outbox_main, "verify_topic_partitioning", _fail_verify)

    with caplog.at_level(logging.INFO):
        assert outbox_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "KafkaTopicVerificationError" in caplog.text


def test_main_runs_poll_loop_and_cleans_up_on_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    monkeypatch.setattr(
        outbox_main, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        outbox_main, "verify_topic_partitioning", lambda **_kwargs: None
    )

    def _immediate_shutdown_relay(**_kwargs: object) -> _FakeRelay:
        return _FakeRelay([])

    monkeypatch.setattr(outbox_main, "OutboxRelay", _immediate_shutdown_relay)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)

    # Simulate SIGINT arriving before the first poll iteration by making the
    # poll loop's own shutdown check observe True immediately: patch
    # _run_poll_loop is unnecessary since an empty outcome list plus a
    # zero-call shutdown predicate exits cleanly on the first iteration.
    monkeypatch.setattr(
        outbox_main,
        "_run_poll_loop",
        lambda relay, settings, shutdown_requested: 0,
    )

    assert outbox_main.main() == 0
    assert _FakeProducer.instances[0].closed is True
    assert _FakeLock.instances[0].released is True


def test_main_exits_nonzero_and_cleans_up_on_fatal_poll_loop_result(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    monkeypatch.setattr(
        outbox_main, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        outbox_main, "verify_topic_partitioning", lambda **_kwargs: None
    )
    monkeypatch.setattr(outbox_main, "OutboxRelay", lambda **_kwargs: _FakeRelay([]))
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        outbox_main,
        "_run_poll_loop",
        lambda relay, settings, shutdown_requested: 1,
    )

    assert outbox_main.main() == 1
    assert _FakeProducer.instances[0].closed is True
    assert _FakeLock.instances[0].released is True


# --- cleanup ordering (Slice 13C1 correction pass) ----------------------


def _patch_for_cleanup_test(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(outbox_main, "KafkaEventProducer", _FakeProducer)
    monkeypatch.setattr(outbox_main, "PostgresOutboxRelayLock", _FakeLock)
    monkeypatch.setattr(
        outbox_main, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        outbox_main, "verify_topic_partitioning", lambda **_kwargs: None
    )
    monkeypatch.setattr(outbox_main, "OutboxRelay", lambda **_kwargs: _FakeRelay([]))
    monkeypatch.setattr(
        outbox_main,
        "_run_poll_loop",
        lambda relay, settings, shutdown_requested: 0,
    )


def test_cleanup_calls_lock_release_even_if_producer_close_raises(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    _patch_for_cleanup_test(monkeypatch)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    _FakeProducer.raise_on_close = RuntimeError("close failed")

    assert outbox_main.main() == 1
    assert _FakeProducer.instances[0].closed is True
    assert _FakeLock.instances[0].released is True


def test_cleanup_restores_signal_handlers_even_if_lock_release_raises(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    _patch_for_cleanup_test(monkeypatch)
    signal_calls: list[tuple[int, object]] = []

    def _fake_signal(signum: int, handler: object) -> object:
        signal_calls.append((signum, handler))
        return None

    monkeypatch.setattr(signal, "signal", _fake_signal)
    _FakeLock.raise_on_release = RuntimeError("release failed")

    assert outbox_main.main() == 1
    assert _FakeLock.instances[0].released is True
    # 2 calls install handlers at startup; 2 more restore them at shutdown,
    # even though lock.release() raised in between.
    assert len(signal_calls) == 4
    assert {call[0] for call in signal_calls[2:]} == {signal.SIGINT, signal.SIGTERM}


def test_cleanup_attempts_both_operations_when_both_fail(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    _patch_for_cleanup_test(monkeypatch)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    _FakeProducer.raise_on_close = RuntimeError("close failed")
    _FakeLock.raise_on_release = RuntimeError("release failed")

    assert outbox_main.main() == 1
    assert _FakeProducer.instances[0].closed is True
    assert _FakeLock.instances[0].released is True


def test_main_exits_nonzero_when_cleanup_fails_despite_successful_poll_loop(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    """Even a clean ``_run_poll_loop() == 0`` must not mask a cleanup failure."""
    _patch_for_cleanup_test(monkeypatch)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    _FakeProducer.raise_on_close = RuntimeError("close failed")

    assert outbox_main.main() == 1


def test_cleanup_logs_are_sanitized_when_both_operations_fail(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _patch_for_cleanup_test(monkeypatch)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    _FakeProducer.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)
    _FakeLock.raise_on_release = RuntimeError(_SENSITIVE_MESSAGE)

    with caplog.at_level(logging.INFO):
        assert outbox_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text
