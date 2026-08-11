"""Network-free unit tests for ``python -m atlas.consumer``'s sanitized
startup boundary (Slice 13C2A review correction).

Covers failures while loading settings, constructing database dependencies,
constructing the Kafka consumer, and installing shutdown signal handlers:
every one of these must yield a fixed, sanitized log line and a controlled
nonzero exit -- never an uncaught traceback, and never a leaked database
URL, Kafka bootstrap address, secret, environment value, or raw exception
message. No real PostgreSQL or Kafka connection is made in this file.
"""

from __future__ import annotations

import logging
import signal
from collections.abc import Callable, Iterator

import pytest

import atlas.consumer.__main__ as consumer_main
from atlas.config.settings import Settings
from atlas.consumer.errors import ConsumerConfigurationError

# Fake sensitive content that must never reach a log line: a credential, a
# broker address, a database URL, and an environment-derived value. Used
# only to prove log sanitization; none of it is a real secret.
_SENSITIVE_MESSAGE = (
    "postgresql://atlas:hunter2@10.0.0.5:5432/atlas_prod "
    "kafka bootstrap 203.0.113.9:9094 "
    "ATLAS_KAFKA_BOOTSTRAP_SERVERS=203.0.113.9:9094 "
    "SELECT * FROM consumer_inbox WHERE event_id='sekret-token'"
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


def _make_fake_signal(
    *, fail_at_indices: set[int] | None = None
) -> tuple[Callable[[int, object], object], list[tuple[int, object]]]:
    """Build a fake replacement for ``signal.signal``.

    Records every ``(signum, handler)`` call, in order, in the returned
    list. Raises a sensitive-message-carrying ``RuntimeError`` on any call
    whose zero-based call index is in ``fail_at_indices``; every other call
    succeeds and returns a signum-specific marker string standing in for
    "the previous handler", so a later restore call can be asserted to
    have received exactly the value an earlier install call returned.
    """
    calls: list[tuple[int, object]] = []
    fail_at = fail_at_indices or set()

    def _fake_signal(signum: int, handler: object) -> object:
        index = len(calls)
        calls.append((signum, handler))
        if index in fail_at:
            raise RuntimeError(_SENSITIVE_MESSAGE)
        return f"previous-handler-for-{signum}"

    return _fake_signal, calls


class _FakeKafkaEventConsumer:
    instances: list[_FakeKafkaEventConsumer] = []
    close_call_count = 0
    raise_on_construct: Exception | None = None
    raise_on_close: Exception | None = None

    def __init__(self, **kwargs: object) -> None:
        if _FakeKafkaEventConsumer.raise_on_construct is not None:
            # Mirrors a genuine construction failure: no instance is ever
            # returned, so it must never be appended to ``instances``.
            raise _FakeKafkaEventConsumer.raise_on_construct
        self.kwargs = kwargs
        self.closed = False
        _FakeKafkaEventConsumer.instances.append(self)

    def close(self) -> None:
        self.closed = True
        _FakeKafkaEventConsumer.close_call_count += 1
        if _FakeKafkaEventConsumer.raise_on_close is not None:
            raise _FakeKafkaEventConsumer.raise_on_close


@pytest.fixture(autouse=True)
def _reset_fake_consumer() -> Iterator[None]:
    _FakeKafkaEventConsumer.instances.clear()
    _FakeKafkaEventConsumer.close_call_count = 0
    _FakeKafkaEventConsumer.raise_on_construct = None
    _FakeKafkaEventConsumer.raise_on_close = None
    yield
    _FakeKafkaEventConsumer.instances.clear()
    _FakeKafkaEventConsumer.close_call_count = 0
    _FakeKafkaEventConsumer.raise_on_construct = None
    _FakeKafkaEventConsumer.raise_on_close = None


def _fake_build_consumer_engine(
    database_url: str,
    *,
    connect_timeout_seconds: float,
    pool_timeout_seconds: float,
    statement_timeout_seconds: float,
) -> object:
    del database_url, connect_timeout_seconds, pool_timeout_seconds
    del statement_timeout_seconds
    return object()


@pytest.fixture
def _patched_composition(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Patch every dependency past settings/engine construction with a working fake.

    Individual tests further monkeypatch one specific step to fail.
    """
    settings = Settings(kafka_bootstrap_servers="unit-test-broker:9092")
    monkeypatch.setattr(consumer_main, "get_settings", lambda: settings)
    monkeypatch.setattr(
        consumer_main, "build_consumer_engine", _fake_build_consumer_engine
    )
    monkeypatch.setattr(consumer_main, "get_session_factory", lambda _engine: object())
    monkeypatch.setattr(consumer_main, "SqlAlchemyInboxRepository", lambda: object())
    monkeypatch.setattr(
        consumer_main, "SqlAlchemyResearchJobProjectionRepository", lambda: object()
    )
    monkeypatch.setattr(
        consumer_main, "SqlAlchemyDeadLetterRepository", lambda: object()
    )
    monkeypatch.setattr(
        consumer_main, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        consumer_main, "verify_topic_partitioning", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        consumer_main,
        "_run_poll_loop",
        lambda runner, *, shutdown_requested, wait, kafka_retry_backoff_seconds: 0,
    )
    return settings


# --- settings / database dependency construction ------------------------


def test_main_exits_nonzero_when_settings_load_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_get_settings() -> Settings:
        raise RuntimeError("synthetic-settings-failure")

    monkeypatch.setattr(consumer_main, "get_settings", _fail_get_settings)

    # main() must return a controlled exit code, never raise.
    assert consumer_main.main() == 1
    # No consumer was ever constructed, so nothing should have been closed.
    assert _FakeKafkaEventConsumer.close_call_count == 0


def test_main_logs_are_sanitized_when_settings_load_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def _fail_get_settings() -> Settings:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(consumer_main, "get_settings", _fail_get_settings)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text


def test_main_exits_nonzero_when_database_engine_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(kafka_bootstrap_servers="unit-test-broker:9092")
    monkeypatch.setattr(consumer_main, "get_settings", lambda: settings)

    def _fail_build_consumer_engine(_url: str, **_kwargs: object) -> object:
        raise RuntimeError("synthetic-engine-failure")

    monkeypatch.setattr(
        consumer_main, "build_consumer_engine", _fail_build_consumer_engine
    )

    assert consumer_main.main() == 1
    assert _FakeKafkaEventConsumer.close_call_count == 0


def test_main_logs_are_sanitized_when_database_engine_construction_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    settings = Settings(kafka_bootstrap_servers="unit-test-broker:9092")
    monkeypatch.setattr(consumer_main, "get_settings", lambda: settings)

    def _fail_build_consumer_engine(_url: str, **_kwargs: object) -> object:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(
        consumer_main, "build_consumer_engine", _fail_build_consumer_engine
    )

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text


def test_main_builds_the_consumer_engine_with_the_settings_derived_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blocker 1 drift guard: the exact settings values ``atlas.consumer.timing``
    assumes are enforced must be exactly what ``main()`` passes to
    ``build_consumer_engine`` -- never a different or hardcoded value."""
    settings = Settings(
        kafka_bootstrap_servers="unit-test-broker:9092",
        consumer_db_connect_timeout_seconds=1.5,
        consumer_db_pool_timeout_seconds=2.5,
        consumer_db_statement_timeout_seconds=3.5,
    )
    monkeypatch.setattr(consumer_main, "get_settings", lambda: settings)
    captured: dict[str, object] = {}

    def _capturing_build_consumer_engine(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(
        consumer_main, "build_consumer_engine", _capturing_build_consumer_engine
    )
    monkeypatch.setattr(consumer_main, "get_session_factory", lambda _engine: object())
    monkeypatch.setattr(consumer_main, "SqlAlchemyInboxRepository", lambda: object())
    monkeypatch.setattr(
        consumer_main, "SqlAlchemyResearchJobProjectionRepository", lambda: object()
    )
    monkeypatch.setattr(
        consumer_main, "SqlAlchemyDeadLetterRepository", lambda: object()
    )
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        consumer_main, "verify_broker_connectivity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        consumer_main, "verify_topic_partitioning", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        consumer_main,
        "_run_poll_loop",
        lambda runner, *, shutdown_requested, wait, kafka_retry_backoff_seconds: 0,
    )

    assert consumer_main.main() == 0
    assert captured["url"] == settings.database_url
    assert captured["connect_timeout_seconds"] == 1.5
    assert captured["pool_timeout_seconds"] == 2.5
    assert captured["statement_timeout_seconds"] == 3.5


# --- Kafka consumer construction -----------------------------------------


def test_main_exits_nonzero_when_kafka_consumer_construction_fails(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    _FakeKafkaEventConsumer.raise_on_construct = ConsumerConfigurationError("BadConfig")
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    assert consumer_main.main() == 1
    # Construction never completed: nothing was ever appended, and no close
    # attempt should have been made against a nonexistent consumer.
    assert _FakeKafkaEventConsumer.instances == []
    assert _FakeKafkaEventConsumer.close_call_count == 0


def test_main_logs_are_sanitized_when_kafka_consumer_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _FakeKafkaEventConsumer.raise_on_construct = ConsumerConfigurationError(
        _SENSITIVE_MESSAGE
    )
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "ConsumerConfigurationError" in caplog.text


def test_main_exits_nonzero_on_unexpected_kafka_consumer_construction_error(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    """A non-``ConsumerError`` construction failure must also fail closed."""
    _FakeKafkaEventConsumer.raise_on_construct = RuntimeError("unexpected")
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    assert consumer_main.main() == 1
    assert _FakeKafkaEventConsumer.instances == []


# --- signal handler installation -----------------------------------------


def test_signal_handler_installation_failure_closes_consumer_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    def _fail_signal(signum: int, handler: object) -> object:
        raise RuntimeError("synthetic-signal-failure")

    monkeypatch.setattr(signal, "signal", _fail_signal)

    assert consumer_main.main() == 1
    # The consumer was fully constructed before signal installation failed,
    # so it must still be closed rather than leaked.
    assert _FakeKafkaEventConsumer.instances[0].closed is True
    assert _FakeKafkaEventConsumer.close_call_count == 1


def test_signal_handler_installation_failure_logs_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    def _fail_signal(signum: int, handler: object) -> object:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(signal, "signal", _fail_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text


def test_signal_handler_installation_failure_close_failure_is_also_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Even if the best-effort close during this path also fails, both
    failures must be logged, sanitized, and the process must still exit 1."""
    _FakeKafkaEventConsumer.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)

    def _fail_signal(signum: int, handler: object) -> object:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(signal, "signal", _fail_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text
    assert _FakeKafkaEventConsumer.close_call_count == 1


# --- partial signal-installation failure (SIGINT ok, SIGTERM fails) ------


def test_partial_signal_install_failure_restores_the_already_installed_handler(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    """SIGINT installs successfully, SIGTERM installation then fails: the
    already-replaced SIGINT handler must be restored, not left dangling."""
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    fake_signal, calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    assert consumer_main.main() == 1

    # Call 0: install SIGINT (succeeds). Call 1: install SIGTERM (fails).
    # Call 2: restore SIGINT to the value call 0 returned.
    assert len(calls) == 3
    assert calls[0][0] == signal.SIGINT
    assert calls[1][0] == signal.SIGTERM
    assert calls[2] == (signal.SIGINT, f"previous-handler-for-{signal.SIGINT}")
    # The consumer was fully constructed, so it must still be closed.
    assert _FakeKafkaEventConsumer.close_call_count == 1


def test_partial_signal_install_failure_logs_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    fake_signal, _calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Failed to install shutdown signal handlers" in caplog.text
    assert "RuntimeError" in caplog.text


def test_partial_signal_install_failure_with_close_failure_preserves_classification(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Even if the best-effort consumer close during this path also fails,
    restoration of the already-installed SIGINT handler must still be
    attempted, and the original "failed to install" classification (not the
    close failure) remains the primary logged reason for termination."""
    _FakeKafkaEventConsumer.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    fake_signal, calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Failed to install shutdown signal handlers" in caplog.text
    assert "Kafka consumer close failed during shutdown" in caplog.text
    # Restoration of the already-installed SIGINT handler still happened.
    assert calls[-1] == (signal.SIGINT, f"previous-handler-for-{signal.SIGINT}")
    assert _FakeKafkaEventConsumer.close_call_count == 1


# --- signal-handler restoration during normal cleanup --------------------


def test_cleanup_sigint_restoration_failure_still_attempts_sigterm_restoration(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    # Calls 0-1 install successfully; call 2 (SIGINT restore) fails.
    fake_signal, calls = _make_fake_signal(fail_at_indices={2})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Failed to restore a shutdown signal handler" in caplog.text
    # SIGTERM restoration (call 3) was still attempted despite the SIGINT
    # restoration (call 2) failing first.
    assert len(calls) == 4
    assert calls[2][0] == signal.SIGINT
    assert calls[3] == (signal.SIGTERM, f"previous-handler-for-{signal.SIGTERM}")
    assert _FakeKafkaEventConsumer.close_call_count == 1


def test_cleanup_sigterm_restoration_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    # Calls 0-1 install successfully; call 2 (SIGINT restore) succeeds;
    # call 3 (SIGTERM restore) fails.
    fake_signal, calls = _make_fake_signal(fail_at_indices={3})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Failed to restore a shutdown signal handler" in caplog.text
    assert len(calls) == 4
    assert calls[2] == (signal.SIGINT, f"previous-handler-for-{signal.SIGINT}")
    assert calls[3][0] == signal.SIGTERM


def test_cleanup_consumer_close_failure_still_attempts_both_signal_restorations(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _FakeKafkaEventConsumer.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    fake_signal, calls = _make_fake_signal()
    monkeypatch.setattr(signal, "signal", fake_signal)

    with caplog.at_level(logging.INFO):
        assert consumer_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Kafka consumer close failed during shutdown" in caplog.text
    # Both restorations still happened despite the close failure.
    assert len(calls) == 4
    assert calls[2] == (signal.SIGINT, f"previous-handler-for-{signal.SIGINT}")
    assert calls[3] == (signal.SIGTERM, f"previous-handler-for-{signal.SIGTERM}")
    assert _FakeKafkaEventConsumer.close_call_count == 1


# --- successful startup still reaches the poll loop -----------------------


def test_main_runs_poll_loop_and_cleans_up_on_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    monkeypatch.setattr(consumer_main, "KafkaEventConsumer", _FakeKafkaEventConsumer)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)

    assert consumer_main.main() == 0
    assert _FakeKafkaEventConsumer.instances[0].closed is True
