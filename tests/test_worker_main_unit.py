"""Network-free unit tests for ``python -m atlas.worker`` startup sanitization.

Covers only the narrow startup-boundary fix added alongside Milestone 14
Slice 14A's portable-init correction pass: a checkpoint-schema
initialization failure (e.g. a real ``psycopg_pool.PoolTimeout`` against an
unreachable PostgreSQL) must never let a raw traceback, connection string,
or credential reach stdout/stderr. No real PostgreSQL connection is made in
this file; ``create_checkpoint_runtime``/``initialize_checkpointer_schema``
are monkeypatched.
"""

from __future__ import annotations

import signal
from pathlib import Path

import pytest
from pydantic import SecretStr

import atlas.worker.__main__ as worker_main
from atlas.config.settings import Settings
from atlas.observability.events import Event
from atlas.observability.metrics.exposition import MetricsServerHandle
from atlas.observability.testing import CapturedLogs, capture_logs

# Fake sensitive content that must never reach a log line: a credential and
# a host:port. Used only to prove log sanitization; not a real secret.
_SENSITIVE_MESSAGE = (
    "connection to server at 10.0.0.5:5432 failed: password authentication "
    "failed for user 'atlas' (hunter2)"
)
_SENSITIVE_FRAGMENTS = ("hunter2", "10.0.0.5", "password authentication")


def _assert_no_sensitive_fragments(text: str) -> None:
    for fragment in _SENSITIVE_FRAGMENTS:
        assert fragment not in text


def _rendered(captured: CapturedLogs) -> str:
    return captured.text


def _events(captured: CapturedLogs) -> list[str | None]:
    return captured.events


class _FakeCheckpointRuntime:
    instances: list[_FakeCheckpointRuntime] = []
    raise_on_close: Exception | None = None

    def __init__(self) -> None:
        self.closed = False
        _FakeCheckpointRuntime.instances.append(self)

    def close(self) -> None:
        self.closed = True
        if _FakeCheckpointRuntime.raise_on_close is not None:
            raise _FakeCheckpointRuntime.raise_on_close


@pytest.fixture(autouse=True)
def _reset_fakes() -> None:
    _FakeCheckpointRuntime.instances.clear()
    _FakeCheckpointRuntime.raise_on_close = None


class _RecordingMetricsServerHandle(MetricsServerHandle):
    """An unbound handle (no real socket) that records ``close()`` calls."""

    def __init__(self) -> None:
        super().__init__(None, None)
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        super().close()


#: Populated by ``_patched_composition`` each test run; read by tests that
#: need to assert on the fake metrics-server handle/requested port without
#: real socket binding (avoids CI port-conflict flakiness).
_metrics_ports_requested: list[int] = []
_metrics_handle = _RecordingMetricsServerHandle()


@pytest.fixture
def _patched_composition(monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings()
    monkeypatch.setattr(worker_main, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_main, "get_session_factory", lambda: object())
    monkeypatch.setattr(
        worker_main,
        "create_checkpoint_runtime",
        lambda _database_url: _FakeCheckpointRuntime(),
    )
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    _metrics_ports_requested.clear()
    global _metrics_handle
    _metrics_handle = _RecordingMetricsServerHandle()

    def _fake_start(*, port: int, metrics: object | None = None) -> MetricsServerHandle:
        del metrics
        _metrics_ports_requested.append(port)
        return _metrics_handle

    monkeypatch.setattr(worker_main, "start_metrics_http_server", _fake_start)
    return settings


def test_main_exits_nonzero_when_checkpoint_schema_init_fails(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    def _fail_init(_runtime: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    assert worker_main.main() == 1
    assert _FakeCheckpointRuntime.instances[0].closed is True


def test_main_logs_are_sanitized_when_checkpoint_schema_init_fails(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
) -> None:
    def _fail_init(_runtime: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    with capture_logs("atlas.worker.__main__") as captured:
        assert worker_main.main() == 1
    rendered = _rendered(captured)
    _assert_no_sensitive_fragments(rendered)
    assert "RuntimeError" in rendered


def test_main_exits_nonzero_on_checkpoint_schema_init_failure_of_any_class(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    """The catch is broad by design: PoolTimeout is not an OutboxError-style

    typed exception, so this must not be scoped to one exception hierarchy.
    """

    class _PoolTimeoutLookalike(Exception):
        pass

    def _fail_init(_runtime: object) -> None:
        raise _PoolTimeoutLookalike(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    assert worker_main.main() == 1


def test_cleanup_close_failure_does_not_mask_original_failure_or_crash(
    monkeypatch: pytest.MonkeyPatch,
    _patched_composition: Settings,
) -> None:
    """A close() failure during startup-failure cleanup must still return 1

    (not propagate an uncaught exception) and must not hide the original
    checkpoint-schema failure's own sanitized log line.
    """

    def _fail_init(_runtime: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)
    _FakeCheckpointRuntime.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)

    with capture_logs("atlas.worker.__main__") as captured:
        assert worker_main.main() == 1
    _assert_no_sensitive_fragments(_rendered(captured))
    # Both the original checkpoint-schema-init failure and the
    # startup-failure cleanup's own pool-close failure were logged as
    # separate, distinct lines -- neither masks the other.
    events = _events(captured)
    assert events.count(Event.STARTUP_FAILED.value) == 1
    assert events.count(Event.SHUTDOWN_CLEANUP_FAILED.value) == 1


def test_main_does_not_call_initialize_checkpointer_schema_twice(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    calls = {"n": 0}

    def _fail_init(_runtime: object) -> None:
        calls["n"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    assert worker_main.main() == 1
    assert calls["n"] == 1


def test_main_starts_and_closes_metrics_server_on_startup_failure(
    monkeypatch: pytest.MonkeyPatch, _patched_composition: Settings
) -> None:
    def _fail_init(_runtime: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    assert worker_main.main() == 1

    assert _metrics_ports_requested == [_patched_composition.metrics_port]
    assert _metrics_handle.close_calls == 1


def test_main_exits_before_checkpoint_when_live_ai_missing_langsmith_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("ATLAS_LANGSMITH_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_PROJECT", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_API_URL", raising=False)
    monkeypatch.delenv("ATLAS_LANGSMITH_TIMEOUT_MS", raising=False)
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        model_provider="openai",
        openai_api_key=SecretStr("sk-test-not-a-real-key"),
    )
    monkeypatch.setattr(worker_main, "get_settings", lambda: settings)
    called = {"checkpoint": 0}

    def _must_not_run(_database_url: str) -> _FakeCheckpointRuntime:
        called["checkpoint"] += 1
        return _FakeCheckpointRuntime()

    monkeypatch.setattr(worker_main, "create_checkpoint_runtime", _must_not_run)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)

    with capture_logs("atlas.worker.__main__") as captured:
        assert worker_main.main() == 1
    assert called["checkpoint"] == 0
    assert captured.events == [Event.STARTUP_FAILED.value]
    assert captured.json(0)["error_class"] == "LangSmithConfigurationError"
    assert "sk-test-not-a-real-key" not in captured.text
    assert "lsv2" not in captured.text
