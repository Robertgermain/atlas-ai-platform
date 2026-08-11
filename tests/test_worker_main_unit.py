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

import logging
import signal

import pytest

import atlas.worker.__main__ as worker_main
from atlas.config.settings import Settings

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
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _fail_init(_runtime: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)

    with caplog.at_level(logging.INFO):
        assert worker_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "RuntimeError" in caplog.text


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
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A close() failure during startup-failure cleanup must still return 1

    (not propagate an uncaught exception) and must not hide the original
    checkpoint-schema failure's own sanitized log line.
    """

    def _fail_init(_runtime: object) -> None:
        raise RuntimeError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(worker_main, "initialize_checkpointer_schema", _fail_init)
    _FakeCheckpointRuntime.raise_on_close = RuntimeError(_SENSITIVE_MESSAGE)

    with caplog.at_level(logging.INFO):
        assert worker_main.main() == 1
    _assert_no_sensitive_fragments(caplog.text)
    assert "Failed to initialize the LangGraph checkpoint schema" in caplog.text
    assert "Failed to close the checkpoint connection pool" in caplog.text


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
