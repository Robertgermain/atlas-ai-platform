"""Network-free unit tests for ``atlas.observability.alert_receiver``'s
startup/shutdown hardening (Slice 15A3 final correction pass), plus one
real-socket/real-signal integration-style test for normal SIGTERM
behavior. Mirrors ``tests/consumer/test_main_unit.py``'s equivalent
signal-installation/cleanup test shape for full consistency across the
worker/outbox-relay/consumer/alert-receiver executable boundaries.

No real PostgreSQL, Redis, or Kafka connection is made in this file.
"""

from __future__ import annotations

import os
import signal
import socket
import threading
import time
from collections.abc import Callable

import pytest

from atlas.observability import alert_receiver
from atlas.observability.alert_receiver import (
    TOTAL_SHUTDOWN_BOUND_SECONDS,
    AlertReceiverHandle,
)
from atlas.observability.events import Event
from atlas.observability.testing import CapturedLogs, capture_logs

# Fake sensitive content that must never reach a log line: a credential, an
# address, and an environment-derived value. Used only to prove log
# sanitization; none of it is a real secret.
_SENSITIVE_MESSAGE = (
    "bind failed for 10.0.0.5:9465 with token sekret-alert-receiver-token "
    "ATLAS_ALERT_RECEIVER_PORT=10.0.0.5:9465"
)
_SENSITIVE_FRAGMENTS = ("10.0.0.5", "sekret-alert-receiver-token")


def _assert_no_sensitive_fragments(text: str) -> None:
    for fragment in _SENSITIVE_FRAGMENTS:
        assert fragment not in text


def _rendered(captured: CapturedLogs) -> str:
    return captured.text


def _events(captured: CapturedLogs) -> list[str | None]:
    return captured.events


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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


class _FakeHandle:
    """A fake ``AlertReceiverHandle`` recording ``close()`` calls."""

    def __init__(
        self,
        *,
        raise_on_close: Exception | None = None,
        close_ok: bool = True,
    ) -> None:
        self.close_calls = 0
        self._raise_on_close = raise_on_close
        self._close_ok = close_ok

    def close(self) -> bool:
        self.close_calls += 1
        if self._raise_on_close is not None:
            raise self._raise_on_close
        return self._close_ok


# --- start_alert_receiver: bind failure is not fail-open -------------------


def test_start_alert_receiver_raises_on_bind_failure() -> None:
    """Unlike the internal metrics server, a bind failure must propagate:
    serving this endpoint is this process's entire purpose."""
    port = _free_port()
    first = alert_receiver.start_alert_receiver(port=port, bind_host="127.0.0.1")
    try:
        with pytest.raises(OSError):
            alert_receiver.start_alert_receiver(port=port, bind_host="127.0.0.1")
    finally:
        first.close()


def test_main_returns_1_and_logs_startup_failed_on_bind_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATLAS_ALERT_RECEIVER_PORT", "9465")
    signal_calls: list[tuple[int, object]] = []
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: signal_calls.append((signum, handler)),
    )

    def _fail_bind(**_kwargs: object) -> AlertReceiverHandle:
        raise OSError(_SENSITIVE_MESSAGE)

    monkeypatch.setattr(alert_receiver, "start_alert_receiver", _fail_bind)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        assert alert_receiver.main() == 1

    assert _events(captured) == [Event.STARTUP_FAILED.value]
    _assert_no_sensitive_fragments(_rendered(captured))
    assert "OSError" in _rendered(captured)
    # No signal handler is ever installed when the server never started.
    assert signal_calls == []


# --- partial signal-installation failure (SIGINT ok, SIGTERM fails) -------


def test_partial_signal_install_failure_restores_the_already_installed_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SIGINT installs successfully, SIGTERM installation then fails: the
    already-replaced SIGINT handler must be restored, not left dangling."""
    fake_handle = _FakeHandle()
    monkeypatch.setattr(
        alert_receiver, "start_alert_receiver", lambda **_kwargs: fake_handle
    )
    fake_signal, calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    assert alert_receiver.main() == 1

    # Call 0: install SIGINT (succeeds). Call 1: install SIGTERM (fails).
    # Call 2: restore SIGINT to the value call 0 returned.
    assert len(calls) == 3
    assert calls[0][0] == signal.SIGINT
    assert calls[1][0] == signal.SIGTERM
    assert calls[2] == (signal.SIGINT, f"previous-handler-for-{signal.SIGINT}")
    # The server was fully constructed, so it must still be closed.
    assert fake_handle.close_calls == 1


def test_partial_signal_install_failure_logs_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_handle = _FakeHandle()
    monkeypatch.setattr(
        alert_receiver, "start_alert_receiver", lambda **_kwargs: fake_handle
    )
    fake_signal, _calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        assert alert_receiver.main() == 1
    rendered = _rendered(captured)
    _assert_no_sensitive_fragments(rendered)
    assert Event.STARTUP_FAILED.value in _events(captured)
    assert "RuntimeError" in rendered


def test_partial_signal_install_failure_close_failure_does_not_mask_startup_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A close() failure during the signal-install-failure path must be
    logged separately and never replace the original STARTUP_FAILED
    classification; the return code is still 1 either way."""
    fake_handle = _FakeHandle(raise_on_close=RuntimeError(_SENSITIVE_MESSAGE))
    monkeypatch.setattr(
        alert_receiver, "start_alert_receiver", lambda **_kwargs: fake_handle
    )
    fake_signal, _calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        assert alert_receiver.main() == 1
    events = _events(captured)
    assert events[0] == Event.STARTUP_FAILED.value
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in events
    _assert_no_sensitive_fragments(_rendered(captured))
    assert fake_handle.close_calls == 1


def test_partial_signal_install_failure_false_close_does_not_mask_startup_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A close() that returns False (without raising) during the
    signal-install-failure path must not replace the original
    STARTUP_FAILED classification; the return code is still 1."""
    fake_handle = _FakeHandle(close_ok=False)
    monkeypatch.setattr(
        alert_receiver, "start_alert_receiver", lambda **_kwargs: fake_handle
    )
    fake_signal, _calls = _make_fake_signal(fail_at_indices={1})
    monkeypatch.setattr(signal, "signal", fake_signal)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        assert alert_receiver.main() == 1
    events = _events(captured)
    assert events[0] == Event.STARTUP_FAILED.value
    assert Event.PROCESS_STOPPED.value not in events
    assert fake_handle.close_calls == 1


# --- normal-shutdown cleanup: restoration, ordering, failure isolation ----


def test_cleanup_restores_both_signal_handlers_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, object]] = []
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: calls.append((signum, handler))
    )
    fake_handle = _FakeHandle()
    previous_handlers = {signal.SIGINT: "prev-int", signal.SIGTERM: "prev-term"}

    ok = alert_receiver._cleanup(
        handle=fake_handle,  # type: ignore[arg-type]
        previous_handlers=previous_handlers,  # type: ignore[arg-type]
    )

    assert ok is True
    assert fake_handle.close_calls == 1
    assert calls == [(signal.SIGINT, "prev-int"), (signal.SIGTERM, "prev-term")]


def test_cleanup_restores_signal_handlers_even_if_handle_close_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_handle = _FakeHandle(raise_on_close=RuntimeError(_SENSITIVE_MESSAGE))
    fake_signal, calls = _make_fake_signal()
    monkeypatch.setattr(signal, "signal", fake_signal)
    previous_handlers = {signal.SIGINT: "prev-int", signal.SIGTERM: "prev-term"}

    with capture_logs("atlas.observability.alert_receiver") as captured:
        ok = alert_receiver._cleanup(
            handle=fake_handle,  # type: ignore[arg-type]
            previous_handlers=previous_handlers,  # type: ignore[arg-type]
        )

    assert ok is False
    assert fake_handle.close_calls == 1
    assert calls == [(signal.SIGINT, "prev-int"), (signal.SIGTERM, "prev-term")]
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    _assert_no_sensitive_fragments(_rendered(captured))


def test_cleanup_treats_a_false_close_result_as_failure_without_requiring_a_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_handle = _FakeHandle(close_ok=False)
    fake_signal, calls = _make_fake_signal()
    monkeypatch.setattr(signal, "signal", fake_signal)
    previous_handlers = {signal.SIGINT: "prev-int", signal.SIGTERM: "prev-term"}

    ok = alert_receiver._cleanup(
        handle=fake_handle,  # type: ignore[arg-type]
        previous_handlers=previous_handlers,  # type: ignore[arg-type]
    )

    assert ok is False
    assert fake_handle.close_calls == 1
    assert calls == [(signal.SIGINT, "prev-int"), (signal.SIGTERM, "prev-term")]


def test_cleanup_attempts_both_restorations_when_the_first_restoration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_handle = _FakeHandle()
    fake_signal, calls = _make_fake_signal(fail_at_indices={0})
    monkeypatch.setattr(signal, "signal", fake_signal)
    previous_handlers = {signal.SIGINT: "prev-int", signal.SIGTERM: "prev-term"}

    with capture_logs("atlas.observability.alert_receiver") as captured:
        ok = alert_receiver._cleanup(
            handle=fake_handle,  # type: ignore[arg-type]
            previous_handlers=previous_handlers,  # type: ignore[arg-type]
        )

    assert ok is False
    # Restoring SIGTERM was still attempted despite SIGINT's restoration
    # failing first.
    assert len(calls) == 2
    assert calls[1] == (signal.SIGTERM, "prev-term")
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    _assert_no_sensitive_fragments(_rendered(captured))


# --- AlertReceiverHandle.close(): thread-safe, idempotent, bounded --------


class _BlockingShutdownServer:
    """Fake server whose ``shutdown()``/``server_close()`` can each be made
    to block and/or raise independently, simulating a stuck or failing
    request handler on either call. Mirrors
    ``tests/observability/test_metrics_exposition.py``'s identical double."""

    def __init__(
        self,
        *,
        shutdown_block_seconds: float = 0.0,
        shutdown_raises: Exception | None = None,
        server_close_block_seconds: float = 0.0,
        server_close_raises: Exception | None = None,
    ) -> None:
        self._shutdown_block_seconds = shutdown_block_seconds
        self._shutdown_raises = shutdown_raises
        self._server_close_block_seconds = server_close_block_seconds
        self._server_close_raises = server_close_raises
        self.shutdown_calls = 0
        self.server_close_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self._shutdown_block_seconds:
            time.sleep(self._shutdown_block_seconds)
        if self._shutdown_raises is not None:
            raise self._shutdown_raises

    def server_close(self) -> None:
        self.server_close_calls += 1
        if self._server_close_block_seconds:
            time.sleep(self._server_close_block_seconds)
        if self._server_close_raises is not None:
            raise self._server_close_raises


def _handle_with_fake_server(server: _BlockingShutdownServer) -> AlertReceiverHandle:
    thread = threading.Thread(target=lambda: None)
    thread.start()
    thread.join()
    return AlertReceiverHandle(server, thread)  # type: ignore[arg-type]


def test_close_returns_within_its_documented_bound_when_shutdown_blocks_forever() -> (
    None
):
    server = _BlockingShutdownServer(shutdown_block_seconds=999.0)
    handle = _handle_with_fake_server(server)

    started = time.perf_counter()
    with capture_logs("atlas.observability.alert_receiver") as captured:
        result = handle.close()
    elapsed = time.perf_counter() - started

    assert result is False
    assert elapsed < TOTAL_SHUTDOWN_BOUND_SECONDS + 1.0
    assert server.server_close_calls == 1
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    assert captured.json()["error_class"] == "TimeoutError"


def test_close_returns_within_its_documented_bound_when_both_block_forever() -> None:
    server = _BlockingShutdownServer(
        shutdown_block_seconds=999.0, server_close_block_seconds=999.0
    )
    handle = _handle_with_fake_server(server)

    started = time.perf_counter()
    result = handle.close()
    elapsed = time.perf_counter() - started

    assert result is False
    assert elapsed < TOTAL_SHUTDOWN_BOUND_SECONDS + 1.0


def test_close_returns_false_when_close_helper_times_out() -> None:
    server = _BlockingShutdownServer(server_close_block_seconds=999.0)
    handle = _handle_with_fake_server(server)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        result = handle.close()

    assert result is False
    assert server.shutdown_calls == 1
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    assert captured.json()["error_class"] == "TimeoutError"


def test_close_returns_false_when_serve_thread_survives_its_join_bound() -> None:
    started = threading.Event()

    def _never_return() -> None:
        started.set()
        time.sleep(999.0)

    thread = threading.Thread(target=_never_return, daemon=True)
    thread.start()
    started.wait(timeout=2.0)
    server = _BlockingShutdownServer()
    handle = AlertReceiverHandle(server, thread)  # type: ignore[arg-type]

    with capture_logs("atlas.observability.alert_receiver") as captured:
        result = handle.close()

    assert result is False
    assert thread.is_alive()
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    assert captured.json()["error_class"] == "TimeoutError"


def test_close_logs_sanitized_event_when_shutdown_raises() -> None:
    server = _BlockingShutdownServer(
        shutdown_raises=RuntimeError("sekret-alert-receiver-shutdown-failure")
    )
    handle = _handle_with_fake_server(server)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        result = handle.close()

    assert result is False
    assert server.server_close_calls == 1
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    assert "sekret-alert-receiver-shutdown-failure" not in _rendered(captured)
    assert captured.json()["error_class"] == "RuntimeError"


def test_close_logs_sanitized_event_when_server_close_raises() -> None:
    server = _BlockingShutdownServer(
        server_close_raises=RuntimeError("sekret-alert-receiver-close-failure")
    )
    handle = _handle_with_fake_server(server)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        result = handle.close()

    assert result is False
    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1
    assert Event.SHUTDOWN_CLEANUP_FAILED.value in _events(captured)
    assert "sekret-alert-receiver-close-failure" not in _rendered(captured)
    assert captured.json()["error_class"] == "RuntimeError"


def test_close_returns_true_on_normal_success() -> None:
    server = _BlockingShutdownServer()
    handle = _handle_with_fake_server(server)

    assert handle.close() is True
    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1


def test_close_is_idempotent() -> None:
    server = _BlockingShutdownServer()
    handle = _handle_with_fake_server(server)

    first = handle.close()
    second = handle.close()  # must not raise, hang, or repeat the underlying work

    assert first is True
    assert second is True
    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1


def test_repeated_close_returns_the_first_failure_result() -> None:
    server = _BlockingShutdownServer(
        shutdown_raises=RuntimeError("sekret-alert-receiver-shutdown-failure")
    )
    handle = _handle_with_fake_server(server)

    first = handle.close()
    second = handle.close()

    assert first is False
    assert second is False
    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1


def test_close_is_thread_safe_and_shuts_down_exactly_once_under_concurrent_calls() -> (
    None
):
    server = _BlockingShutdownServer(shutdown_block_seconds=0.05)
    handle = _handle_with_fake_server(server)
    results: list[bool] = []
    results_lock = threading.Lock()

    def _call() -> None:
        result = handle.close()
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=_call) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1
    assert results == [True] * 8


def test_concurrent_close_callers_all_receive_the_first_failure_result() -> None:
    server = _BlockingShutdownServer(
        shutdown_block_seconds=0.05,
        shutdown_raises=RuntimeError("sekret-alert-receiver-shutdown-failure"),
    )
    handle = _handle_with_fake_server(server)
    results: list[bool] = []
    results_lock = threading.Lock()

    def _call() -> None:
        result = handle.close()
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=_call) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1
    assert results == [False] * 8


def test_main_exits_1_and_records_process_stopped_outcome_1_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_handle = _FakeHandle(close_ok=False)
    monkeypatch.setattr(
        alert_receiver, "start_alert_receiver", lambda **_kwargs: fake_handle
    )
    fake_signal, _calls = _make_fake_signal()
    monkeypatch.setattr(signal, "signal", fake_signal)
    monkeypatch.setattr(threading.Event, "wait", lambda self, timeout=None: True)

    with capture_logs("atlas.observability.alert_receiver") as captured:
        assert alert_receiver.main() == 1

    events = _events(captured)
    assert events[0] == Event.PROCESS_STARTED.value
    assert events[-1] == Event.PROCESS_STOPPED.value
    stopped_index = events.index(Event.PROCESS_STOPPED.value)
    assert captured.json(stopped_index)["outcome"] == "1"
    assert fake_handle.close_calls == 1


# --- normal SIGTERM behavior: real signal delivery to a real server ------


def test_main_stops_cleanly_and_restores_handlers_on_a_real_sigterm() -> None:
    """End-to-end: a real ephemeral-port server, real ``signal.signal``
    installation, and a real ``SIGTERM`` delivered from a helper thread --
    ``main()`` (running on the main thread, as Python signal delivery
    requires) must return 0, log the full expected event sequence, and
    leave no signal handler behind."""
    port = _free_port()
    original_env = os.environ.get("ATLAS_ALERT_RECEIVER_PORT")
    os.environ["ATLAS_ALERT_RECEIVER_PORT"] = str(port)
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    def _send_sigterm_shortly() -> None:
        time.sleep(0.3)
        os.kill(os.getpid(), signal.SIGTERM)

    sender = threading.Thread(target=_send_sigterm_shortly, daemon=True)
    try:
        sender.start()
        with capture_logs("atlas.observability.alert_receiver") as captured:
            exit_code = alert_receiver.main()
    finally:
        sender.join(timeout=5.0)
        if original_env is None:
            os.environ.pop("ATLAS_ALERT_RECEIVER_PORT", None)
        else:
            os.environ["ATLAS_ALERT_RECEIVER_PORT"] = original_env

    assert exit_code == 0
    events = _events(captured)
    assert events == [
        Event.PROCESS_STARTED.value,
        Event.SIGNAL_RECEIVED.value,
        Event.PROCESS_STOPPED.value,
    ]
    assert captured.json(2)["outcome"] == "0"
    assert signal.getsignal(signal.SIGINT) == original_sigint
    assert signal.getsignal(signal.SIGTERM) == original_sigterm
