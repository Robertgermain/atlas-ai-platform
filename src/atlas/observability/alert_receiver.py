"""Atlas-owned internal Alertmanager webhook receiver (Slice 15A3).

A credential-free, stdlib-only HTTP receiver for locally verifying
Alertmanager's fire -> route -> resolve behavior. Deliberately not a
generic webhook echo (the ``mendhak/http-https-echo`` image this replaces
logs complete request headers/bodies): this module never logs a request
body, header, URL, label, annotation, or alert fingerprint. It records a
fixed structured event with only a bounded, already-approved ``outcome``
label, and separately retains a small bounded set of per-alert fields
(``alertname``/``fingerprint``/``status``) in an in-memory ring buffer,
exposed only through this same process's own ``GET /received`` endpoint --
never through the structured log. Internal-only: never published to the
host (see ``docker-compose.yml``); reuses the existing hardened shared
backend image (no seventh container image).

Endpoints
---------

- ``POST /webhook`` -- Alertmanager's configured webhook receiver URL.
  Rejects a missing/non-numeric/oversized ``Content-Length`` or a
  ``Transfer-Encoding`` header (chunked transfer is not supported) before
  reading any request body at all. A malformed/non-JSON/non-object body is
  rejected with a safe ``400`` and never partially recorded.
- ``GET /received`` -- returns the current ring-buffer contents as JSON,
  for a test to assert fire/resolve was actually delivered.
- ``GET /health`` -- liveness probe.

Every other method/path returns ``404``.

``main()`` startup/shutdown hardening (consistent with ``atlas.worker.
__main__``/``atlas.outbox.__main__``/``atlas.consumer.__main__``): a
server bind or thread-start failure is caught and logged via
:attr:`~atlas.observability.events.Event.STARTUP_FAILED` (class-only,
never the raw exception, address, or environment value) and exits ``1``;
signal-handler installation is all-or-nothing (a SIGTERM-installation
failure immediately restores the just-installed SIGINT handler before
propagating); every successfully installed handler is restored on every
exit path; and :class:`AlertReceiverHandle`'s cleanup is thread-safe,
idempotent, and bounded by :data:`TOTAL_SHUTDOWN_BOUND_SECONDS` even if
the underlying ``WSGIServer.shutdown()``/``server_close()``/thread join
stalls or raises. :meth:`AlertReceiverHandle.close` returns a success/
failure result so :func:`_cleanup` and :func:`main` can report an
unsuccessful shutdown (``process_stopped`` outcome ``1``, exit ``1``)
rather than claiming success when cleanup failed. A cleanup failure is
logged separately (fixed event, class name only) and never masks an
already-classified startup failure.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from collections.abc import Callable
from types import FrameType
from typing import Final, Protocol, cast
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from atlas.observability.events import Event
from atlas.observability.logging import (
    configure_logging,
    log_event,
    log_exception_boundary,
)

logger = logging.getLogger(__name__)

#: The exact type ``signal.signal()`` accepts/returns: a callable handler,
#: ``signal.SIG_DFL``/``signal.SIG_IGN`` (both plain ``int``), or ``None``
#: (a signal previously set outside Python). Used to type the previous
#: handlers captured/restored by :func:`main`/:func:`_cleanup` below.
SignalHandler = Callable[[int, FrameType | None], object] | int | None

#: Bounded wait for ``WSGIServer.shutdown()`` itself to return, run on its
#: own helper thread precisely because ``BaseServer.shutdown()`` blocks
#: uninterruptibly until the ``serve_forever()`` loop notices -- with no
#: timeout parameter of its own. Mirrors
#: ``atlas.observability.metrics.exposition.MetricsServerHandle``'s
#: identical bounded-helper-thread pattern.
_SHUTDOWN_CALL_TIMEOUT_SECONDS: Final[float] = 2.0
#: Bounded wait for ``WSGIServer.server_close()`` itself to return, run on
#: its own independent helper thread for the identical reason as above --
#: attempted (and itself bounded) even when the ``shutdown()`` helper
#: thread never returned.
_SERVER_CLOSE_TIMEOUT_SECONDS: Final[float] = 2.0
#: Bounded wait for the original ``serve_forever()`` thread to actually
#: exit after ``shutdown()``/``server_close()`` have been attempted.
_SHUTDOWN_JOIN_TIMEOUT_SECONDS: Final[float] = 2.0
#: The documented total worst-case bound for ``AlertReceiverHandle.close()``:
#: the sum of the three waits above. ``close()`` never blocks past this,
#: regardless of whether ``shutdown()``, ``server_close()``, either, both,
#: or neither ever return.
TOTAL_SHUTDOWN_BOUND_SECONDS: Final[float] = (
    _SHUTDOWN_CALL_TIMEOUT_SECONDS
    + _SERVER_CLOSE_TIMEOUT_SECONDS
    + _SHUTDOWN_JOIN_TIMEOUT_SECONDS
)

#: Refuses to read a webhook body larger than this many bytes. Alertmanager's
#: own webhook payloads (a handful of alerts with short label/annotation
#: values) are always far smaller than this in any locally testable
#: scenario; this exists to bound memory/CPU for a single request, not to
#: accommodate an expected large payload.
MAX_BODY_BYTES: Final[int] = 65_536

#: Bounded ring-buffer capacity: oldest entries are discarded once full,
#: never grows unbounded across a long-running local session.
_RING_BUFFER_CAPACITY: Final[int] = 200

#: The exact bounded set of per-alert ``status`` values Alertmanager itself
#: sends (both at the payload level and per-alert level). Anything else is
#: normalized to ``"other"`` -- never rendered verbatim into a log line
#: regardless (see ``ReceivedAlert``'s own docstring), but keeping the
#: *stored* value itself bounded too avoids an unbounded ring-buffer field.
_KNOWN_STATUSES: Final[frozenset[str]] = frozenset({"firing", "resolved"})

#: Bounded storage length for the two alert-supplied ring-buffer fields
#: below (``alertname``/``fingerprint``) -- defense in depth against an
#: oversized label value consuming unbounded memory, independent of the
#: overall request-body size cap.
_MAX_STORED_FIELD_LENGTH: Final[int] = 256


class _ReadableInput(Protocol):
    """Structural type for WSGI's ``environ["wsgi.input"]``: a binary stream
    exposing at least ``read(size)`` -- never assumed to be a concrete
    ``BinaryIO`` (a test double only needs to satisfy this shape)."""

    def read(self, size: int, /) -> bytes: ...


class _ReceivedAlertRingBuffer:
    """Thread-safe, fixed-capacity buffer of bounded per-alert records.

    Never holds more than ``_RING_BUFFER_CAPACITY`` entries; appending past
    capacity discards the oldest entry first (a ring, not an unbounded
    log). Each entry is a plain ``dict`` with exactly three string keys
    (``alertname``, ``fingerprint``, ``status``) -- never the original
    request body, headers, annotations, or any other field.
    """

    def __init__(self, *, capacity: int = _RING_BUFFER_CAPACITY) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._entries: list[dict[str, str]] = []

    def append(self, *, alertname: str, fingerprint: str, status: str) -> None:
        entry = {
            "alertname": _bounded(alertname),
            "fingerprint": _bounded(fingerprint),
            "status": status if status in _KNOWN_STATUSES else "other",
        }
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._capacity:
                del self._entries[0]

    def snapshot(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._entries)


def _bounded(value: object) -> str:
    text = value if isinstance(value, str) else ""
    return text[:_MAX_STORED_FIELD_LENGTH]


def _normalize_outcome(value: object) -> str:
    return value if isinstance(value, str) and value in _KNOWN_STATUSES else "other"


class _QuietWSGIRequestHandler(WSGIRequestHandler):
    """Suppresses per-request stderr logging; nothing sensitive, just quieter."""

    def log_message(self, format_: str, *args: object) -> None:
        del format_, args


def _reject(
    start_response: Callable[[str, list[tuple[str, str]]], object], status: str
) -> list[bytes]:
    start_response(status, [("Content-Type", "text/plain; charset=utf-8")])
    return [b""]


def _handle_webhook(
    environ: dict[str, object],
    start_response: Callable[[str, list[tuple[str, str]]], object],
    *,
    buffer: _ReceivedAlertRingBuffer,
    logger_name: str,
) -> list[bytes]:
    request_logger = logging.getLogger(logger_name)

    # Unsupported transfer encoding is rejected before any body is read --
    # this receiver only ever reads a known-length body via Content-Length.
    if environ.get("HTTP_TRANSFER_ENCODING"):
        return _reject(start_response, "501 Not Implemented")

    raw_length = environ.get("CONTENT_LENGTH")
    if not isinstance(raw_length, str) or raw_length == "":
        return _reject(start_response, "411 Length Required")
    try:
        content_length = int(raw_length)
    except ValueError:
        return _reject(start_response, "400 Bad Request")
    if content_length < 0:
        return _reject(start_response, "400 Bad Request")
    if content_length > MAX_BODY_BYTES:
        return _reject(start_response, "413 Content Too Large")

    wsgi_input = environ.get("wsgi.input")
    body = (
        cast(_ReadableInput, wsgi_input).read(content_length)
        if wsgi_input is not None
        else b""
    )
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _reject(start_response, "400 Bad Request")
    if not isinstance(payload, dict):
        return _reject(start_response, "400 Bad Request")

    outcome = _normalize_outcome(payload.get("status"))
    alerts = payload.get("alerts")
    if isinstance(alerts, list):
        for alert in alerts:
            if not isinstance(alert, dict):
                continue
            labels = alert.get("labels")
            alertname = labels.get("alertname") if isinstance(labels, dict) else None
            buffer.append(
                alertname=alertname if isinstance(alertname, str) else "",
                fingerprint=alert.get("fingerprint", ""),
                status=_normalize_outcome(alert.get("status")),
            )

    log_event(request_logger, Event.ALERT_WEBHOOK_RECEIVED, outcome=outcome)
    start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"ok"]


def _handle_received(
    start_response: Callable[[str, list[tuple[str, str]]], object],
    *,
    buffer: _ReceivedAlertRingBuffer,
) -> list[bytes]:
    body = json.dumps(buffer.snapshot()).encode("utf-8")
    start_response(
        "200 OK",
        [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
    )
    return [body]


def build_wsgi_app(
    *,
    buffer: _ReceivedAlertRingBuffer | None = None,
    logger_name: str = __name__,
) -> tuple[
    Callable[
        [dict[str, object], Callable[[str, list[tuple[str, str]]], object]],
        list[bytes],
    ],
    _ReceivedAlertRingBuffer,
]:
    """Build the WSGI app and its backing ring buffer.

    Returns ``(app, buffer)`` so a test can inspect ``buffer`` directly
    without an HTTP round trip, and :func:`start_alert_receiver` can retain
    the same instance the running server uses.
    """
    resolved_buffer = buffer if buffer is not None else _ReceivedAlertRingBuffer()

    def app(
        environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], object],
    ) -> list[bytes]:
        method = environ.get("REQUEST_METHOD")
        path = environ.get("PATH_INFO")
        if method == "POST" and path == "/webhook":
            return _handle_webhook(
                environ,
                start_response,
                buffer=resolved_buffer,
                logger_name=logger_name,
            )
        if method == "GET" and path == "/received":
            return _handle_received(start_response, buffer=resolved_buffer)
        if method == "GET" and path == "/health":
            start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"ok"]
        return _reject(start_response, "404 Not Found")

    return app, resolved_buffer


class AlertReceiverHandle:
    """Owns the lifecycle of the running alert-receiver HTTP server thread.

    ``close()`` is thread-safe and idempotent: a ``threading.Lock`` is held
    for the entire shutdown attempt so concurrent callers wait, only the
    first performs the actual work, and every later caller (including
    those that waited) receives the same stored success/failure result
    without repeating shutdown. Total wall-clock time is bounded by
    :data:`TOTAL_SHUTDOWN_BOUND_SECONDS` regardless of whether the
    underlying ``server.shutdown()`` and ``server.server_close()`` calls
    themselves ever return: *both* are each issued on their own bounded
    helper thread rather than inline, because neither
    ``BaseServer.shutdown()`` nor ``server_close()`` has a timeout
    parameter and either can block uninterruptibly. The ``server_close()``
    attempt is unconditional and independent of the ``shutdown()``
    attempt's outcome -- a wedged ``shutdown()`` still lets
    ``server_close()`` run (on its own thread, within its own separate
    bound) rather than skipping it.

    Returns ``True`` only when ``shutdown()``, ``server_close()``, and the
    serve thread all completed within their bounds without raising.
    Returns ``False`` if ``shutdown()`` raises, ``server_close()`` raises,
    the shutdown helper times out, the close helper times out, or the
    serve thread is still alive after its join bound. Every failure is
    logged as :attr:`~atlas.observability.events.Event.SHUTDOWN_CLEANUP_FAILED`
    with only a class name (``TimeoutError`` for a helper/join timeout).
    """

    def __init__(self, server: WSGIServer, thread: threading.Thread) -> None:
        self._server = server
        self._thread = thread
        self._lock = threading.Lock()
        self._close_result: bool | None = None

    def close(self) -> bool:
        """Thread-safe, idempotent, bounded shutdown.

        Returns the first close attempt's success/failure result. Later
        callers receive that stored result and perform no further work.
        """
        with self._lock:
            if self._close_result is not None:
                return self._close_result
            result = self._perform_close()
            self._close_result = result
            return result

    def _perform_close(self) -> bool:
        ok = True
        shutdown_failed = threading.Event()
        close_failed = threading.Event()

        shutdown_thread = threading.Thread(
            target=self._call_shutdown,
            args=(shutdown_failed,),
            name="atlas-alert-receiver-shutdown",
            daemon=True,
        )
        shutdown_thread.start()
        shutdown_thread.join(timeout=_SHUTDOWN_CALL_TIMEOUT_SECONDS)
        if shutdown_thread.is_alive():
            self._log_timeout()
            ok = False
        if shutdown_failed.is_set():
            ok = False

        # A second, independent bounded helper thread -- deliberately not a
        # plain inline call. ``server_close()`` gets its own bound and its
        # own daemon thread so it is attempted (and itself bounded) even
        # when the ``shutdown()`` helper thread above never returned.
        close_thread = threading.Thread(
            target=self._call_server_close,
            args=(close_failed,),
            name="atlas-alert-receiver-close",
            daemon=True,
        )
        close_thread.start()
        close_thread.join(timeout=_SERVER_CLOSE_TIMEOUT_SECONDS)
        if close_thread.is_alive():
            self._log_timeout()
            ok = False
        if close_failed.is_set():
            ok = False

        self._thread.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            self._log_timeout()
            ok = False

        return ok

    @staticmethod
    def _log_timeout() -> None:
        log_event(
            logger,
            Event.SHUTDOWN_CLEANUP_FAILED,
            level=logging.WARNING,
            error_class="TimeoutError",
        )

    def _call_shutdown(self, failed: threading.Event) -> None:
        """Run ``server.shutdown()`` on its own thread so a blocked/failing
        call cannot make :meth:`close` exceed its documented bound; any
        exception is contained and logged with only its class name."""
        try:
            self._server.shutdown()
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.SHUTDOWN_CLEANUP_FAILED,
                exc,
                level=logging.WARNING,
            )
            failed.set()

    def _call_server_close(self, failed: threading.Event) -> None:
        """Run ``server.server_close()`` on its own thread for the identical
        reason as :meth:`_call_shutdown` -- a blocked/failing close cannot
        make :meth:`close` exceed its documented bound; any exception is
        contained and logged with only its class name."""
        try:
            self._server.server_close()
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.SHUTDOWN_CLEANUP_FAILED,
                exc,
                level=logging.WARNING,
            )
            failed.set()


def start_alert_receiver(
    *, port: int, bind_host: str = "0.0.0.0"
) -> AlertReceiverHandle:
    """Bind and serve the alert receiver over HTTP on a daemon thread.

    Unlike ``atlas.observability.metrics.exposition.
    start_metrics_http_server``, this is **not** fail-open: serving this
    endpoint is this process's entire purpose, so a bind failure
    (``OSError``, e.g. the port is already in use) or a server-thread
    startup failure is raised to the caller rather than absorbed --
    :func:`main` catches it there and exits nonzero instead of running a
    process with no server at all. If the socket binds but starting the
    server thread itself fails, the partially initialized socket is
    closed (best-effort) before the original exception propagates.
    """
    app, _buffer = build_wsgi_app(logger_name=__name__)
    server = make_server(bind_host, port, app, handler_class=_QuietWSGIRequestHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="atlas-alert-receiver",
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        try:
            server.server_close()
        except Exception:
            pass
        raise
    return AlertReceiverHandle(server, thread)


#: Installed/restored in this fixed order everywhere below (SIGINT then
#: SIGTERM), matching ``atlas.consumer.__main__``'s equivalent precedent.
_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM)


def _install_signal_handlers(
    handler: Callable[[int, FrameType | None], object],
) -> tuple[dict[int, SignalHandler], Exception | None]:
    """Install ``handler`` for every signal in ``_SIGNALS``, one at a time.

    Installation is not atomic at the OS level: each successfully installed
    signal's *previous* handler is recorded in ``installed`` as it succeeds,
    so if a later signal's installation fails, the caller can still see
    (and reverse) exactly which signals were already replaced. Returns
    ``(installed, None)`` on full success, or ``(installed_so_far, exc)`` on
    the first failure -- ``exc`` is never logged directly by this function;
    the caller is responsible for sanitized logging. Identical to
    ``atlas.consumer.__main__._install_signal_handlers``.
    """
    installed: dict[int, SignalHandler] = {}
    for signum in _SIGNALS:
        try:
            installed[signum] = signal.signal(signum, handler)
        except Exception as exc:
            return installed, exc
    return installed, None


def _restore_signal_handlers(previous_handlers: dict[int, SignalHandler]) -> bool:
    """Best-effort restore of every given previously-installed signal handler.

    Each signal is restored independently: a failure restoring one must
    never prevent attempting the other, and no restoration failure may ever
    escape as an uncaught exception. Returns ``True`` only if every
    restoration succeeded.
    """
    all_ok = True
    for signum, previous_handler in previous_handlers.items():
        try:
            signal.signal(signum, previous_handler)
        except Exception as exc:
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
            all_ok = False
    return all_ok


def _cleanup(
    *,
    handle: AlertReceiverHandle,
    previous_handlers: dict[int, SignalHandler],
) -> bool:
    """Best-effort shutdown. Returns ``True`` only if every step succeeded.

    Server close and signal-handler restoration are both always attempted
    regardless of whether an earlier step failed. ``AlertReceiverHandle.
    close()`` already contains its own internal exceptions and returns
    ``False`` on any shutdown failure (see its docstring); the outer
    ``try`` here is defense in depth, matching
    ``atlas.consumer.__main__``'s identical cleanup shape.
    """
    cleanup_ok = True
    try:
        if not handle.close():
            cleanup_ok = False
    except Exception as exc:
        log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, exc)
        cleanup_ok = False

    if not _restore_signal_handlers(previous_handlers):
        cleanup_ok = False

    return cleanup_ok


def main() -> int:
    """Run the alert receiver until interrupted. Internal-only; no port published.

    Startup order (fails closed, nonzero exit, on any step): settings/port
    parsing (never fails -- an invalid/missing value falls back to the
    fixed default port, matching this module's pre-existing behavior),
    then binding and starting the HTTP server, then installing SIGINT/
    SIGTERM handlers one at a time (not assumed atomic -- see
    ``_install_signal_handlers``). A failure in either of the latter two
    steps is caught here, logged via a fixed event and class-only
    exception, and returns ``1`` without ever leaving a partially
    installed signal handler behind or letting a cleanup failure mask the
    original failure.
    """
    configure_logging(service_role="alert-receiver")
    port_env = os.environ.get("ATLAS_ALERT_RECEIVER_PORT", "9465")
    try:
        port = int(port_env)
    except ValueError:
        port = 9465

    try:
        handle = start_alert_receiver(port=port)
    except Exception as exc:
        log_exception_boundary(logger, Event.STARTUP_FAILED, exc)
        return 1

    shutdown_requested = threading.Event()

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        del signum
        log_event(logger, Event.SIGNAL_RECEIVED)
        shutdown_requested.set()

    installed_handlers, install_error = _install_signal_handlers(_handle_signal)
    if install_error is not None:
        log_exception_boundary(logger, Event.STARTUP_FAILED, install_error)
        # Reverse whichever signals were already replaced before the
        # failure, then close the server -- both are independent
        # best-effort steps and neither may mask this classification.
        _restore_signal_handlers(installed_handlers)
        try:
            handle.close()
        except Exception as close_exc:
            log_exception_boundary(logger, Event.SHUTDOWN_CLEANUP_FAILED, close_exc)
        return 1

    log_event(logger, Event.PROCESS_STARTED)
    shutdown_requested.wait()

    cleanup_ok = _cleanup(handle=handle, previous_handlers=installed_handlers)
    log_event(logger, Event.PROCESS_STOPPED, outcome=str(0 if cleanup_ok else 1))
    return 0 if cleanup_ok else 1


if __name__ == "__main__":
    sys.exit(main())
