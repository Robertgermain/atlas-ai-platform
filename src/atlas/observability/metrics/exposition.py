"""Prometheus exposition and the internal-only metrics HTTP server lifecycle.

The API exposes ``/metrics`` on its own existing ASGI port using
:func:`render_metrics_safe` directly from a FastAPI route (see
``atlas.main``) -- it never needs its own background HTTP server. The
worker, outbox relay, and Kafka consumer have no other HTTP surface, so
each starts one minimal, internal-only (never published to the host --
see ``docker-compose.yml``) HTTP server via
:func:`start_metrics_http_server` and retains the returned
:class:`MetricsServerHandle` for the rest of the process's lifetime,
closing it during every normal and partial-startup cleanup path.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from atlas.observability.events import Event
from atlas.observability.logging import log_exception_boundary
from atlas.observability.metrics.catalog import AtlasMetrics, default_metrics

logger = logging.getLogger(__name__)

#: Bounded wait for ``server.shutdown()`` itself to return, run on its own
#: helper thread precisely because ``BaseServer.shutdown()`` blocks
#: uninterruptibly until the ``serve_forever()`` loop notices -- with no
#: timeout parameter of its own. If ``shutdown()`` never returns (a stuck
#: request handler), ``close()`` still proceeds after this bound rather
#: than hanging forever.
_SHUTDOWN_CALL_TIMEOUT_SECONDS = 2.0
#: Bounded wait for ``server.server_close()`` itself to return, run on its
#: own helper thread for the identical reason as ``shutdown()`` above:
#: ``socketserver.BaseServer.server_close()`` has no timeout parameter and
#: (via the underlying socket close) is not guaranteed to return promptly
#: on every platform/condition. Issued on its own thread, independent of
#: whether the ``shutdown()`` helper thread above ever finished, so a
#: wedged ``shutdown()`` can never prevent ``server_close()`` from being
#: attempted within its own separate bound.
_SERVER_CLOSE_TIMEOUT_SECONDS = 2.0
#: Bounded wait for the original ``serve_forever()`` thread to actually
#: exit after ``shutdown()``/``server_close()`` have been attempted.
_SHUTDOWN_JOIN_TIMEOUT_SECONDS = 2.0
#: The documented total worst-case bound for ``MetricsServerHandle.close()``:
#: the sum of the three waits above. ``close()`` never blocks past this,
#: regardless of whether ``shutdown()``, ``server_close()``, either, both,
#: or neither ever return.
TOTAL_SHUTDOWN_BOUND_SECONDS = (
    _SHUTDOWN_CALL_TIMEOUT_SECONDS
    + _SERVER_CLOSE_TIMEOUT_SECONDS
    + _SHUTDOWN_JOIN_TIMEOUT_SECONDS
)

#: Sanitized fixed body/content-type for a failed scrape. Never the raw
#: ``generate_latest()`` exception -- see :func:`render_metrics_safe`.
_EXPOSITION_FAILURE_BODY: bytes = b"metrics temporarily unavailable\n"
_EXPOSITION_FAILURE_CONTENT_TYPE = "text/plain; charset=utf-8"


class MetricsServerBindError(Exception):
    """Raised internally for a metrics HTTP server construction failure.

    Never escapes :func:`start_metrics_http_server` itself -- callers
    always receive a :class:`MetricsServerHandle` (fail-open); this type
    exists only to give the internal bind/start failure path one
    sanitized, class-only exception to log via
    :attr:`atlas.observability.events.Event.METRICS_SERVER_BIND_FAILED`.
    """


class _QuietWSGIRequestHandler(WSGIRequestHandler):
    """Suppresses per-request stderr logging; nothing sensitive, just quieter."""

    def log_message(self, format_: str, *args: object) -> None:
        del format_, args


def render_metrics(metrics: AtlasMetrics) -> tuple[bytes, str]:
    """Return ``(body, content_type)`` for one exposition-format scrape.

    May raise if ``generate_latest()`` itself raises (e.g. a corrupted
    collector registered elsewhere in the process). Callers that must
    never propagate that raw exception use :func:`render_metrics_safe`
    instead; this function is kept as the narrow, exception-transparent
    primitive for tests and any future caller that wants to handle a
    failure itself.
    """
    return generate_latest(metrics.registry), CONTENT_TYPE_LATEST


def render_metrics_safe(metrics: AtlasMetrics) -> tuple[bytes, str, int]:
    """Return ``(body, content_type, status)`` for one scrape; never raises.

    A ``generate_latest()`` failure is caught, logged via
    :attr:`atlas.observability.events.Event.METRIC_EXPOSITION_FAILED`
    (class-only, no raw exception text), and reported as a sanitized
    ``503`` with a fixed body -- never a raw traceback. This is the
    boundary both the API's ``/metrics`` route and each role's internal
    metrics HTTP server call: neither may let an exposition failure
    propagate into a crashed request/response or a dead server thread,
    and ordinary application processing elsewhere is entirely unaffected
    either way.
    """
    try:
        body, content_type = render_metrics(metrics)
    except Exception as exc:
        log_exception_boundary(
            logger,
            Event.METRIC_EXPOSITION_FAILED,
            exc,
            level=logging.WARNING,
        )
        return _EXPOSITION_FAILURE_BODY, _EXPOSITION_FAILURE_CONTENT_TYPE, 503
    return body, content_type, 200


class MetricsServerHandle:
    """Owns the lifecycle of one internal-only metrics HTTP server thread.

    ``bound`` is ``False`` when construction failed (fail-open): the
    caller's entrypoint continues without a metrics endpoint, and
    ``close()`` on an unbound handle is always a safe no-op.

    ``close()`` is thread-safe and idempotent (a ``threading.Lock``
    guards the closed check-and-set, so concurrent callers race safely
    and only the first performs the actual shutdown work) and its total
    wall-clock time is bounded by :data:`TOTAL_SHUTDOWN_BOUND_SECONDS`
    regardless of whether the underlying ``server.shutdown()`` and
    ``server.server_close()`` calls themselves ever return: *both* are
    each issued on their own bounded helper thread rather than inline,
    because neither ``BaseServer.shutdown()`` nor ``server_close()`` has
    a timeout parameter and either can block uninterruptibly. The
    ``server_close()`` attempt is unconditional and independent of the
    ``shutdown()`` attempt's outcome -- a wedged ``shutdown()`` still
    lets ``server_close()`` run (on its own thread, within its own
    separate bound) rather than skipping it.
    """

    def __init__(
        self,
        server: WSGIServer | None,
        thread: threading.Thread | None,
    ) -> None:
        self._server = server
        self._thread = thread
        self._closed = False
        self._lock = threading.Lock()

    @property
    def bound(self) -> bool:
        """``True`` when a socket is bound and the server thread is running."""
        return self._server is not None

    def close(self) -> None:
        """Thread-safe, idempotent, bounded shutdown. Safe even if never bound."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            server = self._server
            thread = self._thread
        if server is None:
            return

        shutdown_thread = threading.Thread(
            target=self._call_shutdown,
            args=(server,),
            name="atlas-metrics-server-shutdown",
            daemon=True,
        )
        shutdown_thread.start()
        shutdown_thread.join(timeout=_SHUTDOWN_CALL_TIMEOUT_SECONDS)

        # A second, independent bounded helper thread -- deliberately not a
        # plain inline call. ``server_close()`` gets its own bound and its
        # own daemon thread so it is attempted (and itself bounded) even
        # when the ``shutdown()`` helper thread above never returned.
        close_thread = threading.Thread(
            target=self._call_server_close,
            args=(server,),
            name="atlas-metrics-server-close",
            daemon=True,
        )
        close_thread.start()
        close_thread.join(timeout=_SERVER_CLOSE_TIMEOUT_SECONDS)

        if thread is not None:
            thread.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)

    @staticmethod
    def _call_shutdown(server: WSGIServer) -> None:
        """Run ``server.shutdown()`` on its own thread so a blocked/failing
        call cannot make :meth:`close` exceed its documented bound; any
        exception is contained and logged with only its class name."""
        try:
            server.shutdown()
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.METRICS_SERVER_SHUTDOWN_FAILED,
                exc,
                level=logging.WARNING,
            )

    @staticmethod
    def _call_server_close(server: WSGIServer) -> None:
        """Run ``server.server_close()`` on its own thread for the identical
        reason as :meth:`_call_shutdown` -- a blocked/failing close cannot
        make :meth:`close` exceed its documented bound; any exception is
        contained and logged with only its class name."""
        try:
            server.server_close()
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.METRICS_SERVER_SHUTDOWN_FAILED,
                exc,
                level=logging.WARNING,
            )


def start_metrics_http_server(
    *,
    port: int,
    metrics: AtlasMetrics | None = None,
    bind_host: str = "0.0.0.0",
) -> MetricsServerHandle:
    """Bind and serve ``metrics`` over HTTP on a daemon thread.

    Fail-open: a bind failure (``OSError``, e.g. the port is already in
    use) logs :attr:`~atlas.observability.events.Event.
    METRICS_SERVER_BIND_FAILED` and returns a handle with ``bound is
    False`` rather than raising -- the caller's entrypoint continues
    without a metrics endpoint. If the socket binds but starting the
    server thread itself fails, the partially initialized socket is
    closed before returning the same fail-open, unbound handle.

    A per-scrape ``generate_latest()`` failure inside ``_app`` is
    contained by :func:`render_metrics_safe`: the WSGI app returns a
    sanitized ``503`` for that one request and the server thread itself
    keeps running, available for a later, successful scrape.
    """
    resolved = metrics or default_metrics()

    def _app(
        environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], object],
    ) -> list[bytes]:
        del environ
        body, content_type, status = render_metrics_safe(resolved)
        status_line = "200 OK" if status == 200 else "503 Service Unavailable"
        start_response(status_line, [("Content-Type", content_type)])
        return [body]

    try:
        server = make_server(
            bind_host, port, _app, handler_class=_QuietWSGIRequestHandler
        )
    except OSError as exc:
        log_exception_boundary(
            logger,
            Event.METRICS_SERVER_BIND_FAILED,
            exc,
            level=logging.WARNING,
        )
        return MetricsServerHandle(None, None)

    thread = threading.Thread(
        target=server.serve_forever,
        name="atlas-metrics-server",
        daemon=True,
    )
    try:
        thread.start()
    except Exception as exc:
        try:
            server.server_close()
        except Exception:
            pass
        log_exception_boundary(
            logger,
            Event.METRICS_SERVER_BIND_FAILED,
            exc,
            level=logging.WARNING,
        )
        return MetricsServerHandle(None, None)

    return MetricsServerHandle(server, thread)
