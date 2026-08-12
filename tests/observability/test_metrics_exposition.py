"""Exposition rendering and MetricsServerHandle lifecycle."""

from __future__ import annotations

import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry

from atlas.observability.events import Event
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.observability.metrics.exposition import (
    TOTAL_SHUTDOWN_BOUND_SECONDS,
    MetricsServerHandle,
    render_metrics,
    render_metrics_safe,
    start_metrics_http_server,
)
from atlas.observability.testing import capture_logs


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_render_metrics_returns_exposition_bytes_and_content_type() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    metrics.observe_worker_claim(outcome="claimed")
    body, content_type = render_metrics(metrics)
    assert b"atlas_worker_claims_total" in body
    assert content_type == CONTENT_TYPE_LATEST


def test_render_metrics_safe_returns_exact_content_type_and_200_on_success() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    metrics.observe_worker_claim(outcome="claimed")
    body, content_type, status = render_metrics_safe(metrics)
    assert b"atlas_worker_claims_total" in body
    assert content_type == CONTENT_TYPE_LATEST
    assert status == 200


def test_render_metrics_safe_returns_sanitized_503_when_generate_latest_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``generate_latest()`` failure must never propagate a raw traceback."""
    metrics = AtlasMetrics(CollectorRegistry())

    def _boom(_metrics: AtlasMetrics) -> tuple[bytes, str]:
        raise RuntimeError("registry-secret-corruption")

    monkeypatch.setattr("atlas.observability.metrics.exposition.render_metrics", _boom)
    with capture_logs("atlas.observability.metrics.exposition") as captured:
        body, content_type, status = render_metrics_safe(metrics)

    assert status == 503
    assert content_type != CONTENT_TYPE_LATEST
    assert b"registry-secret-corruption" not in body
    assert captured.events == [Event.METRIC_EXPOSITION_FAILED.value]
    assert "registry-secret-corruption" not in captured.text
    record = captured.json(0)
    assert record["error_class"] == "RuntimeError"


def test_start_metrics_http_server_serves_the_registry_over_http() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    metrics.observe_worker_claim(outcome="claimed")
    port = _free_port()
    handle = start_metrics_http_server(
        port=port, metrics=metrics, bind_host="127.0.0.1"
    )
    try:
        assert handle.bound is True
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as response:
            payload = response.read()
        assert b"atlas_worker_claims_total" in payload
    finally:
        handle.close()


def test_close_is_idempotent_and_bounded() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    port = _free_port()
    handle = start_metrics_http_server(
        port=port, metrics=metrics, bind_host="127.0.0.1"
    )
    handle.close()
    handle.close()  # must not raise or hang the second time


def test_unbound_handle_close_is_a_safe_noop() -> None:
    handle = MetricsServerHandle(None, None)
    assert handle.bound is False
    handle.close()
    handle.close()


def test_internal_server_returns_503_and_stays_alive_for_a_later_scrape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scrape-time ``generate_latest()`` failure must not kill the server thread."""
    metrics = AtlasMetrics(CollectorRegistry())
    metrics.observe_worker_claim(outcome="claimed")
    port = _free_port()
    handle = start_metrics_http_server(
        port=port, metrics=metrics, bind_host="127.0.0.1"
    )
    try:
        assert handle.bound is True

        call_count = 0
        real_render_metrics = render_metrics

        def _flaky_render_metrics(m: AtlasMetrics) -> tuple[bytes, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("scrape-secret-failure")
            return real_render_metrics(m)

        monkeypatch.setattr(
            "atlas.observability.metrics.exposition.render_metrics",
            _flaky_render_metrics,
        )

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5)
        assert exc_info.value.code == 503
        failure_body = exc_info.value.read()
        assert b"scrape-secret-failure" not in failure_body

        # The server thread must still be alive and serve a later, successful
        # scrape rather than having crashed on the first failure.
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as response:
            assert response.status == 200
            assert b"atlas_worker_claims_total" in response.read()
    finally:
        handle.close()


class _BlockingShutdownServer:
    """Fake server whose ``shutdown()``/``server_close()`` can each be made
    to block and/or raise independently, simulating a stuck or failing
    request handler on either call."""

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

    @property
    def server_close_called(self) -> bool:
        return self.server_close_calls > 0

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


class _RaisingShutdownServer:
    """Fake server whose ``shutdown()`` raises with a sensitive message."""

    def __init__(self, message: str) -> None:
        self._message = message
        self.server_close_called = False

    def shutdown(self) -> None:
        raise RuntimeError(self._message)

    def server_close(self) -> None:
        self.server_close_called = True


def test_close_returns_within_its_documented_bound_when_shutdown_blocks_forever() -> (
    None
):
    server = _BlockingShutdownServer(shutdown_block_seconds=999.0)
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    started = time.perf_counter()
    handle.close()
    elapsed = time.perf_counter() - started

    assert elapsed < TOTAL_SHUTDOWN_BOUND_SECONDS + 1.0
    # Best-effort server_close is still attempted -- on its own independent
    # bounded helper thread -- even though shutdown() itself never returned
    # within its own separate bounded helper thread.
    assert server.server_close_called is True


def test_close_bounded_when_server_close_blocks_forever() -> None:
    """A wedged ``server_close()`` alone (``shutdown()`` returns normally)
    must not make ``close()`` exceed its documented total bound either."""
    server = _BlockingShutdownServer(server_close_block_seconds=999.0)
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    started = time.perf_counter()
    handle.close()
    elapsed = time.perf_counter() - started

    assert elapsed < TOTAL_SHUTDOWN_BOUND_SECONDS + 1.0
    assert server.shutdown_calls == 1
    # The close helper thread was started (and is left running as a daemon,
    # abandoned once its own bound elapses) -- this proves the attempt was
    # made, not that it completed.
    assert server.server_close_calls == 1


def test_close_returns_within_its_documented_bound_when_both_block_forever() -> None:
    """The worst case: neither call ever returns. ``close()`` is still bounded
    by the sum of its two independent per-call bounds, not left unbounded."""
    server = _BlockingShutdownServer(
        shutdown_block_seconds=999.0, server_close_block_seconds=999.0
    )
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    started = time.perf_counter()
    handle.close()
    elapsed = time.perf_counter() - started

    assert elapsed < TOTAL_SHUTDOWN_BOUND_SECONDS + 1.0


def test_close_logs_sanitized_event_when_server_close_raises() -> None:
    """A ``server_close()`` failure must be sanitized exactly like a
    ``shutdown()`` failure -- class name only, no raw exception text."""
    server = _BlockingShutdownServer(
        server_close_raises=RuntimeError("sekret-server-close-failure")
    )
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    with capture_logs("atlas.observability.metrics.exposition") as captured:
        handle.close()

    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1
    assert Event.METRICS_SERVER_SHUTDOWN_FAILED.value in captured.events
    assert "sekret-server-close-failure" not in captured.text


def test_close_is_thread_safe_and_shuts_down_exactly_once_under_concurrent_calls() -> (
    None
):
    server = _BlockingShutdownServer(shutdown_block_seconds=0.05)
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    threads = [threading.Thread(target=handle.close) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert server.shutdown_calls == 1
    assert server.server_close_calls == 1


def test_close_logs_sanitized_event_and_still_closes_socket_when_shutdown_raises() -> (
    None
):
    server = _RaisingShutdownServer("sekret-shutdown-failure")
    handle = MetricsServerHandle(server, None)  # type: ignore[arg-type]

    with capture_logs("atlas.observability.metrics.exposition") as captured:
        handle.close()

    assert server.server_close_called is True
    assert Event.METRICS_SERVER_SHUTDOWN_FAILED.value in captured.events
    assert "sekret-shutdown-failure" not in captured.text


def test_bind_failure_is_fail_open_and_logs_sanitized_event() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    port = _free_port()
    first = start_metrics_http_server(port=port, metrics=metrics, bind_host="127.0.0.1")
    try:
        assert first.bound is True
        with capture_logs("atlas.observability.metrics.exposition") as captured:
            second = start_metrics_http_server(
                port=port, metrics=metrics, bind_host="127.0.0.1"
            )
        try:
            assert second.bound is False
            assert captured.events == [Event.METRICS_SERVER_BIND_FAILED.value]
            record = captured.json(0)
            assert record["error_class"] == "OSError"
        finally:
            second.close()
    finally:
        first.close()
