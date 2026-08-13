"""``configure_tracing``/``TracingProviderHandle`` fail-open construction and
bounded, thread-safe, idempotent shutdown (Slice 15A3 final condition #5)."""

from __future__ import annotations

import threading
import time

import pytest
from opentelemetry.sdk.trace import TracerProvider

from atlas.observability.events import Event
from atlas.observability.testing import capture_logs
from atlas.observability.tracing.provider import (
    SHUTDOWN_BOUND_SECONDS,
    TracingProviderHandle,
    configure_tracing,
)


def test_configure_tracing_succeeds_and_returns_a_bound_handle() -> None:
    handle = configure_tracing(
        service_name="atlas-api",
        deployment_environment="local",
        otlp_traces_endpoint="http://127.0.0.1:4318/v1/traces",
    )
    try:
        assert handle.bound is True
        tracer = handle.get_tracer("test-configure-tracing")
        with tracer.start_as_current_span("noop") as span:
            assert span.get_span_context().is_valid
    finally:
        handle.close()


def test_configure_tracing_is_fail_open_when_exporter_construction_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exporter/processor construction failure never raises, never blocks
    startup, and still returns a handle whose tracer creates real, locally
    valid spans -- see the module's own docstring for exactly what "fail
    open" guarantees here."""

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("otlp-exporter-secret-endpoint-failure")

    monkeypatch.setattr("atlas.observability.tracing.provider.OTLPSpanExporter", _boom)

    with capture_logs("atlas.observability.tracing.provider") as captured:
        handle = configure_tracing(
            service_name="atlas-worker",
            deployment_environment="local",
            otlp_traces_endpoint="http://127.0.0.1:4318/v1/traces",
        )
    try:
        assert handle.bound is False
        assert captured.events == [Event.TRACING_INIT_FAILED.value]
        assert "otlp-exporter-secret-endpoint-failure" not in captured.text
        record = captured.json(0)
        assert record["error_class"] == "RuntimeError"

        # Spans are still created locally even though nothing is exported.
        tracer = handle.get_tracer("test-fail-open")
        with tracer.start_as_current_span("noop") as span:
            assert span.get_span_context().is_valid
    finally:
        handle.close()


def test_unbound_handle_close_is_a_safe_immediate_noop() -> None:
    handle = TracingProviderHandle(TracerProvider(), bound=False)
    started = time.perf_counter()
    handle.close()
    elapsed = time.perf_counter() - started
    assert elapsed < 1.0


def test_close_succeeds_and_is_idempotent() -> None:
    handle = TracingProviderHandle(TracerProvider(), bound=True)
    handle.close()
    handle.close()  # must not raise or hang the second time


def test_close_logs_sanitized_event_when_shutdown_raises() -> None:
    class _RaisingProvider:
        def shutdown(self) -> None:
            raise RuntimeError("sekret-tracing-shutdown-failure")

    handle = TracingProviderHandle(_RaisingProvider(), bound=True)  # type: ignore[arg-type]
    with capture_logs("atlas.observability.tracing.provider") as captured:
        handle.close()

    assert Event.TRACING_SHUTDOWN_FAILED.value in captured.events
    assert "sekret-tracing-shutdown-failure" not in captured.text


def test_close_returns_within_its_documented_bound_when_shutdown_never_returns() -> (
    None
):
    class _WedgedProvider:
        def shutdown(self) -> None:
            threading.Event().wait()  # never returns

    handle = TracingProviderHandle(_WedgedProvider(), bound=True)  # type: ignore[arg-type]

    started = time.perf_counter()
    handle.close()
    elapsed = time.perf_counter() - started

    assert elapsed < SHUTDOWN_BOUND_SECONDS + 2.0


def test_close_is_thread_safe_and_shuts_down_exactly_once_under_concurrent_calls() -> (
    None
):
    call_count = 0
    lock = threading.Lock()

    class _CountingProvider:
        def shutdown(self) -> None:
            nonlocal call_count
            with lock:
                call_count += 1

    handle = TracingProviderHandle(_CountingProvider(), bound=True)  # type: ignore[arg-type]
    threads = [threading.Thread(target=handle.close) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=SHUTDOWN_BOUND_SECONDS + 2.0)

    assert call_count == 1
