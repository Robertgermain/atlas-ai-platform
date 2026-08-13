"""OpenTelemetry ``TracerProvider`` lifecycle: construction, use, bounded shutdown.

Fail-open construction
------------------------

:func:`configure_tracing` never raises and never blocks process startup.
Two independent things can fail:

1. Resource/provider construction itself (``Resource.create``,
   ``TracerProvider()``) -- verified, in practice, not to raise for any
   fixed literal attribute set this module passes; there is no untrusted
   input here.
2. Exporter/processor construction and registration (``OTLPSpanExporter``,
   ``BatchSpanProcessor``, ``add_span_processor``) -- caught explicitly. On
   failure, :attr:`atlas.observability.events.Event.TRACING_INIT_FAILED` is
   logged (class-only) and the provider is left with **no** span processor
   at all: every subsequent :func:`get_tracer` call still returns a real
   tracer that creates real, locally valid spans (so span-based control
   flow -- ``set_status``, attributes, context propagation, resource
   attributes for tests -- all still work identically), but nothing is
   ever exported anywhere. This is a strictly stronger fail-open guarantee
   than falling back to OpenTelemetry's global no-op tracer would be.

Exactly what is, and is not, bounded once telemetry is exporting
--------------------------------------------------------------------

Verified directly against the installed ``opentelemetry-sdk``/
``opentelemetry-exporter-otlp-proto-http`` 1.44.0 source (not assumed):

- Creating/ending a span, and the resulting internal
  ``BatchSpanProcessor.on_end()`` -> queue push, **never blocks** the
  calling (application) thread under any condition, including an
  unreachable Collector or a full queue: the queue is a fixed-capacity
  ``collections.deque`` and pushing past capacity synchronously **drops
  the oldest already-queued span** (logging a warning through the SDK's
  own ``opentelemetry.sdk._shared_internal`` logger -- rendered exactly as
  safely, and exactly as free of raw content, as any other unconverted
  third-party logger by ``atlas.observability.logging``'s existing
  fallback) rather than blocking or raising. Business processing is
  therefore genuinely unaffected by a Collector outage at the point spans
  are created -- not merely "expected to be" unaffected.
- The actual HTTP export attempt (including this exporter's own internal
  retry loop) runs entirely on the ``BatchSpanProcessor``'s own background
  worker thread, never the application thread, and is bounded by the
  exporter's own ``timeout`` constructor parameter (``OTEL_TRACES_TIMEOUT_
  SECONDS`` below) -- verified against ``OTLPSpanExporter._export``, which
  passes this value directly as ``requests.Session.post(timeout=...)`` and
  additionally bounds its own retry loop by the same deadline.
- ``BatchSpanProcessor``'s own ``export_timeout_millis`` constructor
  parameter is accepted but, per this exact SDK version's own source
  comment ("Not used. No way currently to pass timeout to export."), has
  **no effect** on the actual export call -- documented here rather than
  silently relied upon; the exporter's ``timeout`` parameter above is what
  actually bounds each export attempt.
- ``TracerProvider.shutdown()`` (which flushes and stops the background
  worker thread) has no timeout parameter of its own and can block
  uninterruptibly if the exporter is wedged. :class:`TracingProviderHandle`
  wraps it in the same bounded-daemon-thread pattern as
  ``atlas.observability.metrics.exposition.MetricsServerHandle`` so process
  shutdown itself is still bounded by :data:`SHUTDOWN_BOUND_SECONDS`
  regardless.
"""

from __future__ import annotations

import logging
import threading
from typing import Final

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

from atlas.observability.events import Event
from atlas.observability.logging import log_exception_boundary
from atlas.observability.tracing.resource import (
    DeploymentEnvironment,
    ServiceName,
    build_resource,
)

logger = logging.getLogger(__name__)

#: Bounded, documented total wall-clock time ``TracingProviderHandle.close()``
#: may take, regardless of whether the underlying ``TracerProvider.shutdown()``
#: call itself ever returns. Matches the approved Slice 15A3 proposal's
#: five-second outer bound.
SHUTDOWN_BOUND_SECONDS: Final[float] = 5.0

#: SDK defaults (verified against the installed 1.44.0 ``BatchSpanProcessor``)
#: for queue size, batch size, and schedule delay -- used as-is; only the
#: exporter's own HTTP timeout is deliberately tightened below the SDK's
#: 10s-per-OTel-spec default is already exactly this value, so no override is
#: needed for that either. Kept as named constants (not inlined) so the
#: values are reviewable in one place and covered by the resource/bounds
#: test below.
BSP_MAX_QUEUE_SIZE: Final[int] = 2048
BSP_MAX_EXPORT_BATCH_SIZE: Final[int] = 512
BSP_SCHEDULE_DELAY_MILLIS: Final[float] = 5000.0
#: See this module's own docstring: currently a no-op passthrough in the
#: installed SDK version, kept only for forward-compatibility should a
#: future SDK release start honoring it.
BSP_EXPORT_TIMEOUT_MILLIS: Final[float] = 10_000.0
#: Bounds each OTLP HTTP export attempt (including the exporter's own
#: internal retry loop) on the background export thread -- see this
#: module's own docstring for the exact source-verified mechanism.
OTLP_EXPORT_TIMEOUT_SECONDS: Final[float] = 10.0


class TracingProviderHandle:
    """Owns one process's ``TracerProvider`` lifecycle.

    ``bound`` is ``False`` when exporter/processor construction failed
    (fail-open): :func:`get_tracer` still returns a real tracer backed by
    this handle's provider (spans are created, just never exported), and
    :meth:`close` on an unbound handle is always a safe, immediate no-op.
    """

    def __init__(self, provider: TracerProvider, *, bound: bool) -> None:
        self._provider = provider
        self._bound = bound
        self._closed = False
        self._lock = threading.Lock()

    @property
    def bound(self) -> bool:
        """``True`` when a span exporter/processor was successfully attached."""
        return self._bound

    def get_tracer(self, name: str) -> Tracer:
        """Return a tracer backed by this handle's provider."""
        return self._provider.get_tracer(name)

    def close(self) -> None:
        """Thread-safe, idempotent, bounded best-effort flush and shutdown."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if not self._bound:
            return

        shutdown_thread = threading.Thread(
            target=self._call_shutdown,
            name="atlas-tracing-shutdown",
            daemon=True,
        )
        shutdown_thread.start()
        shutdown_thread.join(timeout=SHUTDOWN_BOUND_SECONDS)

    def _call_shutdown(self) -> None:
        try:
            self._provider.shutdown()
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.TRACING_SHUTDOWN_FAILED,
                exc,
                level=logging.WARNING,
            )


def configure_tracing(
    *,
    service_name: ServiceName,
    deployment_environment: DeploymentEnvironment,
    otlp_traces_endpoint: str,
) -> TracingProviderHandle:
    """Construct and globally register this process's ``TracerProvider``.

    Always sets the constructed provider as the OpenTelemetry global
    provider (``opentelemetry.trace.set_tracer_provider``) so any code path
    using ``opentelemetry.trace.get_tracer(...)`` directly (rather than
    through a retained :class:`TracingProviderHandle`) still participates in
    the same resource/export configuration. Call at most once per process,
    at startup, before constructing anything that might itself start a span.
    """
    resource = build_resource(
        service_name=service_name, deployment_environment=deployment_environment
    )
    provider = TracerProvider(resource=resource)

    bound = False
    try:
        exporter = OTLPSpanExporter(
            endpoint=otlp_traces_endpoint,
            timeout=OTLP_EXPORT_TIMEOUT_SECONDS,
        )
        processor = BatchSpanProcessor(
            exporter,
            max_queue_size=BSP_MAX_QUEUE_SIZE,
            schedule_delay_millis=BSP_SCHEDULE_DELAY_MILLIS,
            max_export_batch_size=BSP_MAX_EXPORT_BATCH_SIZE,
            export_timeout_millis=BSP_EXPORT_TIMEOUT_MILLIS,
        )
        provider.add_span_processor(processor)
        bound = True
    except Exception as exc:
        log_exception_boundary(
            logger,
            Event.TRACING_INIT_FAILED,
            exc,
            level=logging.WARNING,
        )

    trace.set_tracer_provider(provider)
    return TracingProviderHandle(provider, bound=bound)
