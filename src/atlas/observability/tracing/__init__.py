"""OpenTelemetry distributed tracing (Milestone 15 Slice 15A3).

Public surface:

- :func:`~atlas.observability.tracing.provider.configure_tracing` /
  :class:`~atlas.observability.tracing.provider.TracingProviderHandle` --
  process-lifetime ``TracerProvider`` construction and bounded shutdown.
- :func:`~atlas.observability.tracing.resource.build_resource` -- fixed
  per-process resource identity.
- :mod:`atlas.observability.tracing.propagation` -- strict W3C
  ``traceparent`` (version ``00``) parsing/formatting and the
  parent-or-link resolution used at every trust boundary this slice
  propagates context across (worker claim, outbox publish, Kafka consume).
- :func:`~atlas.observability.tracing.spans.run_in_span` -- the
  ``ThreadPoolExecutor``-boundary propagation helper the worker uses.

Ordinary application code obtains a tracer the standard OpenTelemetry way,
``opentelemetry.trace.get_tracer(__name__)``, at module import time -- the
API's ``ProxyTracer`` design means this is safe to call before
:func:`configure_tracing` has run yet (it lazily binds to whichever
provider becomes global later) and requires no dependency-injection of a
handle through unrelated call sites. Only each entrypoint's ``main()``
calls :func:`configure_tracing` itself and retains the returned handle for
:meth:`~atlas.observability.tracing.provider.TracingProviderHandle.close`
at shutdown.
"""

from __future__ import annotations

from atlas.observability.tracing.propagation import (
    ParsedTraceparent,
    current_traceparent,
    format_traceparent,
    parse_traceparent,
    resolve_parent_or_link,
    trace_and_span_id_hex,
)
from atlas.observability.tracing.provider import (
    SHUTDOWN_BOUND_SECONDS,
    TracingProviderHandle,
    configure_tracing,
)
from atlas.observability.tracing.resource import (
    DeploymentEnvironment,
    ServiceName,
    build_resource,
)
from atlas.observability.tracing.spans import run_in_span

__all__ = [
    "SHUTDOWN_BOUND_SECONDS",
    "DeploymentEnvironment",
    "ParsedTraceparent",
    "ServiceName",
    "TracingProviderHandle",
    "build_resource",
    "configure_tracing",
    "current_traceparent",
    "format_traceparent",
    "parse_traceparent",
    "resolve_parent_or_link",
    "run_in_span",
    "trace_and_span_id_hex",
]
