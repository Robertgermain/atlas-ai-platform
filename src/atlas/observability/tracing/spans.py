"""Span-scoped propagation helper for work executed on another thread.

``contextvars`` (Atlas's own correlation context) and OpenTelemetry's own
``Context`` are both thread-local by design (see
``atlas.observability.context``'s module docstring); neither crosses a
plain ``threading.Thread``/``ThreadPoolExecutor`` boundary automatically.
:func:`run_in_span` is the one place that boundary is crossed explicitly and
symmetrically for both at once, on the thread that actually does the work --
never on the submitting thread, which never attaches anything and therefore
has nothing to leak regardless of how :func:`run_in_span` itself behaves.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

import opentelemetry.context as otel_context_api
from opentelemetry.context import Context
from opentelemetry.trace import Span, Status, StatusCode

from atlas.observability.context import bind_context


def run_in_span[T](
    *,
    span: Span,
    otel_context: Context,
    atlas_fields: Mapping[str, str],
    fn: Callable[[], T],
) -> T:
    """Run ``fn`` with ``otel_context`` attached and ``atlas_fields`` bound.

    Owns ``span``'s entire remaining lifecycle. On any exception from
    ``fn``, records a class-only error class and ``ERROR`` status on
    ``span`` before re-raising unchanged (callers observe exactly the same
    exception they would without tracing). ``span.end()`` and the OTel
    context detach both always run -- in a nested ``finally``, regardless of
    whether ``fn`` raises, returns, or ``span.end()`` itself raises -- so a
    reused thread (e.g. a single-worker ``ThreadPoolExecutor`` processing
    sequential jobs) is guaranteed no OTel or Atlas correlation-context
    leakage between calls: both are always restored to their pre-call state
    before this returns, exactly once, never left attached/bound on a
    thread that goes on to do unrelated work.
    """
    token = otel_context_api.attach(otel_context)
    try:
        with bind_context(**atlas_fields):
            try:
                return fn()
            except Exception as exc:
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute("error.class", exc.__class__.__name__)
                raise
    finally:
        try:
            span.end()
        finally:
            otel_context_api.detach(token)
