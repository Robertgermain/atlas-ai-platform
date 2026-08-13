"""Strict W3C ``traceparent`` (version ``00`` only) parsing and formatting.

This is the single trust boundary between "a string that arrived from
outside this process's own OpenTelemetry SDK" (a database column, a Kafka
header, an inbound HTTP header) and an in-process OpenTelemetry
``SpanContext``/``Context``/``Link``. Every parse here either returns a
fully valid, structurally checked result or ``None`` -- it never raises,
and a malformed or absent value is always treated as simply absent
telemetry, never a business-request failure.

Deliberately unsupported (all treated as "absent", per the Slice 15A3
security/compatibility contract):

- any version other than ``00``;
- ``tracestate`` (never parsed, never forwarded);
- baggage (not part of ``traceparent`` at all; never read from headers);
- a trace ID or parent span ID that is all-zero (explicitly invalid per the
  W3C Trace Context specification);
- anything that is not exactly 55 ASCII characters in the fixed
  ``00-<32 lowercase hex>-<16 lowercase hex>-<2 lowercase hex>`` shape.

External HTTP ``traceparent`` headers are untrusted telemetry input, never
authorization or idempotency input -- nothing in this module (or any of its
callers) uses a parsed value to make an authorization, idempotency, or
business-identity decision. The API's own root-span policy (see
``atlas.main``) never even calls :func:`parse_traceparent` on an inbound
request header; it always starts a new root trace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation import set_span_in_context

_VERSION = "00"
_TRACEPARENT_LENGTH = 55
_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_HEX_16 = re.compile(r"^[0-9a-f]{16}$")
_HEX_2 = re.compile(r"^[0-9a-f]{2}$")
_ALL_ZERO_32 = "0" * 32
_ALL_ZERO_16 = "0" * 16


@dataclass(frozen=True, slots=True)
class ParsedTraceparent:
    """A structurally valid, non-zero W3C version-``00`` trace context."""

    trace_id: int
    span_id: int
    trace_flags: int


def parse_traceparent(value: str | None) -> ParsedTraceparent | None:
    """Strictly parse a W3C ``traceparent`` (version ``00`` only).

    Returns ``None`` for anything not exactly matching the fixed shape --
    wrong length, wrong version, non-hex/uppercase characters, wrong field
    widths, or an all-zero trace ID/span ID. Never raises.
    """
    if value is None or len(value) != _TRACEPARENT_LENGTH:
        return None
    parts = value.split("-")
    if len(parts) != 4:
        return None
    version, trace_id_hex, span_id_hex, flags_hex = parts
    if version != _VERSION:
        return None
    if not (
        _HEX_32.match(trace_id_hex)
        and _HEX_16.match(span_id_hex)
        and _HEX_2.match(flags_hex)
    ):
        return None
    if trace_id_hex == _ALL_ZERO_32 or span_id_hex == _ALL_ZERO_16:
        return None
    return ParsedTraceparent(
        trace_id=int(trace_id_hex, 16),
        span_id=int(span_id_hex, 16),
        trace_flags=int(flags_hex, 16),
    )


def format_traceparent(*, trace_id: int, span_id: int, trace_flags: int) -> str:
    """Format a trace/span id pair as a W3C version-``00`` ``traceparent``.

    Callers only ever pass values sourced from a real OpenTelemetry
    ``SpanContext`` (see :func:`current_traceparent`), whose own
    ``format_trace_id``/``format_span_id`` already zero-pad to exactly
    32/16 lowercase hex characters -- this never produces a value that
    :func:`parse_traceparent` would reject.
    """
    return (
        f"{_VERSION}-{trace.format_trace_id(trace_id)}-"
        f"{trace.format_span_id(span_id)}-{trace_flags:02x}"
    )


def current_traceparent() -> str | None:
    """Format the currently active span's context as a ``traceparent``.

    Returns ``None`` when there is no active span, or the active span's
    context is invalid (e.g. the process-wide tracer was never configured
    and a real span was never started) -- callers treat this exactly like
    "no trace context available", never an error.
    """
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return None
    return format_traceparent(
        trace_id=span_context.trace_id,
        span_id=span_context.span_id,
        trace_flags=int(span_context.trace_flags),
    )


def _span_context_for(parsed: ParsedTraceparent) -> SpanContext:
    return SpanContext(
        trace_id=parsed.trace_id,
        span_id=parsed.span_id,
        is_remote=True,
        trace_flags=TraceFlags(parsed.trace_flags),
    )


def trace_and_span_id_hex(span: trace.Span) -> tuple[str, str]:
    """Format a span's own trace/span IDs as lowercase hex.

    Used to bind the existing structured-log ``trace_id``/``span_id``
    correlation-context fields (see ``atlas.observability.context``) for the
    duration of that span, never to construct a new ``traceparent`` string
    (use :func:`current_traceparent` for that).
    """
    span_context = span.get_span_context()
    return (
        trace.format_trace_id(span_context.trace_id),
        trace.format_span_id(span_context.span_id),
    )


def resolve_parent_or_link(
    traceparent: str | None, *, use_as_parent: bool
) -> tuple[Context | None, tuple[Link, ...]]:
    """Resolve a stored ``traceparent`` into either a live parent or a link.

    This is the one place the "direct parent vs. Span Link" decision is
    applied -- callers (the worker, the outbox relay, the Kafka consumer)
    pass in the already-resolved ``use_as_parent`` boolean from the
    persistence layer (see ``atlas.persistence.repositories.research_job.
    claim_next`` for the worker's case); this function does not itself
    decide eligibility.

    Returns ``(None, ())`` when ``traceparent`` is absent or fails strict
    parsing -- the caller then starts an ordinary new root span with no
    parent and no link, never a failure.
    """
    parsed = parse_traceparent(traceparent)
    if parsed is None:
        return None, ()
    span_context = _span_context_for(parsed)
    if use_as_parent:
        return set_span_in_context(NonRecordingSpan(span_context)), ()
    return None, (Link(span_context),)
