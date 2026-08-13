"""Strict W3C ``traceparent`` parsing, formatting, and parent-or-link resolution."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from atlas.observability.tracing.propagation import (
    current_traceparent,
    format_traceparent,
    parse_traceparent,
    resolve_parent_or_link,
    trace_and_span_id_hex,
)

_VALID = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
_VALID_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
_VALID_SPAN_ID = "00f067aa0ba902b7"


def test_parse_valid_traceparent_round_trips_through_format() -> None:
    parsed = parse_traceparent(_VALID)
    assert parsed is not None
    assert parsed.trace_flags == 0x01
    formatted = format_traceparent(
        trace_id=parsed.trace_id,
        span_id=parsed.span_id,
        trace_flags=parsed.trace_flags,
    )
    assert formatted == _VALID


def test_parse_rejects_none() -> None:
    assert parse_traceparent(None) is None


def test_parse_rejects_wrong_length() -> None:
    assert parse_traceparent(_VALID[:-1]) is None
    assert parse_traceparent(_VALID + "0") is None


def test_parse_rejects_wrong_field_count() -> None:
    assert parse_traceparent("00-" + _VALID_TRACE_ID + "-" + _VALID_SPAN_ID) is None


def test_parse_rejects_non_00_version() -> None:
    assert (
        parse_traceparent("01-" + _VALID_TRACE_ID + "-" + _VALID_SPAN_ID + "-01")
        is None
    )
    assert (
        parse_traceparent("ff-" + _VALID_TRACE_ID + "-" + _VALID_SPAN_ID + "-01")
        is None
    )


def test_parse_rejects_uppercase_hex() -> None:
    assert (
        parse_traceparent(
            "00-" + _VALID_TRACE_ID.upper() + "-" + _VALID_SPAN_ID + "-01"
        )
        is None
    )


def test_parse_rejects_non_hex_characters() -> None:
    bad_trace_id = "g" + _VALID_TRACE_ID[1:]
    assert parse_traceparent(f"00-{bad_trace_id}-{_VALID_SPAN_ID}-01") is None


def test_parse_rejects_wrong_field_widths() -> None:
    # A 31-char trace id padded back to 55 total chars via a longer flags
    # field still fails the strict per-field width check, not merely the
    # overall length check.
    short_trace_id = _VALID_TRACE_ID[:-1]
    value = f"00-{short_trace_id}-{_VALID_SPAN_ID}-011"
    assert len(value) == 55
    assert parse_traceparent(value) is None


def test_parse_rejects_all_zero_trace_id() -> None:
    assert parse_traceparent(f"00-{'0' * 32}-{_VALID_SPAN_ID}-01") is None


def test_parse_rejects_all_zero_span_id() -> None:
    assert parse_traceparent(f"00-{_VALID_TRACE_ID}-{'0' * 16}-01") is None


def test_parse_never_raises_on_arbitrary_garbage() -> None:
    for garbage in ("", "not-a-traceparent", "\x00" * 55, "😀" * 20, "-" * 55):
        assert parse_traceparent(garbage) is None


def test_current_traceparent_is_none_without_an_active_span() -> None:
    # The default global tracer (no provider configured in this test's own
    # process-wide state) never has a real, valid, recording span active.
    tracer = trace.get_tracer("test-current-traceparent-none")
    with tracer.start_as_current_span("noop"):
        pass
    # After the span ends, there is no current span again.
    assert current_traceparent() is None


def test_current_traceparent_formats_the_active_real_span() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-current-traceparent")
    with tracer.start_as_current_span("outer"):
        value = current_traceparent()
    assert value is not None
    parsed = parse_traceparent(value)
    assert parsed is not None


def test_trace_and_span_id_hex_matches_the_spans_own_context() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-trace-and-span-id-hex")
    with tracer.start_as_current_span("outer") as span:
        trace_id_hex, span_id_hex = trace_and_span_id_hex(span)
    span_context = span.get_span_context()
    assert trace_id_hex == trace.format_trace_id(span_context.trace_id)
    assert span_id_hex == trace.format_span_id(span_context.span_id)


def test_resolve_parent_or_link_returns_absent_when_traceparent_is_none() -> None:
    context, links = resolve_parent_or_link(None, use_as_parent=True)
    assert context is None
    assert links == ()


def test_resolve_parent_or_link_returns_absent_when_traceparent_is_malformed() -> None:
    context, links = resolve_parent_or_link("not-a-traceparent", use_as_parent=True)
    assert context is None
    assert links == ()


def test_resolve_parent_or_link_as_parent_makes_a_child_span_share_the_trace_id() -> (
    None
):
    parsed = parse_traceparent(_VALID)
    assert parsed is not None
    context, links = resolve_parent_or_link(_VALID, use_as_parent=True)
    assert links == ()
    assert context is not None

    provider = TracerProvider()
    tracer = provider.get_tracer("test-resolve-parent")
    with tracer.start_as_current_span("child", context=context) as span:
        span_context = span.get_span_context()
    assert span_context.trace_id == parsed.trace_id
    assert span_context.span_id != parsed.span_id


def test_resolve_parent_or_link_as_link_starts_a_new_trace_with_a_link() -> None:
    parsed = parse_traceparent(_VALID)
    assert parsed is not None
    context, links = resolve_parent_or_link(_VALID, use_as_parent=False)
    assert context is None
    assert len(links) == 1
    linked_context = links[0].context
    assert linked_context.trace_id == parsed.trace_id
    assert linked_context.span_id == parsed.span_id

    provider = TracerProvider()
    tracer = provider.get_tracer("test-resolve-link")
    with tracer.start_as_current_span(
        "root", context=context, links=list(links)
    ) as span:
        span_context = span.get_span_context()
    # A new root trace -- never the same trace id as the linked context.
    assert span_context.trace_id != parsed.trace_id


def test_resolve_parent_or_link_parent_context_is_remote_non_recording() -> None:
    context, _links = resolve_parent_or_link(_VALID, use_as_parent=True)
    assert context is not None
    span_in_context = trace.get_current_span(context)
    assert isinstance(span_in_context, NonRecordingSpan)
    assert span_in_context.get_span_context().is_remote is True


def test_span_context_helper_marks_context_as_remote() -> None:
    parsed = parse_traceparent(_VALID)
    assert parsed is not None
    span_context = SpanContext(
        trace_id=parsed.trace_id,
        span_id=parsed.span_id,
        is_remote=True,
        trace_flags=TraceFlags(parsed.trace_flags),
    )
    assert span_context.is_remote is True
    assert span_context.is_valid is True
