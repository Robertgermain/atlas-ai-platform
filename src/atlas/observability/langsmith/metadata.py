"""Build LangSmith metadata from Atlas correlation context and OpenTelemetry."""

from __future__ import annotations

from typing import Any

from opentelemetry import trace

from atlas.observability.context import current_context
from atlas.observability.langsmith.redaction import filter_metadata
from atlas.observability.tracing.propagation import trace_and_span_id_hex

_CONTEXT_TO_METADATA: dict[str, str] = {
    "research_job_id": "atlas.research_job_id",
    "workflow_execution_id": "atlas.workflow_execution_id",
    "node_name": "atlas.node_name",
    "model_invocation_id": "atlas.model_invocation_id",
    "tool_invocation_id": "atlas.tool_invocation_id",
    "evaluation_run_id": "atlas.evaluation_run_id",
}


def correlation_metadata(**overrides: Any) -> dict[str, str]:
    """Return allowlisted LangSmith metadata for the current Atlas/OTel context.

    ``overrides`` win over ambient context. OpenTelemetry ids are read from
    the currently attached span when it is valid. The result is already
    filtered through the closed metadata allowlist.
    """
    raw: dict[str, Any] = {}
    context = current_context()
    for field, meta_key in _CONTEXT_TO_METADATA.items():
        value = context.get(field)
        if value:
            raw[meta_key] = value
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if span_context.is_valid:
        trace_id_hex, span_id_hex = trace_and_span_id_hex(span)
        raw.setdefault("atlas.otel_trace_id", trace_id_hex)
        raw.setdefault("atlas.otel_span_id", span_id_hex)
    raw.update(overrides)
    return filter_metadata(raw)
