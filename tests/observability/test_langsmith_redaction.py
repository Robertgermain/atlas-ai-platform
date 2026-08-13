"""Closed metadata allowlist and hide-callback containment (Slice 15B)."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from atlas.observability.context import bind_context
from atlas.observability.langsmith import (
    ALLOWED_METADATA_KEYS,
    correlation_metadata,
    filter_metadata,
    hide_metadata,
)


def test_hide_metadata_drops_non_allowlisted_keys() -> None:
    raw = {
        "atlas.research_job_id": "job-1",
        "question": "secret user question",
        "draft": "secret draft",
        "ls_run_depth": 3,
        "atlas.otel_trace_id": "a" * 32,
    }
    hidden = hide_metadata(raw)
    assert hidden == {
        "atlas.research_job_id": "job-1",
        "atlas.otel_trace_id": "a" * 32,
    }
    assert "question" not in hidden
    assert "draft" not in hidden


def test_hide_metadata_returns_empty_dict_when_filter_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(_raw: object) -> dict[str, str]:
        raise RuntimeError("graph-state-should-not-leak")

    monkeypatch.setattr(
        "atlas.observability.langsmith.redaction.filter_metadata", _boom
    )
    assert hide_metadata({"atlas.research_job_id": "job-1"}) == {}


def test_filter_metadata_stringifies_bools_and_drops_none() -> None:
    filtered = filter_metadata(
        {
            "atlas.evaluation_passed": True,
            "atlas.evaluation_score": 0.75,
            "atlas.research_job_id": None,
            "atlas.retrieval_k": 5,
        }
    )
    assert filtered["atlas.evaluation_passed"] == "true"
    assert filtered["atlas.evaluation_score"] == "0.75"
    assert "atlas.research_job_id" not in filtered
    assert filtered["atlas.retrieval_k"] == "5"


def test_filter_metadata_truncates_long_strings() -> None:
    filtered = filter_metadata({"atlas.node_name": "n" * 400})
    assert len(filtered["atlas.node_name"]) == 256
    assert filtered["atlas.node_name"].endswith("...<truncated>")


def test_correlation_metadata_merges_context_and_otel() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("langsmith-correlation")
    span = tracer.start_span("job")
    with trace.use_span(span, end_on_exit=True):
        with bind_context(research_job_id="job-9", workflow_execution_id="exec-9"):
            meta = correlation_metadata(
                **{"atlas.node_name": "plan", "question": "must-drop"}
            )
    assert meta["atlas.research_job_id"] == "job-9"
    assert meta["atlas.workflow_execution_id"] == "exec-9"
    assert meta["atlas.node_name"] == "plan"
    assert "atlas.otel_trace_id" in meta
    assert "atlas.otel_span_id" in meta
    assert "question" not in meta
    assert set(meta) <= ALLOWED_METADATA_KEYS


def test_semantic_grader_outcome_is_allowlisted_without_claim_text() -> None:
    filtered = filter_metadata(
        {
            "atlas.semantic_grader_outcome": "quality_fail",
            "atlas.evaluation_dimension": "semantic_groundedness",
            "claim_text": "must-not-export",
            "excerpt_text": "must-not-export-either",
        }
    )
    assert filtered["atlas.semantic_grader_outcome"] == "quality_fail"
    assert "claim_text" not in filtered
    assert "excerpt_text" not in filtered
    assert "atlas.semantic_grader_outcome" in ALLOWED_METADATA_KEYS
