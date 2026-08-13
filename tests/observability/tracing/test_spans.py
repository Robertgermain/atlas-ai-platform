"""``run_in_span`` cross-thread propagation and exactly-once cleanup
(Slice 15A3 final condition #11)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import cast

import opentelemetry.context as otel_context_api
import pytest
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Span as SDKSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Span, StatusCode, Tracer

from atlas.observability.context import bind_context, current_context
from atlas.observability.tracing.spans import run_in_span


def _start_span_and_context(tracer: Tracer, name: str) -> tuple[Span, Context]:
    span = tracer.start_span(name)
    context = trace.set_span_in_context(span)
    return span, context


def test_run_in_span_returns_the_function_result() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-result")
    span, context = _start_span_and_context(tracer, "job")

    result = run_in_span(
        span=span, otel_context=context, atlas_fields={}, fn=lambda: 42
    )

    assert result == 42


def test_run_in_span_binds_atlas_fields_only_for_the_duration_of_fn() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-atlas-fields")
    span, context = _start_span_and_context(tracer, "job")

    observed: dict[str, str] = {}

    def _fn() -> None:
        observed.update(current_context())

    run_in_span(
        span=span,
        otel_context=context,
        atlas_fields={"research_job_id": "job-123"},
        fn=_fn,
    )

    assert observed.get("research_job_id") == "job-123"
    assert "research_job_id" not in current_context()


def test_run_in_span_attaches_the_otel_context_only_for_the_duration_of_fn() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-otel-context")
    span, context = _start_span_and_context(tracer, "job")

    observed_span_id: int | None = None

    def _fn() -> None:
        nonlocal observed_span_id
        observed_span_id = trace.get_current_span().get_span_context().span_id

    run_in_span(span=span, otel_context=context, atlas_fields={}, fn=_fn)

    assert observed_span_id == span.get_span_context().span_id
    # Detached afterward: no current span outside the call.
    assert not trace.get_current_span().get_span_context().is_valid


def test_run_in_span_re_raises_and_marks_the_span_as_errored() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-exception")
    span, context = _start_span_and_context(tracer, "job")

    def _boom() -> None:
        raise ValueError("processor-failure")

    with pytest.raises(ValueError, match="processor-failure"):
        run_in_span(span=span, otel_context=context, atlas_fields={}, fn=_boom)

    assert cast(SDKSpan, span).status.status_code == StatusCode.ERROR


def test_run_in_span_ends_the_span_and_detaches_context_exactly_once_on_success() -> (
    None
):
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-cleanup-success")
    span, context = _start_span_and_context(tracer, "job")

    run_in_span(span=span, otel_context=context, atlas_fields={}, fn=lambda: None)

    assert cast(SDKSpan, span).end_time is not None
    assert not trace.get_current_span().get_span_context().is_valid


def test_run_in_span_ends_the_span_and_detaches_context_exactly_once_on_exception() -> (
    None
):
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-cleanup-exception")
    span, context = _start_span_and_context(tracer, "job")

    def _boom() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run_in_span(span=span, otel_context=context, atlas_fields={}, fn=_boom)

    assert cast(SDKSpan, span).end_time is not None
    assert not trace.get_current_span().get_span_context().is_valid


def test_run_in_span_on_a_reused_thread_leaves_no_leakage_between_two_jobs() -> None:
    """A single-worker ``ThreadPoolExecutor`` processing sequential jobs must
    see neither OTel context nor Atlas correlation context leak from job one
    into job two, even though both run on the very same underlying thread."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-sequential")

    def _observe() -> tuple[int, dict[str, str]]:
        span_id = trace.get_current_span().get_span_context().span_id
        return span_id, dict(current_context())

    def _job(research_job_id: str) -> tuple[int, dict[str, str]]:
        span, context = _start_span_and_context(tracer, "job")
        return run_in_span(
            span=span,
            otel_context=context,
            atlas_fields={"research_job_id": research_job_id},
            fn=lambda: _observe(),
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        first_span_id, first_fields = executor.submit(_job, "job-a").result()
        second_span_id, second_fields = executor.submit(_job, "job-b").result()

    assert first_span_id != second_span_id
    assert first_fields.get("research_job_id") == "job-a"
    assert second_fields.get("research_job_id") == "job-b"


def test_run_in_span_does_not_affect_the_submitting_threads_own_context() -> None:
    """The submitting thread never attaches anything itself, so it has
    nothing to leak regardless of what happens on the worker thread."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-submitter-isolation")
    span, context = _start_span_and_context(tracer, "job")

    def _worker_job() -> None:
        run_in_span(
            span=span,
            otel_context=context,
            atlas_fields={"research_job_id": "worker-thread-job"},
            fn=lambda: None,
        )

    with bind_context(research_job_id="submitter-own-job"):
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(_worker_job).result()
        # The submitting thread's own context is untouched by the worker
        # thread's bind_context call.
        assert current_context().get("research_job_id") == "submitter-own-job"


def test_run_in_span_span_end_failure_still_detaches_context() -> None:
    """Even if ``span.end()`` itself raises, the OTel context token must
    still be detached (the nested ``finally`` in ``run_in_span``)."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test-run-in-span-end-raises")
    span, context = _start_span_and_context(tracer, "job")

    original_end = span.end

    def _raising_end(end_time: int | None = None) -> None:
        original_end(end_time)
        raise RuntimeError("span-end-failure")

    span.end = _raising_end  # type: ignore[method-assign]

    token_before = otel_context_api.get_current()
    with pytest.raises(RuntimeError, match="span-end-failure"):
        run_in_span(span=span, otel_context=context, atlas_fields={}, fn=lambda: None)

    assert otel_context_api.get_current() == token_before
