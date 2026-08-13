"""Network-free evidence that ``ConsumerRunner.run_once`` starts a
``kafka.consume`` span using an extracted Kafka ``traceparent`` header as a
direct parent (Slice 15A3), using the same real fake-Kafka-consumer/in-memory
inbox harness as ``tests/consumer/test_runner_unit.py`` -- no broker, no
PostgreSQL, no Docker required.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider as SDKTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from atlas.consumer.fakes import (
    FakeKafkaConsumer,
    InMemoryDeadLetterRepository,
    InMemoryInboxRepository,
    RecordingProjection,
    build_kafka_message_for_event,
)
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.consumer.timing import RetryTimingParameters
from atlas.eventing import build_research_job_created

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1

_FAST_TIMING = RetryTimingParameters(
    max_attempts=3,
    base_seconds=0.0,
    max_backoff_seconds=0.0,
    jitter_max_seconds=0.0,
    safety_margin_seconds=1.0,
    db_connect_timeout_seconds=0.01,
    db_pool_timeout_seconds=0.01,
    db_statement_timeout_seconds=0.01,
    processing_overhead_seconds=0.0,
    max_db_round_trips_per_attempt=1,
)

_TRACE_ID = "cc" * 16
_SPAN_ID = "dd" * 8
_KAFKA_TRACEPARENT = f"00-{_TRACE_ID}-{_SPAN_ID}-01"


class _FakeSession:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _fake_session_factory() -> _FakeSession:
    return _FakeSession()


def _in_memory_exporter() -> InMemorySpanExporter | None:
    """Attach an in-memory exporter to whatever global SDK provider is
    already active. In a plain ``pytest tests/consumer/...`` invocation
    with no prior import of ``atlas.main``, no SDK provider is configured
    yet, so this legitimately returns ``None`` -- callers using this in
    this module always configure one first (see ``_configure`` below) to
    make the test self-contained regardless of run order/isolation."""
    provider = trace.get_tracer_provider()
    if not isinstance(provider, SDKTracerProvider):
        return None
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


def _configure_local_provider_if_needed() -> InMemorySpanExporter:
    """Ensure a real SDK ``TracerProvider`` is globally active, then attach
    a fresh in-memory exporter to it. Idempotent-safe for repeated test
    runs within one process: the OTel API only ever honors the *first*
    ``set_tracer_provider`` call, so a prior test/module in the same
    process may have already set one -- this reuses it either way."""
    existing = _in_memory_exporter()
    if existing is not None:
        return existing
    trace.set_tracer_provider(SDKTracerProvider())
    attached = _in_memory_exporter()
    assert attached is not None
    return attached


def test_run_once_starts_kafka_consume_span_parented_by_the_header_traceparent() -> (
    None
):
    exporter = _configure_local_provider_if_needed()
    exporter.clear()

    event = build_research_job_created(
        research_job_id="job-tracing-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=5)
    assert message.raw_headers is not None
    message.raw_headers.append(("traceparent", _KAFKA_TRACEPARENT.encode("utf-8")))

    consumer = FakeKafkaConsumer([message])
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        inbox=InMemoryInboxRepository(),
        projection=RecordingProjection(),
        dead_letters=InMemoryDeadLetterRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        max_poll_interval_seconds=300.0,
        timing_params=_FAST_TIMING,
        clock=lambda: T0,
        wait=lambda _seconds: False,
    )

    outcome = runner.run_once()
    assert outcome == ProcessOutcome.APPLIED

    spans = exporter.get_finished_spans()
    consume_spans = [s for s in spans if s.name == "kafka.consume"]
    assert len(consume_spans) == 1
    span = consume_spans[0]

    assert trace.format_trace_id(span.context.trace_id) == _TRACE_ID
    assert span.parent is not None
    assert trace.format_span_id(span.parent.span_id) == _SPAN_ID


def test_run_once_with_no_traceparent_header_starts_an_ordinary_root_span() -> None:
    exporter = _configure_local_provider_if_needed()
    exporter.clear()

    event = build_research_job_created(
        research_job_id="job-tracing-2", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=6)
    consumer = FakeKafkaConsumer([message])
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        inbox=InMemoryInboxRepository(),
        projection=RecordingProjection(),
        dead_letters=InMemoryDeadLetterRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        max_poll_interval_seconds=300.0,
        timing_params=_FAST_TIMING,
        clock=lambda: T0,
        wait=lambda _seconds: False,
    )

    outcome = runner.run_once()
    assert outcome == ProcessOutcome.APPLIED

    spans = exporter.get_finished_spans()
    consume_spans = [s for s in spans if s.name == "kafka.consume"]
    assert len(consume_spans) == 1
    span = consume_spans[0]
    assert span.parent is None
    assert trace.format_trace_id(span.context.trace_id) != _TRACE_ID
