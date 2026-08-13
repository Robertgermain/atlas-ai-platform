"""End-to-end trace-continuity evidence (Slice 15A3): API-simulated root
``traceparent`` -> worker claim -> ``worker.process_job`` -> LangGraph node
spans -> model/tool attempt spans -> outbox enqueue, all sharing one trace,
using only PostgreSQL (no Kafka, no Collector/Tempo, no Docker required).

An ``InMemorySpanExporter`` is attached as an *additional* span processor on
whatever global ``TracerProvider`` is already active (set once, process-wide,
by ``atlas.main``'s own module-level ``configure_tracing()`` -- see
``atlas.observability.tracing`` module docstring on the ``ProxyTracer``
design that makes this safe regardless of import order). This never
replaces or disturbs the real (fail-open, OTLP) exporter also attached to
that same provider.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider as SDKTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.application.worker import ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.eventing import build_research_job_created
from atlas.eventing.contracts import DomainEvent
from atlas.outbox.clock import ControllableClock
from atlas.outbox.relay import OutboxRelay, RelayRunOutcome
from atlas.outbox.relay_lock import PostgresOutboxRelayLock
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)

# A structurally valid W3C version-00 traceparent standing in for what the
# API would have stored at submission time (Slice 15A3's own strict parser
# never distinguishes a real vs. synthetic well-formed value).
_API_TRACE_ID = "aa" * 16
_API_ROOT_SPAN_ID = "bb" * 8
_API_TRACEPARENT = f"00-{_API_TRACE_ID}-{_API_ROOT_SPAN_ID}-01"


def _in_memory_exporter() -> InMemorySpanExporter | None:
    """Attach an in-memory exporter to the real global provider, if one is
    already active. Returns ``None`` (skip-worthy) only if some earlier
    import path never configured tracing at all, which never happens in
    this integration suite (``tests/integration/conftest.py``'s own
    autouse fixture already imports ``atlas.main``, whose module-level
    ``configure_tracing()`` always runs first)."""
    provider = trace.get_tracer_provider()
    if not isinstance(provider, SDKTracerProvider):
        return None
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


def test_worker_to_langgraph_to_outbox_shares_one_trace(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    exporter = _in_memory_exporter()
    assert exporter is not None, (
        "Expected the global SDK TracerProvider to already be configured "
        "by atlas.main's module-level configure_tracing() (see "
        "tests/integration/conftest.py's autouse fixture)."
    )
    exporter.clear()

    repo = SqlAlchemyResearchJobRepository()
    job_id = "trace-continuity-e2e"
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, "What is Atlas?", at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
            traceparent=_API_TRACEPARENT,
        )

    runtime = create_checkpoint_runtime(test_database_url)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=repo,
        processor=LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    try:
        assert worker.run_once() is True
    finally:
        worker.close()
        runtime.close()

    with session_scope(session_factory) as session:
        loaded = repo.get(session, job_id)
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.COMPLETED

    spans = exporter.get_finished_spans()
    worker_spans = [
        s
        for s in spans
        if s.name == "worker.process_job"
        and s.attributes is not None
        and s.attributes.get("atlas.research_job_id") == job_id
    ]
    assert len(worker_spans) == 1
    worker_span = worker_spans[0]

    # The worker span must be a direct child of the API's stored
    # traceparent -- same trace ID, and its parent span ID equal to the
    # stored root span ID (this is exactly what "use_traceparent_as_parent"
    # buys, as opposed to a mere Span Link).
    assert trace.format_trace_id(worker_span.context.trace_id) == _API_TRACE_ID
    assert worker_span.parent is not None
    assert trace.format_span_id(worker_span.parent.span_id) == _API_ROOT_SPAN_ID

    # workflow.node.*/model.attempt/tool.attempt spans carry no job-id
    # attribute of their own (see atlas.workflow.graph._wrap_node and
    # atlas.models/tools.service) -- they are identified here by sharing
    # this run's trace ID instead, which is exactly the property under
    # test: everything nested under worker.process_job's own context
    # inherits the one trace ID that started at the (simulated) API root.
    same_trace_spans = [
        s for s in spans if trace.format_trace_id(s.context.trace_id) == _API_TRACE_ID
    ]
    node_spans = [s for s in same_trace_spans if s.name.startswith("workflow.node.")]
    assert node_spans, "Expected at least one workflow.node.* span."
    model_or_tool_spans = [
        s for s in same_trace_spans if s.name in ("model.attempt", "tool.attempt")
    ]
    assert model_or_tool_spans, "Expected at least one model.attempt/tool.attempt span."

    with session_factory() as session:
        row = (
            session.execute(
                text("SELECT traceparent FROM outbox_events WHERE aggregate_id = :id"),
                {"id": job_id},
            )
            .mappings()
            .first()
        )
    assert row is not None
    outbox_traceparent = row["traceparent"]
    assert outbox_traceparent is not None
    # The outbox row's own stored traceparent was captured from *within*
    # this same trace (it is not required to equal the worker span's own
    # ID -- job completion may finish under a nested span -- only to share
    # the trace).
    assert outbox_traceparent.startswith(f"00-{_API_TRACE_ID}-")


def test_job_with_no_stored_traceparent_starts_an_independent_new_trace(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    """A job submitted with no stored ``traceparent`` (e.g. no inbound
    tracing context at submission time) must still process successfully
    and must never be attributed to the synthetic API trace used by the
    other test in this module -- it gets its own independent root trace."""
    exporter = _in_memory_exporter()
    assert exporter is not None
    exporter.clear()

    repo = SqlAlchemyResearchJobRepository()
    job_id = "trace-continuity-no-parent"
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, "Independent question", at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="b" * 64,
            traceparent=None,
        )

    runtime = create_checkpoint_runtime(test_database_url)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=repo,
        processor=LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    try:
        assert worker.run_once() is True
    finally:
        worker.close()
        runtime.close()

    spans = exporter.get_finished_spans()
    worker_spans = [
        s
        for s in spans
        if s.name == "worker.process_job"
        and s.attributes is not None
        and s.attributes.get("atlas.research_job_id") == job_id
    ]
    assert len(worker_spans) == 1
    worker_span = worker_spans[0]
    assert worker_span.parent is None
    assert trace.format_trace_id(worker_span.context.trace_id) != _API_TRACE_ID


def test_immediate_crash_reclaim_starts_new_root_trace_with_link_not_child(
    session_factory: sessionmaker[Session],
) -> None:
    """A crash/lease reclaim (Slice 15A3 final condition #1) must resolve to
    a *new root trace with a Span Link* at the worker's own span-creation
    boundary, exercising ``resolve_parent_or_link`` end-to-end -- never a
    second direct child of the already-consumed API traceparent."""
    exporter = _in_memory_exporter()
    assert exporter is not None
    exporter.clear()

    repo = SqlAlchemyResearchJobRepository()
    job_id = "trace-continuity-crash-reclaim"
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create(job_id, "question", at=T0),
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
            traceparent=_API_TRACEPARENT,
        )

    now_a = T0 + timedelta(seconds=1)
    with session_scope(session_factory) as session:
        claimed_a = repo.claim_next(
            session,
            now=now_a,
            lease_expires_at=now_a + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )
    assert claimed_a is not None
    assert claimed_a.use_traceparent_as_parent is True

    with session_factory() as session:
        session.execute(
            text("UPDATE research_jobs SET lease_expires_at = :expired WHERE id = :id"),
            {"id": job_id, "expired": now_a - timedelta(seconds=1)},
        )
        session.commit()

    now_b = now_a + timedelta(seconds=60)
    with session_scope(session_factory) as session:
        claimed_b = repo.claim_next(
            session,
            now=now_b,
            lease_expires_at=now_b + timedelta(seconds=30),
            claim_token=secrets.token_hex(32),
        )
    assert claimed_b is not None
    assert claimed_b.use_traceparent_as_parent is False

    from atlas.observability.tracing import resolve_parent_or_link

    parent_context, links = resolve_parent_or_link(
        claimed_b.traceparent, use_as_parent=claimed_b.use_traceparent_as_parent
    )
    assert parent_context is None
    assert len(links) == 1
    assert trace.format_trace_id(links[0].context.trace_id) == _API_TRACE_ID

    tracer = trace.get_tracer("test-crash-reclaim")
    span = tracer.start_span(
        "worker.process_job", context=parent_context, links=list(links)
    )
    span.end()
    finished = exporter.get_finished_spans()
    reclaimed = [s for s in finished if s.name == "worker.process_job"]
    assert reclaimed
    assert reclaimed[-1].parent is None
    assert trace.format_trace_id(reclaimed[-1].context.trace_id) != _API_TRACE_ID


class _CapturingProducer:
    """Minimal local test double for ``EventProducer`` (Slice 15A3): records
    the exact ``traceparent`` the relay passed in, never discarding it the
    way ``atlas.outbox.fakes.FakeEventProducer`` deliberately does for its
    own (pre-15A3) unrelated test scenarios."""

    def __init__(self) -> None:
        self.published: list[DomainEvent] = []
        self.traceparents: list[str | None] = []

    def publish(self, event: DomainEvent, *, traceparent: str | None = None) -> None:
        self.published.append(event)
        self.traceparents.append(traceparent)


def test_outbox_relay_injects_its_own_child_traceparent_not_the_stored_one(
    engine: object,
    session_factory: sessionmaker[Session],
) -> None:
    """The relay's ``outbox.publish`` span must be a genuine child of the
    row's stored ``traceparent`` (same trace, new span) -- and the
    ``traceparent`` handed to the producer must be *that new span's own*,
    never the stored value forwarded unchanged (see
    ``atlas.outbox.relay.OutboxRelay.run_once``'s own docstring)."""
    from sqlalchemy import Engine

    assert isinstance(engine, Engine)
    exporter = _in_memory_exporter()
    assert exporter is not None
    exporter.clear()

    tracer = trace.get_tracer("test-outbox-relay-seed")
    seed_span = tracer.start_span("test-seed-root")
    event = build_research_job_created(
        research_job_id="trace-relay-lineage", created_at=T0
    )
    repo = SqlAlchemyOutboxRepository()
    with trace.use_span(seed_span, end_on_exit=True):
        with session_scope(session_factory) as session:
            repo.enqueue(session, event)

    with session_factory() as session:
        stored_traceparent = (
            session.execute(
                text("SELECT traceparent FROM outbox_events WHERE event_id = :id"),
                {"id": str(event.event_id)},
            )
            .mappings()
            .one()["traceparent"]
        )
    seed_trace_id_hex = trace.format_trace_id(seed_span.get_span_context().trace_id)
    seed_span_id_hex = trace.format_span_id(seed_span.get_span_context().span_id)
    seed_flags_hex = f"{int(seed_span.get_span_context().trace_flags):02x}"
    assert (
        stored_traceparent
        == f"00-{seed_trace_id_hex}-{seed_span_id_hex}-{seed_flags_hex}"
    )

    lock = PostgresOutboxRelayLock(engine)
    lock.acquire()
    producer = _CapturingProducer()
    relay = OutboxRelay(
        session_factory=session_factory,
        repository=repo,
        producer=producer,
        lock=lock,
        clock=ControllableClock(T0),
    )
    try:
        result = relay.run_once()
    finally:
        lock.release()

    assert result.outcome == RelayRunOutcome.PUBLISHED
    assert len(producer.traceparents) == 1
    outgoing = producer.traceparents[0]
    assert outgoing is not None
    # Same trace as the stored value, but a genuinely different span --
    # never the stored traceparent merely echoed back.
    assert outgoing.startswith(f"00-{seed_trace_id_hex}-")
    assert outgoing != stored_traceparent

    publish_spans = [
        s for s in exporter.get_finished_spans() if s.name == "outbox.publish"
    ]
    assert len(publish_spans) == 1
    publish_span = publish_spans[0]
    assert trace.format_trace_id(publish_span.context.trace_id) == seed_trace_id_hex
    assert publish_span.parent is not None
    assert trace.format_span_id(publish_span.parent.span_id) == seed_span_id_hex
    publish_flags_hex = f"{int(publish_span.context.trace_flags):02x}"
    assert (
        outgoing == f"00-{seed_trace_id_hex}-"
        f"{trace.format_span_id(publish_span.context.span_id)}-{publish_flags_hex}"
    )
