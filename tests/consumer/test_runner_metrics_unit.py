"""Consumer-runner metric emission (Slice 15A2 correction: reconciled metrics).

Reuses the same network-free fakes as ``test_runner_unit.py`` (real
``InMemoryInboxRepository``/``InMemoryDeadLetterRepository`` behavior, a fake
Kafka consumer double), but injects a process-local ``AtlasMetrics`` registry
so assertions can read exact sample values without interference from the
shared global ``default_metrics()`` registry other tests use.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from prometheus_client import CollectorRegistry

from atlas.consumer.fakes import (
    FakeKafkaConsumer,
    FakeKafkaMessage,
    InMemoryDeadLetterRepository,
    InMemoryInboxRepository,
    RecordingProjection,
    build_dbapi_error,
    build_kafka_message_for_event,
)
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.consumer.timing import RetryTimingParameters
from atlas.eventing import build_research_job_created
from atlas.observability.metrics.catalog import AtlasMetrics

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


class _FakeSession:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _fake_session_factory() -> _FakeSession:
    return _FakeSession()


def _sample_count(metrics: AtlasMetrics, metric_name: str, **labels: str) -> float:
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != metric_name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def _runner(
    consumer: FakeKafkaConsumer,
    *,
    metrics: AtlasMetrics,
    inbox: InMemoryInboxRepository | None = None,
    dead_letters: InMemoryDeadLetterRepository | None = None,
    projection: RecordingProjection | None = None,
) -> ConsumerRunner:
    return ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        inbox=inbox or InMemoryInboxRepository(),
        projection=projection or RecordingProjection(),
        dead_letters=dead_letters or InMemoryDeadLetterRepository(),
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        max_poll_interval_seconds=300.0,
        timing_params=_FAST_TIMING,
        clock=lambda: T0,
        wait=lambda _seconds: False,
        metrics=metrics,
    )


def test_apply_retry_observes_retry_attempt_stage_apply() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(sqlstate="08006")]
    )
    runner = _runner(consumer, metrics=metrics, inbox=inbox)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert (
        _sample_count(metrics, "atlas_consumer_retry_attempts_total", stage="apply")
        == 1
    )


def test_successful_commit_observes_offset_commit_outcome_success() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    runner = _runner(consumer, metrics=metrics)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert (
        _sample_count(
            metrics, "atlas_consumer_offset_commit_outcomes_total", outcome="success"
        )
        == 1
    )


def test_dead_letter_upsert_observes_dead_letter_metric_by_failure_code() -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    runner = _runner(consumer, metrics=metrics)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.DEAD_LETTERED
    assert (
        _sample_count(
            metrics,
            "atlas_consumer_dead_letters_total",
            failure_code="missing_headers",
        )
        == 1
    )
    # The dead-lettered record's own offset commit is also observed.
    assert (
        _sample_count(
            metrics, "atlas_consumer_offset_commit_outcomes_total", outcome="success"
        )
        == 1
    )


def test_redelivered_dead_letter_observes_a_second_dead_letter_occurrence() -> None:
    """Each redelivery is still counted: the dead-letter *row* is deduplicated
    by its own uniqueness boundary, but each occurrence is a genuine event
    from this consumer's perspective (see the runner's own inline comment)."""
    metrics = AtlasMetrics(CollectorRegistry())
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=9)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=9)
    consumer = FakeKafkaConsumer([message_1, message_2])
    from atlas.consumer.errors import LifecycleOrderViolationError

    projection = RecordingProjection(
        raise_on_apply=[LifecycleOrderViolationError(), LifecycleOrderViolationError()]
    )
    runner = _runner(consumer, metrics=metrics, projection=projection)

    runner.run_once()
    runner.run_once()

    assert (
        _sample_count(
            metrics,
            "atlas_consumer_dead_letters_total",
            failure_code="lifecycle_order_violation",
        )
        == 2
    )
