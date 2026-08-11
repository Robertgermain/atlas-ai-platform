"""Network-free unit tests for ``ConsumerRunner`` (Slice 13C2A).

Uses ``InMemoryInboxRepository`` (a real, not scripted, dedup
implementation) plus a fake Kafka consumer double so the actual
poll-decode-apply-commit branching is exercised without any network I/O.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from atlas.consumer.errors import (
    ConsumerError,
    InvalidHeaderError,
    LifecycleOrderViolationError,
)
from atlas.consumer.fakes import (
    FakeKafkaConsumer,
    FakeKafkaMessage,
    InMemoryInboxRepository,
    RecordingProjection,
    build_kafka_message_for_event,
)
from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.eventing import build_research_job_completed, build_research_job_created

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1


class _FakeSession:
    """Enough of ``Session`` for ``session_scope`` -- never touches PostgreSQL.

    Both ``InMemoryInboxRepository`` and ``RecordingProjection`` ignore the
    session object entirely, so this only needs to satisfy
    ``session_scope``'s own commit/rollback/close calls.
    """

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _fake_session_factory() -> _FakeSession:
    return _FakeSession()


def _runner(
    consumer: FakeKafkaConsumer,
    *,
    inbox: InMemoryInboxRepository | None = None,
    projection: RecordingProjection | None = None,
    clock: object = lambda: T0,
) -> tuple[ConsumerRunner, InMemoryInboxRepository, RecordingProjection]:
    inbox = inbox or InMemoryInboxRepository()
    projection = projection or RecordingProjection()
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        inbox=inbox,
        projection=projection,
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        clock=clock,  # type: ignore[arg-type]
    )
    return runner, inbox, projection


def test_run_once_returns_no_message_when_poll_returns_none() -> None:
    consumer = FakeKafkaConsumer([])
    runner, _inbox, _projection = _runner(consumer)
    assert runner.run_once() == ProcessOutcome.NO_MESSAGE
    assert consumer.poll_calls == 1


def test_run_once_raises_when_the_broker_reports_an_error() -> None:
    message = build_kafka_message_for_event(
        build_research_job_created(
            research_job_id="job-1", created_at=T0, event_id=uuid4()
        )
    )
    message.raw_error = object()
    consumer = FakeKafkaConsumer([message])
    runner, _inbox, _projection = _runner(consumer)
    with pytest.raises(ConsumerError, match="PollReturnedBrokerError"):
        runner.run_once()
    assert consumer.committed == []


def test_run_once_applies_a_new_event_and_commits_the_offset() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=5)
    consumer = FakeKafkaConsumer([message])
    runner, inbox, projection = _runner(consumer)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert projection.applied == [event]
    assert inbox.applied_effects == [event]
    assert consumer.committed == [message]


def test_run_once_skips_reapplying_a_duplicate_but_still_commits() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=5)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=5)
    consumer = FakeKafkaConsumer([message_1, message_2])
    runner, inbox, projection = _runner(consumer)

    first = runner.run_once()
    second = runner.run_once()

    assert first == ProcessOutcome.APPLIED
    assert second == ProcessOutcome.DUPLICATE
    assert projection.applied == [event]  # effect applied exactly once
    assert inbox.applied_effects == [event]
    assert consumer.committed == [message_1, message_2]  # both offsets committed


def test_run_once_propagates_decode_failures_without_committing() -> None:
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    runner, inbox, projection = _runner(consumer)

    with pytest.raises(InvalidHeaderError, match="MissingHeaders"):
        runner.run_once()
    assert consumer.committed == []
    assert projection.applied == []
    assert inbox.applied_effects == []


def test_run_once_propagates_lifecycle_violations_without_committing() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    projection = RecordingProjection(
        raise_on_apply=LifecycleOrderViolationError("TerminalProjectionAlreadyRecorded")
    )
    runner, inbox, _projection = _runner(consumer, projection=projection)

    with pytest.raises(LifecycleOrderViolationError):
        runner.run_once()
    assert consumer.committed == []
    assert inbox.applied_effects == []


def test_run_once_uses_the_same_clock_reading_for_the_whole_cycle() -> None:
    event = build_research_job_completed(
        research_job_id="job-1", completed_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=2)
    consumer = FakeKafkaConsumer([message])
    clock_calls = {"n": 0}

    def clock() -> datetime:
        clock_calls["n"] += 1
        return T0

    runner, _inbox, _projection = _runner(consumer, clock=clock)
    runner.run_once()
    assert clock_calls["n"] == 1
