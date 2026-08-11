"""Network-free unit tests for ``ConsumerRunner`` (Slice 13C2A/13C2B).

Uses ``InMemoryInboxRepository``/``InMemoryDeadLetterRepository`` (real, not
scripted, implementations) plus a fake Kafka consumer double so the actual
poll-decode-apply-commit-or-dead-letter branching, bounded retry, and
processing-deadline logic are exercised without any network I/O.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.exc import DBAPIError

from atlas.consumer.errors import (
    ConsumerError,
    ConsumerShutdownRequestedError,
    DeadLetterPersistenceExhaustedError,
    LifecycleOrderViolationError,
    OffsetCommitFailedAfterDeadLetterError,
    ProcessingDeadlineExceededError,
    RetryExhaustedError,
    TransientKafkaError,
)
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
from atlas.consumer.retention import DeadLetterRetention
from atlas.consumer.runner import ConsumerRunner, ProcessOutcome
from atlas.consumer.timing import RetryTimingParameters
from atlas.eventing import build_research_job_completed, build_research_job_created

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1

#: Small, fast-to-exhaust timing params for retry tests. Deliberately not
#: the production defaults -- these tests care about branching, not the
#: exact worst-case-timing arithmetic (see test_consumer_retry_timing.py).
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
    """Enough of ``Session`` for ``session_scope`` -- never touches PostgreSQL."""

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _fake_session_factory() -> _FakeSession:
    return _FakeSession()


def _retention_of(row: dict[str, object]) -> DeadLetterRetention:
    retention = row["retention"]
    assert isinstance(retention, DeadLetterRetention)
    return retention


class _RecordingWait:
    """A fake ``Waiter``: records each requested duration, never really sleeps.

    Returns ``shutdown_after_call_index`` semantics: by default always
    reports "no shutdown observed" (``False``); ``trigger_shutdown_on_call``
    makes the call at that 1-based index (and every one after) report a
    shutdown was observed instead, so tests can exercise the
    ``ConsumerShutdownRequestedError`` path deterministically.
    """

    def __init__(self, *, trigger_shutdown_on_call: int | None = None) -> None:
        self.calls: list[float] = []
        self._trigger_shutdown_on_call = trigger_shutdown_on_call

    def __call__(self, seconds: float) -> bool:
        self.calls.append(seconds)
        if self._trigger_shutdown_on_call is None:
            return False
        return len(self.calls) >= self._trigger_shutdown_on_call


class _ControllableClock:
    """A settable clock so deadline tests don't depend on wall-clock timing."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now = self._now + delta


def _runner(
    consumer: FakeKafkaConsumer,
    *,
    inbox: InMemoryInboxRepository | None = None,
    dead_letters: InMemoryDeadLetterRepository | None = None,
    projection: RecordingProjection | None = None,
    clock: object = lambda: T0,
    timing_params: RetryTimingParameters | None = None,
    max_poll_interval_seconds: float = 300.0,
    wait: _RecordingWait | None = None,
) -> tuple[
    ConsumerRunner,
    InMemoryInboxRepository,
    InMemoryDeadLetterRepository,
    RecordingProjection,
]:
    inbox = inbox or InMemoryInboxRepository()
    dead_letters = dead_letters or InMemoryDeadLetterRepository()
    projection = projection or RecordingProjection()
    runner = ConsumerRunner(
        consumer=consumer,  # type: ignore[arg-type]
        session_factory=_fake_session_factory,  # type: ignore[arg-type]
        inbox=inbox,
        projection=projection,
        dead_letters=dead_letters,
        consumer_id=_CONSUMER_ID,
        poll_timeout_seconds=1.0,
        max_poll_interval_seconds=max_poll_interval_seconds,
        timing_params=timing_params or _FAST_TIMING,
        clock=clock,  # type: ignore[arg-type]
        wait=wait or (lambda _seconds: False),
    )
    return runner, inbox, dead_letters, projection


# --- baseline behavior (unchanged from Slice 13C2A) -----------------------


def test_run_once_returns_no_message_when_poll_returns_none() -> None:
    consumer = FakeKafkaConsumer([])
    runner, _inbox, _dlq, _projection = _runner(consumer)
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
    runner, _inbox, _dlq, _projection = _runner(consumer)
    with pytest.raises(ConsumerError, match="PollReturnedBrokerError"):
        runner.run_once()
    assert consumer.committed == []


def test_run_once_applies_a_new_event_and_commits_the_offset() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=5)
    consumer = FakeKafkaConsumer([message])
    runner, inbox, _dlq, projection = _runner(consumer)

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
    runner, inbox, _dlq, projection = _runner(consumer)

    first = runner.run_once()
    second = runner.run_once()

    assert first == ProcessOutcome.APPLIED
    assert second == ProcessOutcome.DUPLICATE
    assert projection.applied == [event]
    assert inbox.applied_effects == [event]
    assert consumer.committed == [message_1, message_2]


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

    runner, _inbox, _dlq, _projection = _runner(consumer, clock=clock)
    runner.run_once()
    # One read for "message received at", one admission check before the
    # single successful apply attempt, and one admission check before the
    # (also bounded-retry-capable) commit attempt.
    assert clock_calls["n"] == 3


# --- permanent-poison classification -> dead letter, not a raised error ---


def test_decode_failure_is_dead_lettered_and_offset_still_commits() -> None:
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    runner, inbox, dlq, projection = _runner(consumer)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.DEAD_LETTERED
    assert consumer.committed == [message]
    assert projection.applied == []
    assert inbox.applied_effects == []
    assert len(dlq.rows) == 1
    row = next(iter(dlq.rows.values()))
    assert row["failure_code"] == "missing_headers"
    assert row["processing_attempt_count"] == 1
    retention = _retention_of(row)
    assert retention.replay_eligible is False
    assert retention.event_id is None


def test_lifecycle_violation_is_dead_lettered_with_tier_a_retention() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    projection = RecordingProjection(raise_on_apply=LifecycleOrderViolationError())
    runner, inbox, dlq, _projection = _runner(consumer, projection=projection)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.DEAD_LETTERED
    assert consumer.committed == [message]
    assert inbox.applied_effects == []
    assert len(dlq.rows) == 1
    row = next(iter(dlq.rows.values()))
    assert row["failure_code"] == "lifecycle_order_violation"
    retention = _retention_of(row)
    assert retention.replay_eligible is True
    assert retention.event_id == event.event_id
    assert retention.retained_canonical_value is not None


def test_redelivered_dead_letter_only_increments_delivery_count() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message_1 = build_kafka_message_for_event(event, partition=0, offset=9)
    message_2 = build_kafka_message_for_event(event, partition=0, offset=9)
    consumer = FakeKafkaConsumer([message_1, message_2])
    projection = RecordingProjection(
        raise_on_apply=[LifecycleOrderViolationError(), LifecycleOrderViolationError()]
    )
    runner, _inbox, dlq, _projection = _runner(consumer, projection=projection)

    first = runner.run_once()
    second = runner.run_once()

    assert first == ProcessOutcome.DEAD_LETTERED
    assert second == ProcessOutcome.DEAD_LETTERED
    assert len(dlq.rows) == 1  # same (consumer_id, partition, offset) identity
    row = next(iter(dlq.rows.values()))
    assert row["dead_letter_delivery_count"] == 2
    assert row["processing_attempt_count"] == 1  # unchanged by the redelivery


def test_offset_commit_failure_after_dead_letter_terminates() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    consumer.raise_on_commit = RuntimeError("synthetic-commit-failure")
    projection = RecordingProjection(raise_on_apply=LifecycleOrderViolationError())
    runner, _inbox, dlq, _projection = _runner(consumer, projection=projection)

    with pytest.raises(OffsetCommitFailedAfterDeadLetterError):
        runner.run_once()
    assert len(dlq.rows) == 1  # the dead-letter row is durable regardless


# --- bounded transient-infrastructure retry --------------------------------


def test_transient_database_error_is_retried_then_succeeds() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(sqlstate="08006")]
    )
    wait = _RecordingWait()
    runner, _inbox, _dlq, projection = _runner(consumer, inbox=inbox, wait=wait)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert inbox.call_count == 2
    assert projection.applied == [event]
    assert consumer.committed == [message]
    assert len(wait.calls) == 1
    # poll() is never re-invoked mid-retry: exactly one poll for this record.
    assert consumer.poll_calls == 1


def test_transient_database_error_exhausts_retry_budget() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[
            build_dbapi_error(sqlstate="08006"),
            build_dbapi_error(sqlstate="08006"),
            build_dbapi_error(sqlstate="08006"),
        ]
    )
    runner, _inbox, _dlq, _projection = _runner(
        consumer,
        inbox=inbox,
        timing_params=RetryTimingParameters(
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
        ),
    )

    with pytest.raises(RetryExhaustedError):
        runner.run_once()
    assert inbox.call_count == 3
    assert consumer.committed == []


@pytest.mark.parametrize("sqlstate", [None, "42601", "23505"])
def test_fatal_database_error_is_not_retried(sqlstate: str | None) -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(sqlstate=sqlstate)]
    )
    runner, _inbox, _dlq, _projection = _runner(consumer, inbox=inbox)

    with pytest.raises(DBAPIError):
        runner.run_once()
    assert inbox.call_count == 1
    assert consumer.committed == []


def test_connection_invalidated_error_is_treated_as_transient() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(connection_invalidated=True)]
    )
    runner, _inbox, _dlq, projection = _runner(consumer, inbox=inbox)

    outcome = runner.run_once()
    assert outcome == ProcessOutcome.APPLIED
    assert projection.applied == [event]


def test_unexpected_non_database_exception_is_fatal() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(raise_before_success=[RuntimeError("boom")])
    runner, _inbox, _dlq, _projection = _runner(consumer, inbox=inbox)

    with pytest.raises(RuntimeError, match="boom"):
        runner.run_once()
    assert consumer.committed == []


# --- bounded Kafka commit retry ---------------------------------------------


def test_transient_kafka_commit_failure_is_retried_then_succeeds() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    consumer.raise_on_commit_before_success = [TransientKafkaError("CommitFailed")]
    wait = _RecordingWait()
    runner, _inbox, _dlq, projection = _runner(consumer, wait=wait)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.APPLIED
    assert projection.applied == [event]
    assert consumer.committed == [message]
    assert consumer.commit_calls == 2
    assert len(wait.calls) == 1


def test_transient_kafka_commit_failure_exhausts_and_terminates() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    consumer.raise_on_commit_before_success = [
        TransientKafkaError("CommitFailed"),
        TransientKafkaError("CommitFailed"),
        TransientKafkaError("CommitFailed"),
    ]
    runner, _inbox, _dlq, _projection = _runner(consumer)

    with pytest.raises(TransientKafkaError):
        runner.run_once()
    assert consumer.committed == []
    assert consumer.commit_calls == 3


def test_fatal_kafka_commit_failure_is_not_retried() -> None:
    """A non-``TransientKafkaError`` commit failure (e.g. the plain fatal
    ``ConsumerError`` ``KafkaEventConsumer`` raises for an unrecognized or
    fatal Kafka error) must propagate immediately, never retried."""
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    consumer.raise_on_commit = ConsumerError("CommitFailed")
    runner, _inbox, _dlq, _projection = _runner(consumer)

    with pytest.raises(ConsumerError):
        runner.run_once()
    assert consumer.committed == []
    assert consumer.commit_calls == 1


# --- shutdown-aware retry backoff -------------------------------------------


def test_shutdown_during_apply_retry_backoff_stops_cleanly_uncommitted() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(sqlstate="08006")]
    )
    wait = _RecordingWait(trigger_shutdown_on_call=1)
    runner, _inbox, _dlq, _projection = _runner(consumer, inbox=inbox, wait=wait)

    with pytest.raises(ConsumerShutdownRequestedError):
        runner.run_once()
    assert consumer.committed == []
    assert len(wait.calls) == 1


def test_shutdown_during_dead_letter_retry_backoff_stops_cleanly_uncommitted() -> None:
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    dlq = _FlakyThenSucceedDeadLetters(fail_times=1)
    wait = _RecordingWait(trigger_shutdown_on_call=1)
    runner, _inbox, _dlq, _projection = _runner(consumer, dead_letters=dlq, wait=wait)

    with pytest.raises(ConsumerShutdownRequestedError):
        runner.run_once()
    assert consumer.committed == []


def test_shutdown_during_commit_retry_backoff_stops_cleanly_uncommitted() -> None:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    consumer.raise_on_commit_before_success = [TransientKafkaError("CommitFailed")]
    wait = _RecordingWait(trigger_shutdown_on_call=1)
    runner, _inbox, _dlq, projection = _runner(consumer, wait=wait)

    with pytest.raises(ConsumerShutdownRequestedError):
        runner.run_once()
    # The business effect already durably committed before the commit-retry
    # backoff -- shutdown mid-commit-retry never undoes it, only leaves the
    # Kafka offset uncommitted so it safely redelivers (idempotently) later.
    assert projection.applied == [event]
    assert consumer.committed == []


# --- dead-letter persistence retry -----------------------------------------


class _FlakyThenSucceedDeadLetters(InMemoryDeadLetterRepository):
    def __init__(self, *, fail_times: int) -> None:
        super().__init__()
        self._remaining_failures = fail_times
        self.call_count = 0

    def upsert(self, session: object, **kwargs: object) -> object:  # type: ignore[override]
        self.call_count += 1
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise build_dbapi_error(sqlstate="08006")
        return super().upsert(session, **kwargs)  # type: ignore[arg-type]


def test_dead_letter_persistence_is_retried_then_succeeds() -> None:
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    dlq = _FlakyThenSucceedDeadLetters(fail_times=1)
    wait = _RecordingWait()
    runner, _inbox, _dlq, _projection = _runner(consumer, dead_letters=dlq, wait=wait)

    outcome = runner.run_once()

    assert outcome == ProcessOutcome.DEAD_LETTERED
    assert dlq.call_count == 2
    assert len(wait.calls) == 1
    assert consumer.committed == [message]


def test_dead_letter_persistence_exhausts_and_terminates_without_offset_commit() -> (
    None
):
    message = FakeKafkaMessage(value=b"not json", headers=None)
    consumer = FakeKafkaConsumer([message])
    dlq = _FlakyThenSucceedDeadLetters(fail_times=99)
    runner, _inbox, _dlq, _projection = _runner(consumer, dead_letters=dlq)

    with pytest.raises(DeadLetterPersistenceExhaustedError):
        runner.run_once()
    assert consumer.committed == []


# --- processing deadline ----------------------------------------------------


def test_processing_deadline_exceeded_before_the_first_attempt() -> None:
    """A deadline with no room even for one attempt fails closed immediately."""
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])

    # safety_margin (60s) alone already exceeds max_poll_interval (1s), so
    # the deadline is already in the past relative to "now" the instant the
    # message is received -- no attempt is ever started.
    runner, _inbox, _dlq, _projection = _runner(
        consumer,
        max_poll_interval_seconds=1.0,
        timing_params=RetryTimingParameters(
            max_attempts=3,
            base_seconds=1.0,
            max_backoff_seconds=1.0,
            jitter_max_seconds=0.0,
            safety_margin_seconds=60.0,
            db_connect_timeout_seconds=5.0,
            db_pool_timeout_seconds=5.0,
            db_statement_timeout_seconds=5.0,
            processing_overhead_seconds=2.0,
            max_db_round_trips_per_attempt=8,
        ),
    )

    with pytest.raises(ProcessingDeadlineExceededError):
        runner.run_once()
    assert consumer.committed == []


def test_processing_deadline_exceeded_during_backoff() -> None:
    """A backoff that would run past the deadline terminates before sleeping."""
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=uuid4()
    )
    message = build_kafka_message_for_event(event, partition=0, offset=1)
    consumer = FakeKafkaConsumer([message])
    clock = _ControllableClock(T0)
    inbox = InMemoryInboxRepository(
        raise_before_success=[build_dbapi_error(sqlstate="08006")]
    )
    wait = _RecordingWait()

    # One attempt (0.03s worst case) fits; but the backoff (10s) plus a
    # second attempt does not fit before the 1s-margin-shrunk deadline.
    runner, _inbox, _dlq, _projection = _runner(
        consumer,
        inbox=inbox,
        clock=clock,
        max_poll_interval_seconds=5.0,
        wait=wait,
        timing_params=RetryTimingParameters(
            max_attempts=3,
            base_seconds=10.0,
            max_backoff_seconds=10.0,
            jitter_max_seconds=0.0,
            safety_margin_seconds=1.0,
            db_connect_timeout_seconds=0.01,
            db_pool_timeout_seconds=0.01,
            db_statement_timeout_seconds=0.01,
            processing_overhead_seconds=0.0,
            max_db_round_trips_per_attempt=1,
        ),
    )

    with pytest.raises(ProcessingDeadlineExceededError):
        runner.run_once()
    assert consumer.committed == []
    assert wait.calls == []  # never waited -- the deadline check precedes it
