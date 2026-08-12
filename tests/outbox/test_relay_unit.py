"""Unit tests for fake producer and relay ownership helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from atlas.eventing import build_research_job_created
from atlas.observability.events import Event
from atlas.observability.testing import capture_logs
from atlas.outbox.errors import EventPublishError, RelayNotOwnerError
from atlas.outbox.fakes import FakeEventProducer
from atlas.outbox.ports import ClaimedOutboxRecord
from atlas.outbox.relay import OutboxRelay, RelayRunOutcome
from atlas.outbox.relay_lock import PostgresOutboxRelayLock

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def test_fake_producer_records_success() -> None:
    producer = FakeEventProducer()
    event = build_research_job_created(
        research_job_id="job-1",
        created_at=T0,
        event_id=uuid4(),
    )
    producer.publish(event)
    assert producer.published == [event]
    assert producer.attempts == [event]


def test_fake_producer_failure() -> None:
    event = build_research_job_created(
        research_job_id="job-1",
        created_at=T0,
        event_id=uuid4(),
    )
    producer = FakeEventProducer(fail_on_event_ids={event.event_id})
    with pytest.raises(EventPublishError, match="FakeProducerFailure"):
        producer.publish(event)
    assert producer.published == []
    assert producer.attempts == [event]


class _FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakeLock:
    held = True


def _claimed_record(*, event_id: UUID) -> ClaimedOutboxRecord:
    event = build_research_job_created(
        research_job_id="job-1", created_at=T0, event_id=event_id
    )
    return ClaimedOutboxRecord(
        event_id=event_id,
        outbox_position=1,
        event=event,
        publish_claim_token="claim-token",
        publish_lease_expires_at=T0 + timedelta(seconds=30),
        publish_attempts=1,
    )


class _OwnershipLostRepository:
    """``mark_published``/``release_failed_claim`` always report lost ownership."""

    def __init__(self, records: list[ClaimedOutboxRecord]) -> None:
        self._records = records
        self.released_error_classes: list[str] = []

    def claim_batch(
        self, session: object, **_kwargs: object
    ) -> list[ClaimedOutboxRecord]:
        del session
        records, self._records = self._records, []
        return records

    def mark_published(self, session: object, **_kwargs: object) -> bool:
        del session
        return False

    def release_failed_claim(
        self, session: object, *, error_class: str, **_kwargs: object
    ) -> bool:
        del session
        self.released_error_classes.append(error_class)
        return False

    def enqueue(self, session: object, event: object) -> None:
        raise NotImplementedError


def test_mark_published_ownership_loss_logs_outbox_ownership_lost() -> None:
    event_id = uuid4()
    repo = _OwnershipLostRepository([_claimed_record(event_id=event_id)])
    relay = OutboxRelay(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        producer=FakeEventProducer(),
        lock=_FakeLock(),  # type: ignore[arg-type]
    )
    with capture_logs("atlas.outbox.relay") as captured:
        result = relay.run_once()
    assert result.outcome is RelayRunOutcome.OWNERSHIP_LOST
    assert captured.events == [Event.OUTBOX_OWNERSHIP_LOST.value]
    record = captured.json(0)
    assert record["outcome"] == "mark_published"
    assert record["outbox_event_id"] == str(event_id)


def test_release_failed_claim_ownership_loss_logs_outbox_ownership_lost() -> None:
    event_id = uuid4()
    repo = _OwnershipLostRepository([_claimed_record(event_id=event_id)])
    relay = OutboxRelay(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        producer=FakeEventProducer(fail_on_event_ids={event_id}),
        lock=_FakeLock(),  # type: ignore[arg-type]
    )
    with capture_logs("atlas.outbox.relay") as captured:
        result = relay.run_once()
    assert result.outcome is RelayRunOutcome.RECOVERABLE_FAILURE
    assert captured.events == [Event.OUTBOX_OWNERSHIP_LOST.value]
    record = captured.json(0)
    assert record["outcome"] == "release_failed_claim"
    assert record["outbox_event_id"] == str(event_id)
    # The failed publish's own error class was still stored, independent of
    # this ownership-loss signal about the release attempt itself.
    assert repo.released_error_classes == ["EventPublishError"]


def test_relay_requires_held_lock() -> None:
    class _Unused:
        pass

    lock = object.__new__(PostgresOutboxRelayLock)
    lock._connection = None
    relay = OutboxRelay(
        session_factory=_Unused(),  # type: ignore[arg-type]
        repository=_Unused(),  # type: ignore[arg-type]
        producer=FakeEventProducer(),
        lock=lock,
    )

    with pytest.raises(RelayNotOwnerError):
        relay.run_once()
