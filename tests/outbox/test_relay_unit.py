"""Unit tests for fake producer and relay ownership helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from atlas.eventing import build_research_job_created
from atlas.outbox.errors import EventPublishError, RelayNotOwnerError
from atlas.outbox.fakes import FakeEventProducer
from atlas.outbox.relay import OutboxRelay
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
