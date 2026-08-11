"""Real-PostgreSQL round-trip instrumentation tests (Slice 13C2B).

Asserts the current consumer processing paths stay at or below the
conservative ``consumer_max_db_round_trips_per_attempt`` cap of 8 -- see
``atlas.persistence.instrumentation`` for exactly what is (and is not)
counted, and the module docstring there for why pool checkouts are
excluded from the statement-timeout-bound total.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from atlas.consumer.identity import RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1
from atlas.consumer.ports import InboxOutcome
from atlas.consumer.retention import build_retention
from atlas.eventing import build_research_job_completed, build_research_job_created
from atlas.persistence.db import session_scope
from atlas.persistence.instrumentation import count_database_round_trips
from atlas.persistence.repositories.consumer_dead_letter import (
    SqlAlchemyDeadLetterRepository,
)
from atlas.persistence.repositories.consumer_inbox import SqlAlchemyInboxRepository
from atlas.persistence.repositories.research_job_projection import (
    SqlAlchemyResearchJobProjectionRepository,
)

T0 = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
_CONSUMER_ID = RESEARCH_JOB_PROJECTION_CONSUMER_GROUP_V1

#: Documented, intentionally-larger-than-observed conservative cap (Slice
#: 13C2B); see ``atlas.consumer.timing.worst_case_attempt_seconds``.
_MAX_ROUND_TRIPS_PER_ATTEMPT = 8


def _unique_offset() -> int:
    return uuid4().int & 0x7FFFFFFFFFFF


def test_new_event_apply_path_stays_within_the_round_trip_cap(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    research_job_id = f"rt-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    inbox = SqlAlchemyInboxRepository()
    projection = SqlAlchemyResearchJobProjectionRepository()

    with count_database_round_trips(engine) as counts:
        with session_scope(session_factory) as session:
            outcome = inbox.record_and_apply(
                session,
                consumer_id=_CONSUMER_ID,
                event=event,
                kafka_partition=0,
                kafka_offset=_unique_offset(),
                at=T0,
                apply_effect=lambda s, e: projection.apply(s, e, at=T0),
            )

    assert outcome is InboxOutcome.APPLIED
    assert counts.statement_timeout_bound_total <= _MAX_ROUND_TRIPS_PER_ATTEMPT
    assert counts.commits == 1
    assert counts.rollbacks == 0


def test_duplicate_apply_path_stays_within_the_round_trip_cap(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    research_job_id = f"rt-dup-{uuid4().hex}"
    event = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    inbox = SqlAlchemyInboxRepository()
    projection = SqlAlchemyResearchJobProjectionRepository()
    with session_scope(session_factory) as session:
        inbox.record_and_apply(
            session,
            consumer_id=_CONSUMER_ID,
            event=event,
            kafka_partition=0,
            kafka_offset=_unique_offset(),
            at=T0,
            apply_effect=lambda s, e: projection.apply(s, e, at=T0),
        )

    with count_database_round_trips(engine) as counts:
        with session_scope(session_factory) as session:
            outcome = inbox.record_and_apply(
                session,
                consumer_id=_CONSUMER_ID,
                event=event,
                kafka_partition=0,
                kafka_offset=_unique_offset(),
                at=T0,
                apply_effect=lambda s, e: projection.apply(s, e, at=T0),
            )

    assert outcome is InboxOutcome.DUPLICATE
    assert counts.statement_timeout_bound_total <= _MAX_ROUND_TRIPS_PER_ATTEMPT


def test_lifecycle_violation_apply_path_stays_within_the_round_trip_cap(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    research_job_id = f"rt-lifecycle-{uuid4().hex}"
    created = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    completed = build_research_job_completed(
        research_job_id=research_job_id, completed_at=T0, event_id=uuid4()
    )
    stray = build_research_job_created(
        research_job_id=research_job_id, created_at=T0, event_id=uuid4()
    )
    inbox = SqlAlchemyInboxRepository()
    projection = SqlAlchemyResearchJobProjectionRepository()
    with session_scope(session_factory) as session:
        inbox.record_and_apply(
            session,
            consumer_id=_CONSUMER_ID,
            event=created,
            kafka_partition=0,
            kafka_offset=_unique_offset(),
            at=T0,
            apply_effect=lambda s, e: projection.apply(s, e, at=T0),
        )
    with session_scope(session_factory) as session:
        inbox.record_and_apply(
            session,
            consumer_id=_CONSUMER_ID,
            event=completed,
            kafka_partition=0,
            kafka_offset=_unique_offset(),
            at=T0,
            apply_effect=lambda s, e: projection.apply(s, e, at=T0),
        )

    with count_database_round_trips(engine) as counts:
        try:
            with session_scope(session_factory) as session:
                inbox.record_and_apply(
                    session,
                    consumer_id=_CONSUMER_ID,
                    event=stray,
                    kafka_partition=0,
                    kafka_offset=_unique_offset(),
                    at=T0,
                    apply_effect=lambda s, e: projection.apply(s, e, at=T0),
                )
        except Exception:
            pass  # LifecycleOrderViolationError; only the round trips matter here.

    assert counts.statement_timeout_bound_total <= _MAX_ROUND_TRIPS_PER_ATTEMPT
    assert counts.rollbacks == 1


def test_dead_letter_upsert_insert_stays_within_the_round_trip_cap(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    event = build_research_job_created(
        research_job_id=f"rt-dlq-{uuid4().hex}", created_at=T0, event_id=uuid4()
    )
    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=b"{}",
        decoded_event=event,
    )
    with count_database_round_trips(engine) as counts:
        with session_scope(session_factory) as session:
            SqlAlchemyDeadLetterRepository().upsert(
                session,
                consumer_id=_CONSUMER_ID,
                kafka_partition=0,
                kafka_offset=_unique_offset(),
                failure_code="lifecycle_order_violation",
                processing_attempt_count=1,
                at=T0,
                retention=retention,
            )

    assert counts.statement_timeout_bound_total <= _MAX_ROUND_TRIPS_PER_ATTEMPT


def test_dead_letter_upsert_conflict_stays_within_the_round_trip_cap(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    event = build_research_job_created(
        research_job_id=f"rt-dlq-conflict-{uuid4().hex}",
        created_at=T0,
        event_id=uuid4(),
    )
    retention = build_retention(
        failure_code="lifecycle_order_violation",
        raw_value=b"{}",
        decoded_event=event,
    )
    offset = _unique_offset()
    with session_scope(session_factory) as session:
        SqlAlchemyDeadLetterRepository().upsert(
            session,
            consumer_id=_CONSUMER_ID,
            kafka_partition=0,
            kafka_offset=offset,
            failure_code="lifecycle_order_violation",
            processing_attempt_count=1,
            at=T0,
            retention=retention,
        )

    with count_database_round_trips(engine) as counts:
        with session_scope(session_factory) as session:
            SqlAlchemyDeadLetterRepository().upsert(
                session,
                consumer_id=_CONSUMER_ID,
                kafka_partition=0,
                kafka_offset=offset,
                failure_code="lifecycle_order_violation",
                processing_attempt_count=1,
                at=T0,
                retention=retention,
            )

    assert counts.statement_timeout_bound_total <= _MAX_ROUND_TRIPS_PER_ATTEMPT
