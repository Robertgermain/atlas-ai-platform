"""Advisory snapshot assembly from a seeded research job."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import Engine, event
from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.db import advisory_read_only_scope
from atlas.advisor.fakes import DeterministicAdvisoryAnalyst
from atlas.advisor.service import AdvisoryService
from atlas.advisor.snapshot import assemble_facts
from atlas.domain import ResearchJob
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.advisory_snapshot import (
    SqlAlchemyAdvisorySnapshotReader,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
_JOB_ID = "advisory-snapshot-job-1"


def _seed_job(session_factory: sessionmaker[Session], job_id: str = _JOB_ID) -> str:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create(job_id, "What is Atlas?", at=T0)
    with session_scope(session_factory) as session:
        repo.add(
            session,
            job,
            idempotency_key=f"key-{job_id}",
            request_fingerprint="a" * 64,
        )
    return job_id


def test_snapshot_reader_assembles_pending_job(
    session_factory: sessionmaker[Session],
) -> None:
    job_id = _seed_job(session_factory)
    reader = SqlAlchemyAdvisorySnapshotReader()
    with advisory_read_only_scope(session_factory) as session:
        loaded = reader.load(session, job_id)
        facts = assemble_facts(loaded)
    types = {item.signal_type for item in facts.signals}
    assert facts.research_job_id == job_id
    assert "job.status" in types
    assert "evaluation_profile_absent" in facts.missing_sources
    assert "evaluation_absent" in facts.missing_sources
    assert "outbox_absent" in facts.missing_sources
    assert "consumer_absent" in facts.missing_sources
    encoded = facts.model_dump_json()
    assert "What is Atlas?" not in encoded


def test_missing_job_is_not_found(session_factory: sessionmaker[Session]) -> None:
    from atlas.advisor.errors import AdvisoryJobNotFoundError

    reader = SqlAlchemyAdvisorySnapshotReader()
    with pytest.raises(AdvisoryJobNotFoundError):
        with advisory_read_only_scope(session_factory) as session:
            reader.load(session, "missing-job")


def test_pool_checkin_happens_before_analyst(
    engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    job_id = _seed_job(session_factory, "advisory-pool-job-1")
    checked_out: set[int] = set()
    order: list[str] = []

    def _checkout(
        dbapi_conn: object,
        connection_rec: object,
        connection_proxy: object,
    ) -> None:
        del connection_rec, connection_proxy
        checked_out.add(id(dbapi_conn))
        order.append("checkout")

    def _checkin(dbapi_conn: object, connection_rec: object) -> None:
        del connection_rec
        checked_out.discard(id(dbapi_conn))
        order.append("checkin")

    event.listen(engine, "checkout", _checkout)
    event.listen(engine, "checkin", _checkin)
    try:

        class TrackingAnalyst(DeterministicAdvisoryAnalyst):
            def analyze(self, facts, *, analysis_id=None):  # type: ignore[no-untyped-def]
                order.append("analyze")
                assert checked_out == set()
                return super().analyze(facts, analysis_id=analysis_id)

        service = AdvisoryService(
            read_scope=lambda: advisory_read_only_scope(session_factory),
            snapshot=SqlAlchemyAdvisorySnapshotReader(),
            analyst=TrackingAnalyst(),
            metrics=AtlasMetrics(CollectorRegistry()),
            mode="fake",
        )
        envelope = service.analyze_job(job_id)
    finally:
        event.remove(engine, "checkout", _checkout)
        event.remove(engine, "checkin", _checkin)

    assert envelope.research_job_id == job_id
    assert order.index("checkin") < order.index("analyze")
