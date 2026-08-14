"""Advisory analysis must not mutate durable domain rows."""

from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.db import advisory_read_only_scope
from atlas.advisor.fakes import DeterministicAdvisoryAnalyst
from atlas.advisor.service import AdvisoryService
from atlas.domain import ResearchJob
from atlas.observability.metrics.catalog import AtlasMetrics
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.advisory_snapshot import (
    SqlAlchemyAdvisorySnapshotReader,
)
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository

T0 = datetime(2026, 8, 14, 13, 0, 0, tzinfo=UTC)
_JOB_ID = "advisory-mutation-job-1"
_TABLES = (
    "research_jobs",
    "workflow_executions",
    "workflow_node_executions",
    "model_invocations",
    "tool_invocations",
    "evaluation_runs",
    "policy_decisions",
    "human_review_decisions",
    "outbox_events",
    "consumer_inbox",
    "research_job_event_projection",
    "consumer_dead_letters",
)


def _counts(session: Session) -> dict[str, int]:
    return {
        table: int(session.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())
        for table in _TABLES
    }


def _job_hash(session: Session, job_id: str) -> str:
    return str(
        session.execute(
            text(
                "SELECT md5(CAST((id, status, question, result, failure_reason, "
                "repair_count, job_retry_count, evaluation_attempt_count, "
                "evaluation_profile, continuation_mode, updated_at) AS text)) "
                "FROM research_jobs WHERE id = :id"
            ),
            {"id": job_id},
        ).scalar_one()
    )


def test_analyze_job_leaves_counts_and_row_hash_unchanged(
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create(_JOB_ID, "What is Atlas?", at=T0)
    with session_scope(session_factory) as session:
        repo.add(
            session,
            job,
            idempotency_key=f"key-{_JOB_ID}",
            request_fingerprint="b" * 64,
        )

    with session_scope(session_factory) as session:
        before_counts = _counts(session)
        before_hash = _job_hash(session, _JOB_ID)

    service = AdvisoryService(
        read_scope=lambda: advisory_read_only_scope(session_factory),
        snapshot=SqlAlchemyAdvisorySnapshotReader(),
        analyst=DeterministicAdvisoryAnalyst(),
        metrics=AtlasMetrics(CollectorRegistry()),
        mode="fake",
    )
    envelope = service.analyze_job(_JOB_ID)
    assert envelope.research_job_id == _JOB_ID

    with session_scope(session_factory) as session:
        after_counts = _counts(session)
        after_hash = _job_hash(session, _JOB_ID)

    assert after_counts == before_counts
    assert after_hash == before_hash
