"""Subprocess CLI stream contract against Compose PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from atlas.advisor.contracts import AdvisoryStdoutEnvelope
from atlas.domain import ResearchJob
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from tests.advisor.cli_contract import (
    QUESTION_CANARY,
    assert_canaries_absent,
    assert_failure_streams,
    assert_success_streams,
    run_advisor_cli,
)

T0 = datetime(2026, 8, 14, 17, 0, 0, tzinfo=UTC)
_SUCCESS_JOB_ID = "advisory-cli-stream-job-1"
_MISSING_JOB_ID = "advisory-cli-stream-missing-1"


def _seed_job(session_factory: sessionmaker[Session], job_id: str) -> None:
    repo = SqlAlchemyResearchJobRepository()
    job = ResearchJob.create(job_id, QUESTION_CANARY, at=T0)
    with session_scope(session_factory) as session:
        repo.add(
            session,
            job,
            idempotency_key=f"key-{job_id}",
            request_fingerprint="c" * 64,
        )


def test_subprocess_fake_success_envelope_is_stdout_only(
    session_factory: sessionmaker[Session],
    test_database_url: str,
    tmp_path: Path,
) -> None:
    _seed_job(session_factory, _SUCCESS_JOB_ID)
    result = run_advisor_cli(
        ["--research-job-id", _SUCCESS_JOB_ID],
        cwd=tmp_path,
        extra_env={"ATLAS_DATABASE_URL": test_database_url},
    )
    assert result.returncode == 0
    envelope = assert_success_streams(result.stdout, result.stderr)
    assert isinstance(envelope, AdvisoryStdoutEnvelope)
    assert envelope.research_job_id == _SUCCESS_JOB_ID
    assert envelope.analysis.schema_version == "advisory.analysis.v1"
    assert_canaries_absent(result.stdout, result.stderr)


def test_subprocess_missing_job_leaves_stdout_empty(
    test_database_url: str,
    tmp_path: Path,
) -> None:
    result = run_advisor_cli(
        ["--research-job-id", _MISSING_JOB_ID],
        cwd=tmp_path,
        extra_env={"ATLAS_DATABASE_URL": test_database_url},
    )
    assert result.returncode == 1
    assert_failure_streams(result.stdout, result.stderr)
    assert_canaries_absent(result.stdout, result.stderr)
