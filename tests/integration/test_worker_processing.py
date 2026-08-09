"""API create then worker processing integration."""

from __future__ import annotations

import secrets
import time
from datetime import UTC, datetime, timedelta
from threading import Event

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.api.deps import provide_session_factory
from atlas.application.worker import PROCESSING_TIMEOUT_REASON, ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.main import app
from atlas.persistence.db import reset_engine_cache, session_scope
from atlas.persistence.repositories.research_job import SqlAlchemyResearchJobRepository
from atlas.workflow import LangGraphResearchProcessor, create_checkpoint_runtime

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


def _api_client(session_factory: sessionmaker[Session]) -> TestClient:
    reset_engine_cache()
    app.dependency_overrides[provide_session_factory] = lambda: session_factory
    return TestClient(app)


def _assert_report_structure(result: str, question: str) -> None:
    assert "Question:" in result
    assert question in result
    assert "Plan:" in result
    assert "Findings:" in result
    assert "Draft:" in result


def test_api_create_then_worker_completes(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    client = _api_client(session_factory)
    runtime = create_checkpoint_runtime(test_database_url)
    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=LangGraphResearchProcessor(
            checkpointer=runtime.checkpointer,
            session_factory=session_factory,
        ),
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )

    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Worker path"},
            headers={"Idempotency-Key": "worker-api-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert created.json()["status"] == "PENDING"
        assert "claim_token" not in created.json()
        assert "lease_expires_at" not in created.json()

        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "COMPLETED"
        _assert_report_structure(body["result"], "Worker path")
        assert "claim_token" not in body
        assert "lease_expires_at" not in body
    finally:
        worker.close()
        runtime.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_api_observes_failed_processor(
    session_factory: sessionmaker[Session],
) -> None:
    client = _api_client(session_factory)
    secret = "super-secret-db-password"

    def boom(question: str, *, job_id: str) -> str:
        del question, job_id
        raise ValueError(f"connection failed using {secret}")

    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=boom,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=15.0,
        lease_seconds=30.0,
    )
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Will fail"},
            headers={"Idempotency-Key": "worker-fail-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]

        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "FAILED"
        assert body["failure_reason"] == "Processing failed: ValueError"
        assert secret not in body["failure_reason"]
        assert body["result"] is None
    finally:
        worker.close()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_api_observes_processing_timeout(
    session_factory: sessionmaker[Session],
) -> None:
    client = _api_client(session_factory)
    release = Event()

    def blocked(question: str, *, job_id: str) -> str:
        del question, job_id
        release.wait(timeout=30)
        return "late-should-not-win"

    worker = ResearchJobWorker(
        session_factory=session_factory,
        repository=SqlAlchemyResearchJobRepository(),
        processor=blocked,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=0.05,
        lease_seconds=30.0,
        shutdown_grace_seconds=0.1,
    )
    try:
        created = client.post(
            "/v1/research-jobs",
            json={"question": "Slow work"},
            headers={"Idempotency-Key": "worker-timeout-key"},
        )
        assert created.status_code == 202
        job_id = created.json()["id"]

        assert worker.run_once() is True

        fetched = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched.status_code == 200
        body = fetched.json()
        assert body["status"] == "FAILED"
        assert body["failure_reason"] == PROCESSING_TIMEOUT_REASON
        assert body["result"] is None

        started = time.monotonic()
        worker.close()
        assert time.monotonic() - started < 1.0
        assert worker.processor_wait_abandoned is True

        fetched_again = client.get(f"/v1/research-jobs/{job_id}")
        assert fetched_again.json()["status"] == "FAILED"
    finally:
        release.set()
        app.dependency_overrides.clear()
        reset_engine_cache()


def test_reclaim_after_lease_expiry_and_stale_finalize_rejected(
    test_database_url: str,
    session_factory: sessionmaker[Session],
) -> None:
    repo = SqlAlchemyResearchJobRepository()
    with session_scope(session_factory) as session:
        repo.add(
            session,
            ResearchJob.create("reclaim-1", "Reclaim me", at=T0),
            idempotency_key="reclaim-key",
            request_fingerprint="b" * 64,
        )

    now_a = T0 + timedelta(seconds=1)
    token_a = secrets.token_hex(32)
    with session_scope(session_factory) as session:
        claimed_a = repo.claim_next(
            session,
            now=now_a,
            lease_expires_at=now_a + timedelta(seconds=30),
            claim_token=token_a,
        )
    assert claimed_a is not None

    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE research_jobs
                SET lease_expires_at = :expired
                WHERE id = :id
                """
            ),
            {"id": "reclaim-1", "expired": now_a - timedelta(seconds=1)},
        )
        session.commit()

    runtime = create_checkpoint_runtime(test_database_url)
    worker_b = ResearchJobWorker(
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
        assert worker_b.run_once() is True
    finally:
        worker_b.close()
        runtime.close()

    with session_scope(session_factory) as session:
        owned_a = repo.finalize_completion(
            session,
            job_id="reclaim-1",
            claim_token=token_a,
            result="stale-A",
            at=datetime.now(UTC),
        )
        loaded = repo.get(session, "reclaim-1")

    assert owned_a is False
    assert loaded is not None
    assert loaded.status is ResearchJobStatus.COMPLETED
    _assert_report_structure(loaded.result or "", "Reclaim me")
