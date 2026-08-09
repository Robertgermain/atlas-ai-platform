"""Unit tests for ResearchJobWorker orchestration."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from threading import Event

from sqlalchemy.orm import Session

from atlas.application.ports import ClaimedResearchJob
from atlas.application.worker import PROCESSING_TIMEOUT_REASON, ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus

T0 = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)


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


class _FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[str, ResearchJob] = {}
        self.tokens: dict[str, str] = {}
        self.claim_calls = 0
        self.completions: list[tuple[str, str, str]] = []
        self.failures: list[tuple[str, str, str]] = []
        self._pending: list[ResearchJob] = []

    def seed_pending(self, job: ResearchJob) -> None:
        self._pending.append(job)
        self.jobs[job.id] = job

    def add(self, session: Session, job: ResearchJob, **kwargs: object) -> None:
        del session, job, kwargs
        raise NotImplementedError

    def get(self, session: Session, job_id: str) -> ResearchJob | None:
        del session
        return self.jobs.get(job_id)

    def get_by_idempotency_key(self, session: Session, idempotency_key: str) -> None:
        del session, idempotency_key
        raise NotImplementedError

    def save(self, session: Session, job: ResearchJob) -> None:
        del session
        self.jobs[job.id] = job

    def claim_next(
        self,
        session: Session,
        *,
        now: datetime,
        lease_expires_at: datetime,
        claim_token: str,
    ) -> ClaimedResearchJob | None:
        del session, lease_expires_at
        self.claim_calls += 1
        if not self._pending:
            return None
        job = self._pending.pop(0)
        job.start(at=now)
        self.jobs[job.id] = job
        self.tokens[job.id] = claim_token
        return ClaimedResearchJob(job=job, claim_token=claim_token)

    def finalize_completion(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        result: str,
        at: datetime,
    ) -> bool:
        del session
        if self.tokens.get(job_id) != claim_token:
            return False
        job = self.jobs[job_id]
        job.complete(result, at=at)
        self.completions.append((job_id, claim_token, result))
        del self.tokens[job_id]
        return True

    def finalize_failure(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        reason: str,
        at: datetime,
    ) -> bool:
        del session
        if self.tokens.get(job_id) != claim_token:
            return False
        job = self.jobs[job_id]
        job.fail(reason, at=at)
        self.failures.append((job_id, claim_token, reason))
        del self.tokens[job_id]
        return True


def test_run_once_completes_deterministically() -> None:
    repo = _FakeRepository()
    job = ResearchJob.create("job-1", "What is Atlas?", at=T0)
    repo.seed_pending(job)
    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-1"]
        assert loaded.status is ResearchJobStatus.COMPLETED
        assert loaded.result == "Research completed for: What is Atlas?"
        assert repo.completions
    finally:
        worker.close()


def test_timeout_finalizes_failure_and_ignores_late_result() -> None:
    repo = _FakeRepository()
    job = ResearchJob.create("job-timeout", "slow question", at=T0)
    repo.seed_pending(job)
    late_results: list[str] = []

    def slow_processor(question: str) -> str:
        time.sleep(0.2)
        result = f"LATE:{question}"
        late_results.append(result)
        return result

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=slow_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=0.05,
        lease_seconds=1.0,
        shutdown_grace_seconds=1.0,
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-timeout"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert loaded.failure_reason == PROCESSING_TIMEOUT_REASON
        assert repo.completions == []
        assert len(repo.failures) == 1
        # Allow the slow processor to finish naturally; late success must not complete.
        time.sleep(0.3)
        assert loaded.status is ResearchJobStatus.FAILED
        assert late_results == ["LATE:slow question"]
        assert repo.completions == []
    finally:
        worker.close()


def test_processor_exception_fails_without_leaking_details() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-boom", "question", at=T0))
    secret = "super-secret-token"

    def boom(_question: str) -> str:
        raise RuntimeError(f"db password={secret}")

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=boom,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-boom"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert loaded.failure_reason == "Processing failed: RuntimeError"
        assert secret not in (loaded.failure_reason or "")
        assert "password" not in (loaded.failure_reason or "")
        assert repo.completions == []
    finally:
        worker.close()


def test_shutdown_prevents_new_claims() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-2", "q", at=T0))
    shutdown = Event()
    shutdown.set()
    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        shutdown_event=shutdown,
    )
    try:
        assert worker.run_once() is False
        assert repo.claim_calls == 0
    finally:
        worker.close()


def test_close_is_bounded_while_processor_remains_blocked() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-block", "blocked", at=T0))
    release = Event()

    def blocked_processor(_question: str) -> str:
        release.wait(timeout=30)
        return "should-not-finalize"

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=blocked_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=0.05,
        lease_seconds=1.0,
        shutdown_grace_seconds=0.1,
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-block"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert loaded.failure_reason == PROCESSING_TIMEOUT_REASON

        started = time.monotonic()
        worker.close()
        elapsed = time.monotonic() - started
        assert elapsed < 1.0
        assert worker.processor_wait_abandoned is True
        assert loaded.status is ResearchJobStatus.FAILED
        assert repo.completions == []
        # Further claims must not start while the pool thread is still occupied.
        repo.seed_pending(ResearchJob.create("job-next", "next", at=T0))
        assert worker.run_once() is False
    finally:
        release.set()
        # Give the blocked thread a moment to exit after release.
        time.sleep(0.05)


def test_shutdown_with_processing_in_flight_completes_within_grace() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-inflight", "almost done", at=T0))
    started = Event()

    def slow_ok(question: str) -> str:
        started.set()
        time.sleep(0.15)
        return f"Research completed for: {question}"

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=slow_ok,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        shutdown_grace_seconds=1.0,
    )
    from concurrent.futures import ThreadPoolExecutor

    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(worker.run_once)
        assert started.wait(timeout=1.0)
        worker.request_shutdown()
        assert future.result(timeout=2.0) is True
        loaded = repo.jobs["job-inflight"]
        assert loaded.status is ResearchJobStatus.COMPLETED
        worker.close()
        assert worker.processor_wait_abandoned is False
    finally:
        pool.shutdown(wait=True, cancel_futures=False)
