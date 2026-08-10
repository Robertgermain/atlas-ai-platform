"""Unit tests for ResearchJobWorker orchestration."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from threading import Event

from sqlalchemy.orm import Session

from atlas.application.job_processing import (
    CompletedProcessing,
    ContinuationMode,
    PausedForReview,
    ProcessingOutcome,
    RetryScheduled,
    TerminalFailed,
)
from atlas.application.ports import ClaimedResearchJob
from atlas.application.worker import PROCESSING_TIMEOUT_REASON, ResearchJobWorker
from atlas.domain import ResearchJob, ResearchJobStatus
from atlas.outbox.fakes import RecordingOutbox

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
        return ClaimedResearchJob(
            job=job,
            claim_token=claim_token,
            continuation_mode=ContinuationMode.NONE,
            active_workflow_execution_id=None,
        )

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

    def set_active_workflow_execution(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        execution_id: str,
        at: datetime,
    ) -> bool:
        del session, job_id, claim_token, execution_id, at
        return True

    def schedule_retry(
        self,
        session: Session,
        *,
        job_id: str,
        claim_token: str,
        next_attempt_at: datetime,
        at: datetime,
    ) -> bool:
        del session, job_id, claim_token, next_attempt_at, at
        return True

    def transition_awaiting_review(
        self, session: Session, *, job_id: str, claim_token: str, at: datetime
    ) -> bool:
        del session, job_id, claim_token, at
        return True

    def approve_review_to_pending(
        self,
        session: Session,
        *,
        job_id: str,
        execution_id: str,
        next_attempt_at: datetime,
        at: datetime,
    ) -> bool:
        del session, job_id, execution_id, next_attempt_at, at
        return True

    def fail_from_review(
        self, session: Session, *, job_id: str, reason: str, at: datetime
    ) -> bool:
        del session, job_id, reason, at
        return True


def _echo_processor(
    question: str,
    *,
    job_id: str,
    claim_token: str,
    continuation_mode: str = "NONE",
    active_workflow_execution_id: str | None = None,
) -> ProcessingOutcome:
    del claim_token, continuation_mode, active_workflow_execution_id
    return CompletedProcessing(
        result=f"echo:{job_id}:{question}",
        workflow_execution_id="test-exec",
    )


def test_run_once_completes_deterministically() -> None:
    repo = _FakeRepository()
    job = ResearchJob.create("job-1", "What is Atlas?", at=T0)
    repo.seed_pending(job)
    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=_echo_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-1"]
        assert loaded.status is ResearchJobStatus.COMPLETED
        assert loaded.result == "echo:job-1:What is Atlas?"
        assert repo.completions
    finally:
        worker.close()


def test_timeout_finalizes_failure_and_ignores_late_result() -> None:
    repo = _FakeRepository()
    job = ResearchJob.create("job-timeout", "slow question", at=T0)
    repo.seed_pending(job)
    late_results: list[str] = []

    def slow_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del claim_token, continuation_mode, active_workflow_execution_id
        time.sleep(0.2)
        result = f"LATE:{job_id}:{question}"
        late_results.append(result)
        return CompletedProcessing(result=result, workflow_execution_id="test-exec")

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=slow_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=0.05,
        lease_seconds=1.0,
        shutdown_grace_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-timeout"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert loaded.failure_reason == PROCESSING_TIMEOUT_REASON
        assert repo.completions == []
        assert len(repo.failures) == 1
        time.sleep(0.3)
        assert loaded.status is ResearchJobStatus.FAILED
        assert late_results == ["LATE:job-timeout:slow question"]
        assert repo.completions == []
    finally:
        worker.close()


def test_processor_exception_fails_without_leaking_details() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-boom", "question", at=T0))
    secret = "super-secret-token"

    def boom(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, job_id, claim_token, continuation_mode
        del active_workflow_execution_id
        raise RuntimeError(f"db password={secret}")

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=boom,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
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
        processor=_echo_processor,
        shutdown_event=shutdown,
        outbox=RecordingOutbox(),
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

    def blocked_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, job_id, claim_token, continuation_mode
        del active_workflow_execution_id
        release.wait(timeout=30)
        return CompletedProcessing(
            result="should-not-finalize",
            workflow_execution_id="test-exec",
        )

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=blocked_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=0.05,
        lease_seconds=1.0,
        shutdown_grace_seconds=0.1,
        outbox=RecordingOutbox(),
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
        repo.seed_pending(ResearchJob.create("job-next", "next", at=T0))
        assert worker.run_once() is False
    finally:
        release.set()
        time.sleep(0.05)


def test_shutdown_with_processing_in_flight_completes_within_grace() -> None:
    repo = _FakeRepository()
    repo.seed_pending(ResearchJob.create("job-inflight", "almost done", at=T0))
    started = Event()

    def slow_ok(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del claim_token, continuation_mode, active_workflow_execution_id
        started.set()
        time.sleep(0.15)
        return CompletedProcessing(
            result=f"echo:{job_id}:{question}",
            workflow_execution_id="test-exec",
        )

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=slow_ok,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        shutdown_grace_seconds=1.0,
        outbox=RecordingOutbox(),
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


def test_paused_for_review_no_finalization() -> None:
    """PausedForReview outcome: worker does not finalize job."""
    repo = _FakeRepository()
    job = ResearchJob.create("job-review", "need review", at=T0)
    repo.seed_pending(job)

    def review_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        return PausedForReview(workflow_execution_id="exec-review")

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=review_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        assert repo.completions == []
        assert repo.failures == []
    finally:
        worker.close()


def test_retry_scheduled_no_finalization() -> None:
    """RetryScheduled outcome: worker does not finalize job."""
    repo = _FakeRepository()
    job = ResearchJob.create("job-retry", "transient fail", at=T0)
    repo.seed_pending(job)

    def retry_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        return RetryScheduled(
            workflow_execution_id="exec-retry",
            next_attempt_at=datetime(2026, 8, 9, 12, 0, 10, tzinfo=UTC),
            attempt_number=1,
        )

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=retry_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        assert repo.completions == []
        assert repo.failures == []
    finally:
        worker.close()


def test_terminal_failed_finalizes_failure() -> None:
    """TerminalFailed outcome: worker finalizes job as FAILED."""
    repo = _FakeRepository()
    job = ResearchJob.create("job-terminal", "fatal fail", at=T0)
    repo.seed_pending(job)

    def terminal_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        return TerminalFailed(
            reason_code="HARD_QUALITY_FAIL",
            workflow_execution_id="exec-terminal",
        )

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=terminal_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-terminal"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert len(repo.failures) == 1
    finally:
        worker.close()


def test_invalid_outcome_finalizes_failure() -> None:
    """Unrecognized outcome type: worker finalizes job as FAILED."""
    repo = _FakeRepository()
    job = ResearchJob.create("job-invalid", "unknown outcome", at=T0)
    repo.seed_pending(job)

    def invalid_processor(
        question: str,
        *,
        job_id: str,
        claim_token: str,
        continuation_mode: str = "NONE",
        active_workflow_execution_id: str | None = None,
    ) -> ProcessingOutcome:
        del question, claim_token, continuation_mode, active_workflow_execution_id
        return "unexpected"  # type: ignore[return-value]

    worker = ResearchJobWorker(
        session_factory=_FakeSessionFactory(),  # type: ignore[arg-type]
        repository=repo,
        processor=invalid_processor,
        poll_interval_seconds=0.01,
        processing_timeout_seconds=1.0,
        lease_seconds=1.0,
        outbox=RecordingOutbox(),
    )
    try:
        assert worker.run_once() is True
        loaded = repo.jobs["job-invalid"]
        assert loaded.status is ResearchJobStatus.FAILED
        assert "unrecognized" in (loaded.failure_reason or "").lower()
    finally:
        worker.close()
