"""Background worker orchestration for research jobs."""

from __future__ import annotations

import logging
import secrets
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime, timedelta
from functools import partial
from threading import Event as ThreadingEvent
from typing import TYPE_CHECKING

from atlas.application.exceptions import ClaimOwnershipError
from atlas.application.job_processing import (
    CompletedProcessing,
    PausedForReview,
    ProcessingOutcome,
    ResearchJobProcessor,
    RetryScheduled,
    TerminalFailed,
)
from atlas.application.ports import ClaimedResearchJob, ResearchJobRepository
from atlas.coordination.contracts import HeartbeatRecorder
from atlas.coordination.heartbeat_thread import HeartbeatThread
from atlas.coordination.noop import NoopHeartbeatRecorder
from atlas.eventing.builders import (
    build_research_job_completed,
    build_research_job_failed,
)
from atlas.observability.events import Event
from atlas.observability.logging import log_event, log_exception_boundary
from atlas.observability.metrics import AtlasMetrics, default_metrics
from atlas.outbox.ports import OutboxEnqueuer
from atlas.persistence.db import session_scope
from atlas.persistence.repositories.outbox import SqlAlchemyOutboxRepository

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

PROCESSING_TIMEOUT_REASON = "Processing timed out."
PROCESSING_TIMEOUT_REASON_CLASS = "ProcessingTimeout"
CLAIM_OWNERSHIP_LOST_REASON_CLASS = "ClaimOwnershipLost"
UNRECOGNIZED_OUTCOME_REASON_CLASS = "UnrecognizedOutcome"


class ResearchJobWorker:
    """Claim, process, and fenced-finalize research jobs.

    Shutdown guarantee (Milestone 6/7):
    - After ``request_shutdown`` / ``close``, the worker claims no new jobs.
    - Orchestration waits at most ``shutdown_grace_seconds`` for an in-flight or
      abandoned processor future, then returns without blocking forever.
    - Claim-token fencing still prevents abandoned/stale work from finalizing.
    - At most one processor thread is used (``max_workers=1``).
    - A dedicated daemon heartbeat thread (Slice 13A) refreshes a liveness
      key independently of the poll/process loop and is stopped in
      ``close()``. It never affects claim/lease ownership or job outcomes;
      by default it is a no-op (``coordination_provider=noop``).

    Non-guarantee:
    - Python cannot kill a blocked processor thread.
    - ``ThreadPoolExecutor`` threads are non-daemon; if a processor never
      returns, ``close()`` abandons the wait after the grace period using
      ``shutdown(wait=False)``, but the OS process may still remain alive until
      the thread finishes or the process is force-killed (SIGKILL).
    - Hard termination of arbitrary LLM/tool/graph work requires process
      isolation later. Milestone 8 does not strengthen this fencing.
    """

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: ResearchJobRepository,
        processor: ResearchJobProcessor,
        poll_interval_seconds: float = 1.0,
        processing_timeout_seconds: float = 60.0,
        lease_seconds: float = 90.0,
        shutdown_grace_seconds: float | None = None,
        shutdown_event: ThreadingEvent | None = None,
        heartbeat_recorder: HeartbeatRecorder | None = None,
        heartbeat_interval_seconds: float = 5.0,
        worker_id: str | None = None,
        outbox: OutboxEnqueuer | None = None,
        metrics: AtlasMetrics | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._processor = processor
        self._outbox = outbox or SqlAlchemyOutboxRepository()
        self._metrics = metrics or default_metrics()
        self._poll_interval_seconds = poll_interval_seconds
        self._processing_timeout_seconds = processing_timeout_seconds
        self._lease_seconds = lease_seconds
        self._shutdown_grace_seconds = (
            processing_timeout_seconds
            if shutdown_grace_seconds is None
            else shutdown_grace_seconds
        )
        self._shutdown_event = shutdown_event or ThreadingEvent()
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="atlas-job-processor",
        )
        self._inflight_future: Future[ProcessingOutcome] | None = None
        self._abandoned_future: Future[ProcessingOutcome] | None = None
        self._closed = False
        self._processor_wait_abandoned = False

        self._worker_id = worker_id or secrets.token_hex(8)
        self._heartbeat_thread = HeartbeatThread(
            recorder=heartbeat_recorder or NoopHeartbeatRecorder(),
            worker_id=self._worker_id,
            interval_seconds=heartbeat_interval_seconds,
        )
        self._heartbeat_thread.start()

    @property
    def processor_wait_abandoned(self) -> bool:
        """True when close() stopped waiting on a still-running processor."""
        return self._processor_wait_abandoned

    @property
    def worker_id(self) -> str:
        """Identity stamped on this worker's heartbeat keys."""
        return self._worker_id

    def request_shutdown(self) -> None:
        """Signal the worker loop to stop claiming new work."""
        self._shutdown_event.set()

    def close(self) -> None:
        """Apply the bounded shutdown policy.

        Stops new claims, waits at most ``shutdown_grace_seconds`` for processor
        work, then returns. Does not kill threads and does not guarantee process
        exit if a processor remains blocked.
        """
        if self._closed:
            return
        self._closed = True
        self._shutdown_event.set()
        self._heartbeat_thread.stop()
        self._join_processor_bounded(self._shutdown_grace_seconds)
        # wait=False: do not block forever on a hung callable.
        self._executor.shutdown(wait=False, cancel_futures=True)

    def run_forever(self) -> None:
        """Poll and process jobs until shutdown is requested."""
        try:
            while not self._shutdown_event.is_set():
                processed = self.run_once()
                if not processed and not self._shutdown_event.is_set():
                    self._shutdown_event.wait(self._poll_interval_seconds)
        finally:
            self.close()

    def run_once(self) -> bool:
        """Claim and process at most one job. Return True when a claim occurred."""
        if self._shutdown_event.is_set():
            return False

        if not self._ready_for_new_claim():
            return False

        claimed = self._claim_next()
        if claimed is None:
            self._metrics.observe_worker_claim(outcome="empty")
            return False

        self._metrics.observe_worker_claim(outcome="claimed")
        self._process_claimed(claimed)
        return True

    def _ready_for_new_claim(self) -> bool:
        """Refuse new claims while an abandoned processor still occupies the pool."""
        abandoned = self._abandoned_future
        if abandoned is None:
            return True
        if abandoned.done():
            self._ignore_late_result(abandoned)
            self._abandoned_future = None
            return True
        # Still running: do not submit more work (keeps thread count bounded).
        return False

    def _claim_next(self) -> ClaimedResearchJob | None:
        now = datetime.now(UTC)
        claim_token = secrets.token_hex(32)
        lease_expires_at = now + timedelta(seconds=self._lease_seconds)
        with session_scope(self._session_factory) as session:
            return self._repository.claim_next(
                session,
                now=now,
                lease_expires_at=lease_expires_at,
                claim_token=claim_token,
            )

    def _process_claimed(self, claimed: ClaimedResearchJob) -> None:
        started_at = time.perf_counter()
        future: Future[ProcessingOutcome] = self._executor.submit(
            partial(
                self._processor,
                claimed.job.question,
                job_id=claimed.job.id,
                claim_token=claimed.claim_token,
                continuation_mode=claimed.continuation_mode,
                active_workflow_execution_id=claimed.active_workflow_execution_id,
            )
        )
        self._inflight_future = future
        try:
            outcome = future.result(timeout=self._processing_timeout_seconds)
        except FuturesTimeoutError:
            self._finalize_failure(
                claimed,
                reason=PROCESSING_TIMEOUT_REASON,
                reason_class=PROCESSING_TIMEOUT_REASON_CLASS,
                duration_seconds=time.perf_counter() - started_at,
            )
            self._abandoned_future = future
            self._inflight_future = None
            return
        except ClaimOwnershipError:
            log_event(
                logger,
                Event.CLAIM_OWNERSHIP_LOST,
                level=logging.WARNING,
                research_job_id=claimed.job.id,
                outcome="processing",
            )
            self._finalize_failure(
                claimed,
                reason="Claim ownership lost during processing.",
                reason_class=CLAIM_OWNERSHIP_LOST_REASON_CLASS,
                duration_seconds=time.perf_counter() - started_at,
            )
            self._inflight_future = None
            self._ignore_late_result(future)
            return
        except Exception as exc:
            self._finalize_failure(
                claimed,
                reason=f"Processing failed: {exc.__class__.__name__}",
                reason_class=exc.__class__.__name__,
                duration_seconds=time.perf_counter() - started_at,
            )
            self._inflight_future = None
            self._ignore_late_result(future)
            return

        self._inflight_future = None
        # Outcome finalization (including outbox insert) is outside the
        # processor-exception handler so an outbox failure cannot recurse into
        # finalize_failure.
        self._handle_outcome(
            claimed, outcome, duration_seconds=time.perf_counter() - started_at
        )

    def _handle_outcome(
        self,
        claimed: ClaimedResearchJob,
        outcome: ProcessingOutcome,
        *,
        duration_seconds: float,
    ) -> None:
        if isinstance(outcome, CompletedProcessing):
            self._finalize_completion(
                claimed, result=outcome.result, duration_seconds=duration_seconds
            )
        elif isinstance(outcome, TerminalFailed):
            self._finalize_failure(
                claimed,
                reason=f"Terminal: {outcome.reason_code}",
                reason_class=outcome.reason_code,
                duration_seconds=duration_seconds,
            )
        elif isinstance(outcome, (PausedForReview, RetryScheduled)):
            log_event(
                logger,
                Event.PROCESSOR_OUTCOME_DEFERRED,
                research_job_id=claimed.job.id,
                outcome=type(outcome).__name__,
            )
            deferred_outcome = (
                "paused_for_review"
                if isinstance(outcome, PausedForReview)
                else "retry_scheduled"
            )
            self._metrics.observe_worker_processing(
                outcome=deferred_outcome, duration_seconds=duration_seconds
            )
        else:
            self._finalize_failure(
                claimed,
                reason="Processing returned unrecognized outcome",
                reason_class=UNRECOGNIZED_OUTCOME_REASON_CLASS,
                duration_seconds=duration_seconds,
            )

    def _finalize_completion(
        self, claimed: ClaimedResearchJob, *, result: str, duration_seconds: float
    ) -> bool:
        at = datetime.now(UTC)
        owned = False
        try:
            with session_scope(self._session_factory) as session:
                owned = self._repository.finalize_completion(
                    session,
                    job_id=claimed.job.id,
                    claim_token=claimed.claim_token,
                    result=result,
                    at=at,
                )
                if owned:
                    self._outbox.enqueue(
                        session,
                        build_research_job_completed(
                            research_job_id=claimed.job.id,
                            completed_at=at,
                        ),
                    )
        except Exception as exc:
            log_exception_boundary(
                logger,
                Event.FINALIZATION_FAILED,
                exc,
                level=logging.WARNING,
                research_job_id=claimed.job.id,
                outcome="completion",
            )
            self._metrics.observe_worker_processing(
                outcome="finalization_failed", duration_seconds=duration_seconds
            )
            raise
        if owned:
            self._metrics.observe_worker_processing(
                outcome="completed", duration_seconds=duration_seconds
            )
            self._metrics.observe_research_job_terminal(status="completed")
        else:
            log_event(
                logger,
                Event.CLAIM_OWNERSHIP_LOST,
                level=logging.WARNING,
                research_job_id=claimed.job.id,
                outcome="completion",
            )
            self._metrics.observe_worker_processing(
                outcome="claim_ownership_lost", duration_seconds=duration_seconds
            )
        return owned

    def _finalize_failure(
        self,
        claimed: ClaimedResearchJob,
        *,
        reason: str,
        reason_class: str,
        duration_seconds: float,
    ) -> bool:
        at = datetime.now(UTC)
        owned = False
        try:
            with session_scope(self._session_factory) as session:
                owned = self._repository.finalize_failure(
                    session,
                    job_id=claimed.job.id,
                    claim_token=claimed.claim_token,
                    reason=reason,
                    at=at,
                )
                if owned:
                    self._outbox.enqueue(
                        session,
                        build_research_job_failed(
                            research_job_id=claimed.job.id,
                            failed_at=at,
                            reason_class=reason_class,
                        ),
                    )
        except Exception as exc:
            # Never recurse into finalize_failure when this path itself failed.
            log_exception_boundary(
                logger,
                Event.FINALIZATION_FAILED,
                exc,
                level=logging.WARNING,
                research_job_id=claimed.job.id,
                outcome="failure",
            )
            self._metrics.observe_worker_processing(
                outcome="finalization_failed", duration_seconds=duration_seconds
            )
            raise
        if owned:
            self._metrics.observe_worker_processing(
                outcome="failed", duration_seconds=duration_seconds
            )
            self._metrics.observe_research_job_terminal(status="failed")
        else:
            log_event(
                logger,
                Event.CLAIM_OWNERSHIP_LOST,
                level=logging.WARNING,
                research_job_id=claimed.job.id,
                outcome="failure",
            )
            self._metrics.observe_worker_processing(
                outcome="claim_ownership_lost", duration_seconds=duration_seconds
            )
        return owned

    def _join_processor_bounded(self, timeout: float) -> None:
        """Wait briefly for processor work; never block indefinitely."""
        future = self._inflight_future or self._abandoned_future
        self._inflight_future = None
        self._abandoned_future = None
        if future is None:
            return
        try:
            future.result(timeout=timeout)
        except FuturesTimeoutError:
            self._processor_wait_abandoned = True
            self._abandoned_future = future
            # Claim fencing (not this log line) is what prevents stale
            # finalization; see the class docstring's shutdown guarantee.
            log_event(
                logger,
                Event.SHUTDOWN_WAIT_ABANDONED,
                level=logging.WARNING,
                outcome="processor",
                duration_ms=timeout * 1000,
            )
        except Exception:
            # Late processor failure is ignored permanently.
            return
        # Late processor success is ignored permanently.

    @staticmethod
    def _ignore_late_result(future: Future[ProcessingOutcome]) -> None:
        """Consume a finished future without using its value for finalization."""
        if not future.done():
            return
        try:
            future.result(timeout=0)
        except Exception:
            return
