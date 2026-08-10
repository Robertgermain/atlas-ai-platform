"""Background worker orchestration for research jobs."""

from __future__ import annotations

import logging
import secrets
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime, timedelta
from functools import partial
from threading import Event
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
from atlas.persistence.db import session_scope

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

PROCESSING_TIMEOUT_REASON = "Processing timed out."


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
        shutdown_event: Event | None = None,
        heartbeat_recorder: HeartbeatRecorder | None = None,
        heartbeat_interval_seconds: float = 5.0,
        worker_id: str | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._processor = processor
        self._poll_interval_seconds = poll_interval_seconds
        self._processing_timeout_seconds = processing_timeout_seconds
        self._lease_seconds = lease_seconds
        self._shutdown_grace_seconds = (
            processing_timeout_seconds
            if shutdown_grace_seconds is None
            else shutdown_grace_seconds
        )
        self._shutdown_event = shutdown_event or Event()
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
            return False

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
            self._finalize_failure(claimed, reason=PROCESSING_TIMEOUT_REASON)
            self._abandoned_future = future
            self._inflight_future = None
            return
        except ClaimOwnershipError:
            logger.warning(
                "Claim ownership lost during processing of job %s; "
                "finalize_failure will no-op if claim is gone.",
                claimed.job.id,
            )
            self._finalize_failure(
                claimed, reason="Claim ownership lost during processing."
            )
            self._inflight_future = None
            self._ignore_late_result(future)
            return
        except Exception as exc:
            self._finalize_failure(
                claimed,
                reason=f"Processing failed: {exc.__class__.__name__}",
            )
            self._inflight_future = None
            self._ignore_late_result(future)
            return

        self._inflight_future = None
        self._handle_outcome(claimed, outcome)

    def _handle_outcome(
        self, claimed: ClaimedResearchJob, outcome: ProcessingOutcome
    ) -> None:
        if isinstance(outcome, CompletedProcessing):
            self._finalize_completion(claimed, result=outcome.result)
        elif isinstance(outcome, TerminalFailed):
            self._finalize_failure(claimed, reason=f"Terminal: {outcome.reason_code}")
        elif isinstance(outcome, (PausedForReview, RetryScheduled)):
            logger.info(
                "Processor returned %s for job %s; no worker finalization.",
                type(outcome).__name__,
                claimed.job.id,
            )
        else:
            self._finalize_failure(
                claimed, reason="Processing returned unrecognized outcome"
            )

    def _finalize_completion(self, claimed: ClaimedResearchJob, *, result: str) -> bool:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            owned = self._repository.finalize_completion(
                session,
                job_id=claimed.job.id,
                claim_token=claimed.claim_token,
                result=result,
                at=at,
            )
        if not owned:
            logger.warning(
                "Lost claim ownership while completing research job %s",
                claimed.job.id,
            )
        return owned

    def _finalize_failure(self, claimed: ClaimedResearchJob, *, reason: str) -> bool:
        at = datetime.now(UTC)
        with session_scope(self._session_factory) as session:
            owned = self._repository.finalize_failure(
                session,
                job_id=claimed.job.id,
                claim_token=claimed.claim_token,
                reason=reason,
                at=at,
            )
        if not owned:
            logger.warning(
                "Lost claim ownership while failing research job %s",
                claimed.job.id,
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
            logger.warning(
                "Processor still running after %.3fs shutdown grace; "
                "abandoning wait without killing the thread. Claim fencing "
                "prevents stale finalization. The process may remain alive "
                "until the thread finishes or is force-killed. Hard "
                "termination of arbitrary LLM/tool/graph work requires process "
                "isolation or an external worker system.",
                timeout,
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
