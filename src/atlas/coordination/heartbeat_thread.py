"""Dedicated daemon thread that refreshes a worker heartbeat on an interval.

Runs independently of the worker's poll/processing loop, so a worker that is
busy processing a job still refreshes its heartbeat on schedule. Never
touches job-processing or claim/lease state; any error talking to the
backing store is caught inside the recorder (fail-open) with a last-resort
catch here so an unexpected error can never crash the thread or the worker.
"""

from __future__ import annotations

import logging
import threading

from atlas.coordination.contracts import HeartbeatRecorder
from atlas.coordination.outage_log import OncePerOutageLogger

logger = logging.getLogger(__name__)


class HeartbeatThread:
    """Owns a single background thread that calls ``recorder.beat`` on an interval."""

    def __init__(
        self,
        *,
        recorder: HeartbeatRecorder,
        worker_id: str,
        interval_seconds: float,
    ) -> None:
        self._recorder = recorder
        self._worker_id = worker_id
        self._interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="atlas-worker-heartbeat",
            daemon=True,
        )
        self._unexpected_outage_log = OncePerOutageLogger(
            logger,
            warning_message="Heartbeat recorder raised unexpectedly; continuing.",
        )

    def start(self) -> None:
        self._thread.start()

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, *, join_timeout_seconds: float = 2.0) -> None:
        """Signal the thread to stop and wait briefly for it to exit.

        Being a daemon thread, it can never itself block process exit even
        if the join times out, so this never blocks worker shutdown for long.
        """
        self._stop_event.set()
        self._thread.join(timeout=join_timeout_seconds)
        if self._thread.is_alive():
            logger.warning(
                "Heartbeat thread did not stop within %.1fs; abandoning wait.",
                join_timeout_seconds,
            )

    def _run(self) -> None:
        # Beat immediately on start so a fresh worker's liveness key appears
        # right away, then on each interval until stopped.
        while True:
            try:
                self._recorder.beat(worker_id=self._worker_id)
            except Exception as exc:
                # Bound + sanitize: exception class only, no raw text / traceback.
                if self._unexpected_outage_log.begin_outage_if_new():
                    logger.warning(
                        "Heartbeat recorder raised unexpectedly (%s); continuing.",
                        type(exc).__name__,
                    )
            else:
                self._unexpected_outage_log.note_success()
            if self._stop_event.wait(self._interval_seconds):
                return
