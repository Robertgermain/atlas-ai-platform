"""Thread-safe once-per-outage-episode logging for Redis coordination."""

from __future__ import annotations

import logging
import threading

from atlas.observability.events import Event
from atlas.observability.logging import log_event


class OncePerOutageLogger:
    """Log one sanitized warning for an outage episode; silence repeats.

    A successful operation resets the episode so a later distinct outage can
    emit one warning again. Emits only the fixed ``event`` (with a fixed
    ``outcome`` label identifying which dependency operation failed) via
    :func:`atlas.observability.logging.log_event` -- never a free-text
    message, Redis URL, credential, raw exception message, or traceback.
    """

    def __init__(self, logger: logging.Logger, *, event: Event, outcome: str) -> None:
        self._logger = logger
        self._event = event
        self._outcome = outcome
        self._lock = threading.Lock()
        self._in_outage = False

    @property
    def in_outage(self) -> bool:
        with self._lock:
            return self._in_outage

    def begin_outage_if_new(self) -> bool:
        """Mark an outage episode. Return True only for the first failure."""
        with self._lock:
            if self._in_outage:
                return False
            self._in_outage = True
            return True

    def note_failure(self) -> None:
        """Record a failure; emit at most one warning for the current episode."""
        if self.begin_outage_if_new():
            log_event(
                self._logger,
                self._event,
                level=logging.WARNING,
                outcome=self._outcome,
            )

    def note_success(self) -> None:
        """Clear the outage episode after a successful Redis operation."""
        with self._lock:
            self._in_outage = False
