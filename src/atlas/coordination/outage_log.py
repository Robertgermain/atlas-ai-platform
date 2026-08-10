"""Thread-safe once-per-outage-episode logging for Redis coordination."""

from __future__ import annotations

import logging
import threading


class OncePerOutageLogger:
    """Log one sanitized warning for an outage episode; silence repeats.

    A successful operation resets the episode so a later distinct outage can
    emit one warning again. Never interpolates Redis URLs, credentials, raw
    exception messages, or tracebacks.
    """

    def __init__(self, logger: logging.Logger, *, warning_message: str) -> None:
        self._logger = logger
        self._warning_message = warning_message
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
            self._logger.warning(self._warning_message)

    def note_success(self) -> None:
        """Clear the outage episode after a successful Redis operation."""
        with self._lock:
            self._in_outage = False
