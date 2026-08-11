"""Shutdown-aware, Kafka-free bounded backoff waiting (Slice 13C2B correction pass).

Before this module existed, a bounded retry/backoff episode inside
``ConsumerRunner`` could only be interrupted by SIGINT/SIGTERM *between*
calls to ``run_once()`` -- a mid-episode backoff (up to
``consumer_retry_max_backoff_seconds``, or the whole ~219s worst-case
episode) could not be interrupted, delaying shutdown by that much with the
Kafka offset still uncommitted the entire time.

``build_shutdown_aware_waiter`` returns a ``Waiter``: a callable that sleeps
in small bounded slices (never Kafka I/O -- ``poll()`` is never called
during a wait), checking a plain shutdown predicate between slices, and
returning early the moment shutdown is requested. The bounded interval
within which shutdown is observed is exactly ``slice_seconds`` (default
0.1s), regardless of how long the originally-requested wait was.
"""

from __future__ import annotations

import time
from collections.abc import Callable

#: A plain, non-blocking predicate -- e.g. reading a flag a signal handler
#: sets. Never a blocking wait itself.
ShutdownRequested = Callable[[], bool]

#: Returns ``True`` if shutdown was requested during (or before) the wait --
#: the caller must then abandon its retry episode with the Kafka offset left
#: uncommitted. ``False`` means the full duration elapsed with no shutdown
#: observed.
Waiter = Callable[[float], bool]

_DEFAULT_SLICE_SECONDS = 0.1


def build_shutdown_aware_waiter(
    shutdown_requested: ShutdownRequested,
    *,
    slice_seconds: float = _DEFAULT_SLICE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> Waiter:
    """Build a ``Waiter`` that sleeps in bounded slices, checking for shutdown.

    ``sleep`` is an injectable seam for tests only -- production code always
    uses the real ``time.sleep``. Never calls any Kafka client method.
    """
    if slice_seconds <= 0:
        raise ValueError("slice_seconds must be positive")

    def _wait(total_seconds: float) -> bool:
        if shutdown_requested():
            return True
        remaining = total_seconds
        while remaining > 0:
            this_slice = slice_seconds if slice_seconds < remaining else remaining
            sleep(this_slice)
            remaining -= this_slice
            if shutdown_requested():
                return True
        return False

    return _wait
