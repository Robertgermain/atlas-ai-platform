"""Typed ports for ephemeral Redis-backed coordination (Slice 13A)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Result of a single rate-limit check for one identity."""

    allowed: bool
    # Seconds until the caller may retry. 0 when allowed.
    retry_after_seconds: int


class RateLimiter(Protocol):
    """Bound request attempts per identity within a fixed time window.

    Implementations must fail open: if the backing store is unavailable, a
    call to ``check`` must return ``allowed=True`` rather than raise or block
    for an unbounded amount of time.
    """

    def check(self, *, identity: str) -> RateLimitDecision: ...


class HeartbeatRecorder(Protocol):
    """Write a liveness signal for a running worker process.

    Implementations must fail open: if the backing store is unavailable, a
    call to ``beat`` must return without raising.
    """

    def beat(self, *, worker_id: str) -> None: ...
