"""No-op coordination providers (default; keeps local dev/CI offline)."""

from __future__ import annotations

from atlas.coordination.contracts import RateLimitDecision


class NoopRateLimiter:
    """Always allows. Used when ``coordination_provider=noop`` (the default)."""

    def check(self, *, identity: str) -> RateLimitDecision:
        del identity
        return RateLimitDecision(allowed=True, retry_after_seconds=0)


class NoopHeartbeatRecorder:
    """Discards heartbeats. Used when ``coordination_provider=noop`` (the default)."""

    def beat(self, *, worker_id: str) -> None:
        del worker_id
