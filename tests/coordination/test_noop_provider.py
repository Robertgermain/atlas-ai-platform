"""Unit tests for the default no-op coordination providers."""

from __future__ import annotations

from atlas.coordination.noop import NoopHeartbeatRecorder, NoopRateLimiter


def test_noop_rate_limiter_always_allows() -> None:
    limiter = NoopRateLimiter()
    for _ in range(1000):
        decision = limiter.check(identity="1.2.3.4")
        assert decision.allowed is True
        assert decision.retry_after_seconds == 0


def test_noop_heartbeat_recorder_does_nothing() -> None:
    recorder = NoopHeartbeatRecorder()
    # Must not raise.
    recorder.beat(worker_id="worker-1")
    recorder.beat(worker_id="worker-2")
