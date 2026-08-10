"""Compose Redis-backed or no-op coordination ports from settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.coordination.contracts import HeartbeatRecorder, RateLimiter
from atlas.coordination.noop import NoopHeartbeatRecorder, NoopRateLimiter

if TYPE_CHECKING:
    from atlas.config.settings import Settings


def build_rate_limiter(settings: Settings) -> RateLimiter:
    """Return the configured rate limiter. Default is no-op."""
    if settings.coordination_provider == "noop":
        return NoopRateLimiter()
    if settings.coordination_provider == "redis":
        from atlas.coordination.rate_limit import RedisFixedWindowRateLimiter
        from atlas.coordination.redis_client import build_redis_client

        return RedisFixedWindowRateLimiter(
            client=build_redis_client(settings),
            max_requests=settings.rate_limit_max_requests,
            window_seconds=settings.rate_limit_window_seconds,
        )
    raise ValueError(
        f"Unsupported coordination provider: {settings.coordination_provider!r}"
    )


def build_heartbeat_recorder(settings: Settings) -> HeartbeatRecorder:
    """Return the configured heartbeat recorder. Default is no-op."""
    if settings.coordination_provider == "noop":
        return NoopHeartbeatRecorder()
    if settings.coordination_provider == "redis":
        from atlas.coordination.heartbeat import RedisHeartbeatRecorder
        from atlas.coordination.redis_client import build_redis_client

        return RedisHeartbeatRecorder(
            client=build_redis_client(settings),
            ttl_seconds=settings.heartbeat_ttl_seconds,
        )
    raise ValueError(
        f"Unsupported coordination provider: {settings.coordination_provider!r}"
    )
