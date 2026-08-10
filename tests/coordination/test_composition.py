"""Unit tests for coordination provider composition (network-free).

Redis-backed instances are only checked by type; ``redis.Redis.from_url``
does not open a connection eagerly, so building the client here never
requires a reachable Redis server.
"""

from __future__ import annotations

from atlas.config.settings import Settings
from atlas.coordination.composition import build_heartbeat_recorder, build_rate_limiter
from atlas.coordination.heartbeat import RedisHeartbeatRecorder
from atlas.coordination.noop import NoopHeartbeatRecorder, NoopRateLimiter
from atlas.coordination.rate_limit import RedisFixedWindowRateLimiter
from atlas.coordination.redis_client import reset_redis_client_cache


def test_default_provider_builds_noop_rate_limiter() -> None:
    settings = Settings(coordination_provider="noop")
    assert isinstance(build_rate_limiter(settings), NoopRateLimiter)


def test_default_provider_builds_noop_heartbeat_recorder() -> None:
    settings = Settings(coordination_provider="noop")
    assert isinstance(build_heartbeat_recorder(settings), NoopHeartbeatRecorder)


def test_redis_provider_builds_redis_rate_limiter() -> None:
    reset_redis_client_cache()
    settings = Settings(
        coordination_provider="redis",
        redis_url="redis://127.0.0.1:6380/0",
        rate_limit_max_requests=10,
        rate_limit_window_seconds=60,
    )
    limiter = build_rate_limiter(settings)
    assert isinstance(limiter, RedisFixedWindowRateLimiter)


def test_redis_provider_builds_redis_heartbeat_recorder() -> None:
    reset_redis_client_cache()
    settings = Settings(
        coordination_provider="redis",
        redis_url="redis://127.0.0.1:6380/0",
        heartbeat_ttl_seconds=15,
    )
    recorder = build_heartbeat_recorder(settings)
    assert isinstance(recorder, RedisHeartbeatRecorder)
