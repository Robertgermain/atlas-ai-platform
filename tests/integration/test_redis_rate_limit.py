"""Real-Redis integration tests for the fixed-window rate limiter.

Requires a reachable Redis (Docker Compose `redis` service or CI's Redis
service). URL is read from ``ATLAS_REDIS_URL`` (default matches local
Compose: ``redis://127.0.0.1:6380/0``).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
import redis
from prometheus_client import CollectorRegistry

from atlas.coordination.rate_limit import RedisFixedWindowRateLimiter
from atlas.observability.metrics.catalog import AtlasMetrics
from tests.integration.redis_support import build_test_redis_client, cleanup_atlas_keys


def _label_values(metrics: AtlasMetrics, metric_name: str, label: str) -> list[str]:
    return [
        sample.labels[label]
        for metric in metrics.registry.collect()
        for sample in metric.samples
        if sample.name == metric_name
    ]


@pytest.fixture(scope="module")
def redis_client() -> Iterator[redis.Redis]:
    client = build_test_redis_client()
    try:
        client.ping()
    except redis.RedisError as exc:
        pytest.skip(f"Redis is not reachable for integration tests: {exc}")
    yield client
    client.close()


@pytest.fixture(autouse=True)
def _cleanup(redis_client: redis.Redis) -> Iterator[None]:
    yield
    cleanup_atlas_keys(redis_client)


def _unique_identity() -> str:
    return f"test-{uuid.uuid4().hex}"


def test_allows_up_to_max_requests_then_denies(redis_client: redis.Redis) -> None:
    limiter = RedisFixedWindowRateLimiter(
        client=redis_client, max_requests=10, window_seconds=60
    )
    identity = _unique_identity()

    for _ in range(10):
        decision = limiter.check(identity=identity)
        assert decision.allowed is True
        assert decision.retry_after_seconds == 0

    denied = limiter.check(identity=identity)
    assert denied.allowed is False
    assert 0 < denied.retry_after_seconds <= 60


def test_identities_are_independent(redis_client: redis.Redis) -> None:
    limiter = RedisFixedWindowRateLimiter(
        client=redis_client, max_requests=1, window_seconds=60
    )
    first_identity = _unique_identity()
    second_identity = _unique_identity()

    assert limiter.check(identity=first_identity).allowed is True
    assert limiter.check(identity=first_identity).allowed is False
    # A different identity has its own independent budget.
    assert limiter.check(identity=second_identity).allowed is True


def test_concurrent_requests_are_counted_exactly_once_each(
    redis_client: redis.Redis,
) -> None:
    """Proves the INCR+PEXPIRE Lua script is atomic under real concurrency."""
    max_requests = 10
    limiter = RedisFixedWindowRateLimiter(
        client=redis_client, max_requests=max_requests, window_seconds=60
    )
    identity = _unique_identity()
    attempts = 30

    def _attempt(_: int) -> bool:
        return limiter.check(identity=identity).allowed

    with ThreadPoolExecutor(max_workers=attempts) as pool:
        results = list(pool.map(_attempt, range(attempts)))

    allowed_count = sum(1 for allowed in results if allowed)
    assert allowed_count == max_requests


def test_window_resets_after_ttl_expires(redis_client: redis.Redis) -> None:
    limiter = RedisFixedWindowRateLimiter(
        client=redis_client, max_requests=1, window_seconds=1
    )
    identity = _unique_identity()

    assert limiter.check(identity=identity).allowed is True
    assert limiter.check(identity=identity).allowed is False

    time.sleep(1.2)

    assert limiter.check(identity=identity).allowed is True


def test_fails_open_when_redis_is_unreachable() -> None:
    """A bounded-timeout client pointed at an unreachable port must fail open
    quickly rather than raise or hang the caller.
    """
    unreachable_client: redis.Redis = redis.Redis.from_url(
        "redis://127.0.0.1:6399/0",
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    limiter = RedisFixedWindowRateLimiter(
        client=unreachable_client, max_requests=1, window_seconds=60
    )

    started = time.monotonic()
    decision = limiter.check(identity=_unique_identity())
    elapsed = time.monotonic() - started

    assert decision.allowed is True
    assert decision.retry_after_seconds == 0
    assert elapsed < 1.0


def test_observes_allowed_denied_and_failed_open_metric_outcomes(
    redis_client: redis.Redis,
) -> None:
    metrics = AtlasMetrics(CollectorRegistry())
    limiter = RedisFixedWindowRateLimiter(
        client=redis_client, max_requests=1, window_seconds=60, metrics=metrics
    )
    identity = _unique_identity()

    assert limiter.check(identity=identity).allowed is True
    assert limiter.check(identity=identity).allowed is False

    unreachable_client: redis.Redis = redis.Redis.from_url(
        "redis://127.0.0.1:6399/0",
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    unreachable_limiter = RedisFixedWindowRateLimiter(
        client=unreachable_client, max_requests=1, window_seconds=60, metrics=metrics
    )
    unreachable_limiter.check(identity=_unique_identity())

    assert _label_values(
        metrics, "atlas_redis_rate_limit_decisions_total", "outcome"
    ) == ["allowed", "denied", "failed_open"]
