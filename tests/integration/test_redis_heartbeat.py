"""Real-Redis integration tests for the worker heartbeat recorder + thread.

Requires a reachable Redis (Docker Compose `redis` service or CI's Redis
service). URL is read from ``ATLAS_REDIS_URL`` (default matches local
Compose: ``redis://127.0.0.1:6380/0``).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pytest
import redis

from atlas.coordination.heartbeat import RedisHeartbeatRecorder
from atlas.coordination.heartbeat_thread import HeartbeatThread
from tests.integration.redis_support import build_test_redis_client, cleanup_atlas_keys


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


def _unique_worker_id() -> str:
    return f"test-worker-{uuid.uuid4().hex}"


def test_beat_writes_a_ttl_bound_key(redis_client: redis.Redis) -> None:
    worker_id = _unique_worker_id()
    recorder = RedisHeartbeatRecorder(client=redis_client, ttl_seconds=15)

    recorder.beat(worker_id=worker_id)

    key = f"atlas:heartbeat:v1:worker:{worker_id}"
    assert redis_client.get(key) == b"1"
    ttl = redis_client.ttl(key)
    assert 0 < ttl <= 15


def test_key_expires_after_ttl(redis_client: redis.Redis) -> None:
    worker_id = _unique_worker_id()
    recorder = RedisHeartbeatRecorder(client=redis_client, ttl_seconds=1)

    recorder.beat(worker_id=worker_id)
    key = f"atlas:heartbeat:v1:worker:{worker_id}"
    assert redis_client.exists(key) == 1

    time.sleep(1.3)

    assert redis_client.exists(key) == 0


def test_heartbeat_thread_keeps_key_alive_past_a_single_ttl(
    redis_client: redis.Redis,
) -> None:
    """A short interval relative to the TTL must prevent expiry while running."""
    worker_id = _unique_worker_id()
    recorder = RedisHeartbeatRecorder(client=redis_client, ttl_seconds=1)
    thread = HeartbeatThread(
        recorder=recorder, worker_id=worker_id, interval_seconds=0.2
    )
    key = f"atlas:heartbeat:v1:worker:{worker_id}"

    try:
        thread.start()
        time.sleep(1.5)
        assert redis_client.exists(key) == 1
    finally:
        thread.stop()

    # Once stopped, refreshing stops and the key eventually expires.
    time.sleep(1.3)
    assert redis_client.exists(key) == 0


def test_beat_fails_open_when_redis_is_unreachable() -> None:
    unreachable_client: redis.Redis = redis.Redis.from_url(
        "redis://127.0.0.1:6399/0",
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    recorder = RedisHeartbeatRecorder(client=unreachable_client, ttl_seconds=15)

    started = time.monotonic()
    recorder.beat(worker_id=_unique_worker_id())  # must not raise
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
